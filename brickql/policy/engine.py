"""Policy and authorization layer.

``PolicyEngine`` runs after structural validation.  It enforces:

* **Parameter-bound columns** – columns designated by :class:`TablePolicy` must
  appear with ``{"param": "PARAM_NAME"}`` rather than a literal value.  If a
  predicate is missing, the engine can optionally inject it automatically or
  raise :class:`~brickql.errors.MissingParamError`.
* **Table / column allowlists** – globally or per-table denied columns are
  blocked before compilation.
* **LIMIT enforcement** – clamps or rejects LIMIT values that exceed the max.

Runtime policy is configured entirely in :class:`PolicyConfig` and
:class:`TablePolicy`.  The :class:`~brickql.schema.snapshot.SchemaSnapshot`
remains a pure structural description of the database; it carries no policy.

Example — multi-tenant setup with per-table param names::

    policy = PolicyConfig(
        inject_missing_params=True,
        default_limit=100,
        tables={
            "companies":   TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
            "departments": TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
            "employees":   TablePolicy(
                param_bound_columns={"tenant_id": "TENANT"},
                denied_columns=["salary"],
            ),
            "projects":    TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
        },
    )

    compiled = brickql.validate_and_compile(plan_json, snapshot, dialect, policy)
    params = compiled.merge_runtime_params({"TENANT": tenant_id})
    cursor.execute(compiled.sql, params)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from brickql.errors import (
    DisallowedColumnError,
    DisallowedTableError,
    MissingParamError,
)
from brickql.schema.dialect import DialectProfile
from brickql.schema.expressions import OPERAND_KEYS
from brickql.schema.query_plan import LimitClause, QueryPlan
from brickql.schema.snapshot import SchemaSnapshot


@dataclass
class TablePolicy:
    """Per-table runtime policy rules.

    Attributes:
        param_bound_columns: Maps column names to the runtime parameter they
            must use.  For example ``{"tenant_id": "TENANT"}`` requires every
            predicate on ``tenant_id`` to use ``{"param": "TENANT"}`` rather
            than a literal value.  Different tables can use different param
            names — or the same name if the runtime value is shared.
        denied_columns: Column names that are forbidden in any plan referencing
            this table (SELECT, WHERE, ORDER BY, …).
    """

    param_bound_columns: dict[str, str] = field(default_factory=dict)
    denied_columns: list[str] = field(default_factory=list)


@dataclass
class PolicyConfig:
    """Runtime policy configuration applied to every request.

    Attributes:
        tables: Per-table policies.  Each entry in the dict configures
            param-bound columns and per-table denied columns for that table.
        allowed_tables: If non-empty, only these table names may appear in a
            plan.  Empty means all snapshot tables are allowed.
        denied_columns: Column names denied globally (across all tables).
        inject_missing_params: If ``True``, automatically inject param-bound
            predicates that the LLM omitted.  If ``False``, raise
            :class:`~brickql.errors.MissingParamError` instead.
        default_limit: If the plan has no LIMIT clause, inject this value
            (``0`` = no injection).
    """

    tables: dict[str, TablePolicy] = field(default_factory=dict)
    allowed_tables: list[str] = field(default_factory=list)
    denied_columns: list[str] = field(default_factory=list)
    inject_missing_params: bool = True
    default_limit: int = 100


class PolicyEngine:
    """Applies policy rules to a validated QueryPlan.

    Uses the Strategy pattern (GoF): :class:`PolicyConfig` is the swappable
    strategy; :class:`PolicyEngine` is the context that executes it.

    Args:
        config: Policy rules for this request.
        snapshot: Schema snapshot (structural metadata only — no policy).
        dialect: Dialect profile (for max_limit enforcement).
    """

    def __init__(
        self,
        config: PolicyConfig,
        snapshot: SchemaSnapshot,
        dialect: DialectProfile,
    ) -> None:
        self._config = config
        self._snapshot = snapshot
        self._dialect = dialect

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply(self, plan: QueryPlan) -> QueryPlan:
        """Apply all policy rules and return a (possibly modified) QueryPlan.

        Steps executed in order:

        1. Check table allowlist.
        2. Check denied columns (global + per-table).
        3. Inject / verify parameter-bound column predicates.
        4. Enforce LIMIT.

        Args:
            plan: A structurally-validated QueryPlan.

        Returns:
            A new QueryPlan (deep-copied) with policy injections applied.

        Raises:
            DisallowedTableError: If a referenced table is not in the allowlist.
            DisallowedColumnError: If a denied column appears in the plan.
            MissingParamError: If a param-bound column uses a literal and
                ``inject_missing_params`` is ``False``.
        """
        plan = plan.model_copy(deep=True)
        self._check_table_allowlist(plan)
        self._check_denied_columns(plan)
        plan = self._enforce_param_bound_columns(plan)
        plan = self._enforce_limit(plan)
        return plan

    # ------------------------------------------------------------------
    # Table allowlist
    # ------------------------------------------------------------------

    def _check_table_allowlist(self, plan: QueryPlan) -> None:
        if not self._config.allowed_tables:
            return
        for table in self._collect_table_refs(plan):
            if table not in self._config.allowed_tables:
                raise DisallowedTableError(table, self._config.allowed_tables)

    def _collect_table_refs(self, plan: QueryPlan) -> list[str]:
        """Collect all table names referenced in the plan."""
        tables: list[str] = []
        if plan.FROM and plan.FROM.table:
            tables.append(plan.FROM.table)
        if plan.JOIN:
            for join in plan.JOIN:
                rel = self._snapshot.get_relationship(join.rel)
                if rel:
                    tables.extend([rel.from_table, rel.to_table])
        return tables

    # ------------------------------------------------------------------
    # Denied columns (global + per-table)
    # ------------------------------------------------------------------

    def _check_denied_columns(self, plan: QueryPlan) -> None:
        col_refs = self._collect_col_refs_from_plan(plan)
        for col_ref in col_refs:
            self._assert_col_not_denied(col_ref)

    def _assert_col_not_denied(self, col_ref: str) -> None:
        col_name = col_ref.split(".")[-1]
        table_name = col_ref.split(".")[0] if "." in col_ref else None

        # Global deny list.
        globally_denied = col_ref in self._config.denied_columns or (
            col_name in self._config.denied_columns
        )

        # Per-table deny list.
        per_table_denied = False
        if table_name:
            tpol = self._config.tables.get(table_name)
            per_table_denied = tpol is not None and col_name in tpol.denied_columns

        if globally_denied or per_table_denied:
            if table_name:
                table = self._snapshot.get_table(table_name)
                global_denied_set = set(self._config.denied_columns)
                table_denied_set = set(
                    (self._config.tables.get(table_name) or TablePolicy()).denied_columns
                )
                all_denied = global_denied_set | table_denied_set
                allowed = [
                    c.name
                    for c in (table.columns if table else [])
                    if c.name not in all_denied
                ]
                raise DisallowedColumnError(table_name, col_name, allowed)
            raise DisallowedColumnError("", col_name, [])

    def _collect_col_refs_from_plan(self, plan: QueryPlan) -> list[str]:
        refs: list[str] = []
        self._walk_for_col_refs(plan.model_dump(exclude_none=True), refs)
        return refs

    def _walk_for_col_refs(self, node: Any, refs: list[str]) -> None:
        if isinstance(node, dict):
            if "col" in node and set(node.keys()) <= OPERAND_KEYS:
                refs.append(node["col"])
            else:
                for v in node.values():
                    self._walk_for_col_refs(v, refs)
        elif isinstance(node, list):
            for item in node:
                self._walk_for_col_refs(item, refs)

    # ------------------------------------------------------------------
    # Parameter-bound column enforcement
    # ------------------------------------------------------------------

    def _enforce_param_bound_columns(self, plan: QueryPlan) -> QueryPlan:
        """Inject or verify param-bound predicates defined in TablePolicy."""
        for table_name in set(self._collect_table_refs(plan)):
            tpol = self._config.tables.get(table_name)
            if not tpol or not tpol.param_bound_columns:
                continue
            for col_name, param_name in tpol.param_bound_columns.items():
                plan = self._enforce_single_param(
                    plan, table_name, col_name, param_name
                )
        return plan

    def _enforce_single_param(
        self,
        plan: QueryPlan,
        table_name: str,
        col_name: str,
        param_name: str,
    ) -> QueryPlan:
        """Ensure WHERE contains ``table.col = :param`` for a bound column."""
        col_ref = f"{table_name}.{col_name}"
        required_pred: dict[str, Any] = {
            "EQ": [{"col": col_ref}, {"param": param_name}]
        }

        if plan.WHERE is None:
            if self._config.inject_missing_params:
                return plan.model_copy(update={"WHERE": required_pred}, deep=True)
            raise MissingParamError(col_ref, param_name)

        if self._where_satisfies_param(plan.WHERE, col_ref, param_name):
            return plan

        if self._config.inject_missing_params:
            new_where: dict[str, Any] = {"AND": [plan.WHERE, required_pred]}
            return plan.model_copy(update={"WHERE": new_where}, deep=True)

        raise MissingParamError(col_ref, param_name)

    def _where_satisfies_param(
        self, pred: dict, col_ref: str, param_name: str
    ) -> bool:
        """Return True if pred already enforces ``col = :param``."""
        if not isinstance(pred, dict) or len(pred) != 1:
            return False
        op = next(iter(pred))
        args = pred[op]
        if op == "EQ" and isinstance(args, list) and len(args) == 2:
            lhs, rhs = args
            if (
                isinstance(lhs, dict) and lhs.get("col") == col_ref
                and isinstance(rhs, dict) and rhs.get("param") == param_name
            ):
                return True
        if op in ("AND", "OR") and isinstance(args, list):
            return any(
                self._where_satisfies_param(sub, col_ref, param_name)
                for sub in args
            )
        return False

    # ------------------------------------------------------------------
    # LIMIT enforcement
    # ------------------------------------------------------------------

    def _enforce_limit(self, plan: QueryPlan) -> QueryPlan:
        max_limit = self._dialect.allowed.max_limit
        if plan.LIMIT is None:
            if self._config.default_limit > 0:
                return plan.model_copy(
                    update={"LIMIT": LimitClause(value=self._config.default_limit)},
                    deep=True,
                )
            return plan
        if plan.LIMIT.value > max_limit:
            return plan.model_copy(
                update={"LIMIT": LimitClause(value=max_limit)}, deep=True
            )
        return plan
