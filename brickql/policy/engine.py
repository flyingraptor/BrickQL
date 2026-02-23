"""Policy and authorization layer.

``PolicyEngine`` runs after structural validation.  It enforces:

* **Parameter-bound columns** – columns designated by :class:`TablePolicy` must
  appear with ``{"param": "PARAM_NAME"}`` rather than a literal value.  If a
  predicate is missing, the engine can optionally inject it automatically or
  raise :class:`~brickql.errors.MissingParamError`.
* **Table / column access control** – per-table positive column allowlists
  (``allowed_columns``) and/or negative blocklists (``denied_columns``) are
  enforced before compilation.  A globally denied column list is also supported
  via :attr:`PolicyConfig.denied_columns`.
* **LIMIT enforcement** – clamps or rejects LIMIT values that exceed the max.

Runtime policy is configured entirely in :class:`PolicyConfig` and
:class:`TablePolicy`.  The :class:`~brickql.schema.snapshot.SchemaSnapshot`
remains a pure structural description of the database; it carries no policy.

Example — per-role column allowlist (RBAC pattern)::

    analyst_policy = PolicyConfig(
        inject_missing_params=True,
        default_limit=100,
        tables={
            "employees": TablePolicy(
                param_bound_columns={"tenant_id": "TENANT"},
                allowed_columns=["employee_id", "first_name", "last_name",
                                 "department_id", "hire_date", "active"],
            ),
        },
    )

    compiled = brickql.validate_and_compile(plan_json, snapshot, dialect, analyst_policy)
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
        allowed_columns: Positive allowlist of column names that may appear in
            any plan referencing this table.  When non-empty, **only** the
            listed columns are permitted — any other column is blocked with
            :class:`~brickql.errors.DisallowedColumnError`.  An empty list
            (the default) means all columns are allowed (subject to
            ``denied_columns``).  Useful for RBAC patterns where a role should
            only see a specific subset of columns.
        denied_columns: Column names that are forbidden in any plan referencing
            this table (SELECT, WHERE, ORDER BY, …).  Applied on top of
            ``allowed_columns`` when both are set.
    """

    param_bound_columns: dict[str, str] = field(default_factory=dict)
    allowed_columns: list[str] = field(default_factory=list)
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

    def denied_columns_for(self, table_name: str) -> list[str]:
        """Return the combined denied column list for a specific table.

        Merges the global :attr:`denied_columns` with any per-table denied
        columns configured in :attr:`tables`.  This delegation method avoids
        Law-of-Demeter violations in callers that would otherwise chain through
        ``config.tables.get(table).denied_columns``.

        Args:
            table_name: The table to look up.

        Returns:
            De-duplicated list of denied column names.
        """
        tpol = self.tables.get(table_name)
        table_denied = tpol.denied_columns if tpol is not None else []
        return list(set(self.denied_columns) | set(table_denied))


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
        for table in self._collect_all_table_refs(plan):
            if table not in self._config.allowed_tables:
                raise DisallowedTableError(table, self._config.allowed_tables)

    def _collect_all_table_refs(self, plan: QueryPlan) -> list[str]:
        """Collect direct + JOIN-resolved table names.

        Combines the domain-level ``collect_table_references()`` (FROM only)
        with JOIN relationship resolution that requires the snapshot.
        """
        tables: list[str] = list(plan.collect_table_references())
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
        for col_ref in plan.collect_col_refs():
            self._assert_col_not_denied(col_ref)

    def _assert_col_not_denied(self, col_ref: str) -> None:
        col_name = col_ref.rsplit(".", maxsplit=1)[-1]
        table_name = col_ref.split(".", maxsplit=1)[0] if "." in col_ref else None

        globally_denied = (
            col_ref in self._config.denied_columns or col_name in self._config.denied_columns
        )

        per_table_denied = False
        not_in_allowlist = False
        if table_name:
            tpol = self._config.tables.get(table_name)
            if tpol is not None:
                per_table_denied = col_name in tpol.denied_columns
                if tpol.allowed_columns:
                    not_in_allowlist = col_name not in tpol.allowed_columns

        if globally_denied or per_table_denied or not_in_allowlist:
            if table_name:
                raise DisallowedColumnError(
                    table_name, col_name, self._effective_allowed_columns(table_name)
                )
            raise DisallowedColumnError("", col_name, [])

    def _effective_allowed_columns(self, table_name: str) -> list[str]:
        """Return the columns a plan may reference for *table_name*.

        When the table's :class:`TablePolicy` carries a non-empty
        ``allowed_columns`` list, that list is the starting set.  Otherwise
        every column known to the snapshot is the starting set.  The global
        and per-table ``denied_columns`` are subtracted from the result in
        both cases.
        """
        tpol = self._config.tables.get(table_name)
        all_denied = set(self._config.denied_columns_for(table_name))
        if tpol is not None and tpol.allowed_columns:
            return [c for c in tpol.allowed_columns if c not in all_denied]
        return [c for c in self._snapshot.get_column_names(table_name) if c not in all_denied]

    # ------------------------------------------------------------------
    # Parameter-bound column enforcement
    # ------------------------------------------------------------------

    def _enforce_param_bound_columns(self, plan: QueryPlan) -> QueryPlan:
        """Inject or verify param-bound predicates defined in TablePolicy."""
        for table_name in set(self._collect_all_table_refs(plan)):
            tpol = self._config.tables.get(table_name)
            if not tpol or not tpol.param_bound_columns:
                continue
            for col_name, param_name in tpol.param_bound_columns.items():
                plan = self._enforce_single_param(plan, table_name, col_name, param_name)
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
        required_pred: dict[str, Any] = {"EQ": [{"col": col_ref}, {"param": param_name}]}

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

    def _where_satisfies_param(self, pred: dict, col_ref: str, param_name: str) -> bool:
        """Return True if pred already enforces ``col = :param``."""
        if not isinstance(pred, dict) or len(pred) != 1:
            return False
        op = next(iter(pred))
        args = pred[op]
        if op == "EQ" and isinstance(args, list) and len(args) == 2:
            lhs, rhs = args
            if (
                isinstance(lhs, dict)
                and lhs.get("col") == col_ref
                and isinstance(rhs, dict)
                and rhs.get("param") == param_name
            ):
                return True
        # Only recurse into AND: a binding inside OR does not guarantee the
        # column is filtered — the OR branch makes the restriction optional,
        # which would allow cross-tenant access.
        if op == "AND" and isinstance(args, list):
            return any(self._where_satisfies_param(sub, col_ref, param_name) for sub in args)
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
            return plan.model_copy(update={"LIMIT": LimitClause(value=max_limit)}, deep=True)
        return plan
