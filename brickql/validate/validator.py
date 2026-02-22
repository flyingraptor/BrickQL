"""Plan validation orchestrator.

``PlanValidator`` is the public entry point.  It wires together the
focused sub-validators and drives validation in the correct order.

Sub-validator hierarchy
-----------------------
PlanValidator
  ├── DialectValidator    (dialect_validator.py)  — feature-flag checks
  ├── SchemaValidator     (schema_validator.py)   — table / column existence
  ├── SemanticValidator   (semantic_validator.py) — HAVING / LIMIT rules
  ├── OperandValidator    (operand_validator.py)  — typed operand checks
  └── PredicateValidator  (operand_validator.py)  — predicate dict checks

Item 11 — Inappropriate Intimacy fix: recursive sub-validation (for
subqueries, CTEs, set-ops) no longer hard-codes ``PlanValidator(…)``.
A ``sub_validator_factory`` is used instead; the default replicates the
old behaviour and callers may inject a custom factory.

Item 6 — Data Clumps fix: ``(snapshot, dialect)`` is packaged into the
:class:`~brickql.schema.context.ValidationContext` value object and
threaded to every sub-validator.
"""
from __future__ import annotations

from typing import Callable

from brickql.errors import DialectViolationError, ValidationError
from brickql.schema.context import ValidationContext
from brickql.schema.dialect import DialectProfile
from brickql.schema.query_plan import QueryPlan
from brickql.schema.snapshot import SchemaSnapshot
from brickql.validate.dialect_validator import DialectValidator
from brickql.validate.operand_validator import OperandValidator, PredicateValidator
from brickql.validate.schema_validator import SchemaValidator
from brickql.validate.semantic_validator import SemanticValidator


class PlanValidator:
    """Validates a QueryPlan against a SchemaSnapshot and DialectProfile.

    Responsibilities delegated to sub-validators (in order):
    1. Dialect checks   – feature flags, allowed operators / functions.
    2. Schema checks    – table and column existence, relationship keys.
    3. Expression checks – operand structure, function allowlists.
    4. Semantic checks  – HAVING/GROUP_BY pairing, LIMIT range.

    Raises the first violation as a subclass of ``ValidationError`` so the
    caller can convert it to a structured error response for LLM repair.

    Args:
        snapshot: The schema the LLM was given.
        dialect: The dialect profile controlling allowed features.
        sub_validator_factory: Optional factory for recursive sub-validation
            (subqueries, CTEs, set-ops).  Defaults to
            ``lambda: PlanValidator(snapshot, dialect)``.
    """

    def __init__(
        self,
        snapshot: SchemaSnapshot,
        dialect: DialectProfile,
        sub_validator_factory: Callable[[], "PlanValidator"] | None = None,
    ) -> None:
        self._ctx = ValidationContext(snapshot=snapshot, dialect=dialect)
        self._sub_validator_factory = sub_validator_factory or (
            lambda: PlanValidator(snapshot, dialect)
        )
        # CTE / derived-table virtual names grow during validation.
        self._cte_names: frozenset[str] = frozenset()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(
        self, plan: QueryPlan, cte_names: frozenset[str] | None = None
    ) -> None:
        """Validate ``plan`` and raise on the first violation found.

        Args:
            plan: The parsed QueryPlan to validate.
            cte_names: CTE names defined in an enclosing query.

        Raises:
            ValidationError: (or subclass) on the first violation.
        """
        self._cte_names = cte_names or frozenset()
        if plan.CTE:
            self._cte_names = self._cte_names | frozenset(c.name for c in plan.CTE)

        sub_validators = self._make_sub_validators()

        sub_validators["dialect"].validate_feature_flags(plan)
        sub_validators["dialect"].validate_join_depth(plan)
        sub_validators["dialect"].validate_window_functions(plan)

        self._validate_from(plan, sub_validators)
        sub_validators["schema"].validate_joins(plan)

        self._validate_select(plan, sub_validators)
        self._validate_where(plan, sub_validators)
        self._validate_group_by(plan, sub_validators)

        sub_validators["semantic"].validate_having(plan)
        if plan.HAVING is not None and plan.GROUP_BY is not None:
            sub_validators["pred"].validate(plan.HAVING)

        self._validate_order_by(plan, sub_validators)
        sub_validators["semantic"].validate_limit(plan)

        self._validate_cte(plan)
        self._validate_set_op(plan)

    # ------------------------------------------------------------------
    # Clause validation helpers
    # ------------------------------------------------------------------

    def _validate_from(self, plan: QueryPlan, sv: dict) -> None:
        if plan.FROM is None:
            return
        frm = plan.FROM
        if frm.table is not None:
            sv["schema"].assert_table_allowed(frm.table)
        elif frm.subquery is not None:
            if not self._ctx.dialect.allowed.allow_subqueries:
                raise DialectViolationError(
                    "Subquery in FROM is not enabled.", feature="allow_subqueries"
                )
            if frm.alias:
                self._cte_names = self._cte_names | {frm.alias}
                sv["schema"].cte_names = self._cte_names
                sv["op"].cte_names = self._cte_names
            self._sub_validator_factory().validate(
                frm.subquery, cte_names=self._cte_names
            )
        else:
            raise ValidationError(
                "FROM clause must specify either 'table' or 'subquery'.",
                code="SCHEMA_ERROR",
            )

    def _validate_select(self, plan: QueryPlan, sv: dict) -> None:
        if not plan.SELECT:
            return
        for item in plan.SELECT:
            sv["op"].validate(item.expr)
            if item.over is not None:
                for pb in item.over.partition_by:
                    sv["op"].validate(pb)
                for ob in item.over.order_by:
                    sv["op"].validate(ob.expr)

    def _validate_where(self, plan: QueryPlan, sv: dict) -> None:
        if plan.WHERE is not None:
            sv["pred"].validate(plan.WHERE)

    def _validate_group_by(self, plan: QueryPlan, sv: dict) -> None:
        if not plan.GROUP_BY:
            return
        for expr in plan.GROUP_BY:
            sv["op"].validate(expr)

    def _validate_order_by(self, plan: QueryPlan, sv: dict) -> None:
        if not plan.ORDER_BY:
            return
        for item in plan.ORDER_BY:
            sv["op"].validate(item.expr)

    def _validate_cte(self, plan: QueryPlan) -> None:
        if not plan.CTE:
            return
        for cte in plan.CTE:
            self._sub_validator_factory().validate(cte.query)

    def _validate_set_op(self, plan: QueryPlan) -> None:
        if plan.SET_OP is None:
            return
        self._sub_validator_factory().validate(plan.SET_OP.query)

    # ------------------------------------------------------------------
    # Sub-validator wiring
    # ------------------------------------------------------------------

    def _make_sub_validators(self) -> dict:
        """Construct and wire sub-validators for one validation run."""
        pred_validator = PredicateValidator.__new__(PredicateValidator)
        op_validator = OperandValidator(self._ctx, self._cte_names, pred_validator)
        pred_validator.__init__(self._ctx, op_validator)  # type: ignore[misc]

        schema_validator = SchemaValidator(self._ctx, self._cte_names)

        return {
            "dialect": DialectValidator(self._ctx),
            "schema": schema_validator,
            "op": op_validator,
            "pred": pred_validator,
            "semantic": SemanticValidator(self._ctx),
        }
