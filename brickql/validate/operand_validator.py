"""Operand and predicate validators.

``OperandValidator`` and ``PredicateValidator`` are tightly coupled —
CASE operands contain predicate conditions, and predicates contain
operands — so they share a module.

Both receive a :class:`~brickql.schema.context.ValidationContext` and a
:attr:`cte_names` frozenset that grows as the outer query discovers CTE
and derived-table virtual names.
"""
from __future__ import annotations

from typing import Any

from brickql.errors import DialectViolationError, ValidationError
from brickql.schema.column_reference import ColumnReference
from brickql.schema.context import ValidationContext
from brickql.schema.expressions import (
    ALL_PREDICATE_OPS,
    AGGREGATE_FUNCTIONS,
    COMPARISON_OPS,
    EXISTS_OPS,
    LOGICAL_AND_OR,
    LOGICAL_NOT,
    MEMBERSHIP_OPS,
    NULL_OPS,
    OPERAND_KEYS,
    PATTERN_OPS,
    RANGE_OPS,
    operand_kind,
)
from brickql.schema.operands import (
    CaseOperand,
    ColumnOperand,
    FuncOperand,
    Operand,
    to_operand,
)


class OperandValidator:
    """Validates typed and raw operands against context and schema.

    Args:
        ctx: Validation context (snapshot + dialect).
        cte_names: Virtual table names that bypass schema checks.
    """

    def __init__(
        self,
        ctx: ValidationContext,
        cte_names: frozenset[str],
        predicate_validator: "PredicateValidator",
    ) -> None:
        self._ctx = ctx
        self._cte_names = cte_names
        self._pred = predicate_validator

    @property
    def cte_names(self) -> frozenset[str]:
        return self._cte_names

    @cte_names.setter
    def cte_names(self, value: frozenset[str]) -> None:
        self._cte_names = value

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, expr: Any) -> None:
        """Validate a raw operand dict or a typed Operand.

        Accepts both ``dict[str, Any]`` (from predicate argument lists that
        are not yet parsed) and typed :class:`~brickql.schema.operands.Operand`
        instances (from ``SELECT``, ``GROUP_BY``, etc.).

        Raises:
            ValidationError: On structural or schema violation.
            DialectViolationError: On function not in the allowlist.
        """
        if isinstance(expr, (ColumnOperand, FuncOperand, CaseOperand)):
            self._validate_typed(expr)
            return

        if not isinstance(expr, dict):
            raise ValidationError(
                f"Operand must be a dict, got {type(expr).__name__}.",
                code="SCHEMA_ERROR",
            )

        kind = operand_kind(expr)
        if kind is None:
            raise ValidationError(
                f"Unknown operand type: {list(expr.keys())}. "
                f"Expected one of {sorted(OPERAND_KEYS)}.",
                code="SCHEMA_ERROR",
            )

        typed = to_operand(expr)
        self._validate_typed(typed)

    def _validate_typed(self, operand: Operand) -> None:
        if isinstance(operand, ColumnOperand):
            self._validate_col_ref(operand.col)
        elif isinstance(operand, FuncOperand):
            self._validate_func(operand)
        elif isinstance(operand, CaseOperand):
            self._validate_case(operand)

    # ------------------------------------------------------------------
    # Column reference validation
    # ------------------------------------------------------------------

    def _validate_col_ref(self, col: str) -> None:
        """Validate a ``table.column`` or bare ``column`` reference."""
        ref = ColumnReference.parse(col)
        if ref.table is None:
            return  # Bare column — validated contextually by caller.

        self._assert_table_allowed(ref.table)
        if ref.table in self._cte_names:
            return  # CTE columns bypass schema.

        ref.validate_against(self._ctx.snapshot, self._cte_names)

    def _assert_table_allowed(self, table_name: str) -> None:
        """Raise SchemaError if the table is unknown and not a CTE name."""
        from brickql.errors import SchemaError

        if table_name in self._cte_names:
            return
        if self._ctx.snapshot.get_table(table_name) is None:
            raise SchemaError(
                f"Table '{table_name}' does not exist in the schema snapshot.",
                details={
                    "table": table_name,
                    "allowed_tables": self._ctx.snapshot.table_names,
                },
            )

    # ------------------------------------------------------------------
    # Function validation
    # ------------------------------------------------------------------

    def _validate_func(self, expr: FuncOperand) -> None:
        func_name = expr.func
        allowed_funcs = self._ctx.dialect.allowed.functions
        is_aggregate = func_name in AGGREGATE_FUNCTIONS
        if is_aggregate and func_name not in allowed_funcs:
            raise DialectViolationError(
                f"Aggregate function '{func_name}' is not allowed. "
                f"Allowed functions: {allowed_funcs}.",
                feature="functions",
            )
        if not is_aggregate and allowed_funcs and func_name not in allowed_funcs:
            raise DialectViolationError(
                f"Function '{func_name}' is not in the allowed functions list: "
                f"{allowed_funcs}.",
                feature="functions",
            )
        for arg in expr.args:
            self.validate(arg)

    # ------------------------------------------------------------------
    # CASE validation
    # ------------------------------------------------------------------

    def _validate_case(self, operand: CaseOperand) -> None:
        for when in operand.case.when:
            self._pred.validate(when.condition)
            self.validate(when.then)
        if operand.case.else_val is not None:
            self.validate(operand.case.else_val)


class PredicateValidator:
    """Validates predicate dicts (WHERE / HAVING) recursively.

    Args:
        ctx: Validation context (snapshot + dialect).
        operand_validator: Shared OperandValidator for operand-level checks.
    """

    def __init__(
        self,
        ctx: ValidationContext,
        operand_validator: OperandValidator,
    ) -> None:
        self._ctx = ctx
        self._op = operand_validator

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, pred: Any) -> None:
        """Recursively validate a predicate dict.

        Raises:
            ValidationError: On structural or schema violation.
            DialectViolationError: On disallowed operator.
        """
        if not isinstance(pred, dict) or len(pred) != 1:
            raise ValidationError(
                f"Predicate must be a single-key dict, got: {pred!r}",
                code="SCHEMA_ERROR",
            )
        op = next(iter(pred))
        if op not in ALL_PREDICATE_OPS:
            raise ValidationError(
                f"Unknown predicate operator '{op}'. "
                f"Allowed: {sorted(ALL_PREDICATE_OPS)}.",
                code="SCHEMA_ERROR",
            )
        self._assert_operator_allowed(op)
        args = pred[op]

        if op in COMPARISON_OPS:
            self._expect_operand_list(args, op, count=2)
        elif op in PATTERN_OPS:
            self._expect_operand_list(args, op, count=2)
        elif op in RANGE_OPS:
            self._expect_operand_list(args, op, count=3)
        elif op in MEMBERSHIP_OPS:
            if not isinstance(args, list) or len(args) < 2:
                raise ValidationError(
                    f"IN requires at least 2 elements, got {args!r}.",
                    code="SCHEMA_ERROR",
                )
            self._op.validate(args[0])
            for item in args[1:]:
                if isinstance(item, dict) and "SELECT" in item:
                    if not self._ctx.dialect.allowed.allow_subqueries:
                        raise DialectViolationError(
                            "Subquery in IN predicate is not enabled.",
                            feature="allow_subqueries",
                        )
                else:
                    self._op.validate(item)
        elif op in NULL_OPS:
            self._op.validate(args)
        elif op in EXISTS_OPS:
            if not self._ctx.dialect.allowed.allow_subqueries:
                raise DialectViolationError(
                    "EXISTS requires allow_subqueries=true.",
                    feature="allow_subqueries",
                )
        elif op in LOGICAL_AND_OR:
            if not isinstance(args, list) or len(args) < 2:
                raise ValidationError(
                    f"{op} requires at least 2 sub-predicates.",
                    code="SCHEMA_ERROR",
                )
            for sub in args:
                self.validate(sub)
        elif op in LOGICAL_NOT:
            self.validate(args)

    def _expect_operand_list(
        self, args: Any, op: str, count: int
    ) -> None:
        if not isinstance(args, list) or len(args) != count:
            raise ValidationError(
                f"{op} requires exactly {count} operands, got {args!r}.",
                code="SCHEMA_ERROR",
            )
        for operand in args:
            self._op.validate(operand)

    def _assert_operator_allowed(self, op: str) -> None:
        allowed_ops = self._ctx.dialect.allowed.operators
        if allowed_ops and op not in allowed_ops:
            raise DialectViolationError(
                f"Operator '{op}' is not in the allowed operators list: {allowed_ops}.",
                feature="operators",
            )
