"""Plan validation: structural, semantic, and dialect checks.

``PlanValidator`` is a service class (Clean Architecture: use-case layer) that
takes a parsed ``QueryPlan`` and validates it against a ``SchemaSnapshot`` and
a ``DialectProfile``.  On the first violation it raises the appropriate custom
exception so the caller can return it to the LLM for error repair.
"""
from __future__ import annotations

from typing import Any

from brinkql.errors import (
    DialectViolationError,
    InvalidJoinRelError,
    SchemaError,
    ValidationError,
)
from brinkql.schema.dialect import DialectProfile
from brinkql.schema.expressions import (
    ALL_PREDICATE_OPS,
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
from brinkql.schema.query_plan import QueryPlan
from brinkql.schema.snapshot import SchemaSnapshot


class PlanValidator:
    """Validates a QueryPlan against a SchemaSnapshot and DialectProfile.

    Responsibilities (in order):
    1. Structural check – every referenced table / column exists in snapshot.
    2. Dialect check – only allowed tables, operators, functions; feature flags.
    3. Semantic check – join depth, LIMIT range, consistent FROM.

    Raises the first violation as a subclass of ``ValidationError`` so the
    caller can convert it to a structured error response for LLM repair.

    Args:
        snapshot: The schema the LLM was given.
        dialect: The dialect profile controlling allowed features.
    """

    def __init__(self, snapshot: SchemaSnapshot, dialect: DialectProfile) -> None:
        self._snapshot = snapshot
        self._dialect = dialect
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
            cte_names: CTE names defined in an enclosing query; references to
                these names are allowed even though they don't appear in the
                snapshot.

        Raises:
            ValidationError: (or subclass) on the first policy / schema / dialect
                violation found.
        """
        self._cte_names: frozenset[str] = cte_names or frozenset()
        # Collect CTE names defined in this plan itself so the main query
        # (and nested CTEs) can reference them.
        if plan.CTE:
            self._cte_names = self._cte_names | frozenset(c.name for c in plan.CTE)
        self._check_feature_flags(plan)
        self._check_from(plan)
        self._check_joins(plan)
        self._check_select(plan)
        self._check_where(plan)
        self._check_group_by(plan)
        self._check_having(plan)
        self._check_order_by(plan)
        self._check_limit(plan)
        self._check_cte(plan)
        self._check_set_op(plan)

    # ------------------------------------------------------------------
    # Feature-flag checks
    # ------------------------------------------------------------------

    def _check_feature_flags(self, plan: QueryPlan) -> None:
        allowed = self._dialect.allowed
        if plan.JOIN and allowed.max_join_depth == 0:
            raise DialectViolationError(
                "JOINs are not allowed (max_join_depth=0).",
                feature="join",
            )
        if plan.CTE and not allowed.allow_cte:
            raise DialectViolationError(
                "CTE (WITH) is not enabled in the dialect profile.",
                feature="allow_cte",
            )
        if plan.SET_OP and not allowed.allow_set_operations:
            raise DialectViolationError(
                "Set operations (UNION/INTERSECT/EXCEPT) are not enabled.",
                feature="allow_set_operations",
            )
        if plan.FROM and plan.FROM.subquery and not allowed.allow_subqueries:
            raise DialectViolationError(
                "Derived tables (subqueries in FROM) are not enabled.",
                feature="allow_subqueries",
            )

    # ------------------------------------------------------------------
    # FROM
    # ------------------------------------------------------------------

    def _check_from(self, plan: QueryPlan) -> None:
        if plan.FROM is None:
            return
        frm = plan.FROM
        if frm.table is not None:
            self._assert_table_allowed(frm.table)
        elif frm.subquery is not None:
            if not self._dialect.allowed.allow_subqueries:
                raise DialectViolationError(
                    "Subquery in FROM is not enabled.",
                    feature="allow_subqueries",
                )
            # The alias of a derived table (FROM (subquery) AS alias) is a virtual
            # table name.  Register it so that outer-query column refs like
            # "alias.col" are not rejected as unknown tables.
            if frm.alias:
                self._cte_names = self._cte_names | {frm.alias}
            sub_validator = PlanValidator(self._snapshot, self._dialect)
            sub_validator.validate(frm.subquery, cte_names=self._cte_names)
        else:
            raise ValidationError(
                "FROM clause must specify either 'table' or 'subquery'.",
                code="SCHEMA_ERROR",
            )

    # ------------------------------------------------------------------
    # JOIN
    # ------------------------------------------------------------------

    def _check_joins(self, plan: QueryPlan) -> None:
        if not plan.JOIN:
            return
        allowed = self._dialect.allowed
        if len(plan.JOIN) > allowed.max_join_depth:
            raise DialectViolationError(
                f"Query uses {len(plan.JOIN)} JOIN(s) but max_join_depth="
                f"{allowed.max_join_depth}.",
                feature="max_join_depth",
            )
        for join in plan.JOIN:
            rel = self._snapshot.get_relationship(join.rel)
            if rel is None:
                raise InvalidJoinRelError(
                    join.rel,
                    self._snapshot.relationship_keys,
                )
            self._assert_table_allowed(rel.from_table)
            self._assert_table_allowed(rel.to_table)

    # ------------------------------------------------------------------
    # SELECT
    # ------------------------------------------------------------------

    def _check_select(self, plan: QueryPlan) -> None:
        if not plan.SELECT:
            return
        for item in plan.SELECT:
            self._validate_operand(item.expr)
            if item.over is not None:
                if not self._dialect.allowed.allow_window_functions:
                    raise DialectViolationError(
                        "Window functions (OVER) are not enabled.",
                        feature="allow_window_functions",
                    )
                for pb in item.over.partition_by:
                    self._validate_operand(pb)
                for ob in item.over.order_by:
                    self._validate_operand(ob.expr)

    # ------------------------------------------------------------------
    # WHERE / HAVING
    # ------------------------------------------------------------------

    def _check_where(self, plan: QueryPlan) -> None:
        if plan.WHERE is not None:
            self._validate_predicate(plan.WHERE)

    def _check_having(self, plan: QueryPlan) -> None:
        if plan.GROUP_BY is not None and plan.HAVING is not None:
            self._validate_predicate(plan.HAVING)
        elif plan.HAVING is not None and plan.GROUP_BY is None:
            raise ValidationError(
                "HAVING requires GROUP_BY.",
                code="SCHEMA_ERROR",
            )

    # ------------------------------------------------------------------
    # GROUP BY
    # ------------------------------------------------------------------

    def _check_group_by(self, plan: QueryPlan) -> None:
        if not plan.GROUP_BY:
            return
        for expr in plan.GROUP_BY:
            self._validate_operand(expr)

    # ------------------------------------------------------------------
    # ORDER BY
    # ------------------------------------------------------------------

    def _check_order_by(self, plan: QueryPlan) -> None:
        if not plan.ORDER_BY:
            return
        for item in plan.ORDER_BY:
            self._validate_operand(item.expr)

    # ------------------------------------------------------------------
    # LIMIT
    # ------------------------------------------------------------------

    def _check_limit(self, plan: QueryPlan) -> None:
        if plan.LIMIT is None:
            return
        max_limit = self._dialect.allowed.max_limit
        if plan.LIMIT.value <= 0:
            raise ValidationError(
                "LIMIT value must be a positive integer.",
                code="SCHEMA_ERROR",
            )
        if plan.LIMIT.value > max_limit:
            raise DialectViolationError(
                f"LIMIT {plan.LIMIT.value} exceeds max_limit={max_limit}.",
                feature="max_limit",
            )

    # ------------------------------------------------------------------
    # CTE
    # ------------------------------------------------------------------

    def _check_cte(self, plan: QueryPlan) -> None:
        if not plan.CTE:
            return
        for cte in plan.CTE:
            sub_validator = PlanValidator(self._snapshot, self._dialect)
            sub_validator.validate(cte.query)

    # ------------------------------------------------------------------
    # SET_OP
    # ------------------------------------------------------------------

    def _check_set_op(self, plan: QueryPlan) -> None:
        if plan.SET_OP is None:
            return
        sub_validator = PlanValidator(self._snapshot, self._dialect)
        sub_validator.validate(plan.SET_OP.query)

    # ------------------------------------------------------------------
    # Operand validation
    # ------------------------------------------------------------------

    def _validate_operand(self, expr: Any) -> None:
        """Recursively validate an operand dict."""
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
        if kind == "col":
            self._validate_col_ref(expr["col"])
        elif kind == "func":
            self._validate_func(expr)
        elif kind == "case":
            self._validate_case(expr["case"])

    def _validate_col_ref(self, col: str) -> None:
        """Validate a ``table.column`` or bare ``column`` reference."""
        if "." in col:
            table_name, col_name = col.split(".", 1)
            self._assert_table_allowed(table_name)
            if table_name in self._cte_names:
                return  # CTE columns are not in the snapshot; skip check
            col_info = self._snapshot.get_column(table_name, col_name)
            if col_info is None:
                table = self._snapshot.get_table(table_name)
                allowed = table.column_names if table else []
                raise SchemaError(
                    f"Column '{col_name}' does not exist on table '{table_name}'.",
                    details={
                        "table": table_name,
                        "column": col_name,
                        "allowed_columns": allowed,
                    },
                )

    def _validate_func(self, expr: dict) -> None:
        """Validate a function call operand."""
        from brinkql.schema.expressions import AGGREGATE_FUNCTIONS  # noqa: PLC0415

        func_name = expr.get("func", "")
        allowed_funcs = self._dialect.allowed.functions
        is_aggregate = func_name in AGGREGATE_FUNCTIONS
        # Aggregate functions are blocked when not explicitly listed as allowed.
        if is_aggregate and func_name not in allowed_funcs:
            raise DialectViolationError(
                f"Aggregate function '{func_name}' is not allowed. "
                f"Allowed functions: {allowed_funcs}.",
                feature="functions",
            )
        # Non-aggregate scalar functions: only checked when the allowlist is non-empty.
        if not is_aggregate and allowed_funcs and func_name not in allowed_funcs:
            raise DialectViolationError(
                f"Function '{func_name}' is not in the allowed functions list: "
                f"{allowed_funcs}.",
                feature="functions",
            )
        for arg in expr.get("args", []):
            self._validate_operand(arg)

    def _validate_case(self, case_body: dict) -> None:
        """Validate a CASE expression body."""
        for when in case_body.get("when", []):
            self._validate_predicate(when.get("if") or when.get("condition", {}))
            self._validate_operand(when.get("then", {}))
        if "else" in case_body and case_body["else"] is not None:
            self._validate_operand(case_body["else"])

    # ------------------------------------------------------------------
    # Predicate validation
    # ------------------------------------------------------------------

    def _validate_predicate(self, pred: Any) -> None:
        """Recursively validate a predicate dict."""
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
            self._validate_operand(args[0])
            for item in args[1:]:
                if isinstance(item, dict) and "SELECT" in item:
                    if not self._dialect.allowed.allow_subqueries:
                        raise DialectViolationError(
                            "Subquery in IN predicate is not enabled.",
                            feature="allow_subqueries",
                        )
                else:
                    self._validate_operand(item)
        elif op in NULL_OPS:
            self._validate_operand(args)
        elif op in EXISTS_OPS:
            if not self._dialect.allowed.allow_subqueries:
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
                self._validate_predicate(sub)
        elif op in LOGICAL_NOT:
            self._validate_predicate(args)

    def _expect_operand_list(
        self, args: Any, op: str, count: int
    ) -> None:
        """Assert args is a list of exactly ``count`` operands."""
        if not isinstance(args, list) or len(args) != count:
            raise ValidationError(
                f"{op} requires exactly {count} operands, got {args!r}.",
                code="SCHEMA_ERROR",
            )
        for operand in args:
            self._validate_operand(operand)

    # ------------------------------------------------------------------
    # Allowlist helpers
    # ------------------------------------------------------------------

    def _assert_table_allowed(self, table_name: str) -> None:
        """Raise if the table is not in the snapshot (and not a CTE name)."""
        if table_name in self._cte_names:
            return  # CTE-defined virtual table; no snapshot entry needed
        if self._snapshot.get_table(table_name) is None:
            raise SchemaError(
                f"Table '{table_name}' does not exist in the schema snapshot.",
                details={
                    "table": table_name,
                    "allowed_tables": self._snapshot.table_names,
                },
            )

    def _assert_operator_allowed(self, op: str) -> None:
        """Raise DialectViolationError if the operator is not allowed."""
        allowed_ops = self._dialect.allowed.operators
        if allowed_ops and op not in allowed_ops:
            raise DialectViolationError(
                f"Operator '{op}' is not in the allowed operators list: "
                f"{allowed_ops}.",
                feature="operators",
            )
