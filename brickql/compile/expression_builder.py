"""Operand and predicate SQL compilers.

``OperandBuilder`` and ``PredicateBuilder`` are tightly coupled — CASE
operands contain predicate conditions, and predicates contain operands —
so they share a module.

Both classes receive a :class:`~brickql.compile.context.CompilationContext`
(static config) and a :class:`RuntimeContext` (per-query parameter state).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from brickql.compile.context import CompilationContext
from brickql.errors import CompilationError
from brickql.schema.expressions import ComparisonOp
from brickql.schema.operands import (
    CaseBody,
    CaseOperand,
    ColumnOperand,
    FuncOperand,
    Operand,
    ParamOperand,
    ValueOperand,
    to_operand,
)


# ---------------------------------------------------------------------------
# Runtime parameter accumulator (shared across all sub-builders in one run)
# ---------------------------------------------------------------------------


@dataclass
class RuntimeContext:
    """Accumulates named parameters during a single compilation run.

    A single instance is threaded through every sub-builder and every
    nested sub-query so that placeholder names are globally unique for
    the entire statement (including CTEs, SET_OP branches, and
    correlated subqueries).
    """

    params: dict[str, Any] = field(default_factory=dict)
    _counter: int = 0

    def add_value(self, value: Any) -> str:
        """Store a literal value and return its placeholder name."""
        name = f"param_{self._counter}"
        self._counter += 1
        self.params[name] = value
        return name


# ---------------------------------------------------------------------------
# Operand builder
# ---------------------------------------------------------------------------


class OperandBuilder:
    """Compiles typed :class:`~brickql.schema.operands.Operand` nodes to SQL.

    Args:
        ctx: Static compilation context (compiler + snapshot).
        runtime: Shared parameter accumulator for this query.
        predicate_builder: PredicateBuilder for CASE WHEN conditions.
    """

    def __init__(
        self,
        ctx: CompilationContext,
        runtime: RuntimeContext,
        predicate_builder: "PredicateBuilder",
    ) -> None:
        self._ctx = ctx
        self._runtime = runtime
        self._pred = predicate_builder

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, operand: Operand) -> str:
        """Compile a typed operand to a SQL fragment."""
        if isinstance(operand, ColumnOperand):
            return self._build_col_ref(operand.col)
        if isinstance(operand, ValueOperand):
            name = self._runtime.add_value(operand.value)
            return self._ctx.compiler.param_placeholder(name)
        if isinstance(operand, ParamOperand):
            return self._ctx.compiler.param_placeholder(operand.param)
        if isinstance(operand, FuncOperand):
            return self._build_func(operand)
        if isinstance(operand, CaseOperand):
            return self._build_case(operand.case)
        raise CompilationError(
            f"Unknown operand type: {type(operand).__name__}", clause="expression"
        )

    # ------------------------------------------------------------------
    # Operand sub-compilers
    # ------------------------------------------------------------------

    def _build_col_ref(self, col: str) -> str:
        quote = self._ctx.compiler.quote_identifier
        if "." in col:
            table, column = col.split(".", 1)
            return f"{quote(table)}.{quote(column)}"
        return quote(col)

    def _build_func(self, expr: FuncOperand) -> str:
        args_sql = ", ".join(self.build(a) for a in expr.args)
        return f"{expr.func.upper()}({args_sql})"

    def _build_case(self, case_body: CaseBody) -> str:
        parts = ["CASE"]
        for when in case_body.when:
            cond_sql = self._pred.build(when.condition)
            then_sql = self.build(when.then)
            parts.append(f"WHEN {cond_sql} THEN {then_sql}")
        if case_body.else_val is not None:
            parts.append(f"ELSE {self.build(case_body.else_val)}")
        parts.append("END")
        return " ".join(parts)


# ---------------------------------------------------------------------------
# Predicate builder
# ---------------------------------------------------------------------------


class PredicateBuilder:
    """Compiles predicate dicts (WHERE / HAVING nodes) to SQL.

    Predicates remain as ``dict[str, Any]``; operands embedded within them
    are converted via :func:`~brickql.schema.operands.to_operand`.

    The ``_build_subquery_fn`` is injected by :class:`~brickql.compile.builder.QueryBuilder`
    after construction.  It builds a sub-plan string using the **shared**
    ``RuntimeContext``, so all literal params across the whole statement
    get unique names (no collision between main query and sub-queries).

    Args:
        ctx: Static compilation context.
        runtime: Shared parameter accumulator.
        operand_builder: OperandBuilder for operand-level args.
    """

    def __init__(
        self,
        ctx: CompilationContext,
        runtime: RuntimeContext,
        operand_builder: OperandBuilder,
    ) -> None:
        self._ctx = ctx
        self._runtime = runtime
        self._op = operand_builder
        # Injected by QueryBuilder after construction.
        self._build_subquery_fn: Callable[["QueryPlan"], str] | None = None  # type: ignore[name-defined]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, pred: dict) -> str:
        """Compile a predicate dict to a SQL fragment."""
        if not isinstance(pred, dict) or len(pred) != 1:
            raise CompilationError(f"Invalid predicate shape: {pred!r}")
        op = next(iter(pred))
        args = pred[op]
        return self._dispatch(op, args)

    # ------------------------------------------------------------------
    # Operator dispatch
    # ------------------------------------------------------------------

    _CMP: dict[str, str] = {
        ComparisonOp.EQ: "=",
        ComparisonOp.NE: "!=",
        ComparisonOp.GT: ">",
        ComparisonOp.GTE: ">=",
        ComparisonOp.LT: "<",
        ComparisonOp.LTE: "<=",
    }

    def _dispatch(self, op: str, args: Any) -> str:
        if op in self._CMP:
            left = self._op.build(to_operand(args[0]))
            right = self._op.build(to_operand(args[1]))
            return f"{left} {self._CMP[op]} {right}"

        if op == "BETWEEN":
            val = self._op.build(to_operand(args[0]))
            low = self._op.build(to_operand(args[1]))
            high = self._op.build(to_operand(args[2]))
            return f"{val} BETWEEN {low} AND {high}"

        if op == "IN":
            val = self._op.build(to_operand(args[0]))
            rest = args[1:]
            if len(rest) == 1 and isinstance(rest[0], dict) and "SELECT" in rest[0]:
                sub_sql = self._build_subquery(rest[0])
                return f"{val} IN (\n{sub_sql}\n)"
            values = ", ".join(self._op.build(to_operand(a)) for a in rest)
            return f"{val} IN ({values})"

        if op == "IS_NULL":
            return f"{self._op.build(to_operand(args))} IS NULL"

        if op == "IS_NOT_NULL":
            return f"{self._op.build(to_operand(args))} IS NOT NULL"

        if op in ("LIKE", "ILIKE"):
            left = self._op.build(to_operand(args[0]))
            right = self._op.build(to_operand(args[1]))
            sql_op = self._ctx.compiler.like_operator(op)
            return f"{left} {sql_op} {right}"

        if op == "EXISTS":
            sub_sql = self._build_subquery(args)
            return f"EXISTS (\n{sub_sql}\n)"

        if op == "AND":
            parts = [f"({self.build(p)})" for p in args]
            return " AND ".join(parts)

        if op == "OR":
            parts = [f"({self.build(p)})" for p in args]
            return " OR ".join(parts)

        if op == "NOT":
            return f"NOT ({self.build(args)})"

        raise CompilationError(f"Unknown predicate operator '{op}'.")

    def _build_subquery(self, subquery_dict: dict) -> str:
        """Compile a nested QueryPlan using the shared runtime context."""
        from brickql.schema.query_plan import QueryPlan

        if self._build_subquery_fn is None:
            raise CompilationError("No subquery build function configured.")
        sub_plan = QueryPlan.model_validate(subquery_dict)
        return self._build_subquery_fn(sub_plan)
