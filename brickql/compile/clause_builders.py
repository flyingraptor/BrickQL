"""Clause-level SQL builders.

Each class handles exactly one SQL clause.  ``CteBuilder``,
``SetOpBuilder``, and ``FromClauseBuilder`` receive a *shared build
function* (``Callable[[QueryPlan], str]``) rather than a subquery factory.
This means every nested plan — CTEs, SET_OP branches, derived tables, and
correlated subqueries — is compiled using the **same** :class:`RuntimeContext`
as the outer query, so literal parameter names are globally unique and
never collide.

Classes
-------
SelectClauseBuilder   — ``SELECT [DISTINCT] <items>``
FromClauseBuilder     — ``FROM <table | subquery>``
JoinClauseBuilder     — ``JOIN … ON …``
WindowSpecBuilder     — ``OVER (PARTITION BY … ORDER BY … FRAME)``
CteBuilder            — ``WITH [RECURSIVE] <ctes>``
SetOpBuilder          — ``UNION / INTERSECT / EXCEPT …``
"""
from __future__ import annotations

from typing import Callable

from brickql.compile.context import CompilationContext
from brickql.compile.expression_builder import OperandBuilder, RuntimeContext
from brickql.errors import CompilationError
from brickql.schema.query_plan import (
    CTEClause,
    FromClause,
    JoinClause,
    QueryPlan,
    SelectItem,
    SetOpClause,
    WindowSpec,
)


class SelectClauseBuilder:
    """Builds the ``SELECT [DISTINCT] …`` clause."""

    def __init__(
        self,
        ctx: CompilationContext,
        runtime: RuntimeContext,
        operand_builder: OperandBuilder,
    ) -> None:
        self._ctx = ctx
        self._runtime = runtime
        self._op = operand_builder

    def build(self, plan: QueryPlan) -> str:
        if not plan.SELECT:
            return "SELECT *"

        has_distinct = any(item.distinct for item in plan.SELECT)
        prefix = "SELECT DISTINCT" if has_distinct else "SELECT"
        items = [self._build_item(item) for item in plan.SELECT]
        return f"{prefix} {', '.join(items)}"

    def _build_item(self, item: SelectItem) -> str:
        if item.over is not None:
            expr_sql = self._op.build(item.expr)
            over_sql = WindowSpecBuilder(self._ctx, self._runtime, self._op).build(
                item.over
            )
            expr_sql = f"{expr_sql} OVER ({over_sql})"
        else:
            expr_sql = self._op.build(item.expr)

        if item.alias:
            return f"{expr_sql} AS {self._ctx.compiler.quote_identifier(item.alias)}"
        return expr_sql


class FromClauseBuilder:
    """Builds the ``FROM <table | subquery>`` fragment.

    For subquery FROM clauses, compilation is delegated to ``build_fn``,
    which uses the **shared** ``RuntimeContext`` so literal params receive
    globally unique names.
    """

    def __init__(
        self,
        ctx: CompilationContext,
        build_fn: Callable[[QueryPlan], str],
    ) -> None:
        self._ctx = ctx
        self._build_fn = build_fn

    def build(self, frm: FromClause) -> str:
        quote = self._ctx.compiler.quote_identifier
        if frm.table:
            table_sql = quote(frm.table)
            if frm.alias:
                table_sql = f"{table_sql} AS {quote(frm.alias)}"
            return table_sql
        if frm.subquery:
            sub_sql = self._build_fn(frm.subquery)
            alias_sql = quote(frm.alias) if frm.alias else "_sub"
            return f"(\n{sub_sql}\n) AS {alias_sql}"
        raise CompilationError("FROM clause has no table or subquery.", clause="FROM")


class JoinClauseBuilder:
    """Builds a single ``JOIN … ON …`` fragment."""

    def __init__(self, ctx: CompilationContext) -> None:
        self._ctx = ctx

    def build(self, join: JoinClause) -> str:
        quote = self._ctx.compiler.quote_identifier
        rel = self._ctx.snapshot.get_relationship(join.rel)
        if rel is None:
            raise CompilationError(
                f"Relationship '{join.rel}' not found in snapshot.", clause="JOIN"
            )
        to_qualifier = quote(join.alias if join.alias else rel.to_table)
        from_col = f"{quote(rel.from_table)}.{quote(rel.from_col)}"
        to_col = f"{to_qualifier}.{quote(rel.to_col)}"
        to_table_sql = quote(rel.to_table)
        if join.alias:
            to_table_sql = f"{to_table_sql} AS {quote(join.alias)}"
        return f"{join.type} JOIN {to_table_sql} ON {from_col} = {to_col}"


class WindowSpecBuilder:
    """Builds the ``PARTITION BY … ORDER BY … FRAME`` fragment inside OVER."""

    def __init__(
        self,
        ctx: CompilationContext,
        runtime: RuntimeContext,
        operand_builder: OperandBuilder,
    ) -> None:
        self._ctx = ctx
        self._runtime = runtime
        self._op = operand_builder

    def build(self, spec: WindowSpec) -> str:
        parts: list[str] = []
        if spec.partition_by:
            exprs = ", ".join(self._op.build(e) for e in spec.partition_by)
            parts.append(f"PARTITION BY {exprs}")
        if spec.order_by:
            order_parts = [
                f"{self._op.build(o.expr)} {o.direction}" for o in spec.order_by
            ]
            parts.append(f"ORDER BY {', '.join(order_parts)}")
        if spec.frame:
            parts.append(
                f"{spec.frame.type} BETWEEN {spec.frame.start} AND {spec.frame.end}"
            )
        return " ".join(parts)


class CteBuilder:
    """Builds the ``WITH [RECURSIVE] <name> AS (…)`` block.

    CTE bodies are compiled using ``build_fn`` so they share the outer
    ``RuntimeContext`` — param names stay globally unique.
    """

    def __init__(
        self,
        ctx: CompilationContext,
        build_fn: Callable[[QueryPlan], str],
    ) -> None:
        self._ctx = ctx
        self._build_fn = build_fn

    def build(self, ctes: list[CTEClause]) -> str:
        recursive = any(c.recursive for c in ctes)
        keyword = "WITH RECURSIVE" if recursive else "WITH"
        quote = self._ctx.compiler.quote_identifier
        cte_parts: list[str] = []
        for cte in ctes:
            cte_sql = self._build_fn(cte.query)
            cte_parts.append(f"{quote(cte.name)} AS (\n{cte_sql}\n)")
        return f"{keyword} {', '.join(cte_parts)}"


class SetOpBuilder:
    """Builds a ``UNION / INTERSECT / EXCEPT <right_query>`` fragment.

    The right-hand query is compiled using ``build_fn`` so it shares the
    outer ``RuntimeContext`` — param names stay globally unique across both
    branches of the set operation.
    """

    def __init__(
        self,
        build_fn: Callable[[QueryPlan], str],
    ) -> None:
        self._build_fn = build_fn

    def build(self, set_op: SetOpClause) -> str:
        right_plan = set_op.query.model_copy(
            update={"LIMIT": None, "OFFSET": None}, deep=True
        )
        right_sql = self._build_fn(right_plan)
        keyword = set_op.op.replace("_", " ")
        return f"{keyword}\n{right_sql}"
