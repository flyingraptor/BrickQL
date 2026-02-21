"""Core QueryPlan → SQL compilation logic.

``QueryBuilder`` implements the full compilation algorithm using composition
with an ``SQLCompiler`` instance for dialect-specific operations.  All SQL
features are implemented here; which features may be used is governed by the
DialectProfile passed to PlanValidator and PolicyEngine before compilation
reaches this class.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from brinkql.compile.base import CompiledSQL, SQLCompiler
from brinkql.errors import CompilationError
from brinkql.schema.query_plan import (
    CTEClause,
    FromClause,
    JoinClause,
    OrderByItem,
    QueryPlan,
    SelectItem,
    SetOpClause,
    WindowSpec,
)
from brinkql.schema.snapshot import SchemaSnapshot


@dataclass
class _CompilerContext:
    """Accumulates parameters during a single compilation run."""

    params: dict[str, Any] = field(default_factory=dict)
    _counter: int = 0

    def add_value(self, value: Any) -> str:
        """Store a literal value and return its placeholder name."""
        name = f"param_{self._counter}"
        self._counter += 1
        self.params[name] = value
        return name


class QueryBuilder:
    """Compiles a validated, policy-approved QueryPlan to parameterized SQL.

    Uses the Template Method pattern: dialect-specific behaviour (placeholders,
    quoting, ILIKE) is delegated to the injected ``SQLCompiler``.

    Args:
        compiler: Dialect-specific compiler instance.
        snapshot: Schema snapshot (used for JOIN ON clause resolution).
    """

    def __init__(self, compiler: SQLCompiler, snapshot: SchemaSnapshot) -> None:
        self._compiler = compiler
        self._snapshot = snapshot

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, plan: QueryPlan) -> CompiledSQL:
        """Compile ``plan`` to parameterized SQL.

        Args:
            plan: A validated, policy-approved QueryPlan.

        Returns:
            ``CompiledSQL`` with ``sql`` string and literal ``params``.

        Raises:
            CompilationError: If an unexpected plan shape is encountered.
        """
        ctx = _CompilerContext()

        # Phase 5: CTE (prepended before SELECT)
        cte_sql = self._build_cte_block(plan.CTE, ctx) if plan.CTE else ""

        if plan.SET_OP:
            # LIMIT/OFFSET apply to the *combined* result and must appear after
            # the SET_OP keyword — not inside the left-hand query.  Strip them
            # from the left side and emit them once at the very end.
            left_plan = plan.model_copy(
                update={"SET_OP": None, "LIMIT": None, "OFFSET": None}, deep=True
            )
            left_sql = self._build_core_query(left_plan, ctx)
            set_sql = self._build_set_op(plan.SET_OP, ctx)
            query_sql = f"{left_sql}\n{set_sql}"
            if plan.LIMIT:
                query_sql += f"\nLIMIT {plan.LIMIT.value}"
            if plan.OFFSET:
                query_sql += f"\nOFFSET {plan.OFFSET.value}"
        else:
            query_sql = self._build_core_query(plan, ctx)

        sql = f"{cte_sql}\n{query_sql}".strip() if cte_sql else query_sql
        return CompiledSQL(sql=sql, params=ctx.params, dialect=self._compiler.dialect_name)

    # ------------------------------------------------------------------
    # Core query (SELECT … LIMIT)
    # ------------------------------------------------------------------

    def _build_core_query(self, plan: QueryPlan, ctx: _CompilerContext) -> str:
        parts: list[str] = []

        select_clause = self._build_select_clause(plan, ctx)
        parts.append(select_clause)

        if plan.FROM:
            parts.append(f"FROM {self._build_from(plan.FROM, ctx)}")

        if plan.JOIN:
            for join in plan.JOIN:
                parts.append(self._build_join(join))

        if plan.WHERE:
            parts.append(f"WHERE {self._build_predicate(plan.WHERE, ctx)}")

        if plan.GROUP_BY:
            exprs = ", ".join(
                self._build_operand(e, ctx) for e in plan.GROUP_BY
            )
            parts.append(f"GROUP BY {exprs}")

        if plan.HAVING:
            parts.append(f"HAVING {self._build_predicate(plan.HAVING, ctx)}")

        if plan.ORDER_BY:
            order_parts = [
                f"{self._build_operand(o.expr, ctx)} {o.direction}"
                for o in plan.ORDER_BY
            ]
            parts.append(f"ORDER BY {', '.join(order_parts)}")

        if plan.LIMIT:
            parts.append(f"LIMIT {plan.LIMIT.value}")

        if plan.OFFSET:
            parts.append(f"OFFSET {plan.OFFSET.value}")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # SELECT clause
    # ------------------------------------------------------------------

    def _build_select_clause(
        self, plan: QueryPlan, ctx: _CompilerContext
    ) -> str:
        if not plan.SELECT:
            return "SELECT *"

        has_distinct = any(item.distinct for item in plan.SELECT)
        prefix = "SELECT DISTINCT" if has_distinct else "SELECT"

        items: list[str] = []
        for item in plan.SELECT:
            items.append(self._build_select_item(item, ctx))

        return f"{prefix} {', '.join(items)}"

    def _build_select_item(
        self, item: SelectItem, ctx: _CompilerContext
    ) -> str:
        if item.over is not None:
            expr_sql = self._build_operand(item.expr, ctx)
            over_sql = self._build_window_spec(item.over, ctx)
            expr_sql = f"{expr_sql} OVER ({over_sql})"
        else:
            expr_sql = self._build_operand(item.expr, ctx)

        if item.alias:
            return f"{expr_sql} AS {self._compiler.quote_identifier(item.alias)}"
        return expr_sql

    # ------------------------------------------------------------------
    # FROM
    # ------------------------------------------------------------------

    def _build_from(self, frm: FromClause, ctx: _CompilerContext) -> str:
        if frm.table:
            table_sql = self._compiler.quote_identifier(frm.table)
            if frm.alias:
                table_sql = (
                    f"{table_sql} AS "
                    f"{self._compiler.quote_identifier(frm.alias)}"
                )
            return table_sql
        if frm.subquery:
            # Share ctx so param names don't collide with the outer query.
            sub_sql = self._build_core_query(frm.subquery, ctx)
            alias_sql = (
                self._compiler.quote_identifier(frm.alias)
                if frm.alias
                else "_sub"
            )
            return f"(\n{sub_sql}\n) AS {alias_sql}"
        raise CompilationError("FROM clause has no table or subquery.", clause="FROM")

    # ------------------------------------------------------------------
    # JOIN
    # ------------------------------------------------------------------

    def _build_join(self, join: JoinClause) -> str:
        rel = self._snapshot.get_relationship(join.rel)
        if rel is None:
            raise CompilationError(
                f"Relationship '{join.rel}' not found in snapshot.",
                clause="JOIN",
            )
        # When a JOIN alias is given (required for self-referential joins),
        # use it as the qualifier on the to_col side of the ON clause.
        to_qualifier = self._compiler.quote_identifier(
            join.alias if join.alias else rel.to_table
        )
        from_col = (
            f"{self._compiler.quote_identifier(rel.from_table)}."
            f"{self._compiler.quote_identifier(rel.from_col)}"
        )
        to_col = (
            f"{to_qualifier}."
            f"{self._compiler.quote_identifier(rel.to_col)}"
        )
        to_table_sql = self._compiler.quote_identifier(rel.to_table)
        if join.alias:
            to_table_sql = (
                f"{to_table_sql} AS "
                f"{self._compiler.quote_identifier(join.alias)}"
            )
        return f"{join.type} JOIN {to_table_sql} ON {from_col} = {to_col}"

    # ------------------------------------------------------------------
    # CTE
    # ------------------------------------------------------------------

    def _build_cte_block(
        self, ctes: list[CTEClause], ctx: _CompilerContext
    ) -> str:
        recursive = any(c.recursive for c in ctes)
        keyword = "WITH RECURSIVE" if recursive else "WITH"
        cte_parts: list[str] = []
        for cte in ctes:
            # Share ctx so param names don't collide with the outer query.
            cte_sql = self._build_core_query(cte.query, ctx)
            cte_parts.append(
                f"{self._compiler.quote_identifier(cte.name)} AS (\n{cte_sql}\n)"
            )
        return f"{keyword} {', '.join(cte_parts)}"

    # ------------------------------------------------------------------
    # SET OP
    # ------------------------------------------------------------------

    def _build_set_op(
        self, set_op: SetOpClause, ctx: _CompilerContext
    ) -> str:
        # Strip LIMIT/OFFSET from the right-hand query; they belong on the
        # combined result, not on individual branches of the set operation.
        right_plan = set_op.query.model_copy(
            update={"LIMIT": None, "OFFSET": None}, deep=True
        )
        right_sql = self._build_core_query(right_plan, ctx)
        keyword = set_op.op.replace("_", " ")
        return f"{keyword}\n{right_sql}"

    # ------------------------------------------------------------------
    # Window spec
    # ------------------------------------------------------------------

    def _build_window_spec(
        self, spec: WindowSpec, ctx: _CompilerContext
    ) -> str:
        parts: list[str] = []
        if spec.partition_by:
            exprs = ", ".join(
                self._build_operand(e, ctx) for e in spec.partition_by
            )
            parts.append(f"PARTITION BY {exprs}")
        if spec.order_by:
            order_parts = [
                f"{self._build_operand(o.expr, ctx)} {o.direction}"
                for o in spec.order_by
            ]
            parts.append(f"ORDER BY {', '.join(order_parts)}")
        if spec.frame:
            parts.append(
                f"{spec.frame.type} BETWEEN "
                f"{spec.frame.start} AND {spec.frame.end}"
            )
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Operand compilation
    # ------------------------------------------------------------------

    def _build_operand(self, expr: dict, ctx: _CompilerContext) -> str:
        if "col" in expr:
            return self._build_col_ref(expr["col"])
        if "value" in expr:
            name = ctx.add_value(expr["value"])
            return self._compiler.param_placeholder(name)
        if "param" in expr:
            return self._compiler.param_placeholder(expr["param"])
        if "func" in expr:
            return self._build_func(expr, ctx)
        if "case" in expr:
            return self._build_case(expr["case"], ctx)
        raise CompilationError(
            f"Unknown operand type: {list(expr.keys())}", clause="expression"
        )

    def _build_col_ref(self, col: str) -> str:
        if "." in col:
            table, column = col.split(".", 1)
            return (
                f"{self._compiler.quote_identifier(table)}."
                f"{self._compiler.quote_identifier(column)}"
            )
        return self._compiler.quote_identifier(col)

    def _build_func(self, expr: dict, ctx: _CompilerContext) -> str:
        func_name = expr["func"].upper()
        args_sql = ", ".join(
            self._build_operand(a, ctx) for a in expr.get("args", [])
        )
        return f"{func_name}({args_sql})"

    def _build_case(self, case_body: dict, ctx: _CompilerContext) -> str:
        parts = ["CASE"]
        for when in case_body.get("when", []):
            condition_key = "if" if "if" in when else "condition"
            cond_sql = self._build_predicate(when[condition_key], ctx)
            then_sql = self._build_operand(when["then"], ctx)
            parts.append(f"WHEN {cond_sql} THEN {then_sql}")
        else_val = case_body.get("else")
        if else_val is not None:
            parts.append(f"ELSE {self._build_operand(else_val, ctx)}")
        parts.append("END")
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Predicate compilation
    # ------------------------------------------------------------------

    def _build_predicate(self, pred: dict, ctx: _CompilerContext) -> str:
        if not isinstance(pred, dict) or len(pred) != 1:
            raise CompilationError(f"Invalid predicate shape: {pred!r}")
        op = next(iter(pred))
        args = pred[op]

        _CMP = {"EQ": "=", "NE": "!=", "GT": ">", "GTE": ">=", "LT": "<", "LTE": "<="}
        if op in _CMP:
            left = self._build_operand(args[0], ctx)
            right = self._build_operand(args[1], ctx)
            return f"{left} {_CMP[op]} {right}"

        if op == "BETWEEN":
            val = self._build_operand(args[0], ctx)
            low = self._build_operand(args[1], ctx)
            high = self._build_operand(args[2], ctx)
            return f"{val} BETWEEN {low} AND {high}"

        if op == "IN":
            val = self._build_operand(args[0], ctx)
            rest = args[1:]
            if len(rest) == 1 and isinstance(rest[0], dict) and "SELECT" in rest[0]:
                sub_plan = QueryPlan.model_validate(rest[0])
                sub = QueryBuilder(self._compiler, self._snapshot).build(sub_plan)
                ctx.params.update(sub.params)
                return f"{val} IN (\n{sub.sql}\n)"
            values = ", ".join(self._build_operand(a, ctx) for a in rest)
            return f"{val} IN ({values})"

        if op == "IS_NULL":
            return f"{self._build_operand(args, ctx)} IS NULL"

        if op == "IS_NOT_NULL":
            return f"{self._build_operand(args, ctx)} IS NOT NULL"

        if op in ("LIKE", "ILIKE"):
            left = self._build_operand(args[0], ctx)
            right = self._build_operand(args[1], ctx)
            sql_op = self._compiler.like_operator(op)
            return f"{left} {sql_op} {right}"

        if op == "EXISTS":
            sub_plan = QueryPlan.model_validate(args)
            sub = QueryBuilder(self._compiler, self._snapshot).build(sub_plan)
            ctx.params.update(sub.params)
            return f"EXISTS (\n{sub.sql}\n)"

        if op == "AND":
            parts = [f"({self._build_predicate(p, ctx)})" for p in args]
            return " AND ".join(parts)

        if op == "OR":
            parts = [f"({self._build_predicate(p, ctx)})" for p in args]
            return " OR ".join(parts)

        if op == "NOT":
            return f"NOT ({self._build_predicate(args, ctx)})"

        raise CompilationError(f"Unknown predicate operator '{op}'.")
