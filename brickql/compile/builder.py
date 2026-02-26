"""Core QueryPlan → SQL compilation logic.

``QueryBuilder`` is the top-level orchestrator.  It wires together focused
clause-level and expression-level sub-builders, then drives the compilation
algorithm.  All dialect-specific behaviour is delegated to the injected
``SQLCompiler``; clause rendering is delegated to the sub-builder hierarchy.

Sub-builder hierarchy
---------------------
QueryBuilder
  ├── OperandBuilder       (expression_builder.py)
  ├── PredicateBuilder     (expression_builder.py)
  ├── SelectClauseBuilder  (clause_builders.py)
  ├── FromClauseBuilder    (clause_builders.py)
  ├── JoinClauseBuilder    (clause_builders.py)
  ├── WindowSpecBuilder    (clause_builders.py)
  ├── CteBuilder           (clause_builders.py)
  └── SetOpBuilder         (clause_builders.py)

Runtime context sharing
-----------------------
A single :class:`~brickql.compile.expression_builder.RuntimeContext` is
created per ``build()`` call and threaded through every sub-builder and
every nested sub-plan (CTEs, SET_OP branches, FROM subqueries, correlated
EXISTS / IN subqueries).  This ensures literal parameter names are globally
unique across the whole statement.

Item 11 - Inappropriate Intimacy fix
-------------------------------------
The ``subquery_factory`` parameter lets callers inject a custom
:class:`QueryBuilder` factory for testing or specialisation, replacing the
previous hard-coded ``QueryBuilder(compiler, snapshot)`` self-instantiation.
"""

from __future__ import annotations

from collections.abc import Callable

from brickql.compile.base import CompiledSQL, SQLCompiler
from brickql.compile.clause_builders import (
    CteBuilder,
    FromClauseBuilder,
    JoinClauseBuilder,
    SelectClauseBuilder,
    SetOpBuilder,
)
from brickql.compile.context import CompilationContext
from brickql.compile.expression_builder import (
    OperandBuilder,
    PredicateBuilder,
    RuntimeContext,
)
from brickql.schema.query_plan import QueryPlan
from brickql.schema.snapshot import SchemaSnapshot


class QueryBuilder:
    """Compiles a validated, policy-approved QueryPlan to parameterized SQL.

    Args:
        compiler: Dialect-specific compiler instance.
        snapshot: Schema snapshot (used for JOIN ON clause resolution).
        subquery_factory: Optional callable returning a fresh
            :class:`QueryBuilder` for correlated sub-queries that must NOT
            share the outer runtime context (rare; most nested plans reuse the
            outer runtime via the shared ``build_fn`` closure).  Defaults to
            ``lambda: QueryBuilder(compiler, snapshot)``.
    """

    def __init__(
        self,
        compiler: SQLCompiler,
        snapshot: SchemaSnapshot,
        subquery_factory: Callable[[], QueryBuilder] | None = None,
    ) -> None:
        self._ctx = CompilationContext(compiler=compiler, snapshot=snapshot)
        self._subquery_factory = subquery_factory or (lambda: QueryBuilder(compiler, snapshot))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, plan: QueryPlan) -> CompiledSQL:
        """Compile ``plan`` to parameterized SQL.

        Args:
            plan: A validated, policy-approved QueryPlan.

        Returns:
            :class:`~brickql.compile.base.CompiledSQL` with ``sql`` string
            and literal ``params``.

        Raises:
            CompilationError: If an unexpected plan shape is encountered.
        """
        runtime = RuntimeContext()
        sub_builders = self._make_sub_builders(runtime)
        sql = self._build_full(plan, runtime, sub_builders)
        return CompiledSQL(
            sql=sql,
            params=runtime.params,
            dialect=self._ctx.compiler.dialect_name,
        )

    # ------------------------------------------------------------------
    # Full query assembly (CTE prepend + optional SET_OP)
    # ------------------------------------------------------------------

    def _build_full(
        self,
        plan: QueryPlan,
        runtime: RuntimeContext,
        sub_builders: dict,
    ) -> str:
        cte_sql = ""
        if plan.CTE:
            cte_sql = sub_builders["cte"].build(plan.CTE)

        if plan.SET_OP:
            left_plan = plan.model_copy(
                update={"SET_OP": None, "LIMIT": None, "OFFSET": None}, deep=True
            )
            left_sql = self._build_core_query(left_plan, runtime, sub_builders)
            set_sql = sub_builders["set_op"].build(plan.SET_OP)
            query_sql = f"{left_sql}\n{set_sql}"
            if plan.LIMIT:
                query_sql += f"\nLIMIT {plan.LIMIT.value}"
            if plan.OFFSET:
                query_sql += f"\nOFFSET {plan.OFFSET.value}"
        else:
            query_sql = self._build_core_query(plan, runtime, sub_builders)

        return f"{cte_sql}\n{query_sql}".strip() if cte_sql else query_sql

    # ------------------------------------------------------------------
    # Core query (SELECT … LIMIT)
    # ------------------------------------------------------------------

    def _build_core_query(
        self,
        plan: QueryPlan,
        runtime: RuntimeContext,
        sub_builders: dict,
    ) -> str:
        parts: list[str] = []

        parts.append(sub_builders["select"].build(plan))

        if plan.FROM:
            parts.append(f"FROM {sub_builders['from'].build(plan.FROM)}")

        if plan.JOIN:
            for join in plan.JOIN:
                parts.append(sub_builders["join"].build(join))

        if plan.WHERE:
            parts.append(f"WHERE {sub_builders['pred'].build(plan.WHERE)}")

        if plan.GROUP_BY:
            exprs = ", ".join(sub_builders["op"].build(e) for e in plan.GROUP_BY)
            parts.append(f"GROUP BY {exprs}")

        if plan.HAVING:
            parts.append(f"HAVING {sub_builders['pred'].build(plan.HAVING)}")

        if plan.ORDER_BY:
            order_parts = [
                f"{sub_builders['op'].build(o.expr)} {o.direction}" for o in plan.ORDER_BY
            ]
            parts.append(f"ORDER BY {', '.join(order_parts)}")

        if plan.LIMIT:
            parts.append(f"LIMIT {plan.LIMIT.value}")

        if plan.OFFSET:
            parts.append(f"OFFSET {plan.OFFSET.value}")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Sub-builder wiring
    # ------------------------------------------------------------------

    def _make_sub_builders(self, runtime: RuntimeContext) -> dict:
        """Construct and wire the sub-builder graph for one compilation run.

        All nested plans (CTEs, SET_OPs, FROM subqueries, and correlated
        predicates) share ``runtime`` via the ``build_fn`` closure so that
        literal parameter names are globally unique.
        """
        # Build the operand/predicate pair (mutually dependent).
        pred_builder = PredicateBuilder.__new__(PredicateBuilder)
        op_builder = OperandBuilder(self._ctx, runtime, pred_builder)
        pred_builder.__init__(self._ctx, runtime, op_builder)  # type: ignore[misc]

        sub_builders: dict = {
            "op": op_builder,
            "pred": pred_builder,
            "select": SelectClauseBuilder(self._ctx, runtime, op_builder),
            "join": JoinClauseBuilder(self._ctx),
        }

        # Shared-context build function: any nested QueryPlan compiled here
        # uses the same runtime context as the outer query.
        def build_fn(plan: QueryPlan) -> str:
            return self._build_core_query(plan, runtime, sub_builders)

        pred_builder._build_subquery_fn = build_fn

        sub_builders["from"] = FromClauseBuilder(self._ctx, build_fn)
        sub_builders["cte"] = CteBuilder(self._ctx, build_fn)
        sub_builders["set_op"] = SetOpBuilder(build_fn)

        return sub_builders
