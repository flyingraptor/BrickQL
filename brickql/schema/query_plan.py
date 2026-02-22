"""Pydantic models for the brickQL QueryPlan.

The LLM outputs a single JSON object matching the ``QueryPlan`` shape.
All clause keys are optional; unused keys must be omitted.
Expression operands inside ``SELECT`` items, ``GROUP_BY``, ``ORDER BY``,
and window ``PARTITION BY`` are now typed via the ``Operand`` union.
Predicate nodes (``WHERE``, ``HAVING``) remain as ``dict[str, Any]``
and are validated deeply by ``PlanValidator``.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from brickql.schema.operands import (
    ColumnOperand,
    FuncOperand,
    CaseOperand,
    Operand,
)


class SelectItem(BaseModel):
    """A single item in the SELECT clause.

    Attributes:
        expr: A typed operand (col / value / param / func / case).
        alias: Optional SQL alias for the expression.
        distinct: If True, emit ``SELECT DISTINCT`` for this item.
        over: Optional window specification.
    """

    model_config = ConfigDict(extra="forbid")

    expr: Operand
    alias: str | None = None
    distinct: bool = False
    over: WindowSpec | None = None


class FromClause(BaseModel):
    """The FROM clause: a single table or an inline derived table.

    Attributes:
        table: Table name (mutually exclusive with ``subquery``).
        alias: Optional table alias.
        subquery: Inline derived table (requires ``allow_subqueries``).
    """

    model_config = ConfigDict(extra="forbid")

    table: str | None = None
    alias: str | None = None
    subquery: QueryPlan | None = None


class JoinClause(BaseModel):
    """A single JOIN entry.

    The LLM must NOT invent ON clauses; joins use named relationship keys
    defined in the SchemaSnapshot.

    Attributes:
        rel: Named relationship key from the schema snapshot.
        type: SQL join type.
        alias: Optional alias for the joined table.
    """

    model_config = ConfigDict(extra="forbid")

    rel: str
    type: Literal["INNER", "LEFT", "RIGHT", "FULL", "CROSS"] = "INNER"
    alias: str | None = None


class OrderByItem(BaseModel):
    """A single ORDER BY expression.

    Attributes:
        expr: Typed operand to order by.
        direction: Sort direction.
    """

    model_config = ConfigDict(extra="forbid")

    expr: Operand
    direction: Literal["ASC", "DESC"] = "ASC"


class LimitClause(BaseModel):
    """LIMIT clause.

    Attributes:
        value: Maximum number of rows; must be a positive integer.
    """

    model_config = ConfigDict(extra="forbid")

    value: int


class OffsetClause(BaseModel):
    """OFFSET clause.

    Attributes:
        value: Number of rows to skip; must be a non-negative integer.
    """

    model_config = ConfigDict(extra="forbid")

    value: int


class WindowFrame(BaseModel):
    """ROWS or RANGE frame specification for a window function.

    Attributes:
        type: Frame type (ROWS or RANGE).
        start: Frame start boundary.
        end: Frame end boundary.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["ROWS", "RANGE"] = "ROWS"
    start: str = "UNBOUNDED PRECEDING"
    end: str = "CURRENT ROW"


class WindowSpec(BaseModel):
    """OVER (...) specification for a window function.

    Attributes:
        partition_by: List of typed operands for PARTITION BY.
        order_by: Ordered list of items for the window ORDER BY.
        frame: Optional frame clause.
    """

    model_config = ConfigDict(extra="forbid")

    partition_by: list[Operand] = Field(default_factory=list)
    order_by: list[OrderByItem] = Field(default_factory=list)
    frame: WindowFrame | None = None


class SetOpClause(BaseModel):
    """A set operation applied to the main query (UNION, INTERSECT, EXCEPT).

    Attributes:
        op: The set operation type.
        query: The right-hand QueryPlan.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["UNION", "UNION_ALL", "INTERSECT", "EXCEPT"]
    query: QueryPlan


class CTEClause(BaseModel):
    """A single CTE (``WITH name AS (...)``) definition.

    Attributes:
        name: CTE name referenced in the main query.
        query: The CTE body.
        recursive: If True, emit ``WITH RECURSIVE``.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    query: QueryPlan
    recursive: bool = False


class QueryPlan(BaseModel):
    """Top-level QueryPlan as output by the LLM planner.

    All keys are optional; omit unused keys.
    Do NOT rename, add, or reorder keys.

    Attributes:
        SELECT: List of select items.
        FROM: The primary table or derived table.
        JOIN: List of relationship-based joins.
        WHERE: Root predicate dict.
        GROUP_BY: List of typed grouping operands.
        HAVING: Root predicate dict for aggregate filtering.
        ORDER_BY: List of ordering items.
        LIMIT: Maximum rows (required by default policy).
        OFFSET: Rows to skip.
        SET_OP: Set operation applied to the main query.
        CTE: List of CTE definitions.
    """

    model_config = ConfigDict(extra="forbid")

    SELECT: list[SelectItem] | None = None
    FROM: FromClause | None = None
    JOIN: list[JoinClause] | None = None
    WHERE: dict[str, Any] | None = None
    GROUP_BY: list[Operand] | None = None
    HAVING: dict[str, Any] | None = None
    ORDER_BY: list[OrderByItem] | None = None
    LIMIT: LimitClause | None = None
    OFFSET: OffsetClause | None = None
    SET_OP: SetOpClause | None = None
    CTE: list[CTEClause] | None = None

    # ------------------------------------------------------------------
    # Domain methods (Item 7 — Anemic Domain Model fix)
    # ------------------------------------------------------------------

    def collect_table_references(self) -> set[str]:
        """Collect direct table names referenced in FROM.

        Does not resolve JOIN relationship keys — callers that need
        JOIN-resolved tables should use :meth:`collect_joined_tables`.

        Returns:
            Set of table names (may be empty if FROM is absent or uses a
            subquery rather than a direct table reference).
        """
        tables: set[str] = set()
        if self.FROM and self.FROM.table:
            tables.add(self.FROM.table)
        return tables

    def collect_col_refs(self) -> list[str]:
        """Collect every ``table.column`` reference in this plan.

        Walks SELECT, GROUP BY, ORDER BY, window specs, and recursively
        through subqueries.  Predicate dicts (WHERE / HAVING) are also
        walked since they embed operand dicts.

        Returns:
            List of column reference strings in encounter order.
        """
        refs: list[str] = []
        _collect_col_refs_from_plan(self, refs)
        return refs


# ---------------------------------------------------------------------------
# Internal helpers for collect_col_refs
# ---------------------------------------------------------------------------


def _collect_from_operand(operand: Operand, refs: list[str]) -> None:
    """Append column references found inside a typed operand."""
    if isinstance(operand, ColumnOperand):
        refs.append(operand.col)
    elif isinstance(operand, FuncOperand):
        for arg in operand.args:
            _collect_from_operand(arg, refs)
    elif isinstance(operand, CaseOperand):
        for when in operand.case.when:
            _collect_from_pred_dict(when.condition, refs)
            _collect_from_operand(when.then, refs)
        if operand.case.else_val is not None:
            _collect_from_operand(operand.case.else_val, refs)


def _collect_from_pred_dict(pred: Any, refs: list[str]) -> None:
    """Append column references found inside a predicate dict."""
    if isinstance(pred, dict):
        for v in pred.values():
            _collect_from_pred_or_operand(v, refs)
    elif isinstance(pred, list):
        for item in pred:
            _collect_from_pred_or_operand(item, refs)


def _collect_from_pred_or_operand(node: Any, refs: list[str]) -> None:
    """Dispatch to operand or predicate walker depending on node type."""
    if isinstance(node, (ColumnOperand, FuncOperand, CaseOperand)):
        _collect_from_operand(node, refs)
    elif isinstance(node, dict):
        # Could be a predicate dict or a legacy operand dict — handle both.
        if "col" in node:
            refs.append(node["col"])
        elif "func" in node:
            for arg in node.get("args", []):
                _collect_from_pred_or_operand(arg, refs)
        elif "case" in node:
            _collect_from_pred_dict(node["case"], refs)
        else:
            _collect_from_pred_dict(node, refs)
    elif isinstance(node, list):
        for item in node:
            _collect_from_pred_or_operand(item, refs)


def _collect_col_refs_from_plan(plan: QueryPlan, refs: list[str]) -> None:
    """Walk every expression-bearing clause and accumulate column refs."""
    if plan.SELECT:
        for item in plan.SELECT:
            _collect_from_operand(item.expr, refs)
            if item.over:
                for pb in item.over.partition_by:
                    _collect_from_operand(pb, refs)
                for ob in item.over.order_by:
                    _collect_from_operand(ob.expr, refs)
    if plan.WHERE:
        _collect_from_pred_dict(plan.WHERE, refs)
    if plan.GROUP_BY:
        for operand in plan.GROUP_BY:
            _collect_from_operand(operand, refs)
    if plan.HAVING:
        _collect_from_pred_dict(plan.HAVING, refs)
    if plan.ORDER_BY:
        for item in plan.ORDER_BY:
            _collect_from_operand(item.expr, refs)
    if plan.FROM and plan.FROM.subquery:
        _collect_col_refs_from_plan(plan.FROM.subquery, refs)
    if plan.CTE:
        for cte in plan.CTE:
            _collect_col_refs_from_plan(cte.query, refs)
    if plan.SET_OP:
        _collect_col_refs_from_plan(plan.SET_OP.query, refs)


# Resolve forward references created by the recursive QueryPlan type.
FromClause.model_rebuild()
SetOpClause.model_rebuild()
CTEClause.model_rebuild()
QueryPlan.model_rebuild()
SelectItem.model_rebuild()
WindowSpec.model_rebuild()
OrderByItem.model_rebuild()
