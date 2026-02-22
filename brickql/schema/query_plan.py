"""Pydantic models for the brickQL QueryPlan.

The LLM outputs a single JSON object matching the ``QueryPlan`` shape.
All clause keys are optional; unused keys must be omitted.
Expression operands and predicate nodes inside ``WHERE``, ``HAVING``,
``GROUP_BY``, and ``SELECT`` items are represented as ``dict[str, Any]``
and validated deeply by ``PlanValidator``.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class SelectItem(BaseModel):
    """A single item in the SELECT clause.

    Attributes:
        expr: An operand dict (col / value / param / func / case).
        alias: Optional SQL alias for the expression.
        distinct: If True, emit ``SELECT DISTINCT`` for this item.
        over: Optional window specification.
    """

    model_config = ConfigDict(extra="forbid")

    expr: dict[str, Any]
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
        expr: Operand dict to order by.
        direction: Sort direction.
    """

    model_config = ConfigDict(extra="forbid")

    expr: dict[str, Any]
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
        partition_by: List of operand dicts for PARTITION BY.
        order_by: Ordered list of items for the window ORDER BY.
        frame: Optional frame clause.
    """

    model_config = ConfigDict(extra="forbid")

    partition_by: list[dict[str, Any]] = Field(default_factory=list)
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
        GROUP_BY: List of grouping operand dicts.
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
    GROUP_BY: list[dict[str, Any]] | None = None
    HAVING: dict[str, Any] | None = None
    ORDER_BY: list[OrderByItem] | None = None
    LIMIT: LimitClause | None = None
    OFFSET: OffsetClause | None = None
    SET_OP: SetOpClause | None = None
    CTE: list[CTEClause] | None = None


# Resolve forward references created by the recursive QueryPlan type.
FromClause.model_rebuild()
SetOpClause.model_rebuild()
CTEClause.model_rebuild()
QueryPlan.model_rebuild()
SelectItem.model_rebuild()
