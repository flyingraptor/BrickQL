"""Constants and helpers for QueryPlan expression and predicate types.

Operands and predicates are represented as plain dicts in the QueryPlan model
(using ``dict[str, Any]``) and validated structurally by PlanValidator.
This module defines the allowable key sets and helpers used by both the
validator and the compiler.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Operand key constants
# ---------------------------------------------------------------------------

#: The single key that identifies each operand type.
OPERAND_KEYS: frozenset[str] = frozenset({"col", "value", "param", "func", "case"})

# ---------------------------------------------------------------------------
# Predicate operator groups
# ---------------------------------------------------------------------------

#: Binary comparison operators: take a 2-element list of operands.
COMPARISON_OPS: frozenset[str] = frozenset({"EQ", "NE", "GT", "GTE", "LT", "LTE"})

#: Pattern-match operators: take [operand, pattern_operand].
PATTERN_OPS: frozenset[str] = frozenset({"LIKE", "ILIKE"})

#: Range operator: takes [value, low, high].
RANGE_OPS: frozenset[str] = frozenset({"BETWEEN"})

#: Membership operator: takes [operand, val1, val2, ...] or [operand, subquery].
MEMBERSHIP_OPS: frozenset[str] = frozenset({"IN"})

#: Null-check operators: take a single operand.
NULL_OPS: frozenset[str] = frozenset({"IS_NULL", "IS_NOT_NULL"})

#: Existence operator: takes a subquery.
EXISTS_OPS: frozenset[str] = frozenset({"EXISTS"})

#: Logical AND / OR: take a list of sub-predicates.
LOGICAL_AND_OR: frozenset[str] = frozenset({"AND", "OR"})

#: Logical NOT: takes a single sub-predicate.
LOGICAL_NOT: frozenset[str] = frozenset({"NOT"})

#: All logical operators.
LOGICAL_OPS: frozenset[str] = LOGICAL_AND_OR | LOGICAL_NOT

#: Complete set of supported predicate operators.
ALL_PREDICATE_OPS: frozenset[str] = (
    COMPARISON_OPS
    | PATTERN_OPS
    | RANGE_OPS
    | MEMBERSHIP_OPS
    | NULL_OPS
    | EXISTS_OPS
    | LOGICAL_OPS
)

# ---------------------------------------------------------------------------
# Function groups
# ---------------------------------------------------------------------------

#: Built-in aggregate functions.
AGGREGATE_FUNCTIONS: frozenset[str] = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX"})

#: Window-only ranking / navigation functions.
WINDOW_FUNCTIONS: frozenset[str] = frozenset({
    "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
    "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
})

#: All registered functions.
REGISTERED_FUNCTIONS: frozenset[str] = AGGREGATE_FUNCTIONS | WINDOW_FUNCTIONS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def operand_kind(expr: dict) -> str | None:
    """Returns the operand kind (``'col'``, ``'value'``, etc.) or ``None``.

    Args:
        expr: A dict that should represent one operand.

    Returns:
        The operand kind string if recognised, otherwise ``None``.
    """
    for key in OPERAND_KEYS:
        if key in expr:
            return key
    return None


def predicate_op(pred: dict) -> str | None:
    """Returns the predicate operator key or ``None`` if not recognised.

    Args:
        pred: A dict representing a predicate node.

    Returns:
        The operator string (e.g. ``'EQ'``, ``'AND'``) or ``None``.
    """
    if not isinstance(pred, dict) or len(pred) != 1:
        return None
    key = next(iter(pred))
    return key if key in ALL_PREDICATE_OPS else None
