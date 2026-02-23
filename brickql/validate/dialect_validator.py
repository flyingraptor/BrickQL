"""Dialect feature-flag validator.

Checks that the plan does not use SQL features disabled in the
``DialectProfile`` (CTEs, subqueries, window functions, set operations,
and JOIN depth).
"""

from __future__ import annotations

from brickql.errors import DialectViolationError
from brickql.schema.context import ValidationContext
from brickql.schema.query_plan import QueryPlan


class DialectValidator:
    """Validates plan features against the dialect's ``AllowedFeatures``.

    Args:
        ctx: Validation context (snapshot + dialect).
    """

    def __init__(self, ctx: ValidationContext) -> None:
        self._ctx = ctx

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_feature_flags(self, plan: QueryPlan) -> None:
        """Raise on the first disabled feature found.

        Raises:
            DialectViolationError: If the plan uses a feature that is not
                enabled in the dialect profile.
        """
        allowed = self._ctx.dialect.allowed

        if plan.JOIN and allowed.max_join_depth == 0:
            raise DialectViolationError("JOINs are not allowed (max_join_depth=0).", feature="join")
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

    def validate_join_depth(self, plan: QueryPlan) -> None:
        """Raise if the number of JOINs exceeds ``max_join_depth``.

        Raises:
            DialectViolationError: If join count exceeds the configured max.
        """
        if not plan.JOIN:
            return
        allowed = self._ctx.dialect.allowed
        if len(plan.JOIN) > allowed.max_join_depth:
            raise DialectViolationError(
                f"Query uses {len(plan.JOIN)} JOIN(s) but max_join_depth={allowed.max_join_depth}.",
                feature="max_join_depth",
            )

    def validate_window_functions(self, plan: QueryPlan) -> None:
        """Raise if any SELECT item uses OVER without window_functions enabled.

        Raises:
            DialectViolationError: If window functions are disabled.
        """
        if not plan.SELECT:
            return
        if not self._ctx.dialect.allowed.allow_window_functions:
            for item in plan.SELECT:
                if item.over is not None:
                    raise DialectViolationError(
                        "Window functions (OVER) are not enabled.",
                        feature="allow_window_functions",
                    )
