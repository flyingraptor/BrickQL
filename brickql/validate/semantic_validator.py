"""Semantic / business-rule validator.

Validates rules that are not purely structural (schema existence) or
purely dialect-based (feature flags) - e.g. HAVING requires GROUP BY,
LIMIT must be a positive integer within the dialect's maximum.
"""

from __future__ import annotations

from brickql.errors import DialectViolationError, ValidationError
from brickql.schema.context import ValidationContext
from brickql.schema.query_plan import QueryPlan


class SemanticValidator:
    """Validates semantic constraints on a QueryPlan.

    Args:
        ctx: Validation context (snapshot + dialect).
    """

    def __init__(self, ctx: ValidationContext) -> None:
        self._ctx = ctx

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_having(self, plan: QueryPlan) -> None:
        """Raise if HAVING appears without a GROUP BY clause.

        Raises:
            ValidationError: If HAVING is present but GROUP_BY is absent.
        """
        if plan.HAVING is not None and plan.GROUP_BY is None:
            raise ValidationError(
                "HAVING requires GROUP_BY.",
                code="SCHEMA_ERROR",
            )

    def validate_limit(self, plan: QueryPlan) -> None:
        """Raise if LIMIT value is out of range.

        Raises:
            ValidationError: If LIMIT is non-positive.
            DialectViolationError: If LIMIT exceeds the dialect's maximum.
        """
        if plan.LIMIT is None:
            return
        max_limit = self._ctx.dialect.allowed.max_limit
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
