"""Schema existence validator.

Checks that every table and column referenced in the plan actually exists
in the ``SchemaSnapshot``, and that JOINs use valid relationship keys.
"""

from __future__ import annotations

from brickql.errors import InvalidJoinRelError, SchemaError, ValidationError
from brickql.schema.context import ValidationContext
from brickql.schema.query_plan import QueryPlan


class SchemaValidator:
    """Validates table and column existence against the schema snapshot.

    Args:
        ctx: Validation context (snapshot + dialect).
    """

    def __init__(self, ctx: ValidationContext, cte_names: frozenset[str]) -> None:
        self._ctx = ctx
        self._cte_names = cte_names

    @property
    def cte_names(self) -> frozenset[str]:
        return self._cte_names

    @cte_names.setter
    def cte_names(self, value: frozenset[str]) -> None:
        self._cte_names = value

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def assert_table_allowed(self, table_name: str) -> None:
        """Raise :class:`~brickql.errors.SchemaError` if table not found.

        Args:
            table_name: Table to look up.

        Raises:
            SchemaError: If the table is neither in the snapshot nor a CTE name.
        """
        if table_name in self._cte_names:
            return
        if self._ctx.snapshot.get_table(table_name) is None:
            raise SchemaError(
                f"Table '{table_name}' does not exist in the schema snapshot.",
                details={
                    "table": table_name,
                    "allowed_tables": self._ctx.snapshot.table_names,
                },
            )

    def validate_from(self, plan: QueryPlan) -> None:
        """Validate the FROM clause table existence."""
        if plan.FROM is None:
            return
        frm = plan.FROM
        if frm.table is not None:
            self.assert_table_allowed(frm.table)
        elif frm.subquery is None:
            raise ValidationError(
                "FROM clause must specify either 'table' or 'subquery'.",
                code="SCHEMA_ERROR",
            )

    def validate_joins(self, plan: QueryPlan) -> None:
        """Validate that JOIN relationship keys exist and their tables are known."""
        if not plan.JOIN:
            return
        for join in plan.JOIN:
            rel = self._ctx.snapshot.get_relationship(join.rel)
            if rel is None:
                raise InvalidJoinRelError(
                    join.rel,
                    self._ctx.snapshot.relationship_keys,
                )
            self.assert_table_allowed(rel.from_table)
            self.assert_table_allowed(rel.to_table)
