"""Typed column-reference class.

Replaces scattered ``col.split(".", 1)`` pattern-matching with a single
object that owns both the parsing and schema-validation logic (Feature Envy
fix for ``validator.py``, ``engine.py``, and ``builder.py``).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnReference:
    """A parsed ``table.column`` or bare ``column`` reference.

    Attributes:
        table: Table qualifier, or ``None`` for unqualified references.
        column: Column name.
    """

    table: str | None
    column: str

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def parse(cls, ref: str) -> ColumnReference:
        """Parse a ``"table.column"`` or bare ``"column"`` string.

        Args:
            ref: The raw column reference string from a plan operand.

        Returns:
            A :class:`ColumnReference` instance.
        """
        if "." in ref:
            table, column = ref.split(".", 1)
            return cls(table=table, column=column)
        return cls(table=None, column=ref)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_against(
        self,
        snapshot: SchemaSnapshot,  # type: ignore[name-defined]  # noqa: F821
        cte_names: frozenset[str] = frozenset(),
    ) -> None:
        """Raise :class:`~brickql.errors.SchemaError` if invalid.

        Checks that:
        * The table (if qualified) exists in the snapshot or is a CTE name.
        * The column exists on that table.

        Args:
            snapshot: The schema snapshot to validate against.
            cte_names: Virtual table names from CTEs / derived tables that
                are allowed even though they don't appear in the snapshot.

        Raises:
            SchemaError: On table or column not found.
        """
        from brickql.errors import SchemaError  # avoid circular import

        if self.table is None:
            return  # Unqualified references are validated by the caller.

        if self.table in cte_names:
            return  # CTE columns are not in the snapshot; skip.

        col_info = snapshot.get_column(self.table, self.column)
        if col_info is None:
            allowed = snapshot.get_column_names(self.table)
            raise SchemaError(
                f"Column '{self.column}' does not exist on table '{self.table}'.",
                details={
                    "table": self.table,
                    "column": self.column,
                    "allowed_columns": allowed,
                },
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def qualified(self) -> bool:
        """True when the reference includes a table qualifier."""
        return self.table is not None

    def __str__(self) -> str:
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column
