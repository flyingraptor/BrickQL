"""Pydantic models for the SchemaSnapshot passed to the LLM and validator.

The SchemaSnapshot describes the subset of the database schema that the LLM
planner is allowed to reference.  It is produced by the caller (not brickQL
itself) and injected into prompts and validation.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ColumnInfo(BaseModel):
    """Metadata for a single column.

    Attributes:
        name: Column name.
        type: SQL type string (e.g. ``'TEXT'``, ``'INTEGER'``, ``'TIMESTAMP'``).
        nullable: Whether the column can be NULL.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    nullable: bool = True


class RelationshipInfo(BaseModel):
    """A named, pre-defined JOIN relationship between two tables.

    The LLM must reference relationships by ``key``; it must NOT invent
    ad-hoc ON clauses.

    Attributes:
        key: Unique relationship identifier (e.g. ``'departments__employees'``).
        from_table: Left-hand table name.
        from_col: Join column on the left-hand table.
        to_table: Right-hand table name.
        to_col: Join column on the right-hand table.
    """

    model_config = ConfigDict(extra="forbid")

    key: str
    from_table: str
    from_col: str
    to_table: str
    to_col: str


class TableInfo(BaseModel):
    """Metadata for a single table visible to the LLM.

    Attributes:
        name: Table name.
        columns: Ordered list of column metadata.
        relationships: Relationship keys where this table is the left side.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    columns: list[ColumnInfo]
    relationships: list[str] = Field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        """Returns all column names for this table."""
        return [c.name for c in self.columns]


class SchemaSnapshot(BaseModel):
    """Describes the schema the LLM planner is allowed to use.

    The planner may only reference tables, columns, and relationship keys
    listed here.

    Attributes:
        tables: All tables visible to the LLM.
        relationships: All valid named relationships.
    """

    model_config = ConfigDict(extra="forbid")

    tables: list[TableInfo]
    relationships: list[RelationshipInfo] = Field(default_factory=list)

    def get_table(self, name: str) -> TableInfo | None:
        """Returns the TableInfo for the given table name, or ``None``."""
        for table in self.tables:
            if table.name == name:
                return table
        return None

    def get_relationship(self, key: str) -> RelationshipInfo | None:
        """Returns the RelationshipInfo for the given key, or ``None``."""
        for rel in self.relationships:
            if rel.key == key:
                return rel
        return None

    def get_column(self, table_name: str, column_name: str) -> ColumnInfo | None:
        """Returns the ColumnInfo for a table.column pair, or ``None``."""
        table = self.get_table(table_name)
        if table is None:
            return None
        for col in table.columns:
            if col.name == column_name:
                return col
        return None

    def get_column_names(self, table_name: str) -> list[str]:
        """Returns column names for ``table_name``, or ``[]`` if not found.

        Prefer this over ``get_table(name).column_names`` to satisfy the
        Law of Demeter — callers do not need to handle ``None`` themselves.

        Args:
            table_name: The table to look up.

        Returns:
            List of column name strings, empty if the table is unknown.
        """
        table = self.get_table(table_name)
        return table.column_names if table is not None else []

    @property
    def table_names(self) -> list[str]:
        """Returns all table names in the snapshot."""
        return [t.name for t in self.tables]

    @property
    def relationship_keys(self) -> list[str]:
        """Returns all relationship keys in the snapshot."""
        return [r.key for r in self.relationships]
