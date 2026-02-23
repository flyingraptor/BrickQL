"""Utilities for building a SchemaSnapshot from external sources.

SQLAlchemy converter
--------------------
:func:`schema_from_sqlalchemy` reflects a live database engine and returns a
:class:`~brickql.schema.snapshot.SchemaSnapshot`.

Install the optional dependency before using this module::

    pip install "brickql[sqlalchemy]"

Example::

    from sqlalchemy import create_engine
    from brickql.schema.converters import schema_from_sqlalchemy

    engine = create_engine("sqlite:///mydb.db")
    snapshot = schema_from_sqlalchemy(engine)
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from brickql.schema.snapshot import ColumnInfo, RelationshipInfo, SchemaSnapshot, TableInfo

if TYPE_CHECKING:
    from sqlalchemy import Engine, MetaData


def schema_from_sqlalchemy(
    engine: Engine,
    *,
    include_tables: list[str] | None = None,
    schema: str | None = None,
) -> SchemaSnapshot:
    """Build a :class:`SchemaSnapshot` by reflecting a SQLAlchemy engine.

    All tables visible to the engine (or a subset via *include_tables*) are
    reflected using SQLAlchemy's :class:`~sqlalchemy.schema.MetaData`.  Each
    table's columns and foreign-key constraints are translated into BrickQL's
    schema model.

    **Relationship key convention**

    Keys follow the ``{referenced_table}__{referencing_table}`` pattern
    (parent table first), which mirrors the hand-authored BrickQL convention.
    Two situations require disambiguation with the FK column name
    (``{referenced_table}__{referencing_table}__{fk_col}``):

    * **Self-referential FK** — the table references itself (e.g.
      ``employees.manager_id → employees.employee_id``).
    * **Multiple FKs to the same parent** — a table has two or more FK
      columns pointing to the same parent table.

    Each table's ``relationships`` list contains every key in which that
    table participates (either as the referencing or the referenced side).

    For databases that omit ``FOREIGN KEY`` declarations, relationships must
    be added manually to the returned snapshot (or its JSON representation)
    before use.

    Args:
        engine: A connected :class:`sqlalchemy.engine.Engine` instance.
        include_tables: Optional allowlist of table names to reflect.
            When ``None`` all tables in the schema are reflected.
        schema: Optional database schema name (e.g. ``"public"`` for
            PostgreSQL).  Passed directly to
            :meth:`sqlalchemy.schema.MetaData.reflect`.

    Returns:
        A fully populated :class:`SchemaSnapshot`.

    Raises:
        ImportError: If ``sqlalchemy`` is not installed.

    Example::

        from sqlalchemy import create_engine
        from brickql.schema.converters import schema_from_sqlalchemy

        engine = create_engine("postgresql+psycopg://user:pw@host/db")
        snapshot = schema_from_sqlalchemy(engine)
    """
    try:
        from sqlalchemy import MetaData as _MetaData
    except ImportError as exc:
        raise ImportError(
            "SQLAlchemy is required for schema_from_sqlalchemy(). "
            'Install it with: pip install "brickql[sqlalchemy]"'
        ) from exc

    metadata = _MetaData()
    with engine.connect() as conn:
        metadata.reflect(bind=conn, only=include_tables, schema=schema)

    return _metadata_to_snapshot(metadata)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _metadata_to_snapshot(metadata: MetaData) -> SchemaSnapshot:
    """Convert a reflected :class:`~sqlalchemy.schema.MetaData` into a
    :class:`SchemaSnapshot`.

    Separated from :func:`schema_from_sqlalchemy` so it can be reused by
    other converters that already hold a reflected ``MetaData`` object
    (e.g. a LangChain ``SQLDatabase._metadata``).
    """
    # Pre-scan: count (referencing_table, referenced_table) pairs so we can
    # detect when disambiguation via the FK column name is required.
    fk_pair_count: dict[tuple[str, str], int] = defaultdict(int)
    for table in metadata.sorted_tables:
        for fk in table.foreign_keys:
            fk_pair_count[(table.name, fk.column.table.name)] += 1

    # Collect all relationships and a per-table index of participating keys.
    all_relationships: list[RelationshipInfo] = []
    table_rel_keys: dict[str, list[str]] = {t.name: [] for t in metadata.sorted_tables}

    for table in metadata.sorted_tables:
        for fk in table.foreign_keys:
            from_table = table.name
            from_col = fk.parent.name
            to_table = fk.column.table.name
            to_col = fk.column.name

            key = _rel_key(from_table, from_col, to_table, fk_pair_count)

            all_relationships.append(
                RelationshipInfo(
                    key=key,
                    from_table=from_table,
                    from_col=from_col,
                    to_table=to_table,
                    to_col=to_col,
                )
            )

            # Register on both participating tables so callers can look up
            # relationships from either side, matching schema.json behaviour.
            table_rel_keys[from_table].append(key)
            if to_table != from_table and to_table in table_rel_keys:
                table_rel_keys[to_table].append(key)

    tables = [
        TableInfo(
            name=table.name,
            columns=[
                ColumnInfo(
                    name=col.name,
                    type=str(col.type),
                    # col.nullable is True/False for reflected columns; treat
                    # an unset value (None) as nullable for safety.
                    nullable=col.nullable is not False,
                )
                for col in table.columns
            ],
            relationships=table_rel_keys.get(table.name, []),
        )
        for table in metadata.sorted_tables
    ]

    return SchemaSnapshot(tables=tables, relationships=all_relationships)


def _rel_key(
    from_table: str,
    from_col: str,
    to_table: str,
    fk_pair_count: dict[tuple[str, str], int],
) -> str:
    """Return the BrickQL relationship key for a single foreign key.

    Uses the short ``{to_table}__{from_table}`` form when unambiguous, and
    the longer ``{to_table}__{from_table}__{from_col}`` form when:

    * the FK is self-referential (``from_table == to_table``), or
    * there are multiple FK columns in *from_table* pointing to *to_table*.
    """
    ambiguous = from_table == to_table or fk_pair_count.get((from_table, to_table), 0) > 1
    if ambiguous:
        return f"{to_table}__{from_table}__{from_col}"
    return f"{to_table}__{from_table}"
