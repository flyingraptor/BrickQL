"""Unit tests for brickql.schema.converters.schema_from_sqlalchemy."""

from __future__ import annotations

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine

from brickql.schema.converters import (
    _metadata_to_snapshot,
    infer_relationships_from_names,
    schema_from_sqlalchemy,
)
from brickql.schema.snapshot import SchemaSnapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine() -> Engine:
    """Return an in-memory SQLite engine."""
    return create_engine("sqlite:///:memory:")


def _simple_schema(engine: Engine) -> None:
    """Create a two-table schema: departments → companies (FK)."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE companies (
                    company_id INTEGER PRIMARY KEY,
                    name       TEXT    NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE departments (
                    department_id INTEGER PRIMARY KEY,
                    company_id    INTEGER NOT NULL,
                    name          TEXT    NOT NULL,
                    FOREIGN KEY (company_id) REFERENCES companies(company_id)
                )
                """
            )
        )


def _self_ref_schema(engine: Engine) -> None:
    """Create a single self-referential table: employees.manager_id → employees."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE employees (
                    employee_id INTEGER PRIMARY KEY,
                    name        TEXT    NOT NULL,
                    manager_id  INTEGER,
                    FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
                )
                """
            )
        )


def _multi_fk_schema(engine: Engine) -> None:
    """Create a schema where a table has two FKs pointing to the same parent."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE users (
                    user_id INTEGER PRIMARY KEY,
                    name    TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE messages (
                    message_id  INTEGER PRIMARY KEY,
                    sender_id   INTEGER NOT NULL,
                    receiver_id INTEGER NOT NULL,
                    FOREIGN KEY (sender_id)   REFERENCES users(user_id),
                    FOREIGN KEY (receiver_id) REFERENCES users(user_id)
                )
                """
            )
        )


def _three_table_chain(engine: Engine) -> None:
    """Create a three-table chain used by fixture-parity tests."""
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT NOT NULL)"))
        conn.execute(
            text(
                "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER NOT NULL, "
                "FOREIGN KEY (a_id) REFERENCES a(id))"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE c (id INTEGER PRIMARY KEY, b_id INTEGER NOT NULL, "
                "FOREIGN KEY (b_id) REFERENCES b(id))"
            )
        )


# ---------------------------------------------------------------------------
# Column reflection
# ---------------------------------------------------------------------------


class TestColumnReflection:
    def test_column_names(self) -> None:
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        dept = snapshot.get_table("departments")
        assert dept is not None
        assert dept.column_names == ["department_id", "company_id", "name"]

    def test_column_types_are_strings(self) -> None:
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        dept = snapshot.get_table("departments")
        assert dept is not None
        types = {c.name: c.type for c in dept.columns}
        # SQLite reflects INTEGER and TEXT; just check they are non-empty strings.
        assert all(isinstance(t, str) and t for t in types.values())

    def test_not_null_columns_are_not_nullable(self) -> None:
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        dept = snapshot.get_table("departments")
        assert dept is not None
        col_map = {c.name: c for c in dept.columns}
        assert col_map["company_id"].nullable is False
        assert col_map["name"].nullable is False

    def test_nullable_columns(self) -> None:
        engine = _make_engine()
        _self_ref_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        emps = snapshot.get_table("employees")
        assert emps is not None
        col_map = {c.name: c for c in emps.columns}
        assert col_map["manager_id"].nullable is True

    def test_table_names(self) -> None:
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)
        assert set(snapshot.table_names) == {"companies", "departments"}


# ---------------------------------------------------------------------------
# Relationship key convention: {to_table}__{from_table}
# ---------------------------------------------------------------------------


class TestRelationshipKeys:
    def test_simple_fk_key_convention(self) -> None:
        """FK on departments → companies produces key 'companies__departments'."""
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        assert len(snapshot.relationships) == 1
        rel = snapshot.relationships[0]
        assert rel.key == "companies__departments"
        assert rel.from_table == "departments"
        assert rel.from_col == "company_id"
        assert rel.to_table == "companies"
        assert rel.to_col == "company_id"

    def test_relationship_appears_on_both_tables(self) -> None:
        """The relationship key is listed in both tables' relationships."""
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        key = "companies__departments"
        assert key in (snapshot.get_table("departments") or object()).relationships  # type: ignore[union-attr]
        assert key in (snapshot.get_table("companies") or object()).relationships  # type: ignore[union-attr]

    def test_three_table_chain_produces_two_relationships(self) -> None:
        engine = _make_engine()
        _three_table_chain(engine)
        snapshot = schema_from_sqlalchemy(engine)

        assert len(snapshot.relationships) == 2
        keys = snapshot.relationship_keys
        assert "a__b" in keys
        assert "b__c" in keys


# ---------------------------------------------------------------------------
# Self-referential FK disambiguation
# ---------------------------------------------------------------------------


class TestSelfReferentialFK:
    def test_self_ref_key_includes_fk_col(self) -> None:
        """Self-ref FK is disambiguated: '{table}__{table}__{fk_col}'."""
        engine = _make_engine()
        _self_ref_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        assert len(snapshot.relationships) == 1
        rel = snapshot.relationships[0]
        assert rel.key == "employees__employees__manager_id"
        assert rel.from_table == "employees"
        assert rel.from_col == "manager_id"
        assert rel.to_table == "employees"
        assert rel.to_col == "employee_id"

    def test_self_ref_key_in_table_relationships(self) -> None:
        engine = _make_engine()
        _self_ref_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        emps = snapshot.get_table("employees")
        assert emps is not None
        assert "employees__employees__manager_id" in emps.relationships


# ---------------------------------------------------------------------------
# Multiple FKs to same parent — disambiguation
# ---------------------------------------------------------------------------


class TestMultiFKDisambiguation:
    def test_two_fks_to_same_parent_are_disambiguated(self) -> None:
        """Two FKs from messages to users produce distinct, column-qualified keys."""
        engine = _make_engine()
        _multi_fk_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        assert len(snapshot.relationships) == 2
        keys = snapshot.relationship_keys
        assert "users__messages__sender_id" in keys
        assert "users__messages__receiver_id" in keys

    def test_disambiguated_keys_in_messages_relationships(self) -> None:
        engine = _make_engine()
        _multi_fk_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        msgs = snapshot.get_table("messages")
        assert msgs is not None
        assert "users__messages__sender_id" in msgs.relationships
        assert "users__messages__receiver_id" in msgs.relationships

    def test_disambiguated_keys_in_parent_relationships(self) -> None:
        """Both keys also appear on the parent (users) side."""
        engine = _make_engine()
        _multi_fk_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        users = snapshot.get_table("users")
        assert users is not None
        assert "users__messages__sender_id" in users.relationships
        assert "users__messages__receiver_id" in users.relationships


# ---------------------------------------------------------------------------
# include_tables filtering
# ---------------------------------------------------------------------------


class TestIncludeTablesFilter:
    def test_only_requested_tables_are_reflected(self) -> None:
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine, include_tables=["companies"])

        assert snapshot.table_names == ["companies"]

    def test_fk_relationship_excluded_when_child_not_included(self) -> None:
        """If departments is not included, its FK to companies is not reflected."""
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine, include_tables=["companies"])

        assert snapshot.relationships == []

    def test_all_tables_included_when_no_filter(self) -> None:
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)

        assert set(snapshot.table_names) == {"companies", "departments"}


# ---------------------------------------------------------------------------
# Column type strings — VARCHAR and other non-affinity types
# ---------------------------------------------------------------------------


class TestColumnTypeStrings:
    """Verify that SQLAlchemy type objects are faithfully serialised as strings.

    SQLite uses type affinity, so VARCHAR/CHAR are stored as TEXT internally,
    but SQLAlchemy preserves the declared type in the reflected metadata.
    On PostgreSQL or MySQL the reflected type string would be 'VARCHAR(n)'.
    We create columns programmatically (not via raw DDL) so that SQLAlchemy
    stores the exact type object we pass in, giving us a dialect-independent
    way to assert the string representation.
    """

    def _snapshot_from_metadata(self, *col_defs: Column) -> SchemaSnapshot:  # type: ignore[type-arg]
        """Build a snapshot from a programmatically defined MetaData table."""
        from sqlalchemy import Table

        metadata = MetaData()
        Table("items", metadata, *col_defs)
        return _metadata_to_snapshot(metadata)

    def test_varchar_column_type_string(self) -> None:
        snapshot = self._snapshot_from_metadata(
            Column("id", Integer, primary_key=True),
            Column("code", String(50), nullable=False),
        )
        col = snapshot.get_column("items", "code")
        assert col is not None
        assert "VARCHAR" in col.type.upper() or "CHAR" in col.type.upper()

    def test_boolean_column_type_string(self) -> None:
        snapshot = self._snapshot_from_metadata(
            Column("id", Integer, primary_key=True),
            Column("active", Boolean, nullable=False),
        )
        col = snapshot.get_column("items", "active")
        assert col is not None
        assert col.type  # non-empty string

    def test_date_column_type_string(self) -> None:
        snapshot = self._snapshot_from_metadata(
            Column("id", Integer, primary_key=True),
            Column("created", Date, nullable=True),
        )
        col = snapshot.get_column("items", "created")
        assert col is not None
        assert "DATE" in col.type.upper()

    def test_text_column_type_string(self) -> None:
        snapshot = self._snapshot_from_metadata(
            Column("id", Integer, primary_key=True),
            Column("notes", Text, nullable=True),
        )
        col = snapshot.get_column("items", "notes")
        assert col is not None
        assert "TEXT" in col.type.upper()


# ---------------------------------------------------------------------------
# Return type and round-trip sanity
# ---------------------------------------------------------------------------


class TestReturnType:
    def test_returns_schema_snapshot(self) -> None:
        engine = _make_engine()
        _simple_schema(engine)
        assert isinstance(schema_from_sqlalchemy(engine), SchemaSnapshot)

    def test_empty_schema_returns_empty_snapshot(self) -> None:
        engine = _make_engine()
        snapshot = schema_from_sqlalchemy(engine)
        assert snapshot.tables == []
        assert snapshot.relationships == []

    def test_snapshot_serialises_to_json(self) -> None:
        """SchemaSnapshot.model_dump() should round-trip without error."""
        engine = _make_engine()
        _simple_schema(engine)
        snapshot = schema_from_sqlalchemy(engine)
        data = snapshot.model_dump()
        restored = SchemaSnapshot.model_validate(data)
        assert restored.table_names == snapshot.table_names
        assert restored.relationship_keys == snapshot.relationship_keys


# ---------------------------------------------------------------------------
# infer_relationships_from_names
# ---------------------------------------------------------------------------


class TestInferRelationshipsFromNames:
    """Tests for the naming-convention heuristic used when a database has no
    FK constraints (common with CSV-sourced or legacy schemas)."""

    def _snapshot_no_rels(self, *table_dicts: dict) -> SchemaSnapshot:  # type: ignore[type-arg]
        """Build a SchemaSnapshot from raw dicts without any relationships."""
        return SchemaSnapshot.model_validate({"tables": list(table_dicts), "relationships": []})

    def _col(self, name: str, nullable: bool = True) -> dict:  # type: ignore[type-arg]
        return {"name": name, "type": "TEXT", "nullable": nullable}

    # ------------------------------------------------------------------
    # Basic inference
    # ------------------------------------------------------------------

    def test_infers_simple_fk_via_plural(self) -> None:
        """defects.defect_id → defects.id  (prefix 'defect' + 's' = 'defects')."""
        snapshot = self._snapshot_no_rels(
            {"name": "defects",         "columns": [self._col("id"), self._col("name")]},
            {"name": "defect_comments", "columns": [self._col("defect_id"), self._col("content")]},
        )
        enriched = infer_relationships_from_names(snapshot)

        assert len(enriched.relationships) == 1
        rel = enriched.relationships[0]
        assert rel.key == "defects__defect_comments"
        assert rel.from_table == "defect_comments"
        assert rel.from_col == "defect_id"
        assert rel.to_table == "defects"
        assert rel.to_col == "id"

    def test_infers_exact_table_name_match(self) -> None:
        """order.order_id → order.id  (prefix 'order' matches exactly)."""
        snapshot = self._snapshot_no_rels(
            {"name": "order",      "columns": [self._col("id"), self._col("total")]},
            {"name": "order_item", "columns": [self._col("order_id"), self._col("qty")]},
        )
        enriched = infer_relationships_from_names(snapshot)

        assert len(enriched.relationships) == 1
        assert enriched.relationships[0].to_table == "order"

    def test_infers_chained_relationships(self) -> None:
        """defect_details_id → defect_details.id, and defect_id → defects.id."""
        snapshot = self._snapshot_no_rels(
            {"name": "defects",        "columns": [self._col("id")]},
            {"name": "defect_details", "columns": [self._col("id"), self._col("defect_id")]},
            {
                "name": "defect_details_installation_running_hours",
                "columns": [self._col("defect_details_id"), self._col("running_hours")],
            },
        )
        enriched = infer_relationships_from_names(snapshot)

        keys = enriched.relationship_keys
        assert "defects__defect_details" in keys
        assert "defect_details__defect_details_installation_running_hours" in keys

    # ------------------------------------------------------------------
    # Key appears on both participating tables
    # ------------------------------------------------------------------

    def test_inferred_key_on_both_tables(self) -> None:
        snapshot = self._snapshot_no_rels(
            {"name": "defects",         "columns": [self._col("id")]},
            {"name": "defect_comments", "columns": [self._col("defect_id")]},
        )
        enriched = infer_relationships_from_names(snapshot)

        key = "defects__defect_comments"
        assert key in (enriched.get_table("defect_comments") or object()).relationships  # type: ignore[union-attr]
        assert key in (enriched.get_table("defects") or object()).relationships  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # Columns that don't match any table are silently skipped
    # ------------------------------------------------------------------

    def test_unresolved_id_columns_are_skipped(self) -> None:
        """vessel_id has no 'vessel' or 'vessels' table — must be ignored."""
        snapshot = self._snapshot_no_rels(
            {"name": "defects", "columns": [self._col("id"), self._col("vessel_id")]},
        )
        enriched = infer_relationships_from_names(snapshot)
        assert enriched.relationships == []

    def test_candidate_without_id_column_is_skipped(self) -> None:
        """A matching table that has no 'id' column must not produce a relationship."""
        snapshot = self._snapshot_no_rels(
            {"name": "vessels",  "columns": [self._col("vessel_code")]},  # no 'id'
            {"name": "defects",  "columns": [self._col("id"), self._col("vessel_id")]},
        )
        enriched = infer_relationships_from_names(snapshot)
        assert enriched.relationships == []

    # ------------------------------------------------------------------
    # Idempotency — existing relationships are not duplicated
    # ------------------------------------------------------------------

    def test_existing_relationships_not_duplicated(self) -> None:
        """Calling infer on a snapshot that already has the relationship is a no-op."""
        snapshot = self._snapshot_no_rels(
            {"name": "defects",         "columns": [self._col("id")]},
            {"name": "defect_comments", "columns": [self._col("defect_id")]},
        )
        once = infer_relationships_from_names(snapshot)
        twice = infer_relationships_from_names(once)
        assert len(twice.relationships) == 1

    # ------------------------------------------------------------------
    # No-op on snapshot with no _id columns
    # ------------------------------------------------------------------

    def test_no_id_columns_returns_same_structure(self) -> None:
        snapshot = self._snapshot_no_rels(
            {"name": "companies", "columns": [self._col("name"), self._col("industry")]},
        )
        enriched = infer_relationships_from_names(snapshot)
        assert enriched.relationships == []
        assert enriched.tables[0].relationships == []

    # ------------------------------------------------------------------
    # schema_from_sqlalchemy infer_relationships=True integration
    # ------------------------------------------------------------------

    def test_flag_on_schema_from_sqlalchemy(self) -> None:
        """infer_relationships=True produces relationships on a no-FK SQLite DB."""
        engine = _make_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.execute(
                text(
                    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, "
                    "item_id INTEGER, qty INTEGER)"
                )
            )
        snapshot = schema_from_sqlalchemy(engine, infer_relationships=True)
        assert "items__orders" in snapshot.relationship_keys

    def test_flag_false_produces_no_relationships_when_no_fks(self) -> None:
        """infer_relationships=False (default) leaves relationships empty when no FKs."""
        engine = _make_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.execute(
                text(
                    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, "
                    "item_id INTEGER, qty INTEGER)"
                )
            )
        snapshot = schema_from_sqlalchemy(engine, infer_relationships=False)
        assert snapshot.relationships == []

    # ------------------------------------------------------------------
    # Defects domain — matches the hand-authored schema.json
    # ------------------------------------------------------------------

    def test_defects_domain_infers_all_seven_relationships(self) -> None:
        """Reproduce the defects schema without FK constraints and verify all
        seven relationships from preprocess_config.yaml are inferred."""
        snapshot = self._snapshot_no_rels(
            {"name": "defects",        "columns": [self._col("id"), self._col("vessel_id"), self._col("name")]},
            {"name": "defect_details", "columns": [self._col("id"), self._col("defect_id")]},
            {"name": "defect_comments","columns": [self._col("defect_id"), self._col("content")]},
            {
                "name": "defect_details_installation_running_hours",
                "columns": [self._col("defect_details_id"), self._col("running_hours")],
            },
            {"name": "defect_equipment_details",   "columns": [self._col("defect_id")]},
            {"name": "defect_linked_events",       "columns": [self._col("defect_id")]},
            {"name": "defect_repair_requirements", "columns": [self._col("defect_id")]},
            {"name": "defect_work_permits",        "columns": [self._col("defect_id")]},
        )
        enriched = infer_relationships_from_names(snapshot)
        keys = set(enriched.relationship_keys)

        assert "defects__defect_details" in keys
        assert "defects__defect_comments" in keys
        assert "defect_details__defect_details_installation_running_hours" in keys
        assert "defects__defect_equipment_details" in keys
        assert "defects__defect_linked_events" in keys
        assert "defects__defect_repair_requirements" in keys
        assert "defects__defect_work_permits" in keys
        # vessel_id has no matching table → should NOT produce a relationship
        assert not any("vessel" in k for k in keys)


# ---------------------------------------------------------------------------
# Fixture-parity: reflect the canonical SQLite DDL and cross-check structure
# ---------------------------------------------------------------------------


class TestFixtureParity:
    """Reflect the same DDL used in tests/fixtures/ddl_sqlite.sql and verify
    the structural shape matches schema.json (ignoring minor type differences
    between SQLite's type affinity and the hand-authored JSON types)."""

    @pytest.fixture()
    def snapshot(self) -> SchemaSnapshot:
        from tests.fixtures import load_ddl

        engine = _make_engine()
        with engine.begin() as conn:
            # SQLite doesn't support IF NOT EXISTS inside executescript via
            # SQLAlchemy text(), so we strip the guard before executing.
            ddl = load_ddl("sqlite").replace("IF NOT EXISTS ", "")
            for statement in ddl.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
        return schema_from_sqlalchemy(engine)

    def test_all_tables_present(self, snapshot: SchemaSnapshot) -> None:
        expected = {
            "companies",
            "departments",
            "employees",
            "skills",
            "employee_skills",
            "projects",
            "project_assignments",
            "salary_history",
        }
        assert set(snapshot.table_names) == expected

    def test_relationship_count(self, snapshot: SchemaSnapshot) -> None:
        # The DDL defines 10 FK constraints across all tables.
        assert len(snapshot.relationships) == 10

    def test_self_ref_employees_manager(self, snapshot: SchemaSnapshot) -> None:
        """employees.manager_id → employees.employee_id must be detected."""
        keys = snapshot.relationship_keys
        assert "employees__employees__manager_id" in keys

    def test_simple_fk_departments_companies(self, snapshot: SchemaSnapshot) -> None:
        keys = snapshot.relationship_keys
        assert "companies__departments" in keys

    def test_every_relationship_has_valid_tables(self, snapshot: SchemaSnapshot) -> None:
        for rel in snapshot.relationships:
            assert snapshot.get_table(rel.from_table) is not None, (
                f"from_table '{rel.from_table}' not found for key '{rel.key}'"
            )
            assert snapshot.get_table(rel.to_table) is not None, (
                f"to_table '{rel.to_table}' not found for key '{rel.key}'"
            )

    def test_every_relationship_key_in_table_relationships(
        self, snapshot: SchemaSnapshot
    ) -> None:
        for rel in snapshot.relationships:
            from_table = snapshot.get_table(rel.from_table)
            assert from_table is not None
            assert rel.key in from_table.relationships, (
                f"key '{rel.key}' missing from {rel.from_table}.relationships"
            )
