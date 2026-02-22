"""Shared pytest fixtures for brickQL unit and integration tests."""
from __future__ import annotations

import pytest

from brickql.schema.dialect import DialectProfile
from brickql.schema.snapshot import SchemaSnapshot
from tests.fixtures import load_schema_snapshot

ALL_TABLES = [
    "companies", "departments", "employees",
    "skills", "employee_skills",
    "projects", "project_assignments", "salary_history",
]


@pytest.fixture(scope="session")
def snapshot() -> SchemaSnapshot:
    """Canonical schema snapshot shared across all tests."""
    return load_schema_snapshot()


@pytest.fixture(scope="session")
def dialect_pg_select(snapshot: SchemaSnapshot) -> DialectProfile:
    """Single-table SELECT only."""
    return DialectProfile.builder(ALL_TABLES, "postgres").build()


@pytest.fixture(scope="session")
def dialect_sq_select(snapshot: SchemaSnapshot) -> DialectProfile:
    """Single-table SELECT only (SQLite)."""
    return DialectProfile.builder(ALL_TABLES, "sqlite").build()


@pytest.fixture(scope="session")
def dialect_pg_joins(snapshot: SchemaSnapshot) -> DialectProfile:
    return DialectProfile.builder(ALL_TABLES, "postgres").joins().build()


@pytest.fixture(scope="session")
def dialect_pg_agg(snapshot: SchemaSnapshot) -> DialectProfile:
    return DialectProfile.builder(ALL_TABLES, "postgres").joins().aggregations().build()


@pytest.fixture(scope="session")
def dialect_pg_subq(snapshot: SchemaSnapshot) -> DialectProfile:
    return (
        DialectProfile.builder(ALL_TABLES, "postgres")
        .joins()
        .aggregations()
        .subqueries()
        .build()
    )


@pytest.fixture(scope="session")
def dialect_pg_ctes(snapshot: SchemaSnapshot) -> DialectProfile:
    return (
        DialectProfile.builder(ALL_TABLES, "postgres")
        .joins()
        .aggregations()
        .subqueries()
        .ctes()
        .build()
    )


@pytest.fixture(scope="session")
def dialect_pg_setop(snapshot: SchemaSnapshot) -> DialectProfile:
    return (
        DialectProfile.builder(ALL_TABLES, "postgres")
        .joins()
        .aggregations()
        .subqueries()
        .ctes()
        .set_operations()
        .build()
    )


@pytest.fixture(scope="session")
def dialect_pg_window(snapshot: SchemaSnapshot) -> DialectProfile:
    return (
        DialectProfile.builder(ALL_TABLES, "postgres")
        .joins()
        .aggregations()
        .subqueries()
        .ctes()
        .set_operations()
        .window_functions()
        .build()
    )
