"""Shared setup for BrinkQL examples.

Loads the schema snapshot and provides helpers for building standard
dialect profiles and policies used across example cases.
"""
from __future__ import annotations

import json
from pathlib import Path

from brinkql import DialectProfile, PolicyConfig, SchemaSnapshot, TablePolicy

_FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"

ALL_TABLES: list[str] = [
    "companies",
    "departments",
    "employees",
    "skills",
    "employee_skills",
    "projects",
    "project_assignments",
    "salary_history",
]

#: Tables that carry a tenant_id column requiring TENANT isolation.
TENANT_TABLES: list[str] = [
    "companies",
    "departments",
    "employees",
    "projects",
]

_snapshot: SchemaSnapshot | None = None


def load_snapshot() -> SchemaSnapshot:
    """Load and cache the shared schema snapshot."""
    global _snapshot
    if _snapshot is None:
        _snapshot = SchemaSnapshot.model_validate(
            json.loads((_FIXTURES_DIR / "schema.json").read_text())
        )
    return _snapshot


def standard_policy(default_limit: int = 50) -> PolicyConfig:
    """Return a PolicyConfig with TENANT isolation on all tenant-owning tables.

    Uses ``inject_missing_params=True`` so the PolicyEngine will automatically
    inject tenant_id predicates even if the LLM forgets them.

    Args:
        default_limit: LIMIT value to enforce when the plan omits one.
    """
    return PolicyConfig(
        inject_missing_params=True,
        default_limit=default_limit,
        tables={
            t: TablePolicy(param_bound_columns={"tenant_id": "TENANT"})
            for t in TENANT_TABLES
        },
    )


def full_dialect(
    target: str = "sqlite",
    max_limit: int = 100,
) -> DialectProfile:
    """All features enabled — useful for complex / weird cases."""
    return (
        DialectProfile.builder(ALL_TABLES, target=target, max_limit=max_limit)
        .joins(max_join_depth=3)
        .aggregations()
        .subqueries()
        .ctes()
        .set_operations()
        .window_functions()
        .build()
    )


def select_dialect(
    tables: list[str] | None = None,
    target: str = "sqlite",
    max_limit: int = 100,
) -> DialectProfile:
    """Basic single-table SELECT / WHERE / LIMIT only."""
    return DialectProfile.builder(
        tables or ALL_TABLES, target=target, max_limit=max_limit
    ).build()


def joins_dialect(
    tables: list[str] | None = None,
    target: str = "sqlite",
    max_limit: int = 100,
    max_join_depth: int = 3,
) -> DialectProfile:
    """Joins + ORDER BY / OFFSET / ILIKE."""
    return (
        DialectProfile.builder(
            tables or ALL_TABLES, target=target, max_limit=max_limit
        )
        .joins(max_join_depth=max_join_depth)
        .build()
    )


def agg_dialect(
    tables: list[str] | None = None,
    target: str = "sqlite",
    max_limit: int = 100,
) -> DialectProfile:
    """Joins + aggregations (GROUP BY / HAVING / COUNT / SUM / AVG …)."""
    return (
        DialectProfile.builder(
            tables or ALL_TABLES, target=target, max_limit=max_limit
        )
        .joins(max_join_depth=3)
        .aggregations()
        .build()
    )


def subquery_dialect(
    tables: list[str] | None = None,
    target: str = "sqlite",
    max_limit: int = 100,
) -> DialectProfile:
    """Joins + aggregations + subqueries."""
    return (
        DialectProfile.builder(
            tables or ALL_TABLES, target=target, max_limit=max_limit
        )
        .joins(max_join_depth=3)
        .aggregations()
        .subqueries()
        .build()
    )


def cte_dialect(
    tables: list[str] | None = None,
    target: str = "sqlite",
    max_limit: int = 100,
) -> DialectProfile:
    """Joins + aggregations + subqueries + CTEs."""
    return (
        DialectProfile.builder(
            tables or ALL_TABLES, target=target, max_limit=max_limit
        )
        .joins(max_join_depth=3)
        .aggregations()
        .subqueries()
        .ctes()
        .build()
    )


def window_dialect(
    tables: list[str] | None = None,
    target: str = "sqlite",
    max_limit: int = 100,
) -> DialectProfile:
    """Joins + aggregations + window functions."""
    return (
        DialectProfile.builder(
            tables or ALL_TABLES, target=target, max_limit=max_limit
        )
        .joins(max_join_depth=3)
        .aggregations()
        .window_functions()
        .build()
    )
