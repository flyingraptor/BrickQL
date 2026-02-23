"""Integration tests: compile → execute against a real PostgreSQL instance.

Uses brickQL_PG_DSN (e.g. from Makefile when running make test-integration-postgres).
Skips all tests if the env var is unset or connection fails.
Mirrors the SQLite integration tests for the same capability levels.
"""
from __future__ import annotations

import json
import os

import pytest

try:
    import psycopg
except ImportError:
    psycopg = None  # type: ignore[assignment]

import brickql
from brickql.errors import DisallowedColumnError
from brickql.policy.engine import PolicyConfig, TablePolicy
from brickql.schema.dialect import DialectProfile
from brickql.schema.query_plan import (
    CTEClause,
    FromClause,
    JoinClause,
    LimitClause,
    OffsetClause,
    OrderByItem,
    QueryPlan,
    SelectItem,
    SetOpClause,
)
from tests.fixtures import load_ddl, load_schema_snapshot

pytest.importorskip("psycopg", reason="psycopg required for Postgres integration tests")

SNAPSHOT = load_schema_snapshot()
ALL_TABLES = [
    "companies", "departments", "employees",
    "skills", "employee_skills",
    "projects", "project_assignments", "salary_history",
]
TENANT = "tenant_acme"
OTHER = "other_corp"

_TENANT = TablePolicy(param_bound_columns={"tenant_id": "TENANT"})
POLICY = PolicyConfig(
    inject_missing_params=True,
    default_limit=0,
    tables={
        "companies": _TENANT,
        "departments": _TENANT,
        "employees": _TENANT,
        "projects": _TENANT,
    },
)


def _get_pg_connection():
    dsn = os.environ.get("brickQL_PG_DSN")
    if not dsn:
        pytest.skip("brickQL_PG_DSN not set")
    try:
        return psycopg.connect(dsn)
    except Exception as e:
        pytest.skip(f"Cannot connect to Postgres: {e}")


@pytest.fixture(scope="module")
def pg_conn():
    """Module-scoped Postgres connection with schema and seed data."""
    conn = _get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(load_ddl("postgres"))

        cur.executemany(
            """INSERT INTO companies (company_id, tenant_id, name, industry, founded_year, active, metadata, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s::timestamptz)
               ON CONFLICT (company_id) DO NOTHING""",
            [
                (1, TENANT, "Acme Corp", "Technology", 2010, True, json.dumps({"size": "medium"}), "2025-01-01T00:00:00"),
                (2, OTHER, "Beta LLC", "Finance", 2015, True, None, "2025-01-01T00:00:00"),
            ],
        )
        cur.executemany(
            """INSERT INTO departments (department_id, tenant_id, company_id, name, code, budget, headcount)
               VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (department_id) DO NOTHING""",
            [
                (1, TENANT, 1, "Engineering", "ENG", 500000.0, 3),
                (2, TENANT, 1, "Human Resources", "HR", 200000.0, 1),
                (3, TENANT, 1, "Sales", "SALES", None, 1),
            ],
        )
        cur.executemany(
            """INSERT INTO employees (employee_id, tenant_id, company_id, department_id, first_name, last_name, middle_name,
               email, phone, employment_type, salary, hire_date, birth_date, active, remote, manager_id, notes)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (employee_id) DO NOTHING""",
            [
                (4, TENANT, 1, 3, "Diana", "Prince", "D", "diana@acme.com", "+4567", "full_time", 120000.0, "2018-06-01", "1988-08-30", True, False, None, "Director"),
                (1, TENANT, 1, 1, "Alice", "Smith", None, "alice@acme.com", "+1234", "full_time", 95000.0, "2020-03-15", "1990-05-20", True, False, 4, "Senior engineer"),
                (2, TENANT, 1, 1, "Bob", "Jones", "Robert", "bob@acme.com", None, "part_time", 45000.0, "2021-07-01", None, True, True, 4, None),
                (3, TENANT, 1, 2, "Charlie", "Brown", None, "charlie@acme.com", "+3456", "contractor", None, "2022-01-10", "1985-11-11", True, False, None, ""),
                (5, TENANT, 1, None, "Eve", "Foster", None, "eve@acme.com", None, "contractor", None, "2023-09-01", None, False, True, None, None),
                (6, OTHER, 2, None, "Frank", "Miller", None, "frank@beta.com", None, "full_time", 80000.0, "2019-04-15", None, True, False, None, None),
            ],
        )
        cur.executemany(
            """INSERT INTO skills (skill_id, name, category) VALUES (%s, %s, %s) ON CONFLICT (skill_id) DO NOTHING""",
            [(1, "Python", "programming"), (2, "JavaScript", "programming"), (3, "SQL", "programming"),
            (4, "Leadership", "management"), (5, "Communication", "soft_skill")],
        )
        cur.executemany(
            """INSERT INTO employee_skills (employee_id, skill_id, proficiency) VALUES (%s, %s, %s)
               ON CONFLICT (employee_id, skill_id) DO NOTHING""",
            [(1, 1, 5), (1, 3, 4), (2, 2, 3), (2, 3, 2), (3, 4, 4), (4, 4, 5), (4, 5, 4)],
        )
        cur.executemany(
            """INSERT INTO projects (project_id, tenant_id, company_id, name, status, budget, start_date, end_date)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (project_id) DO NOTHING""",
            [
                (1, TENANT, 1, "Alpha", "active", 100000.0, "2025-01-01", None),
                (2, TENANT, 1, "Beta", "planning", None, None, None),
                (3, OTHER, 2, "Gamma", "completed", 50000.0, "2024-01-01", "2024-12-31"),
            ],
        )
        cur.executemany(
            """INSERT INTO project_assignments (project_id, employee_id, role, hours_per_week)
               VALUES (%s, %s, %s, %s) ON CONFLICT (project_id, employee_id) DO NOTHING""",
            [(1, 1, "tech_lead", 40.0), (1, 2, "developer", 20.0), (2, 4, "project_manager", None)],
        )
        cur.executemany(
            """INSERT INTO salary_history (employee_id, salary, effective_date, reason) VALUES (%s, %s, %s, %s)""",
            [(1, 85000.0, "2020-03-15", "initial"), (2, 95000.0, "2022-01-01", "raise"), (3, 45000.0, "2021-07-01", "initial"),
             (4, 110000.0, "2018-06-01", "initial"), (4, 120000.0, "2021-01-01", "raise")],
        )
    conn.commit()
    yield conn
    conn.close()


def _build_dialect(level: int) -> DialectProfile:
    b = DialectProfile.builder(ALL_TABLES, "postgres")
    if level >= 2:
        b = b.joins()
    if level >= 3:
        b = b.aggregations()
    if level >= 4:
        b = b.subqueries()
    if level >= 5:
        b = b.ctes()
    if level >= 6:
        b = b.set_operations()
    if level >= 7:
        b = b.window_functions()
    return b.build()


def _run(conn, plan: QueryPlan, level: int, runtime: dict | None = None) -> list:
    plan_json = plan.model_dump_json(exclude_none=True)
    dialect = _build_dialect(level)
    compiled = brickql.validate_and_compile(plan_json, SNAPSHOT, dialect, POLICY)
    effective_runtime = {"TENANT": TENANT}
    if runtime:
        effective_runtime.update(runtime)
    params = compiled.merge_runtime_params(effective_runtime)
    with conn.cursor() as cur:
        cur.execute(compiled.sql, params)
        columns = [d.name for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def _run_with_policy(
    conn,
    plan: QueryPlan,
    level: int,
    policy: PolicyConfig,
    runtime: dict | None = None,
) -> list:
    plan_json = plan.model_dump_json(exclude_none=True)
    dialect = _build_dialect(level)
    compiled = brickql.validate_and_compile(plan_json, SNAPSHOT, dialect, policy)
    params = compiled.merge_runtime_params(runtime or {})
    with conn.cursor() as cur:
        cur.execute(compiled.sql, params)
        columns = [d.name for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Phase 1 – basic filters
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.postgres
def test_p1_tenant_isolation(pg_conn):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=100),
    )
    rows = _run(pg_conn, plan, 1, {"TENANT": TENANT})
    assert len(rows) == 5
    rows_other = _run(pg_conn, plan, 1, {"TENANT": OTHER})
    assert len(rows_other) == 1


@pytest.mark.integration
@pytest.mark.postgres
def test_p1_integer_filter(pg_conn):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={"GT": [{"col": "employees.employee_id"}, {"value": 3}]},
        LIMIT=LimitClause(value=10),
    )
    rows = _run(pg_conn, plan, 1)
    assert all(r["id"] > 3 for r in rows)


@pytest.mark.integration
@pytest.mark.postgres
def test_p1_is_null_nullable_column(pg_conn):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={
            "AND": [
                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                {"IS_NULL": {"col": "employees.middle_name"}},
            ]
        },
        LIMIT=LimitClause(value=10),
    )
    rows = _run(pg_conn, plan, 1, {"TENANT": TENANT})
    ids = [r["id"] for r in rows]
    assert 2 not in ids
    assert 4 not in ids
    assert 1 in ids


@pytest.mark.integration
@pytest.mark.postgres
def test_p1_empty_string_notes(pg_conn):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.notes"}, {"value": ""}]},
        LIMIT=LimitClause(value=10),
    )
    rows = _run(pg_conn, plan, 1)
    ids = [r["id"] for r in rows]
    assert 3 in ids
    assert 1 not in ids


@pytest.mark.integration
@pytest.mark.postgres
def test_p1_enum_like_in_list(pg_conn):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={
            "AND": [
                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                {"IN": [
                    {"col": "employees.employment_type"},
                    {"value": "full_time"},
                    {"value": "part_time"},
                ]},
            ]
        },
        LIMIT=LimitClause(value=10),
    )
    rows = _run(pg_conn, plan, 1, {"TENANT": TENANT})
    ids = [r["id"] for r in rows]
    assert 1 in ids and 4 in ids and 2 in ids and 3 not in ids


# ---------------------------------------------------------------------------
# Phase 2 – JOINs
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.postgres
def test_p2_one_to_many_join(pg_conn):
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employee_id"}, alias="id"),
            SelectItem(expr={"col": "departments.name"}, alias="dept"),
        ],
        FROM=FromClause(table="employees"),
        JOIN=[JoinClause(rel="departments__employees", type="LEFT")],
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        ORDER_BY=[OrderByItem(expr={"col": "employees.employee_id"}, direction="ASC")],
        LIMIT=LimitClause(value=10),
    )
    rows = _run(pg_conn, plan, 2, {"TENANT": TENANT})
    assert len(rows) == 4
    dept_names = {r["dept"] for r in rows if r["dept"] is not None}
    assert "Engineering" in dept_names


@pytest.mark.integration
@pytest.mark.postgres
def test_p2_self_referential_manager_join(pg_conn):
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employee_id"}, alias="id"),
            SelectItem(expr={"col": "employees.manager_id"}, alias="mgr_id"),
        ],
        FROM=FromClause(table="employees"),
        JOIN=[JoinClause(rel="employees__manager", type="INNER", alias="mgr")],
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=10),
    )
    rows = _run(pg_conn, plan, 2, {"TENANT": TENANT})
    ids = [r["id"] for r in rows]
    assert 1 in ids and 2 in ids and 4 not in ids


@pytest.mark.integration
@pytest.mark.postgres
def test_p2_order_by_and_offset(pg_conn):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        ORDER_BY=[OrderByItem(expr={"col": "employees.employee_id"}, direction="ASC")],
        LIMIT=LimitClause(value=2),
        OFFSET=OffsetClause(value=1),
    )
    rows = _run(pg_conn, plan, 2, {"TENANT": TENANT})
    assert len(rows) == 2 and rows[0]["id"] == 2 and rows[1]["id"] == 3


# ---------------------------------------------------------------------------
# Phase 3 – aggregations
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.postgres
def test_p3_count_by_employment_type(pg_conn):
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employment_type"}, alias="etype"),
            SelectItem(expr={"func": "COUNT", "args": [{"col": "employees.employee_id"}]}, alias="cnt"),
        ],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        GROUP_BY=[{"col": "employees.employment_type"}],
        LIMIT=LimitClause(value=10),
    )
    rows = _run(pg_conn, plan, 3, {"TENANT": TENANT})
    totals = {r["etype"]: r["cnt"] for r in rows}
    assert totals.get("full_time", 0) == 2 and totals.get("part_time", 0) == 1 and totals.get("contractor", 0) == 2


@pytest.mark.integration
@pytest.mark.postgres
def test_p3_sum_salary_total(pg_conn):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"func": "SUM", "args": [{"col": "employees.salary"}]}, alias="total")],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=1),
    )
    rows = _run(pg_conn, plan, 3, {"TENANT": TENANT})
    assert rows[0]["total"] == pytest.approx(95000 + 45000 + 120000)


# ---------------------------------------------------------------------------
# Phase 5 – CTE
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.postgres
def test_p5_cte_active_full_time(pg_conn):
    cte_body = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={
            "AND": [
                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                {"EQ": [{"col": "employees.employment_type"}, {"value": "full_time"}]},
                {"EQ": [{"col": "employees.active"}, {"value": True}]},
            ]
        },
        LIMIT=LimitClause(value=200),
    )
    plan = QueryPlan(
        CTE=[CTEClause(name="ft_active", query=cte_body)],
        SELECT=[SelectItem(expr={"col": "ft_active.id"})],
        FROM=FromClause(table="ft_active"),
        LIMIT=LimitClause(value=10),
    )
    rows = _run(pg_conn, plan, 5, {"TENANT": TENANT})
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# Phase 6 – set operations
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.postgres
def test_p6_union_all_active_inactive(pg_conn):
    inactive_plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={
            "AND": [
                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                {"EQ": [{"col": "employees.active"}, {"value": False}]},
            ]
        },
        LIMIT=LimitClause(value=100),
    )
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={
            "AND": [
                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                {"EQ": [{"col": "employees.active"}, {"value": True}]},
            ]
        },
        LIMIT=LimitClause(value=100),
        SET_OP=SetOpClause(op="UNION_ALL", query=inactive_plan),
    )
    rows = _run(pg_conn, plan, 6, {"TENANT": TENANT})
    assert len(rows) == 5


# ---------------------------------------------------------------------------
# Policy — allowed_columns (column allowlist / RBAC pattern)
# ---------------------------------------------------------------------------

_ANALYST_POLICY = PolicyConfig(
    inject_missing_params=True,
    default_limit=0,
    tables={
        "employees": TablePolicy(
            param_bound_columns={"tenant_id": "TENANT"},
            allowed_columns=[
                "employee_id", "tenant_id", "first_name", "last_name",
                "department_id", "hire_date", "active",
            ],
        ),
    },
)


@pytest.mark.integration
@pytest.mark.postgres
def test_policy_allowed_columns_permits_allowed_query(pg_conn):
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.first_name"}, alias="first"),
            SelectItem(expr={"col": "employees.last_name"}, alias="last"),
            SelectItem(expr={"col": "employees.hire_date"}, alias="hired"),
        ],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=10),
    )
    rows = _run_with_policy(pg_conn, plan, 1, _ANALYST_POLICY, {"TENANT": TENANT})
    assert len(rows) == 5
    assert all("hired" in r for r in rows)


@pytest.mark.integration
@pytest.mark.postgres
def test_policy_allowed_columns_blocks_unlisted_column(pg_conn):
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.first_name"}),
            SelectItem(expr={"col": "employees.salary"}),  # not in analyst allowlist
        ],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DisallowedColumnError) as exc_info:
        _run_with_policy(pg_conn, plan, 1, _ANALYST_POLICY, {"TENANT": TENANT})
    err = exc_info.value
    assert err.details["column"] == "salary"
    assert err.details["table"] == "employees"
    assert "salary" not in err.details["allowed_columns"]


@pytest.mark.integration
@pytest.mark.postgres
def test_policy_allowed_columns_error_details_list_allowlist(pg_conn):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.notes"})],  # not in allowlist
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DisallowedColumnError) as exc_info:
        _run_with_policy(pg_conn, plan, 1, _ANALYST_POLICY, {"TENANT": TENANT})
    allowed = set(exc_info.value.details["allowed_columns"])
    assert allowed == {"employee_id", "tenant_id", "first_name", "last_name",
                       "department_id", "hire_date", "active"}
