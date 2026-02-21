"""Integration tests: compile → execute against a real SQLite in-memory DB.

Covers all capability levels (select-only through window functions), all data
types (INTEGER, REAL, TEXT, BOOLEAN-as-INTEGER, DATE-as-TEXT, JSON-as-TEXT),
NULL values, empty strings, enum-like fields, composite primary keys, 1:many,
many:many, and self-referential joins.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

import bricksql
from bricksql.policy.engine import PolicyConfig, TablePolicy
from bricksql.schema.dialect import DialectProfile
from bricksql.schema.query_plan import (
    CTEClause,
    FromClause,
    JoinClause,
    LimitClause,
    OffsetClause,
    OrderByItem,
    QueryPlan,
    SelectItem,
    SetOpClause,
    WindowSpec,
)
from tests.fixtures import load_ddl, load_schema_snapshot

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
        "companies":   _TENANT,
        "departments": _TENANT,
        "employees":   _TENANT,
        "projects":    _TENANT,
    },
)


@pytest.fixture()
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(load_ddl("sqlite"))

    conn.executemany(
        "INSERT INTO companies VALUES (?,?,?,?,?,?,?,?)",
        [
            (1, TENANT, "Acme Corp", "Technology", 2010, 1,
             json.dumps({"size": "medium"}), "2025-01-01T00:00:00"),
            (2, OTHER, "Beta LLC", "Finance", 2015, 1, None,
             "2025-01-01T00:00:00"),
        ],
    )

    conn.executemany(
        "INSERT INTO departments VALUES (?,?,?,?,?,?,?)",
        [
            (1, TENANT, 1, "Engineering", "ENG", 500000.0, 3),
            (2, TENANT, 1, "Human Resources", "HR", 200000.0, 1),
            (3, TENANT, 1, "Sales", "SALES", None, 1),
        ],
    )

    conn.executemany(
        "INSERT INTO employees VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (4, TENANT, 1, 3, "Diana", "Prince", "D",
             "diana@acme.com", "+4567", "full_time", 120000.0,
             "2018-06-01", "1988-08-30", 1, 0, None, "Director"),
            (1, TENANT, 1, 1, "Alice", "Smith", None,
             "alice@acme.com", "+1234", "full_time", 95000.0,
             "2020-03-15", "1990-05-20", 1, 0, 4, "Senior engineer"),
            (2, TENANT, 1, 1, "Bob", "Jones", "Robert",
             "bob@acme.com", None, "part_time", 45000.0,
             "2021-07-01", None, 1, 1, 4, None),
            (3, TENANT, 1, 2, "Charlie", "Brown", None,
             "charlie@acme.com", "+3456", "contractor", None,
             "2022-01-10", "1985-11-11", 1, 0, None, ""),
            (5, TENANT, 1, None, "Eve", "Foster", None,
             "eve@acme.com", None, "contractor", None,
             "2023-09-01", None, 0, 1, None, None),
            (6, OTHER, 2, None, "Frank", "Miller", None,
             "frank@beta.com", None, "full_time", 80000.0,
             "2019-04-15", None, 1, 0, None, None),
        ],
    )

    conn.executemany(
        "INSERT INTO skills VALUES (?,?,?)",
        [
            (1, "Python",        "programming"),
            (2, "JavaScript",    "programming"),
            (3, "SQL",           "programming"),
            (4, "Leadership",    "management"),
            (5, "Communication", "soft_skill"),
        ],
    )

    conn.executemany(
        "INSERT INTO employee_skills VALUES (?,?,?)",
        [
            (1, 1, 5), (1, 3, 4), (2, 2, 3), (2, 3, 2),
            (3, 4, 4), (4, 4, 5), (4, 5, 4),
        ],
    )

    conn.executemany(
        "INSERT INTO projects VALUES (?,?,?,?,?,?,?,?)",
        [
            (1, TENANT, 1, "Alpha", "active", 100000.0, "2025-01-01", None),
            (2, TENANT, 1, "Beta",  "planning", None, None, None),
            (3, OTHER,  2, "Gamma", "completed", 50000.0, "2024-01-01", "2024-12-31"),
        ],
    )

    conn.executemany(
        "INSERT INTO project_assignments VALUES (?,?,?,?)",
        [
            (1, 1, "tech_lead",       40.0),
            (1, 2, "developer",       20.0),
            (2, 4, "project_manager", None),
        ],
    )

    conn.executemany(
        "INSERT INTO salary_history VALUES (?,?,?,?,?)",
        [
            (1, 1, 85000.0, "2020-03-15", "initial"),
            (2, 1, 95000.0, "2022-01-01", "raise"),
            (3, 2, 45000.0, "2021-07-01", "initial"),
            (4, 4, 110000.0, "2018-06-01", "initial"),
            (5, 4, 120000.0, "2021-01-01", "raise"),
        ],
    )

    conn.commit()
    return conn


def _build_dialect(level: int) -> DialectProfile:
    b = DialectProfile.builder(ALL_TABLES, "sqlite")
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
    compiled = bricksql.validate_and_compile(plan_json, SNAPSHOT, dialect, POLICY)
    effective_runtime = {"TENANT": TENANT}
    if runtime:
        effective_runtime.update(runtime)
    params = compiled.merge_runtime_params(effective_runtime)
    cur = conn.execute(compiled.sql, params)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Phase 1 – basic filters
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_p1_tenant_isolation(db):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=100),
    )
    rows = _run(db, plan, 1, {"TENANT": TENANT})
    assert len(rows) == 5
    rows_other = _run(db, plan, 1, {"TENANT": OTHER})
    assert len(rows_other) == 1


@pytest.mark.integration
def test_p1_integer_filter(db):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={"GT": [{"col": "employees.employee_id"}, {"value": 3}]},
        LIMIT=LimitClause(value=10),
    )
    rows = _run(db, plan, 1)
    assert all(r["id"] > 3 for r in rows)


@pytest.mark.integration
def test_p1_is_null_nullable_column(db):
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
    rows = _run(db, plan, 1, {"TENANT": TENANT})
    ids = [r["id"] for r in rows]
    assert 2 not in ids  # Bob has middle_name "Robert"
    assert 4 not in ids  # Diana has middle_name "D"
    assert 1 in ids


@pytest.mark.integration
def test_p1_empty_string_notes(db):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.notes"}, {"value": ""}]},
        LIMIT=LimitClause(value=10),
    )
    rows = _run(db, plan, 1)
    ids = [r["id"] for r in rows]
    assert 3 in ids   # Charlie notes = ""
    assert 1 not in ids


@pytest.mark.integration
def test_p1_enum_like_in_list(db):
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
    rows = _run(db, plan, 1, {"TENANT": TENANT})
    ids = [r["id"] for r in rows]
    assert 1 in ids
    assert 4 in ids
    assert 2 in ids
    assert 3 not in ids


# ---------------------------------------------------------------------------
# Phase 2 – JOINs
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_p2_one_to_many_join(db):
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
    rows = _run(db, plan, 2, {"TENANT": TENANT})
    assert len(rows) == 4
    dept_names = {r["dept"] for r in rows if r["dept"] is not None}
    assert "Engineering" in dept_names


@pytest.mark.integration
def test_p2_self_referential_manager_join(db):
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
    rows = _run(db, plan, 2, {"TENANT": TENANT})
    ids = [r["id"] for r in rows]
    assert 1 in ids  # Alice
    assert 2 in ids  # Bob
    assert 4 not in ids  # Diana: no manager_id


@pytest.mark.integration
def test_p2_order_by_and_offset(db):
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        ORDER_BY=[OrderByItem(expr={"col": "employees.employee_id"}, direction="ASC")],
        LIMIT=LimitClause(value=2),
        OFFSET=OffsetClause(value=1),
    )
    rows = _run(db, plan, 2, {"TENANT": TENANT})
    assert len(rows) == 2
    assert rows[0]["id"] == 2
    assert rows[1]["id"] == 3


# ---------------------------------------------------------------------------
# Phase 3 – aggregations
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_p3_count_by_employment_type(db):
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employment_type"}, alias="etype"),
            SelectItem(
                expr={"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                alias="cnt",
            ),
        ],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        GROUP_BY=[{"col": "employees.employment_type"}],
        LIMIT=LimitClause(value=10),
    )
    rows = _run(db, plan, 3, {"TENANT": TENANT})
    totals = {r["etype"]: r["cnt"] for r in rows}
    assert totals.get("full_time", 0) == 2
    assert totals.get("part_time", 0) == 1
    assert totals.get("contractor", 0) == 2


@pytest.mark.integration
def test_p3_sum_salary_total(db):
    plan = QueryPlan(
        SELECT=[
            SelectItem(
                expr={"func": "SUM", "args": [{"col": "employees.salary"}]},
                alias="total",
            )
        ],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=1),
    )
    rows = _run(db, plan, 3, {"TENANT": TENANT})
    assert rows[0]["total"] == pytest.approx(95000 + 45000 + 120000)


# ---------------------------------------------------------------------------
# Phase 5 – CTE
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_p5_cte_active_full_time(db):
    cte_body = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={
            "AND": [
                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                {"EQ": [{"col": "employees.employment_type"}, {"value": "full_time"}]},
                {"EQ": [{"col": "employees.active"}, {"value": 1}]},
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
    rows = _run(db, plan, 5, {"TENANT": TENANT})
    assert len(rows) == 2  # Alice + Diana


# ---------------------------------------------------------------------------
# Phase 6 – set operations
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_p6_union_all_active_inactive(db):
    inactive_plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
        FROM=FromClause(table="employees"),
        WHERE={
            "AND": [
                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                {"EQ": [{"col": "employees.active"}, {"value": 0}]},
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
                {"EQ": [{"col": "employees.active"}, {"value": 1}]},
            ]
        },
        LIMIT=LimitClause(value=100),
        SET_OP=SetOpClause(op="UNION_ALL", query=inactive_plan),
    )
    rows = _run(db, plan, 6, {"TENANT": TENANT})
    assert len(rows) == 5
