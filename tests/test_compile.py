"""Unit tests for QueryBuilder (both dialects)."""
from __future__ import annotations

from brickql.compile.builder import QueryBuilder
from brickql.compile.postgres import PostgresCompiler
from brickql.compile.sqlite import SQLiteCompiler
from brickql.schema.query_plan import (
    FromClause,
    JoinClause,
    LimitClause,
    OrderByItem,
    QueryPlan,
    SelectItem,
    WindowSpec,
)
from tests.fixtures import load_schema_snapshot

SNAPSHOT = load_schema_snapshot()


def _pg() -> QueryBuilder:
    return QueryBuilder(PostgresCompiler(), SNAPSHOT)


def _sq() -> QueryBuilder:
    return QueryBuilder(SQLiteCompiler(), SNAPSHOT)


def test_select_columns_and_aliases():
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employee_id"}, alias="id"),
            SelectItem(expr={"col": "employees.first_name"}, alias="fname"),
        ],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=20),
    )
    r = _pg().build(plan)
    assert '"employees"."employee_id" AS "id"' in r.sql
    assert "LIMIT 20" in r.sql


def test_integer_eq_predicate():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.employee_id"}, {"value": 42}]},
        LIMIT=LimitClause(value=1),
    )
    r = _pg().build(plan)
    assert "%(param_0)s" in r.sql
    assert r.params["param_0"] == 42


def test_is_null_nullable_column():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"IS_NULL": {"col": "employees.middle_name"}},
        LIMIT=LimitClause(value=10),
    )
    r = _pg().build(plan)
    assert '"employees"."middle_name" IS NULL' in r.sql


def test_runtime_param_placeholder():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=10),
    )
    pg_r = _pg().build(plan)
    sq_r = _sq().build(plan)
    assert "%(TENANT)s" in pg_r.sql
    assert ":TENANT" in sq_r.sql
    assert "TENANT" not in pg_r.params


def test_ilike_postgres_vs_sqlite():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"ILIKE": [{"col": "employees.last_name"}, {"value": "%smith%"}]},
        LIMIT=LimitClause(value=10),
    )
    pg_r = _pg().build(plan)
    sq_r = _sq().build(plan)
    assert "ILIKE" in pg_r.sql
    assert "LIKE" in sq_r.sql
    assert "ILIKE" not in sq_r.sql


def test_one_to_many_join():
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employee_id"}),
            SelectItem(expr={"col": "departments.name"}, alias="dept_name"),
        ],
        FROM=FromClause(table="employees"),
        JOIN=[JoinClause(rel="departments__employees", type="LEFT")],
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=10),
    )
    r = _pg().build(plan)
    assert "LEFT JOIN" in r.sql
    assert '"departments"' in r.sql
    assert "ON" in r.sql


def test_self_referential_join_with_alias():
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employee_id"}),
            SelectItem(expr={"col": "employees.manager_id"}),
        ],
        FROM=FromClause(table="employees"),
        JOIN=[JoinClause(rel="employees__manager", type="LEFT", alias="mgr")],
        WHERE={"IS_NOT_NULL": {"col": "employees.manager_id"}},
        LIMIT=LimitClause(value=10),
    )
    r = _pg().build(plan)
    assert 'LEFT JOIN "employees" AS "mgr"' in r.sql
    assert '"employees"."manager_id" = "mgr"."employee_id"' in r.sql


def test_row_number_over_partition_by():
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employee_id"}),
            SelectItem(
                expr={"func": "ROW_NUMBER", "args": []},
                over=WindowSpec(
                    partition_by=[{"col": "employees.department_id"}],
                    order_by=[
                        OrderByItem(expr={"col": "employees.hire_date"}, direction="ASC")
                    ],
                ),
                alias="rn",
            ),
        ],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    r = _pg().build(plan)
    assert "ROW_NUMBER() OVER" in r.sql
    assert "PARTITION BY" in r.sql
    assert "ORDER BY" in r.sql
