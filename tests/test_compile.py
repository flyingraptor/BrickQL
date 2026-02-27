"""Unit tests for QueryBuilder (both dialects)."""

from __future__ import annotations

from brickql.compile.builder import QueryBuilder
from brickql.compile.mysql import MySQLCompiler
from brickql.compile.postgres import PostgresCompiler
from brickql.compile.sqlite import SQLiteCompiler
from brickql.schema.column_reference import ColumnReference
from brickql.schema.query_plan import (
    CTEClause,
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


def _my() -> QueryBuilder:
    return QueryBuilder(MySQLCompiler(), SNAPSHOT)


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


def test_ilike_mysql_maps_to_like():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"ILIKE": [{"col": "employees.last_name"}, {"value": "%smith%"}]},
        LIMIT=LimitClause(value=10),
    )
    my_r = _my().build(plan)
    assert "LIKE" in my_r.sql
    assert "ILIKE" not in my_r.sql


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
                    order_by=[OrderByItem(expr={"col": "employees.hire_date"}, direction="ASC")],
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


# ---------------------------------------------------------------------------
# ColumnReference.parse - bare-column bug regression
# ---------------------------------------------------------------------------


class TestColumnReferenceParse:
    def test_bare_column_parses_correctly(self):
        ref = ColumnReference.parse("employee_id")
        assert ref.table is None
        assert ref.column == "employee_id"

    def test_qualified_column_parses_correctly(self):
        ref = ColumnReference.parse("employees.employee_id")
        assert ref.table == "employees"
        assert ref.column == "employee_id"

    def test_column_with_underscore_no_dot(self):
        ref = ColumnReference.parse("first_name")
        assert ref.table is None
        assert ref.column == "first_name"


# ---------------------------------------------------------------------------
# build_func_call hook - default behaviour and DATE_PART specialisation
# ---------------------------------------------------------------------------


class TestBuildFuncCall:
    """Tests for the SQLCompiler.build_func_call dispatch hook."""

    def test_default_func_renders_with_commas(self):
        """Non-special functions compile to FUNC(arg1, arg2) on both dialects."""
        plan = QueryPlan(
            SELECT=[
                SelectItem(
                    expr={"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    alias="cnt",
                )
            ],
            FROM=FromClause(table="employees"),
        )
        pg_r = _pg().build(plan)
        sq_r = _sq().build(plan)
        assert 'COUNT("employees"."employee_id") AS "cnt"' in pg_r.sql
        assert 'COUNT("employees"."employee_id") AS "cnt"' in sq_r.sql

    def test_date_part_field_inlined_as_literal(self):
        """DATE_PART first arg must not become a bound parameter on Postgres."""
        plan = QueryPlan(
            SELECT=[
                SelectItem(
                    expr={
                        "func": "DATE_PART",
                        "args": [{"value": "year"}, {"col": "employees.hire_date"}],
                    },
                    alias="yr",
                )
            ],
            FROM=FromClause(table="employees"),
        )
        r = _pg().build(plan)
        # Field name is an inline SQL string literal, NOT a bound param.
        assert "DATE_PART('year'" in r.sql
        assert "param_0" not in r.params
        assert r.params == {}

    def test_date_part_source_gets_timestamp_cast(self):
        """DATE_PART second arg gets a ::TIMESTAMP cast in Postgres."""
        plan = QueryPlan(
            SELECT=[
                SelectItem(
                    expr={
                        "func": "DATE_PART",
                        "args": [{"value": "month"}, {"col": "employees.hire_date"}],
                    },
                    alias="mo",
                )
            ],
            FROM=FromClause(table="employees"),
        )
        r = _pg().build(plan)
        assert '"employees"."hire_date"::TIMESTAMP' in r.sql

    def test_date_part_in_group_by_via_cte(self):
        """DATE_PART compiles correctly inside a CTE's GROUP BY."""
        plan = QueryPlan(
            CTE=[
                CTEClause(
                    name="by_year",
                    query=QueryPlan(
                        SELECT=[
                            SelectItem(expr={"col": "employees.department_id"}),
                            SelectItem(
                                expr={
                                    "func": "DATE_PART",
                                    "args": [
                                        {"value": "year"},
                                        {"col": "employees.hire_date"},
                                    ],
                                },
                                alias="yr",
                            ),
                            SelectItem(
                                expr={
                                    "func": "COUNT",
                                    "args": [{"col": "employees.employee_id"}],
                                },
                                alias="cnt",
                            ),
                        ],
                        FROM=FromClause(table="employees"),
                        GROUP_BY=[
                            {"col": "employees.department_id"},
                            {
                                "func": "DATE_PART",
                                "args": [
                                    {"value": "year"},
                                    {"col": "employees.hire_date"},
                                ],
                            },
                        ],
                    ),
                )
            ],
            SELECT=[
                SelectItem(expr={"col": "by_year.department_id"}),
                SelectItem(expr={"col": "by_year.yr"}),
                SelectItem(expr={"col": "by_year.cnt"}),
            ],
            FROM=FromClause(table="by_year"),
        )
        r = _pg().build(plan)
        assert "WITH" in r.sql
        assert "DATE_PART('year'" in r.sql
        assert "::TIMESTAMP" in r.sql
        assert "GROUP BY" in r.sql
        assert r.params == {}

    def test_date_part_sqlite_uses_default_rendering(self):
        """On SQLite DATE_PART falls through to the default rendering (no cast)."""
        plan = QueryPlan(
            SELECT=[
                SelectItem(
                    expr={
                        "func": "DATE_PART",
                        "args": [{"value": "year"}, {"col": "employees.hire_date"}],
                    },
                    alias="yr",
                )
            ],
            FROM=FromClause(table="employees"),
        )
        r = _sq().build(plan)
        # Default rendering: value becomes a param, no ::TIMESTAMP cast.
        assert "DATE_PART(%(param_0)s" not in r.sql  # SQLite uses :name style
        assert "::TIMESTAMP" not in r.sql
        assert "DATE_PART" in r.sql


# ---------------------------------------------------------------------------
# MySQL dialect - identifier quoting, parameter style, and DATE_PART→EXTRACT
# ---------------------------------------------------------------------------


class TestMySQLDialect:
    """Tests specific to MySQLCompiler behaviour."""

    def test_backtick_quoting(self):
        """MySQL identifiers are quoted with backticks."""
        plan = QueryPlan(
            SELECT=[
                SelectItem(expr={"col": "employees.employee_id"}, alias="id"),
                SelectItem(expr={"col": "employees.first_name"}, alias="fname"),
            ],
            FROM=FromClause(table="employees"),
            LIMIT=LimitClause(value=20),
        )
        r = _my().build(plan)
        assert "`employees`" in r.sql
        assert "`employee_id`" in r.sql
        assert '"employees"' not in r.sql

    def test_param_placeholder_is_percent_style(self):
        """MySQL uses %(name)s placeholders, same as PostgreSQL."""
        plan = QueryPlan(
            SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
            FROM=FromClause(table="employees"),
            WHERE={"EQ": [{"col": "employees.employee_id"}, {"value": 42}]},
            LIMIT=LimitClause(value=1),
        )
        r = _my().build(plan)
        assert "%(param_0)s" in r.sql
        assert r.params["param_0"] == 42

    def test_runtime_param_placeholder(self):
        """MySQL runtime params use %(name)s style."""
        plan = QueryPlan(
            SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
            FROM=FromClause(table="employees"),
            WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            LIMIT=LimitClause(value=10),
        )
        r = _my().build(plan)
        assert "%(TENANT)s" in r.sql
        assert "TENANT" not in r.params

    def test_date_part_translates_to_extract(self):
        """MySQL translates DATE_PART(field, col) to EXTRACT(UNIT FROM col)."""
        plan = QueryPlan(
            SELECT=[
                SelectItem(
                    expr={
                        "func": "DATE_PART",
                        "args": [{"value": "year"}, {"col": "employees.hire_date"}],
                    },
                    alias="yr",
                )
            ],
            FROM=FromClause(table="employees"),
        )
        r = _my().build(plan)
        assert "EXTRACT(YEAR FROM" in r.sql
        assert "DATE_PART" not in r.sql
        assert "::TIMESTAMP" not in r.sql
        assert r.params == {}

    def test_date_part_month_extract(self):
        """DATE_PART with 'month' translates to EXTRACT(MONTH FROM ...)."""
        plan = QueryPlan(
            SELECT=[
                SelectItem(
                    expr={
                        "func": "DATE_PART",
                        "args": [{"value": "month"}, {"col": "employees.hire_date"}],
                    },
                    alias="mo",
                )
            ],
            FROM=FromClause(table="employees"),
        )
        r = _my().build(plan)
        assert "EXTRACT(MONTH FROM" in r.sql

    def test_alias_quoted_with_backticks(self):
        """Column aliases are also quoted with backticks in MySQL."""
        plan = QueryPlan(
            SELECT=[SelectItem(expr={"col": "employees.employee_id"}, alias="id")],
            FROM=FromClause(table="employees"),
            LIMIT=LimitClause(value=5),
        )
        r = _my().build(plan)
        assert '`id`' in r.sql

    def test_dialect_name(self):
        assert MySQLCompiler().dialect_name == "mysql"
