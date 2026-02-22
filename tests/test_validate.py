"""Unit tests for PlanValidator."""
from __future__ import annotations

import pytest

from brickql.errors import (
    DialectViolationError,
    InvalidJoinRelError,
    ProfileConfigError,
    SchemaError,
    ValidationError,
)
from brickql.schema.dialect import DialectProfile
from brickql.schema.query_plan import (
    CTEClause,
    FromClause,
    JoinClause,
    LimitClause,
    OrderByItem,
    QueryPlan,
    SelectItem,
    SetOpClause,
    WindowSpec,
)
from brickql.validate.validator import PlanValidator
from tests.fixtures import load_schema_snapshot

SNAPSHOT = load_schema_snapshot()
ALL_TABLES = [
    "companies", "departments", "employees",
    "skills", "employee_skills",
    "projects", "project_assignments", "salary_history",
]


def _profile(level: int) -> DialectProfile:
    b = DialectProfile.builder(ALL_TABLES)
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


def _v(profile: int = 1) -> PlanValidator:
    return PlanValidator(SNAPSHOT, _profile(profile))


def test_valid_single_table():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_unknown_table_raises():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "ghost.id"})],
        FROM=FromClause(table="ghost"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(SchemaError):
        _v().validate(plan)


def test_unknown_column_raises():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.nonexistent_col"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(SchemaError) as exc_info:
        _v().validate(plan)
    assert "nonexistent_col" in str(exc_info.value)


def test_invalid_operand_key_raises():
    # With typed Operand, Pydantic rejects unknown discriminator keys at
    # model construction time.  The error may surface as a pydantic
    # ValidationError (at build time) or a brickql ValidationError (if
    # validation is run on an already-constructed plan with a bad operand).
    import pydantic

    with pytest.raises((ValidationError, pydantic.ValidationError)):
        plan = QueryPlan(
            SELECT=[SelectItem(expr={"bad_key": "something"})],
            FROM=FromClause(table="employees"),
            LIMIT=LimitClause(value=10),
        )
        _v().validate(plan)


def test_is_null_on_nullable_column():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"IS_NULL": {"col": "employees.middle_name"}},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_is_not_null_on_nullable_column():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"IS_NOT_NULL": {"col": "employees.salary"}},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_is_null_on_nullable_date():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"IS_NULL": {"col": "employees.birth_date"}},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_is_null_on_self_referential_manager():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"IS_NULL": {"col": "employees.manager_id"}},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_integer_comparison():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"GT": [{"col": "employees.employee_id"}, {"value": 0}]},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_real_comparison():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"GTE": [{"col": "employees.salary"}, {"value": 50000.0}]},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_boolean_column():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.remote"}, {"value": True}]},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_date_column_between():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={
            "BETWEEN": [
                {"col": "employees.hire_date"},
                {"value": "2020-01-01"},
                {"value": "2023-12-31"},
            ]
        },
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_enum_like_text_column():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"IN": [
            {"col": "employees.employment_type"},
            {"value": "full_time"},
            {"value": "part_time"},
        ]},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_like_on_text():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"LIKE": [{"col": "employees.email"}, {"value": "%@acme.com"}]},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_empty_string_value():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.notes"}, {"value": ""}]},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_limit_exceeds_max_raises():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=9999),
    )
    with pytest.raises(DialectViolationError) as exc_info:
        _v().validate(plan)
    assert exc_info.value.details["feature"] == "max_limit"


def test_negative_limit_raises():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=-1),
    )
    with pytest.raises(ValidationError):
        _v().validate(plan)


def test_join_blocked_in_phase1():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        JOIN=[JoinClause(rel="departments__employees", type="LEFT")],
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DialectViolationError):
        _v(profile=1).validate(plan)


def test_one_to_many_join_phase2():
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employee_id"}),
            SelectItem(expr={"col": "departments.name"}),
        ],
        FROM=FromClause(table="employees"),
        JOIN=[JoinClause(rel="departments__employees", type="LEFT")],
        LIMIT=LimitClause(value=10),
    )
    _v(profile=2).validate(plan)


def test_self_referential_join_phase2():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        JOIN=[JoinClause(rel="employees__manager", type="LEFT", alias="mgr")],
        WHERE={"IS_NOT_NULL": {"col": "employees.manager_id"}},
        LIMIT=LimitClause(value=10),
    )
    _v(profile=2).validate(plan)


def test_invalid_join_rel_raises():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        JOIN=[JoinClause(rel="no_such_rel", type="INNER")],
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(InvalidJoinRelError) as exc_info:
        _v(profile=2).validate(plan)
    assert "no_such_rel" in exc_info.value.details["rel"]


def test_join_depth_exceeded():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        JOIN=[
            JoinClause(rel="departments__employees", type="LEFT"),
            JoinClause(rel="companies__departments", type="LEFT"),
            JoinClause(rel="employees__salary_history", type="LEFT"),
        ],
        LIMIT=LimitClause(value=10),
    )
    dialect = DialectProfile.builder(ALL_TABLES).joins(max_join_depth=2).build()
    with pytest.raises(DialectViolationError):
        PlanValidator(SNAPSHOT, dialect).validate(plan)


def test_group_by_with_count():
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employment_type"}),
            SelectItem(
                expr={"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                alias="cnt",
            ),
        ],
        FROM=FromClause(table="employees"),
        GROUP_BY=[{"col": "employees.employment_type"}],
        LIMIT=LimitClause(value=10),
    )
    _v(profile=3).validate(plan)


def test_aggregate_function_not_allowed_in_phase1():
    plan = QueryPlan(
        SELECT=[
            SelectItem(
                expr={"func": "SUM", "args": [{"col": "employees.salary"}]},
                alias="total",
            )
        ],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DialectViolationError):
        _v(profile=1).validate(plan)


def test_having_requires_group_by():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        HAVING={"GT": [
            {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
            {"value": 1},
        ]},
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(ValidationError):
        _v(profile=3).validate(plan)


def test_cte_blocked_below_phase5():
    inner = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=100),
    )
    plan = QueryPlan(
        CTE=[CTEClause(name="emp_cte", query=inner)],
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DialectViolationError) as exc_info:
        _v(profile=4).validate(plan)
    assert "allow_cte" in exc_info.value.details["feature"]


def test_cte_allowed_in_phase5():
    inner = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=100),
    )
    plan = QueryPlan(
        CTE=[CTEClause(name="emp_cte", query=inner)],
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    _v(profile=5).validate(plan)


def test_set_op_blocked_below_phase6():
    right = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
        SET_OP=SetOpClause(op="UNION", query=right),
    )
    with pytest.raises(DialectViolationError):
        _v(profile=5).validate(plan)


def test_window_function_blocked_below_phase7():
    plan = QueryPlan(
        SELECT=[
            SelectItem(
                expr={"func": "ROW_NUMBER", "args": []},
                over=WindowSpec(),
            )
        ],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DialectViolationError):
        _v(profile=6).validate(plan)


def test_builder_window_functions_requires_aggregations():
    with pytest.raises(ProfileConfigError) as exc_info:
        DialectProfile.builder(ALL_TABLES).window_functions().build()
    assert "aggregations" in exc_info.value.missing
    assert "window_functions" in str(exc_info.value)


def test_builder_ctes_requires_subqueries():
    with pytest.raises(ProfileConfigError) as exc_info:
        DialectProfile.builder(ALL_TABLES).aggregations().ctes().build()
    assert "subqueries" in exc_info.value.missing
    assert "ctes" in str(exc_info.value)


def test_builder_empty_tables_raises():
    with pytest.raises(ProfileConfigError) as exc_info:
        DialectProfile.builder([]).build()
    assert "tables" in exc_info.value.missing


def test_builder_valid_window_with_aggregations():
    profile = (
        DialectProfile.builder(ALL_TABLES)
        .aggregations()
        .window_functions()
        .build()
    )
    assert profile.allowed.allow_window_functions is True


def test_builder_valid_ctes_with_subqueries():
    profile = (
        DialectProfile.builder(ALL_TABLES)
        .subqueries()
        .ctes()
        .build()
    )
    assert profile.allowed.allow_cte is True


def test_builder_features_are_independent():
    profile = (
        DialectProfile.builder(ALL_TABLES)
        .joins()
        .set_operations()
        .build()
    )
    assert profile.allowed.allow_set_operations is True
    assert not profile.allowed.allow_cte
    assert not profile.allowed.allow_window_functions


def test_not_predicate():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"NOT": {"IS_NULL": {"col": "employees.salary"}}},
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)


def test_deeply_nested_and_or():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={
            "AND": [
                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                {
                    "OR": [
                        {"EQ": [{"col": "employees.employment_type"}, {"value": "full_time"}]},
                        {
                            "AND": [
                                {"EQ": [{"col": "employees.employment_type"}, {"value": "contractor"}]},
                                {"IS_NOT_NULL": {"col": "employees.salary"}},
                            ]
                        },
                    ]
                },
            ]
        },
        LIMIT=LimitClause(value=10),
    )
    _v().validate(plan)
