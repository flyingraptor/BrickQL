"""Unit tests for PolicyEngine."""

from __future__ import annotations

import pytest

from brickql.errors import DisallowedColumnError, DisallowedTableError, MissingParamError
from brickql.policy.engine import PolicyConfig, PolicyEngine, TablePolicy
from brickql.schema.dialect import DialectProfile
from brickql.schema.query_plan import (
    FromClause,
    LimitClause,
    QueryPlan,
    SelectItem,
)
from tests.fixtures import load_schema_snapshot

SNAPSHOT = load_schema_snapshot()
ALL_TABLES = [
    "companies",
    "departments",
    "employees",
    "skills",
    "employee_skills",
    "projects",
    "project_assignments",
    "salary_history",
]
DIALECT = DialectProfile.builder(ALL_TABLES).build()

_TENANT = TablePolicy(param_bound_columns={"tenant_id": "TENANT"})
TENANT_TABLES = {
    "companies": _TENANT,
    "departments": _TENANT,
    "employees": _TENANT,
    "projects": _TENANT,
}


def _engine(
    inject: bool = True,
    denied: list[str] | None = None,
    allowed_tables: list[str] | None = None,
    default_limit: int = 0,
    tables: dict | None = None,
) -> PolicyEngine:
    config = PolicyConfig(
        inject_missing_params=inject,
        denied_columns=denied or [],
        allowed_tables=allowed_tables or [],
        default_limit=default_limit,
        tables=tables if tables is not None else TENANT_TABLES,
    )
    return PolicyEngine(config, SNAPSHOT, DIALECT)


def _emp_plan(where=None) -> QueryPlan:
    return QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE=where,
        LIMIT=LimitClause(value=10),
    )


def test_missing_tenant_injected_into_empty_where():
    plan = _emp_plan(where=None)
    result = _engine(inject=True).apply(plan)
    assert result.WHERE is not None


def test_missing_tenant_injected_alongside_existing_where():
    plan = _emp_plan(where={"EQ": [{"col": "employees.employment_type"}, {"value": "full_time"}]})
    result = _engine(inject=True).apply(plan)
    assert "AND" in result.WHERE


def test_existing_tenant_param_not_duplicated():
    where = {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]}
    plan = _emp_plan(where=where)
    result = _engine(inject=True).apply(plan)
    assert where == result.WHERE


def test_missing_param_raises_when_injection_disabled():
    plan = _emp_plan(where=None)
    with pytest.raises(MissingParamError) as exc_info:
        _engine(inject=False).apply(plan)
    assert exc_info.value.details["required_param"] == "TENANT"


def test_param_bound_on_companies_table():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "companies.company_id"})],
        FROM=FromClause(table="companies"),
        LIMIT=LimitClause(value=10),
    )
    result = _engine(inject=True).apply(plan)
    assert result.WHERE is not None


def test_table_with_no_policy_has_no_injection():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "skills.name"})],
        FROM=FromClause(table="skills"),
        LIMIT=LimitClause(value=10),
    )
    result = _engine(inject=True).apply(plan)
    assert result.WHERE is None


def test_different_param_names_per_table():
    policy = PolicyConfig(
        inject_missing_params=True,
        tables={
            "companies": TablePolicy(param_bound_columns={"tenant_id": "COMPANY_TENANT"}),
            "projects": TablePolicy(param_bound_columns={"tenant_id": "PROJECT_TENANT"}),
        },
    )
    engine = PolicyEngine(policy, SNAPSHOT, DIALECT)

    companies_plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "companies.company_id"})],
        FROM=FromClause(table="companies"),
        LIMIT=LimitClause(value=5),
    )
    result = engine.apply(companies_plan)
    assert result.WHERE == {"EQ": [{"col": "companies.tenant_id"}, {"param": "COMPANY_TENANT"}]}


def test_multiple_param_bound_columns_in_one_table():
    policy = PolicyConfig(
        inject_missing_params=True,
        tables={
            "employees": TablePolicy(
                param_bound_columns={
                    "tenant_id": "TENANT",
                    "company_id": "COMPANY",
                }
            )
        },
    )
    engine = PolicyEngine(policy, SNAPSHOT, DIALECT)
    plan = _emp_plan(where=None)
    result = engine.apply(plan)
    where_str = str(result.WHERE)
    assert "TENANT" in where_str
    assert "COMPANY" in where_str


def test_disallowed_table_raises():
    plan = _emp_plan()
    with pytest.raises(DisallowedTableError) as exc_info:
        _engine(allowed_tables=["companies"]).apply(plan)
    assert exc_info.value.details["table"] == "employees"


def test_allowed_table_passes():
    plan = _emp_plan()
    _engine(allowed_tables=["employees"]).apply(plan)


def test_globally_denied_column_blocked():
    plan = QueryPlan(
        SELECT=[
            SelectItem(expr={"col": "employees.employee_id"}),
            SelectItem(expr={"col": "employees.salary"}),
        ],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DisallowedColumnError) as exc_info:
        _engine(denied=["salary"]).apply(plan)
    assert exc_info.value.details["column"] == "salary"


def test_per_table_denied_column_blocked():
    policy = PolicyConfig(
        inject_missing_params=False,
        tables={
            "employees": TablePolicy(denied_columns=["salary"]),
        },
    )
    engine = PolicyEngine(policy, SNAPSHOT, DIALECT)
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.salary"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DisallowedColumnError) as exc_info:
        engine.apply(plan)
    assert exc_info.value.details["column"] == "salary"


def test_per_table_denied_does_not_affect_other_tables():
    policy = PolicyConfig(
        inject_missing_params=False,
        tables={
            "employees": TablePolicy(denied_columns=["salary"]),
        },
    )
    engine = PolicyEngine(policy, SNAPSHOT, DIALECT)
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "salary_history.salary"})],
        FROM=FromClause(table="salary_history"),
        LIMIT=LimitClause(value=10),
    )
    engine.apply(plan)  # must not raise


def test_limit_clamped_to_max():
    plan = _emp_plan()
    plan = plan.model_copy(update={"LIMIT": LimitClause(value=9999)}, deep=True)
    result = _engine().apply(plan)
    assert result.LIMIT is not None
    assert result.LIMIT.value <= DIALECT.allowed.max_limit


def test_default_limit_injected_when_missing():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
    )
    result = _engine(default_limit=25).apply(plan)
    assert result.LIMIT is not None
    assert result.LIMIT.value == 25


def test_no_default_limit_injected_when_zero():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
    )
    result = _engine(default_limit=0).apply(plan)
    assert result.LIMIT is None


# ---------------------------------------------------------------------------
# allowed_columns — positive column allowlist
# ---------------------------------------------------------------------------


def _allowlist_engine(allowed: list[str]) -> PolicyEngine:
    policy = PolicyConfig(
        inject_missing_params=False,
        tables={
            "employees": TablePolicy(allowed_columns=allowed),
        },
    )
    return PolicyEngine(policy, SNAPSHOT, DIALECT)


def test_allowed_columns_passes_listed_column():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.first_name"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    _allowlist_engine(["first_name", "last_name"]).apply(plan)  # must not raise


def test_allowed_columns_blocks_unlisted_column():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.salary"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DisallowedColumnError) as exc_info:
        _allowlist_engine(["first_name", "last_name"]).apply(plan)
    assert exc_info.value.details["column"] == "salary"
    assert exc_info.value.details["table"] == "employees"


def test_allowed_columns_error_details_lists_allowlist():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.salary"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DisallowedColumnError) as exc_info:
        _allowlist_engine(["first_name", "last_name"]).apply(plan)
    assert set(exc_info.value.details["allowed_columns"]) == {"first_name", "last_name"}


def test_empty_allowed_columns_permits_all_schema_columns():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.salary"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    _allowlist_engine([]).apply(plan)  # empty list = no restriction, must not raise


def test_allowed_columns_does_not_affect_other_tables():
    policy = PolicyConfig(
        inject_missing_params=False,
        tables={
            "employees": TablePolicy(allowed_columns=["employee_id"]),
        },
    )
    engine = PolicyEngine(policy, SNAPSHOT, DIALECT)
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "skills.name"})],
        FROM=FromClause(table="skills"),
        LIMIT=LimitClause(value=10),
    )
    engine.apply(plan)  # skills has no allowlist restriction, must not raise


def test_allowed_columns_blocked_in_where_clause():
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.employee_id"})],
        FROM=FromClause(table="employees"),
        WHERE={"EQ": [{"col": "employees.salary"}, {"value": 50000}]},
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DisallowedColumnError) as exc_info:
        _allowlist_engine(["employee_id", "first_name"]).apply(plan)
    assert exc_info.value.details["column"] == "salary"


# ---------------------------------------------------------------------------
# _where_satisfies_param — OR-bypass security tests
# ---------------------------------------------------------------------------


def test_param_nested_in_and_satisfies_binding():
    """A param binding inside AND must satisfy the check — no re-injection needed."""
    where = {
        "AND": [
            {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            {"EQ": [{"col": "employees.active"}, {"value": True}]},
        ]
    }
    plan = _emp_plan(where=where)
    result = _engine(inject=True).apply(plan)
    assert result.WHERE == where


def test_param_nested_in_or_does_not_satisfy_binding():
    """A param binding nested inside OR must NOT satisfy the check.

    An OR makes the restriction optional: rows from other tenants can be
    returned via the second branch.  The engine must detect this and wrap
    the whole WHERE in AND with the required predicate.
    """
    where = {
        "OR": [
            {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            {"EQ": [{"col": "employees.active"}, {"value": True}]},
        ]
    }
    plan = _emp_plan(where=where)
    result = _engine(inject=True).apply(plan)
    # OR does not enforce the binding, so the engine must wrap in AND
    assert result.WHERE is not None
    assert "AND" in result.WHERE


def test_param_nested_in_or_with_injection_disabled_raises():
    """When inject_missing_params is False, an OR-nested binding must raise."""
    where = {
        "OR": [
            {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            {"EQ": [{"col": "employees.active"}, {"value": True}]},
        ]
    }
    plan = _emp_plan(where=where)
    with pytest.raises(MissingParamError):
        _engine(inject=False).apply(plan)


def test_param_nested_deeply_in_or_does_not_satisfy_binding():
    """Binding inside a nested OR (AND → OR → EQ) must not satisfy the check."""
    where = {
        "AND": [
            {"EQ": [{"col": "employees.department_id"}, {"value": 5}]},
            {
                "OR": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"EQ": [{"col": "employees.active"}, {"value": True}]},
                ]
            },
        ]
    }
    plan = _emp_plan(where=where)
    result = _engine(inject=True).apply(plan)
    assert "AND" in result.WHERE


def test_denied_columns_subtracted_from_allowlist_in_error_details():
    policy = PolicyConfig(
        inject_missing_params=False,
        tables={
            "employees": TablePolicy(
                allowed_columns=["first_name", "last_name", "salary"],
                denied_columns=["salary"],
            ),
        },
    )
    engine = PolicyEngine(policy, SNAPSHOT, DIALECT)
    plan = QueryPlan(
        SELECT=[SelectItem(expr={"col": "employees.salary"})],
        FROM=FromClause(table="employees"),
        LIMIT=LimitClause(value=10),
    )
    with pytest.raises(DisallowedColumnError) as exc_info:
        engine.apply(plan)
    assert "salary" not in exc_info.value.details["allowed_columns"]
    assert set(exc_info.value.details["allowed_columns"]) == {"first_name", "last_name"}
