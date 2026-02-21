"""Category 05 — Aggregations.

GROUP BY, HAVING, COUNT / SUM / AVG / MIN / MAX, COUNT DISTINCT,
and CASE WHEN inside aggregations.
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import agg_dialect, standard_policy

_pol = standard_policy()
_dl = agg_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c05_01",
        category="aggregations",
        question="How many employees are there in total?",
        notes=(
            "Simplest aggregate: COUNT(*). No GROUP BY. "
            "The LLM should use COUNT with a wildcard-style or column arg."
        ),
        expected_plan={
            "SELECT": [
                {
                    "expr": {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    "alias": "total",
                }
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "LIMIT": {"value": 1},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c05_02",
        category="aggregations",
        question="How many employees are in each department?",
        notes=(
            "COUNT + GROUP BY on a nullable FK. "
            "Employees without a department (Henry) are grouped under NULL."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.department_id"}, "alias": "dept_id"},
                {
                    "expr": {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    "alias": "headcount",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "GROUP_BY": [{"col": "employees.department_id"}],
            "ORDER_BY": [{"expr": {"col": "employees.department_id"}, "direction": "ASC"}],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c05_03",
        category="aggregations",
        question=(
            "What is the average, minimum, and maximum salary "
            "per employment type for active employees?"
        ),
        notes=(
            "Multiple aggregates in the same SELECT. AVG / MIN / MAX on a "
            "nullable REAL column — NULL salaries (contractors) are ignored "
            "by SQL aggregate functions."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.employment_type"}, "alias": "type"},
                {
                    "expr": {"func": "AVG", "args": [{"col": "employees.salary"}]},
                    "alias": "avg_salary",
                },
                {
                    "expr": {"func": "MIN", "args": [{"col": "employees.salary"}]},
                    "alias": "min_salary",
                },
                {
                    "expr": {"func": "MAX", "args": [{"col": "employees.salary"}]},
                    "alias": "max_salary",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"EQ": [{"col": "employees.active"}, {"value": 1}]},
                ]
            },
            "GROUP_BY": [{"col": "employees.employment_type"}],
            "ORDER_BY": [
                {"expr": {"col": "employees.employment_type"}, "direction": "ASC"}
            ],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c05_04",
        category="aggregations",
        question="Which departments have more than 2 employees?",
        notes=(
            "HAVING clause on a COUNT aggregate. The LLM must use HAVING "
            "rather than putting COUNT in WHERE."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.department_id"}, "alias": "dept_id"},
                {
                    "expr": {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    "alias": "cnt",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "GROUP_BY": [{"col": "employees.department_id"}],
            "HAVING": {
                "GT": [
                    {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    {"value": 2},
                ]
            },
            "ORDER_BY": [{"expr": {"col": "employees.department_id"}, "direction": "ASC"}],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c05_05",
        category="aggregations",
        question=(
            "For each department, show the total salary budget "
            "and count of employees, joined with the department name."
        ),
        notes=(
            "Aggregation over a JOIN: employees → departments. "
            "Tests GROUP BY after JOIN and selecting from the joined table."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "departments.name"}, "alias": "department"},
                {
                    "expr": {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    "alias": "headcount",
                },
                {
                    "expr": {"func": "SUM", "args": [{"col": "employees.salary"}]},
                    "alias": "total_salary",
                },
            ],
            "FROM": {"table": "employees"},
            "JOIN": [{"rel": "departments__employees", "type": "INNER"}],
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "GROUP_BY": [{"col": "departments.name"}],
            "ORDER_BY": [
                {"expr": {"col": "departments.name"}, "direction": "ASC"}
            ],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c05_06",
        category="aggregations",
        question=(
            "Count employees grouped by employment type and active status, "
            "showing only groups with at least 2 employees."
        ),
        notes=(
            "GROUP BY two columns, HAVING COUNT >= 2. "
            "Tests multi-column grouping."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.employment_type"}, "alias": "type"},
                {"expr": {"col": "employees.active"}},
                {
                    "expr": {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    "alias": "cnt",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "GROUP_BY": [
                {"col": "employees.employment_type"},
                {"col": "employees.active"},
            ],
            "HAVING": {
                "GTE": [
                    {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    {"value": 2},
                ]
            },
            "ORDER_BY": [
                {"expr": {"col": "employees.employment_type"}, "direction": "ASC"}
            ],
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c05_07",
        category="aggregations",
        question=(
            "How many employees have each proficiency level across all skills? "
            "Show the proficiency level and count."
        ),
        notes=(
            "Aggregation on a tenant-free table (employee_skills). "
            "No TENANT param. Tests that the LLM avoids spurious filters."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employee_skills.proficiency"}, "alias": "level"},
                {
                    "expr": {"func": "COUNT", "args": [{"col": "employee_skills.employee_id"}]},
                    "alias": "cnt",
                },
            ],
            "FROM": {"table": "employee_skills"},
            "GROUP_BY": [{"col": "employee_skills.proficiency"}],
            "ORDER_BY": [
                {"expr": {"col": "employee_skills.proficiency"}, "direction": "ASC"}
            ],
            "LIMIT": {"value": 10},
        },
        dialect=agg_dialect(tables=["employee_skills"]),
        policy=standard_policy(),
        runtime_params={},
    ),
]
