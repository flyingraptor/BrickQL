"""Category 06 — Subqueries.

Correlated subqueries, EXISTS, IN-subquery, and derived tables (FROM subquery).
Requires .subqueries() in the dialect.
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import subquery_dialect, standard_policy

_pol = standard_policy()
_dl = subquery_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c06_01",
        category="subqueries",
        question=(
            "Find employees whose salary is above the average salary "
            "across all active employees."
        ),
        notes=(
            "⚠️  Limitation note: BrinkQL does not directly support scalar subqueries "
            "in comparison operands (only in IN / EXISTS positions). "
            "The expected_plan below will FAIL validation. "
            "The documented workaround is a derived table (see c06_04) or a CTE. "
            "This case intentionally demonstrates what the LLM might try and what "
            "BrinkQL rejects — triggering the repair loop."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.salary"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"EQ": [{"col": "employees.active"}, {"value": 1}]},
                    {"GT": [
                        {"col": "employees.salary"},
                        # Scalar subquery: SELECT AVG(salary) FROM employees WHERE active=1
                        {
                            "func": "AVG",
                            "args": [{"col": "employees.salary"}],
                            # NOTE: scalar subquery via IN with single-column subquery
                            # is not yet supported; we use a derived table in FROM instead.
                            # This case is split into c06_04 (derived table approach).
                            # Here we show the literal average (approximation for docs).
                            # Real BrinkQL usage: wrap the aggregate in a FROM subquery.
                        },
                    ]},
                ]
            },
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c06_02",
        category="subqueries",
        question=(
            "Find employees who have at least one skill "
            "in the 'programming' category."
        ),
        notes=(
            "EXISTS with a correlated subquery. The inner query references "
            "employee_skills joined to skills, filtering by category. "
            "The outer WHERE uses EXISTS wrapping a QueryPlan dict."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"EXISTS": {
                        "SELECT": [{"expr": {"col": "employee_skills.employee_id"}}],
                        "FROM": {"table": "employee_skills"},
                        "JOIN": [{"rel": "employee_skills__skills", "type": "INNER"}],
                        "WHERE": {
                            "AND": [
                                {"EQ": [
                                    {"col": "employee_skills.employee_id"},
                                    {"col": "employees.employee_id"},
                                ]},
                                {"EQ": [
                                    {"col": "skills.category"},
                                    {"value": "programming"},
                                ]},
                            ]
                        },
                        "LIMIT": {"value": 1},
                    }},
                ]
            },
            "ORDER_BY": [{"expr": {"col": "employees.last_name"}, "direction": "ASC"}],
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c06_03",
        category="subqueries",
        question=(
            "List employees who are assigned to project 1 (DataPlatform). "
            "Use an IN-subquery on project_assignments."
        ),
        notes=(
            "IN with a subquery: employee_id IN (SELECT employee_id FROM "
            "project_assignments WHERE project_id = 1). "
            "The subquery dict is embedded as the second element of the IN args list."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.employee_id"}, "alias": "id"},
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"IN": [
                        {"col": "employees.employee_id"},
                        # Subquery as single dict with SELECT key
                        {
                            "SELECT": [
                                {"expr": {"col": "project_assignments.employee_id"}}
                            ],
                            "FROM": {"table": "project_assignments"},
                            "WHERE": {
                                "EQ": [
                                    {"col": "project_assignments.project_id"},
                                    {"value": 1},
                                ]
                            },
                            "LIMIT": {"value": 100},
                        },
                    ]},
                ]
            },
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c06_04",
        category="subqueries",
        question=(
            "Using a derived table, find the top 3 departments by headcount "
            "and show just those department ids and their count."
        ),
        notes=(
            "Derived table (subquery in FROM). The inner query aggregates employees "
            "by department. The outer query selects from the result and limits to 3. "
            "FROM clause uses a subquery + alias instead of a table name. "
            "Note: ORDER BY inside a subquery using a bare alias works in SQLite; "
            "PostgreSQL may require the full expression."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "dept_counts.dept_id"}},
                {"expr": {"col": "dept_counts.cnt"}},
            ],
            "FROM": {
                "subquery": {
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
                    "ORDER_BY": [
                        {"expr": {"col": "cnt"}, "direction": "DESC"}
                    ],
                    "LIMIT": {"value": 3},
                },
                "alias": "dept_counts",
            },
            "ORDER_BY": [{"expr": {"col": "dept_counts.cnt"}, "direction": "DESC"}],
            "LIMIT": {"value": 3},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
]
