"""Category 07 — CTEs (Common Table Expressions).

WITH … AS (…) patterns: simple CTE, multiple CTEs, aggregation inside CTE,
and CTEs that feed into EXISTS / IN predicates.

CTEs require .subqueries().ctes() in the dialect builder.
The validator skips schema checks for CTE-defined table names and their
columns (they are treated as virtual tables).
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import cte_dialect, standard_policy

_pol = standard_policy()
_dl = cte_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c07_01",
        category="ctes",
        question=(
            "Using a CTE, find all active full-time employees "
            "and show their names."
        ),
        notes=(
            "Simplest CTE pattern. The CTE 'active_ft' pre-filters employees. "
            "The main query selects from the CTE by name. "
            "Column aliases defined in the CTE are referenced in the outer query "
            "as 'cte_name.alias'."
        ),
        expected_plan={
            "CTE": [
                {
                    "name": "active_ft",
                    "query": {
                        "SELECT": [
                            {"expr": {"col": "employees.employee_id"}, "alias": "id"},
                            {"expr": {"col": "employees.first_name"}, "alias": "first_name"},
                            {"expr": {"col": "employees.last_name"},  "alias": "last_name"},
                        ],
                        "FROM": {"table": "employees"},
                        "WHERE": {
                            "AND": [
                                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                                {"EQ": [{"col": "employees.active"}, {"value": 1}]},
                                {"EQ": [
                                    {"col": "employees.employment_type"},
                                    {"value": "full_time"},
                                ]},
                            ]
                        },
                        "LIMIT": {"value": 100},
                    },
                }
            ],
            "SELECT": [
                {"expr": {"col": "active_ft.first_name"}},
                {"expr": {"col": "active_ft.last_name"}},
            ],
            "FROM": {"table": "active_ft"},
            "ORDER_BY": [
                {"expr": {"col": "active_ft.last_name"}, "direction": "ASC"}
            ],
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c07_02",
        category="ctes",
        question=(
            "Using a CTE, find each manager's employee id and how many "
            "direct reports they have. Order by most reports first."
        ),
        notes=(
            "CTE aggregates direct reports per manager by grouping on manager_id. "
            "The outer query reads from the CTE. "
            "This is the idiomatic single-CTE pattern for aggregation-then-select, "
            "since BrinkQL v1 JOIN clauses require relationship keys from the "
            "SchemaSnapshot and cannot join two CTEs directly."
        ),
        expected_plan={
            "CTE": [
                {
                    "name": "report_counts",
                    "query": {
                        "SELECT": [
                            {"expr": {"col": "employees.manager_id"}, "alias": "mgr_id"},
                            {
                                "expr": {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                                "alias": "reports",
                            },
                        ],
                        "FROM": {"table": "employees"},
                        "WHERE": {
                            "AND": [
                                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                                {"IS_NOT_NULL": {"col": "employees.manager_id"}},
                            ]
                        },
                        "GROUP_BY": [{"col": "employees.manager_id"}],
                        "LIMIT": {"value": 100},
                    },
                },
            ],
            "SELECT": [
                {"expr": {"col": "report_counts.mgr_id"}},
                {"expr": {"col": "report_counts.reports"}},
            ],
            "FROM": {"table": "report_counts"},
            "ORDER_BY": [
                {"expr": {"col": "report_counts.reports"}, "direction": "DESC"}
            ],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c07_03",
        category="ctes",
        question=(
            "Using a CTE, calculate the total salary per department, "
            "then show departments whose total salary exceeds 150,000."
        ),
        notes=(
            "CTE with aggregation inside, then main query filters on the aggregated "
            "result. This is the idiomatic CTE pattern for HAVING-on-derived-columns."
        ),
        expected_plan={
            "CTE": [
                {
                    "name": "dept_salary",
                    "query": {
                        "SELECT": [
                            {"expr": {"col": "employees.department_id"}, "alias": "dept_id"},
                            {
                                "expr": {"func": "SUM", "args": [{"col": "employees.salary"}]},
                                "alias": "total",
                            },
                        ],
                        "FROM": {"table": "employees"},
                        "WHERE": {
                            "AND": [
                                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                                {"IS_NOT_NULL": {"col": "employees.salary"}},
                            ]
                        },
                        "GROUP_BY": [{"col": "employees.department_id"}],
                        "LIMIT": {"value": 100},
                    },
                }
            ],
            "SELECT": [
                {"expr": {"col": "dept_salary.dept_id"}},
                {"expr": {"col": "dept_salary.total"}},
            ],
            "FROM": {"table": "dept_salary"},
            "WHERE": {"GT": [{"col": "dept_salary.total"}, {"value": 150000}]},
            "ORDER_BY": [
                {"expr": {"col": "dept_salary.total"}, "direction": "DESC"}
            ],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c07_04",
        category="ctes",
        question=(
            "Using a CTE over salary_history, find employees who have had "
            "more than one salary change. Show the employee id and change count."
        ),
        notes=(
            "CTE over a tenant-free table (salary_history) with aggregation + HAVING. "
            "The outer query reads from the CTE directly. "
            "Simpler than a multi-CTE join, and demonstrates that CTEs compose "
            "naturally with aggregation and filtering."
        ),
        expected_plan={
            "CTE": [
                {
                    "name": "change_counts",
                    "query": {
                        "SELECT": [
                            {"expr": {"col": "salary_history.employee_id"}, "alias": "emp_id"},
                            {
                                "expr": {"func": "COUNT", "args": [{"col": "salary_history.history_id"}]},
                                "alias": "changes",
                            },
                        ],
                        "FROM": {"table": "salary_history"},
                        "GROUP_BY": [{"col": "salary_history.employee_id"}],
                        "HAVING": {
                            "GT": [
                                {"func": "COUNT", "args": [{"col": "salary_history.history_id"}]},
                                {"value": 1},
                            ]
                        },
                        "LIMIT": {"value": 100},
                    },
                },
            ],
            "SELECT": [
                {"expr": {"col": "change_counts.emp_id"}},
                {"expr": {"col": "change_counts.changes"}},
            ],
            "FROM": {"table": "change_counts"},
            "ORDER_BY": [
                {"expr": {"col": "change_counts.changes"}, "direction": "DESC"}
            ],
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
]
