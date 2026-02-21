"""Category 10 — Complex and Weird Cases.

These cases combine multiple features or probe edge cases:
  - CTE + window function + outer filter
  - CASE WHEN inside SELECT (salary bracket labelling)
  - Multi-table aggregation with HAVING
  - CASE WHEN in GROUP BY
  - Aggregation HAVING on joined data
  - Set operation inside a CTE
  - Window function with no PARTITION BY over a join
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import full_dialect, standard_policy

_pol = standard_policy()
_dl = full_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c10_01",
        category="complex",
        question=(
            "Using a CTE that ranks employees by salary within each department, "
            "return only the highest-paid employee per department."
        ),
        notes=(
            "Classic 'top-N per group' pattern. "
            "CTE computes RANK() OVER (PARTITION BY dept ORDER BY salary DESC). "
            "Main query filters WHERE salary_rank = 1. "
            "This requires CTE + window functions together."
        ),
        expected_plan={
            "CTE": [
                {
                    "name": "ranked",
                    "query": {
                        "SELECT": [
                            {"expr": {"col": "employees.first_name"}},
                            {"expr": {"col": "employees.last_name"}},
                            {"expr": {"col": "employees.department_id"}, "alias": "dept_id"},
                            {"expr": {"col": "employees.salary"}},
                            {
                                "expr": {"func": "RANK", "args": []},
                                "over": {
                                    "partition_by": [{"col": "employees.department_id"}],
                                    "order_by": [
                                        {"expr": {"col": "employees.salary"}, "direction": "DESC"}
                                    ],
                                },
                                "alias": "rnk",
                            },
                        ],
                        "FROM": {"table": "employees"},
                        "WHERE": {
                            "AND": [
                                {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                                {"IS_NOT_NULL": {"col": "employees.salary"}},
                            ]
                        },
                        "LIMIT": {"value": 100},
                    },
                }
            ],
            "SELECT": [
                {"expr": {"col": "ranked.first_name"}},
                {"expr": {"col": "ranked.last_name"}},
                {"expr": {"col": "ranked.dept_id"}},
                {"expr": {"col": "ranked.salary"}},
            ],
            "FROM": {"table": "ranked"},
            "WHERE": {"EQ": [{"col": "ranked.rnk"}, {"value": 1}]},
            "ORDER_BY": [{"expr": {"col": "ranked.dept_id"}, "direction": "ASC"}],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c10_02",
        category="complex",
        question=(
            "Categorise each employee by salary bracket: "
            "'senior' (≥ 90,000), 'mid' (≥ 60,000), or 'junior' (below 60,000). "
            "Employees with no salary should show 'unknown'."
        ),
        notes=(
            "CASE WHEN in a SELECT expression. "
            "Tests that the LLM uses the 'case' operand type with "
            "when[].if and when[].then fields. "
            "NULL salary hits the ELSE clause and shows 'junior' — technically wrong "
            "(should be 'unknown'). A production query adds IS_NULL as the first WHEN."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.salary"}},
                {
                    "expr": {
                        "case": {
                            "when": [
                                {
                                    "if": {"GTE": [
                                        {"col": "employees.salary"}, {"value": 90000}
                                    ]},
                                    "then": {"value": "senior"},
                                },
                                {
                                    "if": {"GTE": [
                                        {"col": "employees.salary"}, {"value": 60000}
                                    ]},
                                    "then": {"value": "mid"},
                                },
                            ],
                            "else": {"value": "junior"},
                        }
                    },
                    "alias": "bracket",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [{"expr": {"col": "employees.salary"}, "direction": "DESC"}],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c10_03",
        category="complex",
        question=(
            "Find projects that have at least 2 active employees assigned to them. "
            "Show the project name and the count of active assignees."
        ),
        notes=(
            "Three-table join (employees + project_assignments + projects) "
            "with aggregation + HAVING. Tests combining joins, WHERE, GROUP BY, "
            "and HAVING in a single plan."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "projects.name"}, "alias": "project"},
                {
                    "expr": {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    "alias": "active_assignees",
                },
            ],
            "FROM": {"table": "project_assignments"},
            "JOIN": [
                {"rel": "employees__project_assignments", "type": "INNER"},
                {"rel": "projects__project_assignments",  "type": "INNER"},
            ],
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"EQ": [{"col": "employees.active"}, {"value": 1}]},
                ]
            },
            "GROUP_BY": [{"col": "projects.name"}],
            "HAVING": {
                "GTE": [
                    {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    {"value": 2},
                ]
            },
            "ORDER_BY": [{"expr": {"col": "projects.name"}, "direction": "ASC"}],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c10_04",
        category="complex",
        question=(
            "Group employees by a salary bracket (using CASE WHEN) and "
            "count how many fall into each bracket."
        ),
        notes=(
            "CASE WHEN inside GROUP BY — computed grouping key. "
            "Tests whether the LLM can place a 'case' operand as a GROUP_BY item "
            "and repeat it in SELECT. This is the 'GROUP BY expression' pattern."
        ),
        expected_plan={
            "SELECT": [
                {
                    "expr": {
                        "case": {
                            "when": [
                                {
                                    "if": {"GTE": [
                                        {"col": "employees.salary"}, {"value": 90000}
                                    ]},
                                    "then": {"value": "senior"},
                                },
                                {
                                    "if": {"GTE": [
                                        {"col": "employees.salary"}, {"value": 60000}
                                    ]},
                                    "then": {"value": "mid"},
                                },
                            ],
                            "else": {"value": "other"},
                        }
                    },
                    "alias": "bracket",
                },
                {
                    "expr": {"func": "COUNT", "args": [{"col": "employees.employee_id"}]},
                    "alias": "cnt",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "GROUP_BY": [
                {
                    "case": {
                        "when": [
                            {
                                "if": {"GTE": [
                                    {"col": "employees.salary"}, {"value": 90000}
                                ]},
                                "then": {"value": "senior"},
                            },
                            {
                                "if": {"GTE": [
                                    {"col": "employees.salary"}, {"value": 60000}
                                ]},
                                "then": {"value": "mid"},
                            },
                        ],
                        "else": {"value": "other"},
                    }
                }
            ],
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c10_05",
        category="complex",
        question=(
            "Using a CTE, find the total hours per week each employee is "
            "committed across all active projects, then rank them by total "
            "hours using DENSE_RANK."
        ),
        notes=(
            "CTE (joins project_assignments + projects) + window function "
            "in the outer query on the CTE result. "
            "Combines: join, aggregation, window function, CTE."
        ),
        expected_plan={
            "CTE": [
                {
                    "name": "emp_hours",
                    "query": {
                        "SELECT": [
                            {"expr": {"col": "project_assignments.employee_id"}, "alias": "emp_id"},
                            {
                                "expr": {"func": "SUM", "args": [
                                    {"col": "project_assignments.hours_per_week"}
                                ]},
                                "alias": "total_hours",
                            },
                        ],
                        "FROM": {"table": "project_assignments"},
                        "JOIN": [{"rel": "projects__project_assignments", "type": "INNER"}],
                        "WHERE": {"EQ": [{"col": "projects.status"}, {"value": "active"}]},
                        "GROUP_BY": [{"col": "project_assignments.employee_id"}],
                        "LIMIT": {"value": 100},
                    },
                }
            ],
            "SELECT": [
                {"expr": {"col": "emp_hours.emp_id"}},
                {"expr": {"col": "emp_hours.total_hours"}},
                {
                    "expr": {"func": "DENSE_RANK", "args": []},
                    "over": {
                        "partition_by": [],
                        "order_by": [
                            {"expr": {"col": "emp_hours.total_hours"}, "direction": "DESC"}
                        ],
                    },
                    "alias": "hours_rank",
                },
            ],
            "FROM": {"table": "emp_hours"},
            "ORDER_BY": [{"expr": {"col": "emp_hours.total_hours"}, "direction": "DESC"}],
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c10_06",
        category="complex",
        question=(
            "Get employees who joined before 2020 and still have no notes, "
            "OR who are contractors with a salary explicitly recorded — "
            "ordered by hire date."
        ),
        notes=(
            "Deliberately weird OR condition mixing IS_NULL and a nullable REAL. "
            "Tests deep predicate nesting: OR(AND(old, null_notes), AND(contractor, not_null_salary)). "
            "An edge case to see if the LLM correctly models complex boolean logic."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.hire_date"}},
                {"expr": {"col": "employees.employment_type"}},
                {"expr": {"col": "employees.notes"}},
                {"expr": {"col": "employees.salary"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"OR": [
                        {"AND": [
                            {"LT": [
                                {"col": "employees.hire_date"},
                                {"value": "2020-01-01"},
                            ]},
                            {"IS_NULL": {"col": "employees.notes"}},
                        ]},
                        {"AND": [
                            {"EQ": [
                                {"col": "employees.employment_type"},
                                {"value": "contractor"},
                            ]},
                            {"IS_NOT_NULL": {"col": "employees.salary"}},
                        ]},
                    ]},
                ]
            },
            "ORDER_BY": [{"expr": {"col": "employees.hire_date"}, "direction": "ASC"}],
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
]
