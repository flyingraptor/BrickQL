"""Category 09 — Window Functions.

ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, and running aggregates over OVER clauses.
Window functions require .aggregations().window_functions() in the dialect.

The SelectItem.over field carries a WindowSpec with:
  - partition_by: list of operand dicts
  - order_by: list of OrderByItem
  - frame: optional WindowFrame (ROWS / RANGE BETWEEN …)
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import standard_policy, window_dialect

_pol = standard_policy()
_dl = window_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c09_01",
        category="window_functions",
        question=(
            "Rank employees by salary within their department, "
            "highest salary first. Show employee name, salary, and their rank."
        ),
        notes=(
            "RANK() OVER (PARTITION BY department_id ORDER BY salary DESC). "
            "Employees with the same salary get the same rank (RANK skips numbers). "
            "Employees with NULL salary will rank last (NULLs sort in SQLite as low values)."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.department_id"}},
                {"expr": {"col": "employees.salary"}},
                {
                    "expr": {"func": "RANK", "args": []},
                    "over": {
                        "partition_by": [{"col": "employees.department_id"}],
                        "order_by": [
                            {"expr": {"col": "employees.salary"}, "direction": "DESC"}
                        ],
                    },
                    "alias": "salary_rank",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.department_id"}, "direction": "ASC"},
                {"expr": {"col": "employees.salary"},        "direction": "DESC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c09_02",
        category="window_functions",
        question=(
            "Assign a sequential row number to each employee ordered by hire date, "
            "oldest first."
        ),
        notes=(
            "ROW_NUMBER() OVER (ORDER BY hire_date ASC). "
            "No PARTITION BY — single global ordering. "
            "Grace (2017) gets row 1, Frank (2018) gets 2, etc."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.hire_date"}},
                {
                    "expr": {"func": "ROW_NUMBER", "args": []},
                    "over": {
                        "partition_by": [],
                        "order_by": [
                            {"expr": {"col": "employees.hire_date"}, "direction": "ASC"}
                        ],
                    },
                    "alias": "rn",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.hire_date"}, "direction": "ASC"}
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c09_03",
        category="window_functions",
        question=(
            "For each salary history entry, show the employee id, "
            "the current salary, and the previous salary using LAG."
        ),
        notes=(
            "LAG(salary, 1) OVER (PARTITION BY employee_id ORDER BY effective_date). "
            "salary_history has no tenant_id. The first entry per employee "
            "will have NULL as the previous salary."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "salary_history.employee_id"}, "alias": "emp_id"},
                {"expr": {"col": "salary_history.effective_date"}},
                {"expr": {"col": "salary_history.salary"}, "alias": "current_salary"},
                {
                    "expr": {"func": "LAG", "args": [
                        {"col": "salary_history.salary"},
                        {"value": 1},
                    ]},
                    "over": {
                        "partition_by": [{"col": "salary_history.employee_id"}],
                        "order_by": [
                            {"expr": {"col": "salary_history.effective_date"}, "direction": "ASC"}
                        ],
                    },
                    "alias": "prev_salary",
                },
            ],
            "FROM": {"table": "salary_history"},
            "ORDER_BY": [
                {"expr": {"col": "salary_history.employee_id"},   "direction": "ASC"},
                {"expr": {"col": "salary_history.effective_date"}, "direction": "ASC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=window_dialect(tables=["salary_history"]),
        policy=standard_policy(),
        runtime_params={},
    ),
    # ------------------------------------------------------------------
    Case(
        id="c09_04",
        category="window_functions",
        question=(
            "Calculate a running total of salary for each employee "
            "ordered by hire date — show cumulative salary count across all employees."
        ),
        notes=(
            "SUM(salary) OVER (ORDER BY hire_date ROWS BETWEEN UNBOUNDED PRECEDING "
            "AND CURRENT ROW). Aggregate window function with a frame clause. "
            "NULL salaries contribute 0 to the running sum."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.hire_date"}},
                {"expr": {"col": "employees.salary"}},
                {
                    "expr": {"func": "SUM", "args": [{"col": "employees.salary"}]},
                    "over": {
                        "partition_by": [],
                        "order_by": [
                            {"expr": {"col": "employees.hire_date"}, "direction": "ASC"}
                        ],
                        "frame": {
                            "type": "ROWS",
                            "start": "UNBOUNDED PRECEDING",
                            "end": "CURRENT ROW",
                        },
                    },
                    "alias": "running_total",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.hire_date"}, "direction": "ASC"}
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c09_05",
        category="window_functions",
        question=(
            "For each employee, compute their DENSE_RANK within their department "
            "by salary (highest first), and LEAD to show the next hire date "
            "in the same department."
        ),
        notes=(
            "Two window functions in the same SELECT with the same PARTITION BY "
            "but different ORDER BY. DENSE_RANK doesn't skip numbers on ties. "
            "LEAD(hire_date, 1) gives the hire_date of the next-hired employee "
            "in the same department."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.department_id"}},
                {"expr": {"col": "employees.salary"}},
                {"expr": {"col": "employees.hire_date"}},
                {
                    "expr": {"func": "DENSE_RANK", "args": []},
                    "over": {
                        "partition_by": [{"col": "employees.department_id"}],
                        "order_by": [
                            {"expr": {"col": "employees.salary"}, "direction": "DESC"}
                        ],
                    },
                    "alias": "salary_dense_rank",
                },
                {
                    "expr": {"func": "LEAD", "args": [
                        {"col": "employees.hire_date"},
                        {"value": 1},
                    ]},
                    "over": {
                        "partition_by": [{"col": "employees.department_id"}],
                        "order_by": [
                            {"expr": {"col": "employees.hire_date"}, "direction": "ASC"}
                        ],
                    },
                    "alias": "next_hire_date",
                },
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.department_id"}, "direction": "ASC"},
                {"expr": {"col": "employees.salary"},        "direction": "DESC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
]
