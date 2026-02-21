"""Category 03 — Ordering and Paging.

ORDER BY (single / multi-column, ASC / DESC) and LIMIT + OFFSET.
ORDER BY requires the .joins() dialect feature even without actual JOINs.
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import joins_dialect, load_snapshot, standard_policy

_pol = standard_policy()
_dl = joins_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c03_01",
        category="ordering_paging",
        question="List employees sorted by hire date, oldest first.",
        notes=(
            "Single-column ORDER BY ASC. ORDER BY is unlocked by .joins() in the "
            "dialect builder, even when no JOIN is present in the plan."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.hire_date"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.hire_date"}, "direction": "ASC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c03_02",
        category="ordering_paging",
        question=(
            "Show employees sorted alphabetically by last name, "
            "then by first name within the same last name."
        ),
        notes="Multi-column ORDER BY — two ASC keys on text columns.",
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.last_name"},  "direction": "ASC"},
                {"expr": {"col": "employees.first_name"}, "direction": "ASC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c03_03",
        category="ordering_paging",
        question="Get the second page of employees, 3 per page (i.e. skip the first 3).",
        notes=(
            "LIMIT + OFFSET paging. OFFSET requires .joins() in the dialect. "
            "With ORDER BY employee_id, page 2 = ids 4, 5, 6 for acme."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.employee_id"}, "alias": "id"},
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.employee_id"}, "direction": "ASC"},
            ],
            "LIMIT":  {"value": 3},
            "OFFSET": {"value": 3},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c03_04",
        category="ordering_paging",
        question="Who are the 3 highest-paid employees?",
        notes=(
            "ORDER BY salary DESC LIMIT 3. salary is REAL and nullable — "
            "contractors (Carol, Henry) have NULL salary and sort last in DESC."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.salary"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.salary"}, "direction": "DESC"},
            ],
            "LIMIT": {"value": 3},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c03_05",
        category="ordering_paging",
        question=(
            "List skills ordered by category ascending, then name ascending — "
            "show all of them."
        ),
        notes=(
            "Ordering a tenant-free table (skills). No TENANT param. "
            "Tests that the LLM doesn't inject a spurious tenant filter."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "skills.name"}},
                {"expr": {"col": "skills.category"}},
            ],
            "FROM": {"table": "skills"},
            "ORDER_BY": [
                {"expr": {"col": "skills.category"}, "direction": "ASC"},
                {"expr": {"col": "skills.name"},     "direction": "ASC"},
            ],
            "LIMIT": {"value": 20},
        },
        dialect=joins_dialect(tables=["skills"]),
        policy=standard_policy(),
        runtime_params={},
    ),
]
