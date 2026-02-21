"""Category 02 — Filtering.

Complex WHERE predicates: LIKE, IN, BETWEEN, AND/OR nesting, NOT, multi-column
conditions. Still single-table — no joins required.
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import load_snapshot, select_dialect, standard_policy

_pol = standard_policy()
_dl = select_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c02_01",
        category="filtering",
        question="Find all employees whose last name starts with the letter 'S'.",
        notes=(
            "LIKE with a prefix pattern. Alice Smith (id=1) and Carol White (id=3) — "
            "only Smith matches. LIKE is in the base operator set (no .joins() needed)."
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
                    {"LIKE": [{"col": "employees.last_name"}, {"value": "S%"}]},
                ]
            },
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c02_02",
        category="filtering",
        question=(
            "List employees who work in department 1 (Engineering) "
            "or department 2 (Marketing)."
        ),
        notes=(
            "IN predicate with multiple literal values. "
            "Value items in IN are each wrapped as {'value': N} operands."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.employee_id"}, "alias": "id"},
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.department_id"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"IN": [
                        {"col": "employees.department_id"},
                        {"value": 1},
                        {"value": 2},
                    ]},
                ]
            },
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c02_03",
        category="filtering",
        question="Find employees hired between January 1 2020 and December 31 2022.",
        notes=(
            "BETWEEN on a DATE column stored as TEXT (ISO-8601 sorts lexicographically). "
            "Alice (2020-03-15), Bob (2021-06-01), Carol (2022-01-10), "
            "Grace (2017-04-22 — excluded), Frank (2018-11-05 — excluded)."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.hire_date"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"BETWEEN": [
                        {"col": "employees.hire_date"},
                        {"value": "2020-01-01"},
                        {"value": "2022-12-31"},
                    ]},
                ]
            },
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c02_04",
        category="filtering",
        question=(
            "Get full-time employees who are either currently active "
            "or who work remotely."
        ),
        notes=(
            "Three-condition AND/OR. The LLM must correctly nest "
            "AND(full_time, OR(active, remote)). Tests boolean logic depth."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.employment_type"}},
                {"expr": {"col": "employees.active"}},
                {"expr": {"col": "employees.remote"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"EQ": [
                        {"col": "employees.employment_type"},
                        {"value": "full_time"},
                    ]},
                    {"OR": [
                        {"EQ": [{"col": "employees.active"}, {"value": 1}]},
                        {"EQ": [{"col": "employees.remote"}, {"value": 1}]},
                    ]},
                ]
            },
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c02_05",
        category="filtering",
        question="List all active employees who are NOT contractors.",
        notes=(
            "NOT operator wrapping a comparison. "
            "Alternatively the LLM may use NE; both are valid."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.employment_type"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"EQ": [{"col": "employees.active"}, {"value": 1}]},
                    {"NOT": {"EQ": [
                        {"col": "employees.employment_type"},
                        {"value": "contractor"},
                    ]}},
                ]
            },
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c02_06",
        category="filtering",
        question="Show all skills in the 'programming' or 'analytics' category.",
        notes=(
            "skills has no tenant_id — no TENANT param needed. "
            "Tests that the LLM does NOT invent a tenant filter on a tenant-free table."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "skills.name"}},
                {"expr": {"col": "skills.category"}},
            ],
            "FROM": {"table": "skills"},
            "WHERE": {
                "IN": [
                    {"col": "skills.category"},
                    {"value": "programming"},
                    {"value": "analytics"},
                ]
            },
            "LIMIT": {"value": 20},
        },
        dialect=select_dialect(tables=["skills"]),
        policy=standard_policy(),
        runtime_params={},
    ),
]
