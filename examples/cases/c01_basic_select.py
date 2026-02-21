"""Category 01 — Basic SELECT.

Covers single-table reads with no joins, no aggregations, and no ORDER BY.
These are the simplest possible queries; the LLM only needs SELECT / FROM /
WHERE / LIMIT.
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import load_snapshot, select_dialect, standard_policy

_snap = load_snapshot()
_pol = standard_policy()
_dl = select_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c01_01",
        category="basic_select",
        question="List the name and industry of all active companies.",
        notes=(
            "Simplest possible query. Active is stored as INTEGER (1=true) in SQLite. "
            "The LLM must filter active=1 and include the TENANT param."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "companies.name"}},
                {"expr": {"col": "companies.industry"}},
            ],
            "FROM": {"table": "companies"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "companies.tenant_id"}, {"param": "TENANT"}]},
                    {"EQ": [{"col": "companies.active"}, {"value": 1}]},
                ]
            },
            "LIMIT": {"value": 10},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c01_02",
        category="basic_select",
        question="Show the email address and employment type of all employees.",
        notes=(
            "No filtering beyond TENANT isolation. Tests that the LLM selects "
            "exactly two specific columns from employees."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.email"}},
                {"expr": {"col": "employees.employment_type"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c01_03",
        category="basic_select",
        question="Which employees have no department assigned?",
        notes=(
            "IS_NULL predicate on a nullable foreign-key column. "
            "Henry (id=8) has no department in the seed data."
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
                    {"IS_NULL": {"col": "employees.department_id"}},
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
        id="c01_04",
        category="basic_select",
        question="List employees who have a phone number on file.",
        notes=(
            "IS_NOT_NULL predicate. Tests the inverse null check. "
            "Carol (id=3), Frank (id=6), Henry (id=8), Karen (id=11) have no phone."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.phone"}},
            ],
            "FROM": {"table": "employees"},
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"IS_NOT_NULL": {"col": "employees.phone"}},
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
        id="c01_05",
        category="basic_select",
        question="Get all remote workers — show their name and employment type.",
        notes=(
            "BOOLEAN stored as INTEGER in SQLite. remote=1 means remote. "
            "Bob (2), Carol (3), Eve (5), Henry (8) are remote for acme."
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
                    {"EQ": [{"col": "employees.remote"}, {"value": 1}]},
                ]
            },
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
]
