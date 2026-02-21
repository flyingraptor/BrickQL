"""Category 08 — Set Operations.

UNION ALL, UNION (dedup), INTERSECT, EXCEPT.
The SET_OP is attached to the main plan; LIMIT applies to the combined result.

Note: Both branches must select the same number of columns (and compatible types)
for set operations to be valid SQL.
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import standard_policy
from brinkql import DialectProfile

_ALL_TABLES = [
    "companies", "departments", "employees", "skills",
    "employee_skills", "projects", "project_assignments", "salary_history",
]


def _set_op_dialect(target: str = "sqlite") -> DialectProfile:
    return (
        DialectProfile.builder(_ALL_TABLES, target=target, max_limit=200)
        .joins()
        .set_operations()
        .build()
    )


_pol = standard_policy()
_dl = _set_op_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c08_01",
        category="set_operations",
        question=(
            "Get all employee emails from both the acme and globex tenants "
            "in a single list (including duplicates)."
        ),
        notes=(
            "UNION ALL — preserves duplicates. Two queries with the same schema "
            "but different TENANT values. "
            "Left branch: TENANT='acme', right branch: TENANT='globex'. "
            "This is a cross-tenant read — only possible by passing two runtime params."
        ),
        expected_plan={
            "SELECT": [{"expr": {"col": "employees.email"}, "alias": "email"}],
            "FROM":   {"table": "employees"},
            "WHERE":  {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "SET_OP": {
                "op": "UNION_ALL",
                "query": {
                    "SELECT": [{"expr": {"col": "employees.email"}, "alias": "email"}],
                    "FROM":   {"table": "employees"},
                    "WHERE":  {"EQ": [
                        {"col": "employees.tenant_id"}, {"param": "OTHER_TENANT"}
                    ]},
                    "LIMIT":  {"value": 100},
                },
            },
            "LIMIT": {"value": 100},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params={"TENANT": "acme", "OTHER_TENANT": "globex"},
    ),
    # ------------------------------------------------------------------
    Case(
        id="c08_02",
        category="set_operations",
        question=(
            "Get a deduplicated list of skill categories that are either "
            "'programming' or 'management'."
        ),
        notes=(
            "UNION (dedup) on a tenant-free table. "
            "Two simple SELECT branches combined with UNION. "
            "No TENANT param needed. Tests that the LLM doesn't invent one."
        ),
        expected_plan={
            "SELECT": [{"expr": {"col": "skills.category"}, "alias": "category"}],
            "FROM":   {"table": "skills"},
            "WHERE":  {"EQ": [{"col": "skills.category"}, {"value": "programming"}]},
            "SET_OP": {
                "op": "UNION",
                "query": {
                    "SELECT": [{"expr": {"col": "skills.category"}, "alias": "category"}],
                    "FROM":   {"table": "skills"},
                    "WHERE":  {"EQ": [{"col": "skills.category"}, {"value": "management"}]},
                    "LIMIT":  {"value": 10},
                },
            },
            "LIMIT": {"value": 10},
        },
        dialect=_set_op_dialect(),
        policy=standard_policy(),
        runtime_params={},
    ),
    # ------------------------------------------------------------------
    Case(
        id="c08_03",
        category="set_operations",
        question=(
            "Find employee IDs that are assigned to BOTH project 1 (DataPlatform) "
            "and project 2 (Website Redesign)."
        ),
        notes=(
            "INTERSECT — returns only rows that appear in both result sets. "
            "project_assignments has no tenant_id, so no TENANT filter. "
            "Expected result: employee_id=1 (Alice is on both projects)."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "project_assignments.employee_id"}, "alias": "emp_id"}
            ],
            "FROM":  {"table": "project_assignments"},
            "WHERE": {"EQ": [
                {"col": "project_assignments.project_id"}, {"value": 1}
            ]},
            "SET_OP": {
                "op": "INTERSECT",
                "query": {
                    "SELECT": [
                        {"expr": {"col": "project_assignments.employee_id"}, "alias": "emp_id"}
                    ],
                    "FROM":  {"table": "project_assignments"},
                    "WHERE": {"EQ": [
                        {"col": "project_assignments.project_id"}, {"value": 2}
                    ]},
                    "LIMIT": {"value": 100},
                },
            },
            "LIMIT": {"value": 20},
        },
        dialect=_set_op_dialect(),
        policy=standard_policy(),
        runtime_params={},
    ),
    # ------------------------------------------------------------------
    Case(
        id="c08_04",
        category="set_operations",
        question=(
            "List employee IDs who are assigned to project 1 (DataPlatform) "
            "but NOT to project 2 (Website Redesign)."
        ),
        notes=(
            "EXCEPT (set difference). "
            "Returns rows in left branch that don't appear in right branch. "
            "Expected: Bob (2) and Grace (7) — they are on project 1 but not 2. "
            "Alice (1) is on both, so she's excluded."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "project_assignments.employee_id"}, "alias": "emp_id"}
            ],
            "FROM":  {"table": "project_assignments"},
            "WHERE": {"EQ": [
                {"col": "project_assignments.project_id"}, {"value": 1}
            ]},
            "SET_OP": {
                "op": "EXCEPT",
                "query": {
                    "SELECT": [
                        {"expr": {"col": "project_assignments.employee_id"}, "alias": "emp_id"}
                    ],
                    "FROM":  {"table": "project_assignments"},
                    "WHERE": {"EQ": [
                        {"col": "project_assignments.project_id"}, {"value": 2}
                    ]},
                    "LIMIT": {"value": 100},
                },
            },
            "LIMIT": {"value": 20},
        },
        dialect=_set_op_dialect(),
        policy=standard_policy(),
        runtime_params={},
    ),
]
