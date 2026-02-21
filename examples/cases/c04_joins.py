"""Category 04 — JOINs.

All join types: one-to-many, left, self-referential (manager hierarchy),
many-to-many (employees ↔ skills), three-table, and joins combined with
WHERE / ORDER BY.

Note on self-referential joins
-------------------------------
BrinkQL v1 resolves column references against the SchemaSnapshot table names,
not against JOIN aliases.  For a self-referential join
``FROM employees LEFT JOIN employees AS mgr ON …``
the SELECT can reference ``employees.*`` columns directly, but NOT ``mgr.*``
(since "mgr" is not a table in the snapshot).  To access manager attributes
by alias, use a CTE or subquery (see c07 / c06 examples).
"""
from __future__ import annotations

from examples._case import Case
from examples._setup import joins_dialect, standard_policy

_pol = standard_policy()
_dl = joins_dialect()
_RT = {"TENANT": "acme"}

CASES: list[Case] = [
    # ------------------------------------------------------------------
    Case(
        id="c04_01",
        category="joins",
        question="List employees with the name of their department.",
        notes=(
            "Classic one-to-many INNER JOIN via the 'departments__employees' "
            "relationship key. Employees without a department (Henry, id=8) "
            "are excluded by INNER — to include them use LEFT JOIN."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "departments.name"}, "alias": "department"},
            ],
            "FROM": {"table": "employees"},
            "JOIN": [{"rel": "departments__employees", "type": "INNER"}],
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.last_name"}, "direction": "ASC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c04_02",
        category="joins",
        question=(
            "Show all employees including those without a manager. "
            "Include the employee id and their manager id."
        ),
        notes=(
            "Self-referential LEFT JOIN using 'employees__manager' with alias 'mgr'. "
            "An INNER JOIN would exclude Alice, Dave, Grace, Frank (no manager_id). "
            "The alias 'mgr' is used only to disambiguate the ON clause; column refs "
            "in SELECT still use 'employees.*'."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.employee_id"}, "alias": "id"},
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "employees.manager_id"}},
            ],
            "FROM": {"table": "employees"},
            "JOIN": [{"rel": "employees__manager", "type": "LEFT", "alias": "mgr"}],
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.employee_id"}, "direction": "ASC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c04_03",
        category="joins",
        question=(
            "Show each employee with both their department name "
            "and the company name."
        ),
        notes=(
            "Three-table join chain: employees → departments → companies. "
            "Two JOINs, max_join_depth must be ≥ 2."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "departments.name"}, "alias": "department"},
                {"expr": {"col": "companies.name"},   "alias": "company"},
            ],
            "FROM": {"table": "employees"},
            "JOIN": [
                {"rel": "departments__employees", "type": "LEFT"},
                {"rel": "companies__employees",   "type": "INNER"},
            ],
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.last_name"}, "direction": "ASC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c04_04",
        category="joins",
        question=(
            "List all employees with the skills they have and their proficiency score."
        ),
        notes=(
            "Many-to-many via the junction table employee_skills. "
            "FROM must be the junction table (employee_skills) — not employees — "
            "because the 'employees__employee_skills' relationship key joins employees "
            "as the to_table. Starting FROM employees would create a duplicate "
            "'employees' reference and cause an ambiguous column error."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "skills.name"},              "alias": "skill"},
                {"expr": {"col": "employee_skills.proficiency"}},
            ],
            "FROM": {"table": "employee_skills"},
            "JOIN": [
                {"rel": "employees__employee_skills", "type": "INNER"},
                {"rel": "employee_skills__skills",    "type": "INNER"},
            ],
            "WHERE": {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
            "ORDER_BY": [
                {"expr": {"col": "employees.last_name"},      "direction": "ASC"},
                {"expr": {"col": "employee_skills.proficiency"}, "direction": "DESC"},
            ],
            "LIMIT": {"value": 100},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c04_05",
        category="joins",
        question=(
            "Show employees assigned to active projects, "
            "including the project name and their role."
        ),
        notes=(
            "FROM is the junction table (project_assignments), which is then joined "
            "to both employees and projects. The relationship keys join employees and "
            "projects as to_tables, so starting FROM project_assignments avoids "
            "duplicate table references."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "projects.name"},             "alias": "project"},
                {"expr": {"col": "project_assignments.role"}},
                {"expr": {"col": "project_assignments.hours_per_week"}, "alias": "hours"},
            ],
            "FROM": {"table": "project_assignments"},
            "JOIN": [
                {"rel": "employees__project_assignments", "type": "INNER"},
                {"rel": "projects__project_assignments",  "type": "INNER"},
            ],
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"}, {"param": "TENANT"}]},
                    {"EQ": [{"col": "projects.status"}, {"value": "active"}]},
                ]
            },
            "ORDER_BY": [
                {"expr": {"col": "projects.name"}, "direction": "ASC"},
            ],
            "LIMIT": {"value": 50},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
    # ------------------------------------------------------------------
    Case(
        id="c04_06",
        category="joins",
        question=(
            "Show active full-time employees in the Engineering department "
            "(department_id = 1) with their salary, from highest to lowest."
        ),
        notes=(
            "JOIN + multi-condition WHERE + ORDER BY. Tests combining a join "
            "with several filter predicates — a common real-world pattern."
        ),
        expected_plan={
            "SELECT": [
                {"expr": {"col": "employees.first_name"}},
                {"expr": {"col": "employees.last_name"}},
                {"expr": {"col": "departments.name"}, "alias": "department"},
                {"expr": {"col": "employees.salary"}},
            ],
            "FROM": {"table": "employees"},
            "JOIN": [{"rel": "departments__employees", "type": "INNER"}],
            "WHERE": {
                "AND": [
                    {"EQ": [{"col": "employees.tenant_id"},       {"param": "TENANT"}]},
                    {"EQ": [{"col": "employees.active"},          {"value": 1}]},
                    {"EQ": [{"col": "employees.employment_type"}, {"value": "full_time"}]},
                    {"EQ": [{"col": "departments.name"},          {"value": "Engineering"}]},
                ]
            },
            "ORDER_BY": [
                {"expr": {"col": "employees.salary"}, "direction": "DESC"},
            ],
            "LIMIT": {"value": 20},
        },
        dialect=_dl,
        policy=_pol,
        runtime_params=_RT,
    ),
]
