"""Seed an SQLite connection with BrinkQL example data.

Provides a richer dataset than the unit-test fixture:
  - 2 companies (tenant_id='acme' and 'globex')
  - 6 departments (3 per company)
  - 12 employees (8 acme, 4 globex, with varied types/salaries/managers)
  - 10 skills + employee_skills assignments
  - 5 projects + project_assignments
  - 10 salary_history entries
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

_DDL_PATH = (
    Path(__file__).parent.parent / "tests" / "fixtures" / "ddl_sqlite.sql"
)

_COMPANIES = [
    (1, "acme",   "Acme Corp",   "Technology", 2000, 1,
     '{"tier":"enterprise"}', "2000-01-01T00:00:00"),
    (2, "globex", "Globex Inc",  "Finance",    1995, 1,
     '{"tier":"enterprise"}', "1995-06-15T00:00:00"),
]

_DEPARTMENTS = [
    (1, "acme",   1, "Engineering",     "ENG",   500_000.0, 8),
    (2, "acme",   1, "Marketing",       "MKT",   200_000.0, 4),
    (3, "acme",   1, "Human Resources", "HR",    150_000.0, 3),
    (4, "globex", 2, "Research",        "RES",   800_000.0, 6),
    (5, "globex", 2, "Operations",      "OPS",   300_000.0, 5),
    (6, "globex", 2, "Finance",         "FIN",   400_000.0, 4),
]

# (employee_id, tenant_id, company_id, department_id,
#  first_name, last_name, middle_name, email, phone,
#  employment_type, salary, hire_date, birth_date,
#  active, remote, manager_id, notes)
_EMPLOYEES = [
    (1,  "acme",   1, 1,    "Alice",  "Smith",   None,     "alice.smith@acme.com",       "+1-555-0101",
     "full_time",  95_000.0, "2020-03-15", "1985-06-20", 1, 0, None,  None),
    (2,  "acme",   1, 1,    "Bob",    "Johnson", None,     "bob.johnson@acme.com",       "+1-555-0102",
     "full_time",  85_000.0, "2021-06-01", "1990-03-12", 1, 1, 1,     None),
    (3,  "acme",   1, 1,    "Carol",  "White",   None,     "carol.white@acme.com",       None,
     "contractor", None,     "2022-01-10", "1988-11-30", 1, 1, 1,     "Remote contractor"),
    (4,  "acme",   1, 2,    "Dave",   "Brown",   None,     "dave.brown@acme.com",        "+1-555-0104",
     "full_time",  70_000.0, "2019-08-20", "1983-04-15", 1, 0, None,  None),
    (5,  "acme",   1, 2,    "Eve",    "Davis",   None,     "eve.davis@acme.com",         "+1-555-0105",
     "part_time",  40_000.0, "2023-02-14", "1995-09-22", 1, 1, 4,     None),
    (6,  "acme",   1, 3,    "Frank",  "Miller",  None,     "frank.miller@acme.com",      None,
     "full_time",  65_000.0, "2018-11-05", "1980-07-08", 0, 0, None,  "Inactive since 2024"),
    (7,  "acme",   1, 1,    "Grace",  "Lee",     None,     "grace.lee@acme.com",         "+1-555-0107",
     "full_time",  105_000.0,"2017-04-22", "1982-01-14", 1, 0, None,  None),
    (8,  "acme",   1, None, "Henry",  "Wilson",  None,     "henry.wilson@acme.com",      None,
     "contractor", None,     "2024-01-01", "1993-08-25", 1, 1, None,  "Onboarding"),
    (9,  "globex", 2, 4,    "Iris",   "Taylor",  None,     "iris.taylor@globex.com",     "+1-555-0201",
     "full_time",  110_000.0,"2016-07-18", "1979-12-03", 1, 0, None,  None),
    (10, "globex", 2, 4,    "Jake",   "Anderson",None,     "jake.anderson@globex.com",   "+1-555-0202",
     "full_time",  95_000.0, "2019-09-30", "1987-05-17", 1, 1, 9,     None),
    (11, "globex", 2, 5,    "Karen",  "Thomas",  None,     "karen.thomas@globex.com",    None,
     "part_time",  45_000.0, "2022-05-15", "1991-02-28", 1, 0, None,  None),
    (12, "globex", 2, 6,    "Leo",    "Jackson", None,     "leo.jackson@globex.com",     "+1-555-0204",
     "full_time",  90_000.0, "2020-11-01", "1986-09-10", 1, 0, None,  None),
]

_SKILLS = [
    (1,  "Python",             "programming"),
    (2,  "JavaScript",         "programming"),
    (3,  "SQL",                "programming"),
    (4,  "Project Management", "management"),
    (5,  "Data Analysis",      "analytics"),
    (6,  "Communication",      "soft_skill"),
    (7,  "Machine Learning",   "analytics"),
    (8,  "Go",                 "programming"),
    (9,  "Leadership",         "management"),
    (10, "DevOps",             "operations"),
]

# (employee_id, skill_id, proficiency 1–5)
_EMPLOYEE_SKILLS = [
    (1, 1, 5), (1, 3, 5), (1, 4, 4),               # Alice: Python, SQL, PM
    (2, 1, 4), (2, 2, 3), (2, 3, 3),               # Bob: Python, JS, SQL
    (3, 1, 5), (3, 7, 4), (3, 2, 2),               # Carol: Python, ML, JS
    (4, 5, 4), (4, 6, 5), (4, 4, 3),               # Dave: DataAnalysis, Comm, PM
    (5, 6, 3), (5, 5, 2),                           # Eve: Comm, DataAnalysis
    (7, 1, 5), (7, 8, 4), (7, 7, 5), (7, 3, 4),   # Grace: Python, Go, ML, SQL
    (9, 3, 5), (9, 7, 5), (9, 4, 4),               # Iris: SQL, ML, PM
    (10, 1, 4), (10, 5, 4), (10, 7, 3),            # Jake: Python, DataAnalysis, ML
]

# (project_id, tenant_id, company_id, name, status, budget, start_date, end_date)
_PROJECTS = [
    (1, "acme",   1, "DataPlatform",    "active",    300_000.0, "2023-01-01", None),
    (2, "acme",   1, "Website Redesign","completed",  50_000.0, "2022-06-01", "2023-01-31"),
    (3, "acme",   1, "Mobile App",      "planning",  150_000.0, None,         None),
    (4, "globex", 2, "ML Research",     "active",    500_000.0, "2023-03-15", None),
    (5, "globex", 2, "System Upgrade",  "active",    200_000.0, "2024-01-01", None),
]

# (project_id, employee_id, role, hours_per_week)
_PROJECT_ASSIGNMENTS = [
    (1, 1,  "lead",        40.0),
    (1, 2,  "developer",   40.0),
    (1, 7,  "architect",   20.0),
    (2, 4,  "lead",        30.0),
    (2, 5,  "content",     20.0),
    (3, 1,  "lead",        10.0),
    (4, 9,  "lead",        40.0),
    (4, 10, "researcher",  40.0),
    (5, 11, "coordinator", 30.0),
    (5, 12, "analyst",     40.0),
]

# (history_id, employee_id, salary, effective_date, reason)
_SALARY_HISTORY = [
    (1,  1,  80_000.0,  "2020-03-15", "initial"),
    (2,  1,  95_000.0,  "2022-01-01", "promotion"),
    (3,  2,  80_000.0,  "2021-06-01", "initial"),
    (4,  2,  85_000.0,  "2023-07-01", "merit raise"),
    (5,  4,  65_000.0,  "2019-08-20", "initial"),
    (6,  4,  70_000.0,  "2022-03-01", "merit raise"),
    (7,  7,  95_000.0,  "2017-04-22", "initial"),
    (8,  7,  105_000.0, "2021-01-01", "promotion"),
    (9,  9,  100_000.0, "2016-07-18", "initial"),
    (10, 9,  110_000.0, "2020-01-01", "promotion"),
]


def seed_connection(conn: sqlite3.Connection) -> None:
    """Create schema and insert all sample data into *conn*."""
    conn.executescript(_DDL_PATH.read_text())

    conn.executemany("INSERT INTO companies VALUES (?,?,?,?,?,?,?,?)", _COMPANIES)
    conn.executemany("INSERT INTO departments VALUES (?,?,?,?,?,?,?)", _DEPARTMENTS)
    conn.executemany(
        "INSERT INTO employees VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        _EMPLOYEES,
    )
    conn.executemany("INSERT INTO skills VALUES (?,?,?)", _SKILLS)
    conn.executemany("INSERT INTO employee_skills VALUES (?,?,?)", _EMPLOYEE_SKILLS)
    conn.executemany("INSERT INTO projects VALUES (?,?,?,?,?,?,?,?)", _PROJECTS)
    conn.executemany(
        "INSERT INTO project_assignments VALUES (?,?,?,?)", _PROJECT_ASSIGNMENTS
    )
    conn.executemany(
        "INSERT INTO salary_history VALUES (?,?,?,?,?)", _SALARY_HISTORY
    )
    conn.commit()


def make_sqlite_conn() -> sqlite3.Connection:
    """Return a seeded in-memory SQLite connection."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    seed_connection(conn)
    return conn
