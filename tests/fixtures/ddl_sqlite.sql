-- SQLite DDL for BrinkQL integration tests
-- BOOLEAN stored as INTEGER (1=true, 0=false)
-- DATE / TIMESTAMP stored as TEXT (ISO-8601)
-- JSON stored as TEXT
-- Enum-like fields stored as TEXT with CHECK constraints
-- Composite primary keys on employee_skills and project_assignments

CREATE TABLE IF NOT EXISTS companies (
    company_id   INTEGER PRIMARY KEY,
    tenant_id    TEXT    NOT NULL,
    name         TEXT    NOT NULL,
    industry     TEXT,
    founded_year INTEGER,
    active       INTEGER NOT NULL DEFAULT 1
                 CHECK (active IN (0, 1)),
    metadata     TEXT,           -- JSON stored as TEXT
    created_at   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS departments (
    department_id INTEGER PRIMARY KEY,
    tenant_id     TEXT    NOT NULL,
    company_id    INTEGER NOT NULL,
    name          TEXT    NOT NULL,
    code          TEXT    NOT NULL,
    budget        REAL,           -- nullable
    headcount     INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id     INTEGER PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    company_id      INTEGER NOT NULL,
    department_id   INTEGER,      -- nullable: unassigned employees
    first_name      TEXT    NOT NULL,
    last_name       TEXT    NOT NULL,
    middle_name     TEXT,         -- nullable
    email           TEXT    NOT NULL,
    phone           TEXT,         -- nullable
    employment_type TEXT    NOT NULL
                    CHECK (employment_type IN ('full_time', 'part_time', 'contractor')),
    salary          REAL,         -- nullable (contractors may have no fixed salary)
    hire_date       TEXT    NOT NULL,
    birth_date      TEXT,         -- nullable
    active          INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    remote          INTEGER NOT NULL DEFAULT 0 CHECK (remote IN (0, 1)),
    manager_id      INTEGER,      -- nullable self-referential
    notes           TEXT,         -- nullable (can also be empty string)
    FOREIGN KEY (company_id)    REFERENCES companies(company_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id),
    FOREIGN KEY (manager_id)    REFERENCES employees(employee_id)
);

CREATE TABLE IF NOT EXISTS skills (
    skill_id INTEGER PRIMARY KEY,
    name     TEXT NOT NULL,
    category TEXT NOT NULL  -- programming, management, soft_skill, etc.
);

CREATE TABLE IF NOT EXISTS employee_skills (
    employee_id INTEGER NOT NULL,
    skill_id    INTEGER NOT NULL,
    proficiency INTEGER NOT NULL DEFAULT 1 CHECK (proficiency BETWEEN 1 AND 5),
    PRIMARY KEY (employee_id, skill_id),        -- composite PK
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (skill_id)    REFERENCES skills(skill_id)
);

CREATE TABLE IF NOT EXISTS projects (
    project_id INTEGER PRIMARY KEY,
    tenant_id  TEXT    NOT NULL,
    company_id INTEGER NOT NULL,
    name       TEXT    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'planning'
               CHECK (status IN ('planning', 'active', 'completed', 'cancelled')),
    budget     REAL,             -- nullable
    start_date TEXT,             -- nullable DATE
    end_date   TEXT,             -- nullable DATE
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

CREATE TABLE IF NOT EXISTS project_assignments (
    project_id     INTEGER NOT NULL,
    employee_id    INTEGER NOT NULL,
    role           TEXT,
    hours_per_week REAL,          -- nullable
    PRIMARY KEY (project_id, employee_id),      -- composite PK
    FOREIGN KEY (project_id)  REFERENCES projects(project_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE IF NOT EXISTS salary_history (
    history_id     INTEGER PRIMARY KEY,
    employee_id    INTEGER NOT NULL,
    salary         REAL    NOT NULL,
    effective_date TEXT    NOT NULL,
    reason         TEXT,          -- nullable
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);
