-- PostgreSQL DDL for brickQL integration tests
-- BOOLEAN native type (TRUE / FALSE)
-- DATE / TIMESTAMP WITH TIME ZONE native types
-- JSONB for structured metadata
-- TEXT with CHECK constraints for enum-like fields
-- Composite primary keys on employee_skills and project_assignments
-- BIGINT PRIMARY KEY (explicit IDs inserted in tests, no SERIAL needed)

CREATE TABLE IF NOT EXISTS companies (
    company_id   BIGINT PRIMARY KEY,
    tenant_id    TEXT        NOT NULL,
    name         TEXT        NOT NULL,
    industry     TEXT,
    founded_year INTEGER,
    active       BOOLEAN     NOT NULL DEFAULT TRUE,
    metadata     JSONB,
    created_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS departments (
    department_id BIGINT PRIMARY KEY,
    tenant_id     TEXT    NOT NULL,
    company_id    BIGINT  NOT NULL REFERENCES companies(company_id),
    name          TEXT    NOT NULL,
    code          TEXT    NOT NULL,
    budget        NUMERIC,        -- nullable
    headcount     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id     BIGINT  PRIMARY KEY,
    tenant_id       TEXT    NOT NULL,
    company_id      BIGINT  NOT NULL REFERENCES companies(company_id),
    department_id   BIGINT  REFERENCES departments(department_id),  -- nullable
    first_name      TEXT    NOT NULL,
    last_name       TEXT    NOT NULL,
    middle_name     TEXT,           -- nullable
    email           TEXT    NOT NULL,
    phone           TEXT,           -- nullable
    employment_type TEXT    NOT NULL
                    CHECK (employment_type IN ('full_time', 'part_time', 'contractor')),
    salary          NUMERIC,        -- nullable
    hire_date       DATE    NOT NULL,
    birth_date      DATE,           -- nullable
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    remote          BOOLEAN NOT NULL DEFAULT FALSE,
    manager_id      BIGINT  REFERENCES employees(employee_id),   -- self-referential
    notes           TEXT            -- nullable (may be empty string)
);

CREATE TABLE IF NOT EXISTS skills (
    skill_id INTEGER PRIMARY KEY,
    name     TEXT NOT NULL,
    category TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS employee_skills (
    employee_id BIGINT  NOT NULL REFERENCES employees(employee_id),
    skill_id    INTEGER NOT NULL REFERENCES skills(skill_id),
    proficiency INTEGER NOT NULL DEFAULT 1 CHECK (proficiency BETWEEN 1 AND 5),
    PRIMARY KEY (employee_id, skill_id)         -- composite PK
);

CREATE TABLE IF NOT EXISTS projects (
    project_id BIGINT PRIMARY KEY,
    tenant_id  TEXT   NOT NULL,
    company_id BIGINT NOT NULL REFERENCES companies(company_id),
    name       TEXT   NOT NULL,
    status     TEXT   NOT NULL DEFAULT 'planning'
               CHECK (status IN ('planning', 'active', 'completed', 'cancelled')),
    budget     NUMERIC,         -- nullable
    start_date DATE,            -- nullable
    end_date   DATE             -- nullable
);

CREATE TABLE IF NOT EXISTS project_assignments (
    project_id     BIGINT  NOT NULL REFERENCES projects(project_id),
    employee_id    BIGINT  NOT NULL REFERENCES employees(employee_id),
    role           TEXT,
    hours_per_week NUMERIC,     -- nullable
    PRIMARY KEY (project_id, employee_id)       -- composite PK
);

CREATE TABLE IF NOT EXISTS salary_history (
    history_id     BIGSERIAL PRIMARY KEY,
    employee_id    BIGINT  NOT NULL REFERENCES employees(employee_id),
    salary         NUMERIC NOT NULL,
    effective_date DATE    NOT NULL,
    reason         TEXT            -- nullable
);
