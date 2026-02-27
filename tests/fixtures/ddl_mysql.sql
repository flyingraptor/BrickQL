-- MySQL DDL for brickQL integration tests
-- BOOLEAN stored as TINYINT(1) (1=true, 0=false)
-- DATE / DATETIME native types (no TIMESTAMPTZ)
-- JSON for structured metadata
-- DECIMAL for numeric values
-- TEXT with CHECK constraints for enum-like fields (MySQL 8.0.16+)
-- Composite primary keys on employee_skills and project_assignments
-- BIGINT AUTO_INCREMENT for auto-generated IDs

CREATE TABLE IF NOT EXISTS companies (
    company_id   BIGINT       NOT NULL,
    tenant_id    VARCHAR(255) NOT NULL,
    name         VARCHAR(255) NOT NULL,
    industry     VARCHAR(255),
    founded_year INT,
    active       TINYINT(1)   NOT NULL DEFAULT 1,
    metadata     JSON,
    created_at   DATETIME     NOT NULL,
    PRIMARY KEY (company_id)
);

CREATE TABLE IF NOT EXISTS departments (
    department_id BIGINT       NOT NULL,
    tenant_id     VARCHAR(255) NOT NULL,
    company_id    BIGINT       NOT NULL,
    name          VARCHAR(255) NOT NULL,
    code          VARCHAR(50)  NOT NULL,
    budget        DECIMAL(15,2),
    headcount     INT          NOT NULL DEFAULT 0,
    PRIMARY KEY (department_id),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id     BIGINT       NOT NULL,
    tenant_id       VARCHAR(255) NOT NULL,
    company_id      BIGINT       NOT NULL,
    department_id   BIGINT,
    first_name      VARCHAR(255) NOT NULL,
    last_name       VARCHAR(255) NOT NULL,
    middle_name     VARCHAR(255),
    email           VARCHAR(255) NOT NULL,
    phone           VARCHAR(50),
    employment_type VARCHAR(50)  NOT NULL
                    CHECK (employment_type IN ('full_time', 'part_time', 'contractor')),
    salary          DECIMAL(15,2),
    hire_date       DATE         NOT NULL,
    birth_date      DATE,
    active          TINYINT(1)   NOT NULL DEFAULT 1,
    remote          TINYINT(1)   NOT NULL DEFAULT 0,
    manager_id      BIGINT,
    notes           TEXT,
    PRIMARY KEY (employee_id),
    FOREIGN KEY (company_id)    REFERENCES companies(company_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id),
    FOREIGN KEY (manager_id)    REFERENCES employees(employee_id)
);

CREATE TABLE IF NOT EXISTS skills (
    skill_id INTEGER      NOT NULL,
    name     VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    PRIMARY KEY (skill_id)
);

CREATE TABLE IF NOT EXISTS employee_skills (
    employee_id BIGINT  NOT NULL,
    skill_id    INTEGER NOT NULL,
    proficiency INTEGER NOT NULL DEFAULT 1 CHECK (proficiency BETWEEN 1 AND 5),
    PRIMARY KEY (employee_id, skill_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (skill_id)    REFERENCES skills(skill_id)
);

CREATE TABLE IF NOT EXISTS projects (
    project_id BIGINT       NOT NULL,
    tenant_id  VARCHAR(255) NOT NULL,
    company_id BIGINT       NOT NULL,
    name       VARCHAR(255) NOT NULL,
    status     VARCHAR(50)  NOT NULL DEFAULT 'planning'
               CHECK (status IN ('planning', 'active', 'completed', 'cancelled')),
    budget     DECIMAL(15,2),
    start_date DATE,
    end_date   DATE,
    PRIMARY KEY (project_id),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

CREATE TABLE IF NOT EXISTS project_assignments (
    project_id     BIGINT       NOT NULL,
    employee_id    BIGINT       NOT NULL,
    role           VARCHAR(100),
    hours_per_week DECIMAL(6,2),
    PRIMARY KEY (project_id, employee_id),
    FOREIGN KEY (project_id)  REFERENCES projects(project_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE IF NOT EXISTS salary_history (
    history_id     BIGINT NOT NULL AUTO_INCREMENT,
    employee_id    BIGINT NOT NULL,
    salary         DECIMAL(15,2) NOT NULL,
    effective_date DATE   NOT NULL,
    reason         VARCHAR(255),
    PRIMARY KEY (history_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);
