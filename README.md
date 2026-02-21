# BrinkQL

**Policy-driven, SQL-standard-aligned query orchestration for LLM planners.**

> Build Queries. Don't Generate Them.

BrinkQL separates concerns cleanly: the LLM outputs a structured **QueryPlan (JSON)**; BrinkQL validates it against your schema, enforces policy rules, and compiles it to safe, parameterized SQL. Raw SQL never touches the LLM.

---

## How it works

```
User question
     │
     ▼
  LLM Planner  ──►  QueryPlan JSON
                          │
                    ┌─────▼──────┐
                    │ BrinkQL   │
                    │            │
                    │  1. Parse  │  Pydantic model validation
                    │  2. Validate│  Schema + dialect rules
                    │  3. Policy │  Param injection, limits
                    │  4. Compile│  → parameterized SQL
                    └─────┬──────┘
                          │
                     CompiledSQL
                    (sql + params)
                          │
                          ▼
                    Your DB executor
```

---

## Installation

```bash
# Core library (SQLite only)
pip install brinkql

# With PostgreSQL driver (psycopg v3)
pip install "brinkql[postgres]"
```

Requires Python ≥ 3.10.

---

## Quick start

```python
import brinkql
from brinkql import SchemaSnapshot, DialectProfile, PolicyConfig, TablePolicy

# 1. Load your schema snapshot (describes tables, columns, relationships)
import json
snapshot = SchemaSnapshot.model_validate(json.loads(open("schema.json").read()))

# 2. Choose a dialect profile (compose exactly the features you need)
dialect = (
    DialectProfile.builder(["employees", "departments"], target="postgres")
    .joins()
    .aggregations()
    .build()
)

# 3. Configure policy (tenant isolation, row limits)
policy = PolicyConfig(
    inject_missing_params=True,  # auto-inject tenant_id predicates
    default_limit=100,
    tables={
        "employees":   TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
        "departments": TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
    },
)

# 4. Compile the LLM's QueryPlan JSON
plan_json = llm_response  # {"SELECT": [...], "FROM": {...}, "JOIN": [...], ...}

compiled = brinkql.validate_and_compile(plan_json, snapshot, dialect, policy)

# 5. Execute with your own connection — BrinkQL does not execute queries
cursor.execute(compiled.sql, compiled.merge_runtime_params({"TENANT": tenant_id}))
```

---

## Key concepts

### QueryPlan JSON

The only output the LLM must produce. A structured, SQL-grammar-aligned JSON object — never raw SQL.

```json
{
  "SELECT": [
    {"expr": {"col": "employees.first_name"}},
    {"expr": {"col": "departments.name"}, "alias": "dept"}
  ],
  "FROM": {"table": "employees"},
  "JOIN": [{"rel": "departments__employees", "type": "LEFT"}],
  "WHERE": {"EQ": [{"col": "employees.active"}, {"value": true}]},
  "LIMIT": {"value": 50}
}
```

### SchemaSnapshot

Describes your database structure: tables, columns (name, type, nullability), and named relationships. It is purely structural — no policy or access-control concerns. Loaded once at startup and shared across requests.

```python
snapshot = SchemaSnapshot.model_validate({
    "tables": [
        {
            "name": "employees",
            "columns": [
                {"name": "employee_id", "type": "INTEGER", "nullable": False},
                {"name": "tenant_id",   "type": "TEXT",    "nullable": False},
                {"name": "first_name",  "type": "TEXT",    "nullable": False},
            ],
            "relationships": ["departments__employees"]
        }
    ],
    "relationships": [
        {"key": "departments__employees", "from_table": "employees",
         "from_col": "department_id", "to_table": "departments", "to_col": "department_id"}
    ]
})
```

> **Note** — `tenant_id` is just a regular column in the snapshot. Which columns
> require runtime parameters and what those params are named is configured in
> `PolicyConfig` via `TablePolicy`, not in the schema.

### DialectProfile — builder

Compose exactly the SQL features you need. Each method is independent — no hidden stacking, no implicit dependencies:

| Builder method | SQL capabilities unlocked | Requires |
|---|---|---|
| *(base)* | Single-table `SELECT` / `WHERE` / `LIMIT` | — |
| `.joins(max_join_depth=2)` | `JOIN` (inner, left, self-referential, many-to-many), `ORDER BY`, `OFFSET`, `ILIKE` | — |
| `.aggregations()` | `GROUP BY` / `HAVING` / `COUNT` `SUM` `AVG` `MIN` `MAX` / `CASE` | — |
| `.subqueries()` | `EXISTS`, correlated and derived-table subqueries | — |
| `.ctes()` | `WITH` / `WITH RECURSIVE` | **`.subqueries()`** |
| `.set_operations()` | `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` | — |
| `.window_functions()` | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, `OVER`, `PARTITION BY` + aggregate window functions | **`.aggregations()`** |

Dependencies are enforced at `build()` time with a `ProfileConfigError` and a clear message.

```python
# Joins + aggregations only
profile = (
    DialectProfile.builder(tables, target="postgres")
    .joins(max_join_depth=2)
    .aggregations()
    .build()
)

# Window functions without join support
profile = (
    DialectProfile.builder(tables, target="sqlite")
    .aggregations()
    .window_functions()
    .build()
)

# Everything
profile = (
    DialectProfile.builder(tables)
    .joins()
    .aggregations()
    .subqueries()
    .ctes()
    .set_operations()
    .window_functions()
    .build()
)
```

### PolicyConfig and TablePolicy

`PolicyConfig` controls the overall request policy. `TablePolicy` configures
per-table rules — each table can have its own param-bound columns and denied columns.

```python
from brinkql import PolicyConfig, TablePolicy

policy = PolicyConfig(
    inject_missing_params=True,
    default_limit=200,
    tables={
        "companies":   TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
        "departments": TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
        "employees":   TablePolicy(
            param_bound_columns={"tenant_id": "TENANT"},
            denied_columns=["salary"],
        ),
        "projects":    TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
    },
)
```

Different tables can use **different param names**:

```python
policy = PolicyConfig(
    tables={
        "employees": TablePolicy(param_bound_columns={"tenant_id": "TENANT"}),
        "audit_log": TablePolicy(param_bound_columns={"org_id": "ORG"}),
    }
)
params = compiled.merge_runtime_params({"TENANT": "acme", "ORG": "acme-org-42"})
```

### CompiledSQL

The output of `validate_and_compile`. Contains the parameterized SQL string and a `params` dict. Runtime parameters (e.g. `TENANT`) are merged in before execution:

```python
sql_params = compiled.merge_runtime_params({"TENANT": "acme"})
cursor.execute(compiled.sql, sql_params)
```

---

## Prompting the LLM

```python
components = brinkql.get_prompt_components(
    snapshot=snapshot,
    dialect=dialect,
    question="List the top 5 highest-paid employees in Engineering",
    policy_summary='Always filter by tenant_id using {"param": "TENANT"}.',
)

# Send to your LLM
response = llm.chat(system=components.system_prompt, user=components.user_prompt)
```

---

## Error handling

All errors are subclasses of `BrinkQLError` and carry a machine-readable `code` and `details` dict — designed for LLM repair loops.

```python
from brinkql import ParseError, ValidationError, CompilationError

try:
    compiled = brinkql.validate_and_compile(plan_json, snapshot, dialect, policy)
except ParseError as e:
    # Malformed JSON or invalid QueryPlan structure
    pass
except ValidationError as e:
    # Schema or dialect rule violated — pass e.to_error_response() back to LLM
    pass
except CompilationError as e:
    raise
```

---

## Development

```bash
# Set up virtual environment and install all dev dependencies
make install

# Lint
make lint

# Auto-format
make fmt

# Type check
make typecheck

# Unit tests only (no database required)
make test-unit

# Unit + SQLite integration tests (in-memory, no Docker)
make test

# All tests including PostgreSQL (requires Docker)
make test-integration-postgres
```

---

## Repository layout

```
brinkql/
  schema/           # QueryPlan, SchemaSnapshot, DialectProfile, expression constants
  validate/         # PlanValidator — structural, semantic, dialect checks
  policy/           # PolicyEngine, PolicyConfig — param injection and limits
  compile/          # QueryBuilder, PostgresCompiler, SQLiteCompiler → parameterized SQL
  prompt/           # PromptBuilder → system + user prompts for the LLM
  errors.py         # Exception hierarchy (BrinkQLError and subclasses)
tests/
  fixtures/         # schema.json, ddl_sqlite.sql, ddl_postgres.sql
  integration/      # SQLite (in-memory) and PostgreSQL (Docker) integration tests
docker-compose.yml  # PostgreSQL service for integration tests
pyproject.toml      # Package metadata, dependencies, ruff, mypy config
Makefile            # Development task runner
```

---

## License

MIT — see [LICENSE](LICENSE).
