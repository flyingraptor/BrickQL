<p align="center">
  <img src="logo.svg" alt="brickQL" width="420" />
</p>

# Description

**Policy-driven, SQL-standard-aligned query orchestration for LLM planners.**

> Build Queries. Don't Generate Them.

brickQL separates concerns cleanly: the LLM outputs a structured **QueryPlan (JSON)**; brickQL validates it against your schema, enforces policy rules, and compiles it to safe, parameterized SQL. Raw SQL never touches the LLM.

---

## How it works

<p align="center">
  <img src="docs/how-it-works.png" alt="brickQL flow diagram" width="500" />
</p>

---

## Installation

```bash
# Core library (SQLite only)
pip install brickql

# With PostgreSQL driver (psycopg v3)
pip install "brickql[postgres]"
```

Requires Python ≥ 3.10.

---

## Quick start

```python
import brickql
from brickql import SchemaSnapshot, DialectProfile, PolicyConfig, TablePolicy

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

compiled = brickql.validate_and_compile(plan_json, snapshot, dialect, policy)

# 5. Execute with your own connection — brickQL does not execute queries
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

Each expression field (`expr`, `GROUP_BY` items, `ORDER_BY` items, window `partition_by`) is parsed into a **typed operand** automatically by Pydantic:

| JSON shape | Python type |
|---|---|
| `{"col": "t.col"}` | `ColumnOperand(col="t.col")` |
| `{"value": 42}` | `ValueOperand(value=42)` |
| `{"param": "TENANT"}` | `ParamOperand(param="TENANT")` |
| `{"func": "COUNT", "args": [...]}` | `FuncOperand(func="COUNT", args=[...])` |
| `{"case": {"when": [...], "else": ...}}` | `CaseOperand(case=CaseBody(...))` |

These types are importable if you need to inspect or construct plans programmatically:

```python
from brickql import ColumnOperand, ValueOperand, ParamOperand, FuncOperand, CaseOperand, Operand
```

### SchemaSnapshot

Describes your database structure: tables, columns (name, type, nullability), and named relationships. It is purely structural — no policy or access-control concerns. Loaded once at startup and shared across requests.

Both `TableInfo` and `ColumnInfo` accept an optional `description` field. When present, descriptions are included in the LLM system prompt so the model can make better join and filter decisions without guessing from column names alone.

```python
snapshot = SchemaSnapshot.model_validate({
    "tables": [
        {
            "name": "employees",
            "description": "One row per employee. Joined to departments via department_id.",
            "columns": [
                {"name": "employee_id", "type": "INTEGER", "nullable": False},
                {"name": "tenant_id",   "type": "TEXT",    "nullable": False},
                {"name": "status",      "type": "TEXT",    "nullable": True,
                 "description": "Employment status. Values: ACTIVE, TERMINATED, ON_LEAVE."},
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

#### Reflecting a schema from a live database

Use `schema_from_sqlalchemy` to populate a `SchemaSnapshot` directly from an existing database instead of writing the JSON by hand:

```python
from sqlalchemy import create_engine
from brickql import schema_from_sqlalchemy

engine = create_engine("postgresql+psycopg://user:pass@localhost:5432/mydb")
snapshot = schema_from_sqlalchemy(engine)
```

`schema_from_sqlalchemy` requires the `sqlalchemy` optional dependency:

```bash
pip install "brickql[sqlalchemy]"
```

The reflected snapshot is a starting point — add `description` fields and manually define any relationships that naming heuristics cannot detect, then save it to a JSON file for inspection and version control.

### DialectProfile — builder

Compose exactly the SQL features you need. Each method is independent — no hidden stacking, no implicit dependencies:

| Builder method | SQL capabilities unlocked | Requires |
|---|---|---|
| *(base)* | Single-table `SELECT` / `WHERE` / `LIMIT` | — |
| `.joins(max_join_depth=2)` | `JOIN` (inner, left, self-referential, many-to-many), `ORDER BY`, `OFFSET`, `ILIKE` | — |
| `.aggregations()` | `GROUP BY` / `HAVING` / `COUNT` `SUM` `AVG` `MIN` `MAX` / `CASE` | — |
| `.scalar_functions(*funcs)` | Additional scalar functions by name (e.g. `DATE_PART`, `COALESCE`) | — |
| `.subqueries()` | `EXISTS`, correlated and derived-table subqueries | — |
| `.ctes()` | `WITH` / `WITH RECURSIVE` | **`.subqueries()`** |
| `.set_operations()` | `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` | — |
| `.window_functions()` | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, `OVER`, `PARTITION BY` + aggregate window functions | **`.aggregations()`** |

`.scalar_functions()` is additive and can be chained with any other method:

```python
profile = (
    DialectProfile.builder(tables, target="postgres")
    .aggregations()
    .scalar_functions("DATE_PART", "COALESCE")
    .ctes()
    .subqueries()
    .build()
)
```

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
from brickql import PolicyConfig, TablePolicy

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
components = brickql.get_prompt_components(
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

All errors are subclasses of `brickQLError` and carry a machine-readable `code` and `details` dict — designed for LLM repair loops.

```python
from brickql import ParseError, ValidationError, CompilationError

try:
    compiled = brickql.validate_and_compile(plan_json, snapshot, dialect, policy)
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

## Extensibility

### Adding a new dialect

Register a custom `SQLCompiler` subclass once; `validate_and_compile` picks it up automatically for any `DialectProfile` with that target:

```python
from brickql.compile.base import SQLCompiler
from brickql.compile.registry import CompilerFactory

@CompilerFactory.register("mysql")
class MySQLCompiler(SQLCompiler):
    @property
    def dialect_name(self) -> str:
        return "mysql"

    def param_placeholder(self, name: str) -> str:
        return f"%({name})s"

    def like_operator(self, op: str) -> str:
        return op

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

# Now you can use target="mysql" in DialectProfile.builder(...)
```

#### Customising function compilation per dialect

Override `build_func_call` to control how specific functions are rendered for your dialect — inline literal args, add type casts, rename functions, etc. The default renders `FUNC(arg1, arg2, …)`:

```python
from typing import Any, Callable

@CompilerFactory.register("mysql")
class MySQLCompiler(SQLCompiler):
    # ... required abstract methods ...

    def build_func_call(
        self,
        func_name: str,
        args: list[Any],
        build_arg: Callable[[Any], str],
    ) -> str:
        if func_name.upper() == "DATE_PART":
            # MySQL uses YEAR(col) instead of DATE_PART('year', col)
            return f"YEAR({build_arg(args[1])})"
        return super().build_func_call(func_name, args, build_arg)
```

`build_arg` is a callback that compiles a single typed `Operand` to SQL, so the full operand chain (column quoting, param binding, nested functions) works correctly for any arg you forward.

### Adding a new operator

Register a rendering handler; `OperatorRegistry` wires it in without touching the built-in `PredicateBuilder`:

```python
from brickql.compile.registry import OperatorRegistry

@OperatorRegistry.register("REGEXP")
def _regexp_handler(op, args, build_operand):
    left  = build_operand(args[0])
    right = build_operand(args[1])
    return f"{left} REGEXP {right}"
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
brickql/
  schema/
    expressions.py        # Operator/operand enums and frozenset constants
    operands.py           # Typed operand models (ColumnOperand, ValueOperand, …) + Operand union
    query_plan.py         # QueryPlan Pydantic model + domain methods (collect_col_refs, …)
    snapshot.py           # SchemaSnapshot, TableInfo, ColumnInfo, RelationshipInfo
    dialect.py            # DialectProfile + DialectProfileBuilder (fluent API)
    column_reference.py   # ColumnReference — parse + validate table.column strings
    context.py            # ValidationContext value object (snapshot + dialect)
  validate/
    validator.py          # PlanValidator — orchestrates all sub-validators
    dialect_validator.py  # Feature-flag checks (CTE, subquery, join depth, window)
    schema_validator.py   # Table / column existence, JOIN relationship keys
    semantic_validator.py # HAVING/GROUP_BY pairing, LIMIT range
    operand_validator.py  # OperandValidator + PredicateValidator (mutually recursive)
  policy/
    engine.py             # PolicyEngine, PolicyConfig, TablePolicy
  compile/
    base.py               # SQLCompiler ABC + CompiledSQL dataclass
    registry.py           # CompilerFactory + OperatorRegistry (OCP extension points)
    context.py            # CompilationContext value object (compiler + snapshot)
    expression_builder.py # RuntimeContext + OperandBuilder + PredicateBuilder
    clause_builders.py    # SelectClause / From / Join / Window / CTE / SetOp builders
    builder.py            # QueryBuilder — orchestrates all sub-builders
    postgres.py           # PostgresCompiler  (%(name)s placeholders, ILIKE, DATE_PART specialisation)
    sqlite.py             # SQLiteCompiler    (:name placeholders, LIKE fallback)
  prompt/
    builder.py            # PromptBuilder + PromptComponents
  errors.py               # Exception hierarchy (brickQLError and subclasses)
examples/
  cases/                  # Named test cases (c01–c10) covering SQL feature categories
  trials/                 # Runtime trial outputs — gitignored, generated locally
  runner.py               # CLI: run cases against SQLite/Postgres with optional Ollama
  _case.py                # Case dataclass definition
  _ollama.py              # Thin Ollama client (langchain-ollama)
  _seed.py                # In-memory SQLite seeder
  _setup.py               # Shared schema snapshot, dialect, and policy helpers
  _trial.py               # TrialResult serialisation
  README.md               # Examples usage guide
docs/
  how-it-works.mmd        # Simple end-to-end flow (Mermaid)
  how-it-works.png        # Rendered architecture diagram (used in README)
  how-it-works.excalidraw # Visual flow diagram (Excalidraw)
  flow-diagram.mmd        # Detailed processing flow with sub-component breakdown (Mermaid)
  class-diagram.mmd       # Full class relations (Mermaid)
  code-walkthrough.md     # Per-file explanations with full call-sequence trace
tests/
  fixtures/               # schema.json, ddl_sqlite.sql, ddl_postgres.sql
  integration/            # SQLite (in-memory) and PostgreSQL (Docker) integration tests
docker-compose.yml        # PostgreSQL service for integration tests
pyproject.toml            # Package metadata, dependencies, ruff, mypy config
Makefile                  # Development task runner
```

---

## License

MIT — see [LICENSE](LICENSE).
