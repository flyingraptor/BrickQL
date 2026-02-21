# BrinkQL Examples

End-to-end trials that run real queries through BrinkQL against SQLite (and
optionally PostgreSQL), with Ollama generating the actual `QueryPlan` JSON for
comparison.

Each trial records:
- The **question** sent to the LLM
- The **expected** hand-crafted `QueryPlan` → compiled SQL → DB rows
- The **actual** Ollama output → compiled SQL (or error) → DB rows
- A **comparison** (SQL match, row match, BrinkQL validity)

Trials are saved as JSON in `trials/{case_id}/{timestamp}.json` and serve as
living how-to documentation.

---

## Running the examples

```bash
# Make sure the package is installed
make install

# Run all cases against SQLite (no Ollama needed)
python examples/runner.py --no-ollama

# Run all cases with Ollama (requires Ollama running locally)
python examples/runner.py

# Run only join cases
python examples/runner.py --case c04

# Run one specific case
python examples/runner.py --case c04_02 --verbose

# Show verbose SQL output
python examples/runner.py --no-ollama --verbose

# List all cases
python examples/runner.py --list

# Use a different model
python examples/runner.py --model llama3:latest
```

Ollama is auto-detected at `http://localhost:11434`. If it is not reachable the
runner falls back to `--no-ollama` mode automatically.

---

## Case categories

| Prefix | Category | What it tests |
|--------|----------|---------------|
| `c01`  | basic_select | Single-table SELECT / WHERE / LIMIT; IS_NULL, IS_NOT_NULL, BOOLEAN columns |
| `c02`  | filtering | LIKE, IN, BETWEEN, AND/OR/NOT nesting, tenant-free tables |
| `c03`  | ordering_paging | ORDER BY (single/multi), LIMIT + OFFSET, DESC sorting |
| `c04`  | joins | INNER / LEFT / self-referential / many-to-many / three-table joins |
| `c05`  | aggregations | COUNT / SUM / AVG / MIN / MAX, GROUP BY, HAVING, multi-aggregate |
| `c06`  | subqueries | EXISTS, IN-subquery, derived tables (FROM subquery), scalar subquery limitation |
| `c07`  | ctes | Simple CTE, multiple CTEs, CTE + aggregation, CTE + EXISTS |
| `c08`  | set_operations | UNION ALL, UNION, INTERSECT, EXCEPT |
| `c09`  | window_functions | RANK, ROW_NUMBER, DENSE_RANK, LAG, LEAD, SUM OVER with frame |
| `c10`  | complex | CTE + window, CASE WHEN, multi-join + HAVING, mixed boolean, top-N per group |

**Total: 46 cases.**

---

## Seed data

The runner seeds an in-memory SQLite database with the following dataset:

| Table | Rows | Notes |
|-------|------|-------|
| `companies` | 2 | `tenant_id='acme'` and `'globex'` |
| `departments` | 6 | 3 per company |
| `employees` | 12 | 8 acme (varied types, managers, remote), 4 globex |
| `skills` | 10 | programming, analytics, management, soft_skill, operations |
| `employee_skills` | 24 | varied proficiency 1–5 |
| `projects` | 5 | 3 acme, 2 globex (active/completed/planning) |
| `project_assignments` | 10 | with hours_per_week |
| `salary_history` | 10 | initial + raises for 5 employees |

---

## Trial format

```json
{
  "case_id": "c04_04",
  "category": "joins",
  "question": "List all employees with the skills they have and their proficiency score.",
  "case_notes": "...",
  "timestamp": "20260221T143000Z",
  "target_db": "sqlite",
  "ollama_model": "gpt-oss:latest",
  "expected": {
    "plan": { "SELECT": [...], "FROM": {...}, "JOIN": [...], ... },
    "sql": "SELECT ... FROM \"employees\" INNER JOIN ...",
    "params": { "param_0": "acme" },
    "rows": [ {"first_name": "Alice", "last_name": "Smith", "skill": "Python", ...} ],
    "row_count": 14,
    "error": null
  },
  "actual": {
    "plan_raw": "{ ... }",
    "plan": { ... },
    "sql": "SELECT ...",
    "params": { ... },
    "rows": [...],
    "row_count": 14,
    "error": null
  },
  "comparison": {
    "brinkql_valid": true,
    "sql_match": true,
    "rows_match": true,
    "notes": "All checks passed"
  }
}
```

---

## Project structure

```
examples/
  __init__.py           — package marker
  _case.py              — Case dataclass
  _seed.py              — SQLite seeder with sample data
  _ollama.py            — Ollama HTTP client (stdlib only)
  _trial.py             — Trial dataclass + JSON persistence
  _setup.py             — Shared snapshot / dialect / policy helpers
  runner.py             — CLI runner (entry point)
  cases/
    __init__.py         — ALL_CASES registry
    c01_basic_select.py
    c02_filtering.py
    c03_ordering_paging.py
    c04_joins.py
    c05_aggregations.py
    c06_subqueries.py
    c07_ctes.py
    c08_set_operations.py
    c09_window_functions.py
    c10_complex.py
  trials/
    .gitkeep            — created by runner; JSON trial files stored here
```

---

## Adding a new case

1. Open the relevant `cases/cXX_*.py` file (or create a new one).
2. Append a `Case(...)` to the `CASES` list.
3. If creating a new file, import and add it to `cases/__init__.py`.
4. Run the runner to verify: `python examples/runner.py --case cXX_YY --verbose`.

---

## Known limitations (BrinkQL v1)

- **Scalar subqueries in comparison operators** are not supported (`salary > (SELECT AVG …)`).
  Use a CTE or derived table instead (see `c06_04`, `c07_03`).
- **JOIN alias column references**: column refs in SELECT must use the original table name,
  not a JOIN alias (except CTE names). The self-referential join cases (`c04_02`)
  demonstrate this constraint.
- **PostgreSQL integration**: the runner only runs against SQLite by default.
  PostgreSQL support requires `docker-compose up -d` and extending the runner's
  `_run_actual` to use `psycopg`.
