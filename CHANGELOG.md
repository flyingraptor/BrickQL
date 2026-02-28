# Changelog

## [0.1.8] — 2026-02-28

### Fixed
- README images now render correctly on PyPI (switched to absolute GitHub raw URLs)

---

## [0.1.7] — 2026-02-28 — First public release

brickQL is a policy-driven query orchestration layer for LLMs. Instead of letting the model generate free-form SQL, the LLM outputs a structured `QueryPlan` JSON which brickQL validates, enforces policy rules against, and compiles to safe, parameterized SQL.

### What's included

- **QueryPlan compiler** — typed Pydantic model; free-form SQL is structurally impossible
- **Policy engine** — per-table column allowlists, deny lists, and param-bound column enforcement with OR-bypass hardening
- **Dialect profiles** — opt-in SQL feature allowlists with dependency enforcement at build time
- **Built-in compilers** for SQLite, PostgreSQL, and MySQL
- **SQLAlchemy schema reflector** — generate `SchemaSnapshot` from an existing engine
- **Structured error responses** — typed exception hierarchy with `to_error_response()` for LLM repair loops
- **Python 3.10 – 3.12** support

### Install

```bash
pip install brickql                     # SQLite
pip install "brickql[postgres]"         # + PostgreSQL
pip install "brickql[mysql]"            # + MySQL
pip install "brickql[sqlalchemy]"       # + SQLAlchemy reflector
```
