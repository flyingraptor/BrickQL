"""SQLite dialect compiler."""
from __future__ import annotations

from bricksql.compile.base import SQLCompiler


class SQLiteCompiler(SQLCompiler):
    """Compiles QueryPlan to SQLite-flavoured parameterized SQL.

    Parameter style: ``:name`` – compatible with Python's built-in
    ``sqlite3`` named-parameter execution (``cursor.execute(sql, dict)``).

    Note: SQLite does not support ``ILIKE``; it is mapped to ``LIKE``.
    SQLite's ``LIKE`` is case-insensitive for ASCII by default.
    """

    @property
    def dialect_name(self) -> str:
        return "sqlite"

    def param_placeholder(self, name: str) -> str:
        return f":{name}"

    def like_operator(self, op: str) -> str:
        return "LIKE"  # SQLite has no ILIKE; fall back to LIKE

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'
