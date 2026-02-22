"""PostgreSQL dialect compiler."""
from __future__ import annotations

from brickql.compile.base import SQLCompiler


class PostgresCompiler(SQLCompiler):
    """Compiles QueryPlan to PostgreSQL-flavoured parameterized SQL.

    Parameter style: ``%(name)s`` – compatible with ``psycopg2`` and
    ``psycopg`` named-parameter execution.
    """

    @property
    def dialect_name(self) -> str:
        return "postgres"

    def param_placeholder(self, name: str) -> str:
        return f"%({name})s"

    def like_operator(self, op: str) -> str:
        return op  # 'LIKE' or 'ILIKE' — PostgreSQL supports both natively

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'
