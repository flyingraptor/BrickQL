"""PostgreSQL dialect compiler."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

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
        return op  # 'LIKE' or 'ILIKE' - PostgreSQL supports both natively

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def build_func_call(
        self,
        func_name: str,
        args: list[Any],
        build_arg: Callable[[Any], str],
    ) -> str:
        if func_name.upper() == "DATE_PART":
            return self._build_date_part(args, build_arg)
        return super().build_func_call(func_name, args, build_arg)

    @staticmethod
    def _build_date_part(
        args: list[Any],
        build_arg: Callable[[Any], str],
    ) -> str:
        """Compile ``DATE_PART`` with Postgres-safe argument handling.

        Two issues require special treatment in PostgreSQL:

        1. The field-name argument (e.g. ``'year'``) must be an inline SQL
           string literal.  Passing it as a bound parameter leaves its type as
           ``unknown``, which PostgreSQL cannot resolve to a ``date_part``
           overload.

        2. When the source column is stored as ``TEXT`` (e.g. ISO-8601 strings
           imported from CSV), ``date_part(text, text)`` has no overload.  An
           explicit ``::TIMESTAMP`` cast is added so the call resolves
           regardless of the column's physical type.
        """
        from brickql.schema.operands import ValueOperand

        parts: list[str] = []
        for i, arg in enumerate(args):
            if i == 0 and isinstance(arg, ValueOperand) and isinstance(arg.value, str):
                safe = arg.value.replace("'", "''")
                parts.append(f"'{safe}'")
            elif i == 1:
                parts.append(f"{build_arg(arg)}::TIMESTAMP")
            else:
                parts.append(build_arg(arg))
        return f"DATE_PART({', '.join(parts)})"
