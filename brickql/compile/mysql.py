"""MySQL dialect compiler."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from brickql.compile.base import SQLCompiler


class MySQLCompiler(SQLCompiler):
    """Compiles QueryPlan to MySQL-flavoured parameterized SQL.

    Parameter style: ``%(name)s`` – compatible with ``PyMySQL`` and
    ``mysql-connector-python`` named-parameter execution.

    Note: MySQL does not support ``ILIKE``; it is mapped to ``LIKE``.
    MySQL's ``LIKE`` is case-insensitive for non-binary TEXT/VARCHAR columns
    by default.

    Identifiers are quoted with backticks (`` ` ``) rather than double-quotes.
    """

    @property
    def dialect_name(self) -> str:
        return "mysql"

    def param_placeholder(self, name: str) -> str:
        return f"%({name})s"

    def like_operator(self, op: str) -> str:
        return "LIKE"  # MySQL has no ILIKE; LIKE is case-insensitive for TEXT by default

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace("`", "``")
        return f"`{escaped}`"

    def build_func_call(
        self,
        func_name: str,
        args: list[Any],
        build_arg: Callable[[Any], str],
    ) -> str:
        if func_name.upper() == "DATE_PART":
            return self._build_extract(args, build_arg)
        return super().build_func_call(func_name, args, build_arg)

    @staticmethod
    def _build_extract(
        args: list[Any],
        build_arg: Callable[[Any], str],
    ) -> str:
        """Translate ``DATE_PART(field, col)`` to MySQL's ``EXTRACT(unit FROM col)``.

        MySQL does not have a ``DATE_PART`` function.  The equivalent is
        ``EXTRACT(unit FROM expr)``, where the unit is an unquoted keyword.
        PostgreSQL's first argument is a quoted string like ``'year'``; this
        method strips the quoting and uppercases the unit keyword for MySQL.
        """
        from brickql.schema.operands import ValueOperand

        if len(args) < 2:
            args_sql = ", ".join(build_arg(a) for a in args)
            return f"DATE_PART({args_sql})"

        unit_arg = args[0]
        source_arg = args[1]

        if isinstance(unit_arg, ValueOperand) and isinstance(unit_arg.value, str):
            unit = unit_arg.value.upper()
        else:
            unit = build_arg(unit_arg).strip("'\"").upper()

        return f"EXTRACT({unit} FROM {build_arg(source_arg)})"
