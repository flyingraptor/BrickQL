"""Compiler abstractions: CompiledSQL and the SQLCompiler ABC.

The Template Method pattern (GoF) is used:
- ``SQLCompiler`` defines the algorithm skeleton for compiling each clause.
- ``PostgresCompiler`` and ``SQLiteCompiler`` override dialect-specific steps
  (parameter placeholder style, ILIKE support, quoting).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CompiledSQL:
    """The output of a successful compilation.

    Attributes:
        sql: The compiled SQL string with named placeholders.
        params: Values for placeholders generated from ``{"value": ...}``
            operands.  Does NOT include runtime params (e.g. ``TENANT``);
            those are supplied by the executor.
        dialect: The target dialect (``'postgres'`` or ``'sqlite'``).
    """

    sql: str
    params: dict[str, Any]
    dialect: str

    def merge_runtime_params(
        self, runtime: dict[str, Any]
    ) -> dict[str, Any]:
        """Return a merged param dict ready for query execution.

        Args:
            runtime: Runtime parameter values (e.g. ``{"TENANT": "acme"}``)
                supplied by the caller.

        Returns:
            A single dict combining compiled literal params and runtime params.
        """
        return {**self.params, **runtime}


class SQLCompiler(ABC):
    """Abstract base for dialect-specific SQL compilers.

    Subclasses implement the dialect-specific methods; the ``QueryBuilder``
    uses this interface via the Strategy / Template Method patterns.
    """

    @abstractmethod
    def param_placeholder(self, name: str) -> str:
        """Return the SQL placeholder string for a named parameter.

        Args:
            name: Parameter name (e.g. ``'TENANT'``, ``'param_0'``).

        Returns:
            Dialect-specific placeholder string.
        """

    @abstractmethod
    def like_operator(self, op: str) -> str:
        """Return the SQL keyword for a LIKE / ILIKE operator.

        SQLite does not support ``ILIKE``; it falls back to ``LIKE``.

        Args:
            op: ``'LIKE'`` or ``'ILIKE'``.

        Returns:
            SQL operator keyword.
        """

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """Return a properly-quoted SQL identifier.

        Args:
            name: Unquoted identifier (table or column name).

        Returns:
            Quoted identifier.
        """

    @property
    @abstractmethod
    def dialect_name(self) -> str:
        """Return the canonical dialect name (``'postgres'`` or ``'sqlite'``)."""
