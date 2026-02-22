"""Pydantic models for the DialectProfile passed to the LLM and validator.

The DialectProfile controls which SQL features are enabled for a given
request.  It is included in the system prompt so the LLM plans only within
allowed capabilities.  Enforcement is done by the validator and compiler.

Create a profile through the builder — compose exactly the features you need,
in any combination, with no hidden stacking::

    from brickql import DialectProfile

    # Joins + aggregations only (no CTEs, no window functions, etc.)
    profile = (
        DialectProfile.builder(tables, target="postgres")
        .joins(max_join_depth=2)
        .aggregations()
        .build()
    )

    # Window functions — aggregations() is required because aggregate window
    # functions (SUM/COUNT OVER ...) share names with regular aggregates
    profile = (
        DialectProfile.builder(tables, target="sqlite")
        .aggregations()       # required when using window_functions()
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
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from brickql.errors import ProfileConfigError
from brickql.schema.expressions import (
    AGGREGATE_FUNCTIONS,
    ALL_PREDICATE_OPS,
    WINDOW_FUNCTIONS,
)

#: Supported compiler targets.
DialectTarget = Literal["postgres", "sqlite"]

#: All operators the validator knows about.
SUPPORTED_OPERATORS: frozenset[str] = ALL_PREDICATE_OPS

#: Aggregate functions supported across backends.
SUPPORTED_FUNCTIONS: frozenset[str] = AGGREGATE_FUNCTIONS

# Base operators available in every profile (single-table filters).
_BASE_OPERATORS: list[str] = [
    "EQ", "NE", "GT", "GTE", "LT", "LTE",
    "BETWEEN", "IN", "IS_NULL", "IS_NOT_NULL", "LIKE",
    "AND", "OR", "NOT",
]

# Extra operators unlocked by .joins() — ILIKE for case-insensitive matching.
_JOIN_EXTRA_OPERATORS: list[str] = ["ILIKE"]

# Operator unlocked by .subqueries() — EXISTS for subquery predicates.
_SUBQUERY_EXTRA_OPERATORS: list[str] = ["EXISTS"]


class AllowedFeatures(BaseModel):
    """Defines which SQL features the LLM is permitted to use.

    Attributes:
        tables: Allowlisted table names (empty = deny all).
        operators: Allowlisted predicate operators.
        functions: Allowlisted aggregate / scalar / window function names.
        allow_subqueries: Permit inline derived tables and subquery predicates.
        allow_cte: Permit WITH (CTE) expressions.
        allow_window_functions: Permit OVER / window functions.
        allow_set_operations: Permit UNION, INTERSECT, EXCEPT.
        max_join_depth: Maximum number of JOINs allowed (0 = no joins).
        max_limit: Upper bound on the LIMIT value.
    """

    model_config = ConfigDict(extra="forbid")

    tables: list[str] = Field(default_factory=list)
    operators: list[str] = Field(
        default_factory=lambda: list(SUPPORTED_OPERATORS)
    )
    functions: list[str] = Field(default_factory=list)
    allow_subqueries: bool = False
    allow_cte: bool = False
    allow_window_functions: bool = False
    allow_set_operations: bool = False
    max_join_depth: int = 0
    max_limit: int = 1000


class DialectProfile(BaseModel):
    """Combines the backend target with the feature allowlist.

    Always created via :meth:`builder` — never instantiated directly in
    application code.

    Attributes:
        target: Backend to compile for (``'postgres'`` or ``'sqlite'``).
        allowed: Feature allowlist for this request.
    """

    model_config = ConfigDict(extra="forbid")

    target: DialectTarget = "postgres"
    allowed: AllowedFeatures = Field(default_factory=AllowedFeatures)

    @classmethod
    def builder(
        cls,
        tables: list[str],
        target: DialectTarget = "postgres",
        max_limit: int = 200,
    ) -> "DialectProfileBuilder":
        """Return a :class:`DialectProfileBuilder` to compose SQL features.

        The base profile allows single-table ``SELECT / WHERE / LIMIT``.
        Chain feature methods to unlock additional capabilities — each method
        is independent and adds exactly one feature group::

            profile = (
                DialectProfile.builder(ALL_TABLES, target="postgres")
                .joins(max_join_depth=3)
                .aggregations()
                .build()
            )

        Args:
            tables: Table names the LLM is allowed to reference.
            target: Compiler backend (``'postgres'`` or ``'sqlite'``).
            max_limit: Maximum value allowed for LIMIT clauses.

        Returns:
            A fresh :class:`DialectProfileBuilder` for this configuration.
        """
        return DialectProfileBuilder(tables=tables, target=target, max_limit=max_limit)


class DialectProfileBuilder:
    """Fluent builder for :class:`DialectProfile`.

    Always obtained via :meth:`DialectProfile.builder`.  Each method enables
    one independent feature group — they can be called in any order and
    combined freely.

    Example — joins with aggregations, but no CTEs or window functions::

        profile = (
            DialectProfile.builder(tables, target="postgres")
            .joins(max_join_depth=2)
            .aggregations()
            .build()
        )

    Example — window functions without join support::

        profile = (
            DialectProfile.builder(tables, target="sqlite")
            .aggregations()
            .window_functions()
            .build()
        )
    """

    def __init__(
        self,
        tables: list[str],
        target: DialectTarget,
        max_limit: int,
    ) -> None:
        self._tables = tables
        self._target = target
        self._max_limit = max_limit
        self._max_join_depth: int = 0
        self._operators: list[str] = list(_BASE_OPERATORS)
        self._functions: list[str] = []
        self._allow_subqueries: bool = False
        self._allow_cte: bool = False
        self._allow_window_functions: bool = False
        self._allow_set_operations: bool = False

    def joins(self, max_join_depth: int = 2) -> "DialectProfileBuilder":
        """Enable JOIN clauses (inner, left, self-referential, many-to-many),
        ORDER BY, OFFSET, DISTINCT, and ILIKE.

        Args:
            max_join_depth: Maximum number of JOIN clauses per query.
        """
        self._max_join_depth = max_join_depth
        for op in _JOIN_EXTRA_OPERATORS:
            if op not in self._operators:
                self._operators.append(op)
        return self

    def aggregations(self) -> "DialectProfileBuilder":
        """Enable GROUP BY, HAVING, aggregate functions (COUNT, SUM, AVG,
        MIN, MAX), and CASE expressions."""
        for fn in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
            if fn not in self._functions:
                self._functions.append(fn)
        return self

    def subqueries(self) -> "DialectProfileBuilder":
        """Enable correlated and uncorrelated subqueries: derived tables,
        EXISTS predicates, and IN subquery."""
        self._allow_subqueries = True
        for op in _SUBQUERY_EXTRA_OPERATORS:
            if op not in self._operators:
                self._operators.append(op)
        return self

    def ctes(self) -> "DialectProfileBuilder":
        """Enable Common Table Expressions (WITH / WITH RECURSIVE)."""
        self._allow_cte = True
        return self

    def set_operations(self) -> "DialectProfileBuilder":
        """Enable set operations: UNION, UNION ALL, INTERSECT, EXCEPT."""
        self._allow_set_operations = True
        return self

    def window_functions(self) -> "DialectProfileBuilder":
        """Enable window functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE,
        LAG, LEAD, FIRST_VALUE, LAST_VALUE, and OVER / PARTITION BY."""
        self._allow_window_functions = True
        for fn in sorted(WINDOW_FUNCTIONS):
            if fn not in self._functions:
                self._functions.append(fn)
        return self

    def build(self) -> DialectProfile:
        """Validate the configuration and return the :class:`DialectProfile`.

        Raises:
            ProfileConfigError: When the chosen combination of features has
                an unresolvable dependency that would cause every query using
                those features to fail validation.
        """
        self._validate()
        return DialectProfile(
            target=self._target,
            allowed=AllowedFeatures(
                tables=self._tables,
                operators=self._operators,
                functions=self._functions,
                allow_subqueries=self._allow_subqueries,
                allow_cte=self._allow_cte,
                allow_window_functions=self._allow_window_functions,
                allow_set_operations=self._allow_set_operations,
                max_join_depth=self._max_join_depth,
                max_limit=self._max_limit,
            ),
        )

    # ------------------------------------------------------------------
    # Internal validation
    # ------------------------------------------------------------------

    def _validate(self) -> None:
        """Raise :class:`ProfileConfigError` for invalid configurations.

        Dependency rules
        ----------------
        ``window_functions`` → ``aggregations``
            Aggregate window functions (``SUM/COUNT/AVG/MIN/MAX OVER (...)``)
            use the same function names as regular aggregations.  Without
            ``.aggregations()``, those names are not in the allowlist and every
            aggregate window query will be rejected by the validator.

        ``ctes`` → ``subqueries``
            CTE bodies may contain correlated subqueries and derived tables.
            Allowing CTEs without subqueries creates a gap where valid CTE
            patterns are silently blocked.

        ``tables`` must not be empty
            A profile with no allowed tables denies every query.
        """
        if not self._tables:
            raise ProfileConfigError(
                "No tables specified. Pass at least one table name to "
                "DialectProfile.builder(tables=[...]).",
                missing=["tables"],
                reason="A profile with an empty table list denies all queries.",
            )

        agg_functions = set(AGGREGATE_FUNCTIONS)
        has_aggregations = bool(agg_functions & set(self._functions))

        if self._allow_window_functions and not has_aggregations:
            raise ProfileConfigError(
                "window_functions() requires aggregations(). "
                "Aggregate window functions — SUM/COUNT/AVG/MIN/MAX OVER (...) — "
                "use the same function names as regular aggregations. "
                "Without .aggregations() they are not in the allowlist and "
                "every aggregate window query will be rejected. "
                "Add .aggregations() to your builder, or remove .window_functions() "
                "if you only need ranking functions (ROW_NUMBER, RANK, ...).",
                missing=["aggregations"],
                reason=(
                    "Aggregate window functions share names with regular aggregates "
                    "and require both features to be enabled."
                ),
            )

        if self._allow_cte and not self._allow_subqueries:
            raise ProfileConfigError(
                "ctes() requires subqueries(). "
                "CTE bodies can contain correlated subqueries and derived tables. "
                "Allowing CTEs without subqueries creates a gap where valid CTE "
                "patterns are silently blocked by the validator. "
                "Add .subqueries() to your builder.",
                missing=["subqueries"],
                reason=(
                    "CTEs may contain correlated subqueries; enabling one "
                    "without the other leaves a gap in allowed SQL patterns."
                ),
            )
