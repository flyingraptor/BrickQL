"""Custom exception hierarchy for BrinkQL.

All public errors inherit from BrinkQLError so callers can catch the base
class for any BrinkQL-specific failure.
"""
from __future__ import annotations

from typing import Any


class BrinkQLError(Exception):
    """Base exception for all BrinkQL errors."""


class ParseError(BrinkQLError):
    """Raised when input cannot be parsed as valid QueryPlan JSON.

    Args:
        message: Human-readable description.
        raw: The raw string that failed to parse.
    """

    def __init__(self, message: str, raw: str | None = None) -> None:
        super().__init__(message)
        self.raw = raw


class ValidationError(BrinkQLError):
    """Raised when a QueryPlan fails structural or semantic validation.

    Args:
        message: Human-readable description.
        code: Machine-readable error code (e.g. DISALLOWED_COLUMN).
        details: Extra context returned to the LLM for error repair.
    """

    def __init__(
        self,
        message: str,
        code: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details: dict[str, Any] = details or {}

    def to_error_response(self) -> dict[str, Any]:
        """Returns a structured error response suitable for LLM repair."""
        return {
            "error": self.code,
            "message": str(self),
            "details": self.details,
        }


class DisallowedColumnError(ValidationError):
    """Raised when the plan references a column not in the policy allowlist."""

    def __init__(
        self,
        table: str,
        column: str,
        allowed_columns: list[str],
    ) -> None:
        super().__init__(
            f"Column '{column}' on table '{table}' is not allowed.",
            code="DISALLOWED_COLUMN",
            details={
                "table": table,
                "column": column,
                "allowed_columns": allowed_columns,
            },
        )


class DisallowedTableError(ValidationError):
    """Raised when the plan references a table not in the policy allowlist."""

    def __init__(self, table: str, allowed_tables: list[str]) -> None:
        super().__init__(
            f"Table '{table}' is not allowed.",
            code="DISALLOWED_TABLE",
            details={"table": table, "allowed_tables": allowed_tables},
        )


class InvalidJoinRelError(ValidationError):
    """Raised when a JOIN clause uses an unknown relationship key."""

    def __init__(self, rel: str, allowed_relationships: list[str]) -> None:
        super().__init__(
            f"Unknown relationship key: '{rel}'.",
            code="INVALID_JOIN_REL",
            details={
                "rel": rel,
                "allowed_relationships": allowed_relationships,
            },
        )


class DialectViolationError(ValidationError):
    """Raised when the plan uses a feature not enabled in the dialect profile."""

    def __init__(self, message: str, feature: str) -> None:
        super().__init__(
            message,
            code="DIALECT_VIOLATION",
            details={"feature": feature},
        )


class MissingParamError(ValidationError):
    """Raised when a policy-bound column is not using the required param."""

    def __init__(self, column: str, required_param: str) -> None:
        super().__init__(
            f'Column \'{column}\' must use {{"param": "{required_param}"}} instead of a literal.',
            code="MISSING_PARAM",
            details={"column": column, "required_param": required_param},
        )


class SchemaError(ValidationError):
    """Raised when the plan references an unknown table or column."""

    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code="SCHEMA_ERROR", details=details or {})


class ProfileConfigError(BrinkQLError):
    """Raised when a DialectProfile is misconfigured.

    Detected at :meth:`DialectProfileBuilder.build` time — before any query
    is executed — so the developer gets a clear, actionable message instead
    of a cryptic validation failure later.

    Args:
        message: Human-readable description.
        missing: Feature method(s) that must be added to the builder.
        reason: Why the dependency exists.
    """

    def __init__(
        self,
        message: str,
        missing: list[str] | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message)
        self.missing = missing or []
        self.reason = reason or ""


class CompilationError(BrinkQLError):
    """Raised when SQL compilation fails for an unexpected reason.

    Args:
        message: Human-readable description.
        clause: The QueryPlan clause being compiled when the error occurred.
    """

    def __init__(self, message: str, clause: str | None = None) -> None:
        super().__init__(message)
        self.clause = clause
