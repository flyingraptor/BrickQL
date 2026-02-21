"""Trial dataclass and persistence for BrinkQL examples.

Each *Trial* records:
  - The case metadata (id, question, notes)
  - The expected path: hand-crafted plan → compiled SQL → DB rows
  - The actual path: Ollama plan → compiled SQL (or error) → DB rows
  - A comparison summary
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class TrialResult:
    """One side of a trial (expected or actual).

    Attributes:
        plan: QueryPlan dict (may be None if Ollama returned unparseable text).
        plan_raw: Raw string (Ollama output or JSON-serialised expected plan).
        sql: Compiled SQL string, or None if compilation failed.
        params: Merged parameters passed to the DB, or None.
        rows: List of row dicts returned by the DB, or None.
        row_count: Convenience count, or None.
        error: Error message if something went wrong.
    """

    plan: dict[str, Any] | None = None
    plan_raw: str = ""
    sql: str | None = None
    params: dict[str, Any] | None = None
    rows: list[dict[str, Any]] | None = None
    row_count: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan": self.plan,
            "plan_raw": self.plan_raw,
            "sql": self.sql,
            "params": self.params,
            "rows": self.rows,
            "row_count": self.row_count,
            "error": self.error,
        }


@dataclass
class TrialComparison:
    """Comparison between expected and actual results.

    Attributes:
        brinkql_valid: True if Ollama's plan compiled without error.
        sql_match: True if compiled SQL strings are identical (after normalisation).
        rows_match: True if DB result sets are identical.
        notes: Human-readable summary of differences.
    """

    brinkql_valid: bool = False
    sql_match: bool = False
    rows_match: bool = False
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "brinkql_valid": self.brinkql_valid,
            "sql_match": self.sql_match,
            "rows_match": self.rows_match,
            "notes": self.notes,
        }


@dataclass
class Trial:
    """Complete record of one case run.

    Attributes:
        case_id: Identifier from the Case.
        category: Category label.
        question: Natural-language question.
        case_notes: Explanation of what makes the case interesting.
        timestamp: ISO-8601 UTC timestamp of when the trial ran.
        target_db: ``"sqlite"`` or ``"postgres"``.
        ollama_model: Model tag used for the actual path.
        expected: TrialResult for the hand-crafted plan.
        actual: TrialResult for Ollama's plan.
        comparison: TrialComparison summary.
    """

    case_id: str
    category: str
    question: str
    case_notes: str
    timestamp: str
    target_db: str
    ollama_model: str
    expected: TrialResult = field(default_factory=TrialResult)
    actual: TrialResult = field(default_factory=TrialResult)
    comparison: TrialComparison = field(default_factory=TrialComparison)

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "category": self.category,
            "question": self.question,
            "case_notes": self.case_notes,
            "timestamp": self.timestamp,
            "target_db": self.target_db,
            "ollama_model": self.ollama_model,
            "expected": self.expected.to_dict(),
            "actual": self.actual.to_dict(),
            "comparison": self.comparison.to_dict(),
        }

    @staticmethod
    def make_timestamp() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def save_trial(trial: Trial, output_dir: Path) -> Path:
    """Serialise *trial* to ``{output_dir}/{case_id}/{timestamp}.json``.

    Creates parent directories as needed.

    Returns:
        Path to the written file.
    """
    dest = output_dir / trial.case_id / f"{trial.timestamp}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(trial.to_dict(), indent=2, default=str))
    return dest


def _normalise_sql(sql: str) -> str:
    """Collapse whitespace for SQL comparison."""
    import re
    return re.sub(r"\s+", " ", sql).strip()


def compare_results(
    expected: TrialResult,
    actual: TrialResult,
) -> TrialComparison:
    """Build a TrialComparison from two TrialResults."""
    valid = actual.error is None and actual.sql is not None
    sql_match = (
        valid
        and expected.sql is not None
        and _normalise_sql(expected.sql) == _normalise_sql(actual.sql or "")
    )
    rows_match = (
        valid
        and expected.rows is not None
        and actual.rows is not None
        and expected.rows == actual.rows
    )

    parts: list[str] = []
    if not valid:
        parts.append(f"Ollama plan failed BrinkQL: {actual.error}")
    elif not sql_match:
        parts.append("SQL differs from expected")
    if valid and not rows_match:
        parts.append(
            f"Row counts: expected={expected.row_count} actual={actual.row_count}"
        )

    return TrialComparison(
        brinkql_valid=valid,
        sql_match=sql_match,
        rows_match=rows_match,
        notes="; ".join(parts) if parts else "All checks passed",
    )
