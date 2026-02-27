"""Test fixtures: sample schema DDL and SchemaSnapshot JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from brickql.schema.snapshot import SchemaSnapshot

_FIXTURES_DIR = Path(__file__).parent


def load_schema_snapshot() -> SchemaSnapshot:
    """Load the canonical sample SchemaSnapshot from schema.json."""
    data = json.loads((_FIXTURES_DIR / "schema.json").read_text())
    return SchemaSnapshot.model_validate(data)


def load_ddl(target: Literal["sqlite", "postgres", "mysql"] = "sqlite") -> str:
    """Return the sample DDL SQL string for the given backend.

    Args:
        target: ``'sqlite'`` (default), ``'postgres'``, or ``'mysql'``.

    Returns:
        DDL string ready to execute against the target backend.
    """
    filename = f"ddl_{target}.sql"
    return (_FIXTURES_DIR / filename).read_text()
