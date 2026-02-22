"""Compilation context value object.

Packages the ``(compiler, snapshot)`` data clump that was repeated across
``QueryBuilder`` and all clause-level sub-builders into a single cohesive
object.
"""
from __future__ import annotations

from dataclasses import dataclass

from brickql.compile.base import SQLCompiler
from brickql.schema.snapshot import SchemaSnapshot


@dataclass(frozen=True)
class CompilationContext:
    """Immutable context for a single compilation run.

    Attributes:
        compiler: Dialect-specific SQL compiler instance.
        snapshot: Schema snapshot (used for JOIN ON clause resolution).
    """

    compiler: SQLCompiler
    snapshot: SchemaSnapshot
