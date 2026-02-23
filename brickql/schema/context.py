"""Validation context value object.

Packages the ``(snapshot, dialect)`` data clump that was repeated across
``PlanValidator``, ``OperandValidator``, ``PredicateValidator``, and all
their sub-validators into a single cohesive object.
"""

from __future__ import annotations

from dataclasses import dataclass

from brickql.schema.dialect import DialectProfile
from brickql.schema.snapshot import SchemaSnapshot


@dataclass(frozen=True)
class ValidationContext:
    """Immutable context for a single validation run.

    Attributes:
        snapshot: The schema snapshot used for table/column existence checks.
        dialect: The dialect profile controlling allowed features.
    """

    snapshot: SchemaSnapshot
    dialect: DialectProfile
