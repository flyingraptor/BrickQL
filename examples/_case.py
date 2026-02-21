"""Case dataclass: describes one example scenario."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from brinkql import DialectProfile, PolicyConfig


@dataclass
class Case:
    """A single named example scenario.

    Attributes:
        id: Unique identifier, e.g. ``"c01_01"``.
        category: Human-readable category, e.g. ``"basic_select"``.
        question: Natural-language question sent to the LLM.
        expected_plan: Hand-crafted QueryPlan dict representing the correct output.
        dialect: DialectProfile governing allowed SQL features.
        policy: PolicyConfig for tenant isolation and limits.
        runtime_params: Values injected at execution time, e.g. ``{"TENANT": "acme"}``.
        target_dbs: Which backends to run against (``"sqlite"`` and/or ``"postgres"``).
        notes: Free-form explanation of what makes this case interesting.
    """

    id: str
    category: str
    question: str
    expected_plan: dict[str, Any]
    dialect: DialectProfile
    policy: PolicyConfig
    runtime_params: dict[str, Any]
    target_dbs: list[str] = field(default_factory=lambda: ["sqlite"])
    notes: str = ""
