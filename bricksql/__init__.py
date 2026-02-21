"""BrickSQL – Policy-driven SQL query orchestration for LLM planners.

Build Queries. Don't Generate Them.

Public API
----------
``validate_and_compile``
    Parse, validate, apply policy, and compile a QueryPlan JSON string to
    parameterized SQL.

``get_prompt_components``
    Build system and user prompts for the LLM planner.

Re-exported types
-----------------
``QueryPlan``, ``SchemaSnapshot``, ``DialectProfile``, ``PolicyConfig``,
``CompiledSQL``, ``PromptComponents``, and all error classes.
"""
from __future__ import annotations

import json
from typing import Any

from bricksql.compile.base import CompiledSQL
from bricksql.compile.builder import QueryBuilder
from bricksql.compile.postgres import PostgresCompiler
from bricksql.compile.sqlite import SQLiteCompiler
from bricksql.errors import (
    BrickSQLError,
    CompilationError,
    DialectViolationError,
    DisallowedColumnError,
    DisallowedTableError,
    InvalidJoinRelError,
    MissingParamError,
    ParseError,
    ProfileConfigError,
    SchemaError,
    ValidationError,
)
from bricksql.policy.engine import PolicyConfig, PolicyEngine, TablePolicy
from bricksql.prompt.builder import PromptBuilder, PromptComponents
from bricksql.schema.dialect import AllowedFeatures, DialectProfile, DialectProfileBuilder
from bricksql.schema.query_plan import QueryPlan
from bricksql.schema.snapshot import (
    ColumnInfo,
    RelationshipInfo,
    SchemaSnapshot,
    TableInfo,
)
from bricksql.validate.validator import PlanValidator

__all__ = [
    # Core pipeline
    "validate_and_compile",
    "get_prompt_components",
    # Schema types
    "QueryPlan",
    "SchemaSnapshot",
    "TableInfo",
    "ColumnInfo",
    "RelationshipInfo",
    "DialectProfile",
    "DialectProfileBuilder",
    "AllowedFeatures",
    # Policy
    "PolicyConfig",
    "TablePolicy",
    "PolicyEngine",
    # Compilation
    "CompiledSQL",
    "PostgresCompiler",
    "SQLiteCompiler",
    "QueryBuilder",
    # Prompting
    "PromptBuilder",
    "PromptComponents",
    # Errors
    "BrickSQLError",
    "ProfileConfigError",
    "ParseError",
    "ValidationError",
    "DisallowedColumnError",
    "DisallowedTableError",
    "InvalidJoinRelError",
    "DialectViolationError",
    "MissingParamError",
    "SchemaError",
    "CompilationError",
]

_COMPILERS = {
    "postgres": PostgresCompiler,
    "sqlite": SQLiteCompiler,
}


def validate_and_compile(
    plan_json: str,
    snapshot: SchemaSnapshot,
    dialect: DialectProfile,
    policy: PolicyConfig | None = None,
) -> CompiledSQL:
    """Parse, validate, apply policy, and compile a QueryPlan JSON string.

    This is the main entry point for the BrickSQL pipeline::

        compiled = bricksql.validate_and_compile(
            plan_json=llm_output,
            snapshot=schema_snapshot,
            dialect=DialectProfile.builder(["employees"]).joins().build(),
            policy=PolicyConfig(default_limit=100),
        )
        cursor.execute(compiled.sql, compiled.merge_runtime_params({"TENANT": tid}))

    Args:
        plan_json: Raw JSON string output by the LLM.
        snapshot: The schema snapshot used for validation and compilation.
        dialect: Dialect profile controlling allowed features and target backend.
        policy: Optional policy configuration; defaults to ``PolicyConfig()``.

    Returns:
        ``CompiledSQL`` with ``sql`` string, literal ``params``, and ``dialect``.

    Raises:
        ParseError: If ``plan_json`` is not valid JSON or not a valid QueryPlan.
        ValidationError: (or subclass) if the plan violates schema or dialect rules.
        CompilationError: If compilation fails for an unexpected reason.
    """
    if policy is None:
        policy = PolicyConfig()

    # 1. Parse
    try:
        raw = json.loads(plan_json)
    except json.JSONDecodeError as exc:
        raise ParseError(f"Invalid JSON: {exc}", raw=plan_json) from exc

    try:
        plan = QueryPlan.model_validate(raw)
    except Exception as exc:
        raise ParseError(
            f"QueryPlan structure is invalid: {exc}", raw=plan_json
        ) from exc

    # 2. Validate
    PlanValidator(snapshot, dialect).validate(plan)

    # 3. Apply policy
    plan = PolicyEngine(policy, snapshot, dialect).apply(plan)

    # 4. Compile
    compiler_cls = _COMPILERS.get(dialect.target)
    if compiler_cls is None:
        raise CompilationError(f"Unsupported dialect target: '{dialect.target}'.")
    compiler = compiler_cls()
    return QueryBuilder(compiler, snapshot).build(plan)


def get_prompt_components(
    snapshot: SchemaSnapshot,
    dialect: DialectProfile,
    question: str,
    policy: PolicyConfig | None = None,
    policy_summary: str = "",
) -> PromptComponents:
    """Build system and user prompts ready to send to the LLM planner.

    Args:
        snapshot: The schema snapshot to include in the system prompt.
        dialect: The dialect profile to include (controls what the LLM may use).
        question: The user's natural-language question.
        policy: Optional policy config.  When provided, param-bound column
            annotations are included in the schema section of the prompt.
        policy_summary: Optional plain-text description of runtime policy rules.

    Returns:
        ``PromptComponents`` with ``system_prompt`` and ``user_prompt``.
    """
    return PromptBuilder(snapshot, dialect, policy, policy_summary).build(question)
