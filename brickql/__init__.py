"""brickQL – Policy-driven SQL query orchestration for LLMs.

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

Extensibility
-------------
New dialect compilers can be registered via::

    from brickql.compile.registry import CompilerFactory

    @CompilerFactory.register("mysql")
    class MySQLCompiler(SQLCompiler):
        ...

After registration, ``validate_and_compile`` picks it up automatically for
any ``DialectProfile`` with ``target="mysql"``.
"""

from __future__ import annotations

import json

from brickql.compile.base import CompiledSQL
from brickql.compile.builder import QueryBuilder
from brickql.compile.mysql import MySQLCompiler
from brickql.compile.postgres import PostgresCompiler
from brickql.compile.registry import CompilerFactory
from brickql.compile.sqlite import SQLiteCompiler
from brickql.errors import (
    CompilationError,
    DialectViolationError,
    DisallowedColumnError,
    DisallowedTableError,
    InvalidJoinRelError,
    MissingParamError,
    ParseError,
    PolicyViolationError,
    ProfileConfigError,
    SchemaError,
    ValidationError,
    brickQLError,
)
from brickql.policy.engine import PolicyConfig, PolicyEngine, TablePolicy
from brickql.prompt.builder import PromptBuilder, PromptComponents
from brickql.schema.context import ValidationContext
from brickql.schema.converters import schema_from_sqlalchemy
from brickql.schema.dialect import AllowedFeatures, DialectProfile, DialectProfileBuilder
from brickql.schema.operands import (
    CaseOperand,
    ColumnOperand,
    FuncOperand,
    Operand,
    ParamOperand,
    ValueOperand,
)
from brickql.schema.query_plan import QueryPlan
from brickql.schema.snapshot import (
    ColumnInfo,
    RelationshipInfo,
    SchemaSnapshot,
    TableInfo,
)
from brickql.validate.validator import PlanValidator

# ---------------------------------------------------------------------------
# Register built-in compilers with CompilerFactory (Item 4 - OCP)
# ---------------------------------------------------------------------------

CompilerFactory.register_class("postgres", PostgresCompiler)
CompilerFactory.register_class("sqlite", SQLiteCompiler)
CompilerFactory.register_class("mysql", MySQLCompiler)

__all__ = [
    # Core pipeline
    "validate_and_compile",
    "get_prompt_components",
    # Schema types
    "QueryPlan",
    "SchemaSnapshot",
    # Converters
    "schema_from_sqlalchemy",
    "TableInfo",
    "ColumnInfo",
    "RelationshipInfo",
    "DialectProfile",
    "DialectProfileBuilder",
    "AllowedFeatures",
    # Typed operands
    "Operand",
    "ColumnOperand",
    "ValueOperand",
    "ParamOperand",
    "FuncOperand",
    "CaseOperand",
    # Contexts
    "ValidationContext",
    # Policy
    "PolicyConfig",
    "TablePolicy",
    "PolicyEngine",
    # Compilation
    "CompiledSQL",
    "CompilerFactory",
    "MySQLCompiler",
    "PostgresCompiler",
    "SQLiteCompiler",
    "QueryBuilder",
    # Prompting
    "PromptBuilder",
    "PromptComponents",
    # Errors
    "brickQLError",
    "ProfileConfigError",
    "ParseError",
    "ValidationError",
    "PolicyViolationError",
    "DisallowedColumnError",
    "DisallowedTableError",
    "InvalidJoinRelError",
    "DialectViolationError",
    "MissingParamError",
    "SchemaError",
    "CompilationError",
]


def validate_and_compile(
    plan_json: str,
    snapshot: SchemaSnapshot,
    dialect: DialectProfile,
    policy: PolicyConfig | None = None,
) -> CompiledSQL:
    """Parse, validate, apply policy, and compile a QueryPlan JSON string.

    This is the main entry point for the brickQL pipeline::

        compiled = brickql.validate_and_compile(
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
        raise ParseError(f"QueryPlan structure is invalid: {exc}", raw=plan_json) from exc

    # 2. Validate
    PlanValidator(snapshot, dialect).validate(plan)

    # 3. Apply policy
    plan = PolicyEngine(policy, snapshot, dialect).apply(plan)

    # 4. Compile - dialect target resolved via CompilerFactory (OCP: no if-chain)
    compiler = CompilerFactory.create(dialect.target)
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
