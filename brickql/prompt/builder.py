"""Prompt builder: constructs system and user prompts for LLM planners.

The LLM receives three structured inputs:
1. **Schema Snapshot** – tables, columns, and relationship keys it may use.
2. **Dialect Profile** – which features and operators are enabled.
3. **Policy Summary** – a plain-text description of runtime constraints.

``PromptBuilder`` assembles these into a system prompt and a user prompt.
The library does NOT call the LLM; callers use the returned strings with
their own model SDK.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from brickql.policy.engine import PolicyConfig
from brickql.schema.dialect import DialectProfile
from brickql.schema.snapshot import SchemaSnapshot

_SYSTEM_PROMPT_TEMPLATE = """\
You are a query planner for a SQL database.

## Your role
- Output a structured QueryPlan (JSON only).
- Do NOT output SQL strings.
- Do NOT output commentary, explanations, or markdown.
- Do NOT invent tables, columns, or relationship keys.
- Do NOT hardcode tenant IDs or other runtime values; use {{"param": "PARAM_NAME"}}.

## Exact output format — follow this precisely

Top-level keys (omit unused): SELECT, FROM, JOIN, WHERE, GROUP_BY, HAVING,
ORDER_BY, LIMIT, OFFSET, SET_OP, CTE

### SELECT
A JSON array where EVERY item is an object with an "expr" key:
  {{"expr": {{"col": "table.column"}}}}
  {{"expr": {{"col": "table.column"}}, "alias": "my_alias"}}
  {{"expr": {{"func": "COUNT", "args": [{{"col": "t.id"}}]}}, "alias": "cnt"}}
NEVER write {{"col": "..."}} directly as a SELECT item — it MUST be wrapped in {{"expr": ...}}.

### FROM
Always an object with a "table" key — NEVER a plain string:
  {{"table": "employees"}}

### LIMIT
Always an object with a "value" key — NEVER a plain integer:
  {{"value": 50}}

### OFFSET
Always an object with a "value" key:
  {{"value": 10}}

### WHERE / HAVING
A predicate object: the operator is the single top-level key, its value is the
argument list.  NEVER use "operator" or "operands" keys.

Comparison (2 operands):  {{"EQ": [left, right]}}  {{"GT": [left, right]}}
Null check (1 operand):   {{"IS_NULL": {{"col": "t.col"}}}}
Range (3 operands):       {{"BETWEEN": [val, low, high]}}
Membership:               {{"IN": [{{"col": "t.col"}}, {{"value": 1}}, {{"value": 2}}]}}
Logical:                  {{"AND": [pred1, pred2, ...]}}  {{"OR": [pred1, pred2]}}
Negation:                 {{"NOT": pred}}

### ORDER_BY
An array of objects with "expr" and optional "direction" ("ASC" or "DESC"):
  [{{"expr": {{"col": "t.salary"}}, "direction": "DESC"}}]

### GROUP_BY
An array of operand dicts:
  [{{"col": "t.department_id"}}, {{"col": "t.employment_type"}}]

### JOIN
An array of objects with "rel" (relationship key from schema), "type", optional "alias":
  [{{"rel": "departments__employees", "type": "LEFT"}}]
  [{{"rel": "employees__manager", "type": "LEFT", "alias": "mgr"}}]

### Operand types
- Column reference:  {{"col": "table.column"}}
- Literal value:     {{"value": 42}}  or  {{"value": "text"}}  or  {{"value": true}}
- Runtime param:     {{"param": "TENANT"}}
- Function call:     {{"func": "COUNT", "args": [{{"col": "t.id"}}]}}
- CASE expression:   {{"case": {{"when": [{{"if": predicate, "then": operand}}], "else": operand}}}}

### Complete example
{{
  "SELECT": [
    {{"expr": {{"col": "employees.first_name"}}}},
    {{"expr": {{"col": "departments.name"}}, "alias": "dept"}},
    {{"expr": {{"func": "COUNT", "args": [{{"col": "employees.employee_id"}}]}}, "alias": "cnt"}}
  ],
  "FROM": {{"table": "employees"}},
  "JOIN": [{{"rel": "departments__employees", "type": "LEFT"}}],
  "WHERE": {{
    "AND": [
      {{"EQ": [{{"col": "employees.tenant_id"}}, {{"param": "TENANT"}}]}},
      {{"EQ": [{{"col": "employees.active"}}, {{"value": 1}}]}}
    ]
  }},
  "GROUP_BY": [{{"col": "departments.name"}}],
  "ORDER_BY": [{{"expr": {{"col": "employees.first_name"}}, "direction": "ASC"}}],
  "LIMIT": {{"value": 50}}
}}

## Dialect profile (what you are allowed to use)
{dialect_profile}

## Policy summary
{policy_summary}

## Schema snapshot (tables, columns, and relationships you may reference)
{schema_snapshot}

## Error repair
If the system returns a structured error, output only a corrected QueryPlan JSON.
Do not include commentary.  Do not change unrelated parts of the plan.
"""

_USER_PROMPT_TEMPLATE = """\
{question}
"""


@dataclass
class PromptComponents:
    """The prompt parts ready to pass to an LLM.

    Attributes:
        system_prompt: Full system prompt including dialect, policy, and schema.
        user_prompt: The user's question formatted for the LLM.
        schema_snapshot_json: The raw schema snapshot as a JSON string (for
            logging or debugging).
        dialect_profile_json: The raw dialect profile as a JSON string.
    """

    system_prompt: str
    user_prompt: str
    schema_snapshot_json: str
    dialect_profile_json: str


class PromptBuilder:
    """Builds structured prompts for the LLM planner.

    Args:
        snapshot: Schema visible to the LLM.
        dialect: Dialect profile controlling allowed features.
        policy: Optional policy config.  When provided, param-bound column
            annotations (e.g. ``"param_bound": "TENANT"``) are included in
            the schema section of the prompt so the LLM knows which columns
            require ``{"param": "NAME"}`` rather than literal values.
        policy_summary: Human-readable description of runtime policy
            (e.g. tenant isolation, PII restrictions).
    """

    def __init__(
        self,
        snapshot: SchemaSnapshot,
        dialect: DialectProfile,
        policy: PolicyConfig | None = None,
        policy_summary: str = "",
    ) -> None:
        self._snapshot = snapshot
        self._dialect = dialect
        self._policy = policy
        self._policy_summary = policy_summary or (
            "All queries must include required runtime parameters "
            "(e.g. TENANT) where specified by the schema. "
            "Always include LIMIT."
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, question: str) -> PromptComponents:
        """Build system and user prompts for the given question.

        Args:
            question: The user's natural-language question.

        Returns:
            ``PromptComponents`` ready to pass to an LLM.
        """
        schema_json = self._build_schema_summary()
        dialect_json = self._build_dialect_summary()

        system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
            dialect_profile=dialect_json,
            policy_summary=self._policy_summary,
            schema_snapshot=schema_json,
        )
        user_prompt = _USER_PROMPT_TEMPLATE.format(question=question)

        return PromptComponents(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_snapshot_json=schema_json,
            dialect_profile_json=dialect_json,
        )

    def build_repair_prompt(
        self,
        error_response: dict,
        previous_plan_json: str,
    ) -> PromptComponents:
        """Build a correction prompt after a validation error.

        Args:
            error_response: The structured error dict returned by
                ``ValidationError.to_error_response()``.
            previous_plan_json: The QueryPlan JSON that caused the error.

        Returns:
            ``PromptComponents`` with a repair-focused user prompt.
        """
        error_text = json.dumps(error_response, indent=2)
        repair_question = (
            f"The following QueryPlan produced an error:\n"
            f"```json\n{previous_plan_json}\n```\n\n"
            f"Error:\n```json\n{error_text}\n```\n\n"
            f"Output only a corrected QueryPlan JSON. "
            f"Do not include commentary. "
            f"Do not repeat the error. "
            f"Do not change unrelated parts of the plan."
        )
        return self.build(repair_question)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_schema_summary(self) -> str:
        """Serialise the schema snapshot as a prompt-friendly JSON string.

        If a :class:`~brickql.policy.engine.PolicyConfig` was supplied,
        param-bound column annotations are included so the LLM knows which
        columns must use ``{"param": "PARAM_NAME"}`` rather than literals.
        """
        summary: dict = {"tables": [], "relationships": []}
        for table in self._snapshot.tables:
            tpol = (
                self._policy.tables.get(table.name)
                if self._policy
                else None
            )
            bound = tpol.param_bound_columns if tpol else {}
            cols = [
                {
                    "name": col.name,
                    "type": col.type,
                    **({"description": col.description} if col.description else {}),
                    **({"param_bound": bound[col.name]} if col.name in bound else {}),
                }
                for col in table.columns
            ]
            table_entry: dict = {"name": table.name, "columns": cols}
            if table.description:
                table_entry["description"] = table.description
            summary["tables"].append(table_entry)
        for rel in self._snapshot.relationships:
            summary["relationships"].append(
                {
                    "key": rel.key,
                    "join": (
                        f"{rel.from_table}.{rel.from_col}"
                        f" = {rel.to_table}.{rel.to_col}"
                    ),
                }
            )
        return json.dumps(summary, indent=2)

    def _build_dialect_summary(self) -> str:
        """Serialise the dialect profile as a prompt-friendly JSON string."""
        allowed = self._dialect.allowed
        return json.dumps(
            {
                "target": self._dialect.target,
                "allowed": {
                    "tables": allowed.tables,
                    "operators": allowed.operators,
                    "functions": allowed.functions,
                    "allow_subqueries": allowed.allow_subqueries,
                    "allow_cte": allowed.allow_cte,
                    "allow_window_functions": allowed.allow_window_functions,
                    "allow_set_operations": allowed.allow_set_operations,
                    "max_join_depth": allowed.max_join_depth,
                    "max_limit": allowed.max_limit,
                },
            },
            indent=2,
        )
