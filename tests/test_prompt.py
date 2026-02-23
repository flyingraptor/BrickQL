"""Unit tests for PromptBuilder."""

from __future__ import annotations

import json

from brickql.policy.engine import PolicyConfig, TablePolicy
from brickql.prompt.builder import PromptBuilder
from brickql.schema.dialect import DialectProfile
from brickql.schema.snapshot import ColumnInfo, SchemaSnapshot, TableInfo
from tests.fixtures import load_schema_snapshot

SNAPSHOT = load_schema_snapshot()
ALL_TABLES = [
    "companies",
    "departments",
    "employees",
    "skills",
    "employee_skills",
    "projects",
    "project_assignments",
    "salary_history",
]
DIALECT = DialectProfile.builder(ALL_TABLES).build()

_TENANT = TablePolicy(param_bound_columns={"tenant_id": "TENANT"})
POLICY = PolicyConfig(
    tables={
        "companies": _TENANT,
        "departments": _TENANT,
        "employees": _TENANT,
        "projects": _TENANT,
    }
)


def _builder(policy_summary: str = "") -> PromptBuilder:
    return PromptBuilder(SNAPSHOT, DIALECT, POLICY, policy_summary)


def test_system_prompt_contains_schema():
    components = _builder().build("List all active employees")
    assert "employees" in components.system_prompt
    assert "departments" in components.system_prompt
    assert "departments__employees" in components.system_prompt


def test_system_prompt_contains_dialect():
    components = _builder().build("Show me remote workers")
    assert "max_limit" in components.system_prompt
    assert "max_join_depth" in components.system_prompt


def test_system_prompt_forbids_sql():
    components = _builder().build("List employees")
    assert "Do NOT output SQL" in components.system_prompt


def test_system_prompt_requires_limit():
    components = _builder().build("List employees")
    assert "LIMIT" in components.system_prompt


def test_user_prompt_contains_question():
    question = "Who are the top 10 highest-paid employees?"
    components = _builder().build(question)
    assert question in components.user_prompt


def test_schema_snapshot_json_is_valid():
    components = _builder().build("List companies")
    data = json.loads(components.schema_snapshot_json)
    table_names = [t["name"] for t in data["tables"]]
    assert "employees" in table_names
    assert "companies" in table_names
    assert "employee_skills" in table_names


def test_dialect_profile_json_is_valid():
    components = _builder().build("List employees")
    data = json.loads(components.dialect_profile_json)
    assert "target" in data
    assert "allowed" in data


def test_custom_policy_summary_included():
    summary = "All queries must filter by TENANT."
    components = _builder(policy_summary=summary).build("List employees")
    assert summary in components.system_prompt


def test_param_bound_annotation_comes_from_policy():
    components = _builder().build("List employees")
    assert "param_bound" in components.schema_snapshot_json
    assert "TENANT" in components.schema_snapshot_json


def test_no_param_bound_without_policy():
    builder = PromptBuilder(SNAPSHOT, DIALECT)  # no policy
    components = builder.build("List employees")
    assert "param_bound" not in components.schema_snapshot_json


def test_self_referential_rel_in_schema():
    components = _builder().build("List managers")
    assert "employees__manager" in components.schema_snapshot_json


# ---------------------------------------------------------------------------
# Description fields
# ---------------------------------------------------------------------------


def _snapshot_with_descriptions() -> SchemaSnapshot:
    return SchemaSnapshot(
        tables=[
            TableInfo(
                name="orders",
                description="All customer orders placed in the system.",
                columns=[
                    ColumnInfo(name="id", type="INTEGER", nullable=False),
                    ColumnInfo(
                        name="status",
                        type="TEXT",
                        nullable=True,
                        description="Order lifecycle status. Values: PENDING, SHIPPED, DELIVERED.",
                    ),
                    ColumnInfo(name="total", type="REAL", nullable=True),
                ],
            )
        ]
    )


def test_table_description_included_in_schema_snapshot_json() -> None:
    snapshot = _snapshot_with_descriptions()
    dialect = DialectProfile.builder(["orders"]).build()
    builder = PromptBuilder(snapshot, dialect)
    components = builder.build("How many orders are shipped?")
    data = json.loads(components.schema_snapshot_json)
    orders = next(t for t in data["tables"] if t["name"] == "orders")
    assert orders.get("description") == "All customer orders placed in the system."


def test_column_description_included_in_schema_snapshot_json() -> None:
    snapshot = _snapshot_with_descriptions()
    dialect = DialectProfile.builder(["orders"]).build()
    builder = PromptBuilder(snapshot, dialect)
    components = builder.build("Count orders by status")
    data = json.loads(components.schema_snapshot_json)
    orders = next(t for t in data["tables"] if t["name"] == "orders")
    status_col = next(c for c in orders["columns"] if c["name"] == "status")
    assert "description" in status_col
    assert "PENDING" in status_col["description"]


def test_column_without_description_has_no_description_key() -> None:
    snapshot = _snapshot_with_descriptions()
    dialect = DialectProfile.builder(["orders"]).build()
    builder = PromptBuilder(snapshot, dialect)
    components = builder.build("Sum order totals")
    data = json.loads(components.schema_snapshot_json)
    orders = next(t for t in data["tables"] if t["name"] == "orders")
    total_col = next(c for c in orders["columns"] if c["name"] == "total")
    assert "description" not in total_col


def test_table_without_description_has_no_description_key() -> None:
    snapshot = SchemaSnapshot(
        tables=[
            TableInfo(
                name="items",
                columns=[ColumnInfo(name="id", type="INTEGER", nullable=False)],
            )
        ]
    )
    dialect = DialectProfile.builder(["items"]).build()
    components = PromptBuilder(snapshot, dialect).build("List items")
    data = json.loads(components.schema_snapshot_json)
    items = next(t for t in data["tables"] if t["name"] == "items")
    assert "description" not in items


def test_description_appears_in_system_prompt() -> None:
    """The description text must reach the system prompt so the LLM can read it."""
    snapshot = _snapshot_with_descriptions()
    dialect = DialectProfile.builder(["orders"]).build()
    components = PromptBuilder(snapshot, dialect).build("How many orders are shipped?")
    assert "All customer orders placed in the system." in components.system_prompt
    assert "PENDING" in components.system_prompt


def test_schema_json_round_trips_with_descriptions() -> None:
    snapshot = _snapshot_with_descriptions()
    data = snapshot.model_dump()
    restored = SchemaSnapshot.model_validate(data)
    assert restored.get_table("orders").description == "All customer orders placed in the system."  # type: ignore[union-attr]
    status = restored.get_column("orders", "status")
    assert status is not None
    assert "PENDING" in (status.description or "")
    total = restored.get_column("orders", "total")
    assert total is not None
    assert total.description is None


def test_repair_prompt_includes_error():
    error = {"error": "DISALLOWED_COLUMN", "details": {"column": "salary"}}
    previous = '{"SELECT": [{"expr": {"col": "employees.salary"}}]}'
    components = _builder().build_repair_prompt(error, previous)
    assert "DISALLOWED_COLUMN" in components.user_prompt
    assert "salary" in components.user_prompt
    assert "corrected QueryPlan JSON" in components.user_prompt
