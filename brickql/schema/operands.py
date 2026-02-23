"""Typed operand models for QueryPlan expressions.

Replaces the ``dict[str, Any]`` primitive obsession with a proper union type.
Pydantic v2 discriminated-union parsing means the LLM's raw JSON
(e.g. ``{"col": "employees.name"}``) is automatically coerced into the
correct typed dataclass without any change to the JSON schema.

Usage::

    from brickql.schema.operands import Operand, ColumnOperand, ValueOperand

    # Pydantic parses {"col": "employees.name"} → ColumnOperand(col="employees.name")
    item = SelectItem(expr={"col": "employees.name"})
    assert isinstance(item.expr, ColumnOperand)
    assert item.expr.col == "employees.name"
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag, TypeAdapter, model_validator

# ---------------------------------------------------------------------------
# Forward-reference-safe base config
# ---------------------------------------------------------------------------

_FORBID = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# Concrete operand types
# ---------------------------------------------------------------------------


class ColumnOperand(BaseModel):
    """A column reference: ``{"col": "table.column"}``."""

    model_config = _FORBID

    col: str


class ValueOperand(BaseModel):
    """A literal value: ``{"value": 42}`` / ``{"value": "text"}``."""

    model_config = _FORBID

    value: Any


class ParamOperand(BaseModel):
    """A runtime parameter: ``{"param": "TENANT"}``."""

    model_config = _FORBID

    param: str


class FuncOperand(BaseModel):
    """A function call: ``{"func": "COUNT", "args": [...]}``."""

    model_config = _FORBID

    func: str
    args: list[Operand] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# CASE expression sub-models
# ---------------------------------------------------------------------------


class CaseWhen(BaseModel):
    """A single ``WHEN <condition> THEN <result>`` clause.

    The LLM uses ``"if"`` as the condition key (matches the prompt template).
    The legacy key ``"condition"`` is also accepted for backward compatibility
    and is normalised to ``"if"`` before Pydantic field population.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # "if" is a Python keyword so we store it under the Python name
    # ``condition`` but tell Pydantic its JSON alias is ``"if"``.
    condition: dict[str, Any] = Field(alias="if")
    then: Operand

    @model_validator(mode="before")
    @classmethod
    def _normalize_condition_key(cls, data: Any) -> Any:
        """Accept ``"condition"`` as an alias for ``"if"``."""
        if isinstance(data, dict) and "condition" in data and "if" not in data:
            data = dict(data)
            data["if"] = data.pop("condition")
        return data


class CaseBody(BaseModel):
    """The body of a ``CASE`` expression."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    when: list[CaseWhen]
    # "else" is a Python keyword; stored as ``else_val``, alias ``"else"``.
    else_val: Operand | None = Field(None, alias="else")


class CaseOperand(BaseModel):
    """A CASE expression: ``{"case": {"when": [...], "else": operand}}``."""

    model_config = _FORBID

    case: CaseBody


# ---------------------------------------------------------------------------
# Discriminated union
# ---------------------------------------------------------------------------


def _operand_discriminator(v: Any) -> str | None:
    """Return the tag for the Pydantic discriminated union."""
    if isinstance(v, dict):
        for key in ("col", "value", "param", "func", "case"):
            if key in v:
                return key
    if isinstance(v, ColumnOperand):
        return "col"
    if isinstance(v, ValueOperand):
        return "value"
    if isinstance(v, ParamOperand):
        return "param"
    if isinstance(v, FuncOperand):
        return "func"
    if isinstance(v, CaseOperand):
        return "case"
    return None


Operand = Annotated[
    Annotated[ColumnOperand, Tag("col")]
    | Annotated[ValueOperand, Tag("value")]
    | Annotated[ParamOperand, Tag("param")]
    | Annotated[FuncOperand, Tag("func")]
    | Annotated[CaseOperand, Tag("case")],
    Discriminator(_operand_discriminator),
]

# Resolve forward references in recursive types.
FuncOperand.model_rebuild()
CaseWhen.model_rebuild()
CaseBody.model_rebuild()
CaseOperand.model_rebuild()

# ---------------------------------------------------------------------------
# TypeAdapter for parsing raw dicts (used inside predicate builders)
# ---------------------------------------------------------------------------

#: Parse a raw dict into a typed Operand at any call site.
OPERAND_ADAPTER: TypeAdapter[Operand] = TypeAdapter(Operand)


def to_operand(v: dict | Operand) -> Operand:
    """Convert a raw operand dict to a typed ``Operand``, or return as-is.

    This bridge function is used in predicate builders which still receive
    operands embedded inside raw predicate dicts.

    Args:
        v: A raw ``{"col": ...}`` / ``{"value": ...}`` dict, or an already-
           typed ``Operand`` instance.

    Returns:
        A typed ``Operand`` instance.
    """
    if isinstance(v, (ColumnOperand, ValueOperand, ParamOperand, FuncOperand, CaseOperand)):
        return v
    return OPERAND_ADAPTER.validate_python(v)
