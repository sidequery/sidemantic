"""Curated semantic consumption contracts."""

from collections.abc import Collection
from typing import Any

from pydantic import ConfigDict, Field, model_validator

from sidemantic.core.governance import GovernedObject


def expression_field_references(
    expressions: Collection[str],
    base_model: str,
    *,
    graph_metrics: Collection[str] = (),
) -> set[str]:
    """Extract and qualify semantic field references from SQL-like expressions."""
    from sqlglot import exp, parse_one

    references: set[str] = set()
    for expression in expressions:
        parsed = parse_one(expression)
        for column in parsed.find_all(exp.Column):
            if column.table:
                references.add(f"{column.table}.{column.name}")
            elif column.name in graph_metrics:
                references.add(column.name)
            else:
                references.add(f"{base_model}.{column.name}")
    return references


class Explore(GovernedObject):
    """Curated entrypoint over one or more models in the semantic graph.

    ``model`` names the base model. Allowed fields constrain callers, defaults
    populate omitted selections, and ``filters`` are mandatory for every query.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Unique explore name")
    model: str = Field(..., description="Base model in the physical semantic graph")
    label: str | None = Field(None, description="Display label")
    description: str | None = Field(None, description="Human-readable description")
    allowed_dimensions: list[str] | None = Field(
        None, description="Dimension allowlist; null means every graph dimension is allowed"
    )
    allowed_metrics: list[str] | None = Field(
        None, description="Metric allowlist; null means every graph metric is allowed"
    )
    allowed_filter_fields: list[str] | None = Field(
        None, description="Field allowlist for caller-supplied filters; null means unrestricted"
    )
    allowed_order_by: list[str] | None = Field(
        None, description="Field allowlist for caller-supplied ordering; null means unrestricted"
    )
    default_dimensions: list[str] = Field(default_factory=list)
    default_metrics: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list, description="Mandatory filters applied to every query")
    default_filters: list[str] = Field(default_factory=list, description="Filters used when callers omit filters")
    default_order_by: list[str] = Field(default_factory=list)
    default_limit: int | None = Field(None, ge=0)
    max_limit: int | None = Field(None, ge=0)
    metadata: dict[str, Any] | None = Field(None, description="Source-adapter metadata for lossless round-tripping")

    @model_validator(mode="after")
    def validate_defaults(self) -> "Explore":
        def qualify(value: str) -> str:
            return value if "." in value else f"{self.model}.{value}"

        if self.allowed_dimensions is not None:
            allowed = {qualify(value) for value in self.allowed_dimensions}
            outside = sorted(value for value in self.default_dimensions if qualify(value) not in allowed)
            if outside:
                raise ValueError(f"default_dimensions are not allowed: {', '.join(outside)}")
        if self.allowed_metrics is not None:
            allowed = {qualify(value) for value in self.allowed_metrics}
            outside = sorted(value for value in self.default_metrics if qualify(value) not in allowed)
            if outside:
                raise ValueError(f"default_metrics are not allowed: {', '.join(outside)}")
        if self.allowed_filter_fields is not None:
            allowed = {qualify(value) for value in self.allowed_filter_fields}
            outside = sorted(expression_field_references(self.default_filters, self.model) - allowed)
            if outside:
                raise ValueError(f"default_filters reference fields that are not allowed: {', '.join(outside)}")
        if self.allowed_order_by is not None:
            allowed = {qualify(value) for value in self.allowed_order_by}
            outside = sorted(expression_field_references(self.default_order_by, self.model) - allowed)
            if outside:
                raise ValueError(f"default_order_by references fields that are not allowed: {', '.join(outside)}")
        if self.default_limit is not None and self.max_limit is not None and self.default_limit > self.max_limit:
            raise ValueError("default_limit cannot exceed max_limit")
        return self


# Hex and other tools call this concept a View. Keep one schema and two names.
View = Explore


class SavedQuery(GovernedObject):
    """Named, immutable structured semantic query."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Unique saved-query name")
    explore: str | None = Field(None, description="Optional Explore contract governing this query")
    label: str | None = Field(None, description="Display label")
    description: str | None = Field(None, description="Human-readable description")
    dimensions: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    segments: list[str] = Field(default_factory=list)
    order_by: list[str] = Field(default_factory=list)
    limit: int | None = Field(None, ge=0)
    parameters: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = Field(None, description="Source-adapter metadata for lossless round-tripping")
