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


def qualify_expression_fields(
    expressions: Collection[str],
    base_model: str,
    *,
    graph_metrics: Collection[str] = (),
) -> list[str]:
    """Qualify bare semantic fields in SQL-like contract expressions."""
    from sqlglot import exp, parse_one

    qualified: list[str] = []
    for expression in expressions:
        parsed = parse_one(expression)

        def qualify_column(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column) or node.table or node.name in graph_metrics:
                return node
            result = node.copy()
            result.set("table", exp.to_identifier(base_model))
            return result

        qualified.append(parsed.transform(qualify_column).sql())
    return qualified


def qualify_order_by_fields(
    expressions: Collection[str],
    base_model: str,
    *,
    graph_metrics: Collection[str] = (),
) -> list[str]:
    """Qualify bare semantic fields while preserving ORDER BY direction."""
    from sqlglot import exp, parse_one

    qualified: list[str] = []
    for expression in expressions:
        statement = parse_one(f"SELECT 1 ORDER BY {expression}")
        ordered = statement.args["order"].expressions

        def qualify_column(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column) or node.table or node.name in graph_metrics:
                return node
            result = node.copy()
            result.set("table", exp.to_identifier(base_model))
            return result

        qualified.extend(item.transform(qualify_column).sql() for item in ordered)
    return qualified


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


def graph_metric_is_public(metric: Any, graph: Any) -> bool:
    """Return whether a graph metric and every source model it queries are public."""
    if not metric.public or metric.visibility != "public":
        return False

    # Use the same dependency/source resolution as SQL generation so derived and
    # ratio metrics cannot hide a private model behind a public graph-level name.
    from sidemantic.sql.generator import SQLGenerator

    source_models = SQLGenerator(graph)._find_required_models([metric.name], [], metric.filters)
    return all(
        (model := graph.models.get(model_name)) is not None and model.visibility == "public"
        for model_name in source_models
    )


def _semantic_reference_is_public(
    reference: str, base_model: str, graph: Any, *, prefer_graph_metric: bool = False
) -> bool:
    if prefer_graph_metric and reference in graph.metrics:
        return graph_metric_is_public(graph.metrics[reference], graph)
    if "." not in reference:
        if not base_model:
            return False
        reference = f"{base_model}.{reference}"
    model_name, field_name = reference.split(".", 1)
    field_name = field_name.rsplit("__", 1)[0]
    model = graph.models.get(model_name)
    if model is None or model.visibility != "public":
        return False
    dimension = model.get_dimension(field_name)
    if dimension is not None:
        return dimension.public
    metric = model.get_metric(field_name)
    if metric is not None:
        return metric.public and metric.visibility == "public"
    return False


def _expressions_are_public(expressions: Collection[str], base_model: str, graph: Any) -> bool:
    try:
        references = expression_field_references(expressions, base_model, graph_metrics=graph.metrics.keys())
    except Exception:
        return False
    return all(
        _semantic_reference_is_public(reference, base_model, graph, prefer_graph_metric=True)
        for reference in references
    )


def consumption_contract_is_public(value: Explore | SavedQuery, graph: Any) -> bool:
    """Return whether a contract can be safely discovered with visibility enforcement."""
    if value.visibility != "public":
        return False
    if isinstance(value, Explore):
        model = graph.models.get(value.model)
        if model is None or model.visibility != "public":
            return False
        dimension_references = [*(value.allowed_dimensions or []), *value.default_dimensions]
        metric_references = [*(value.allowed_metrics or []), *value.default_metrics]
        flexible_references = [*(value.allowed_filter_fields or []), *(value.allowed_order_by or [])]
        return (
            all(_semantic_reference_is_public(reference, value.model, graph) for reference in dimension_references)
            and all(
                _semantic_reference_is_public(reference, value.model, graph, prefer_graph_metric=True)
                for reference in metric_references
            )
            and all(
                _semantic_reference_is_public(reference, value.model, graph, prefer_graph_metric=True)
                for reference in flexible_references
            )
            and _expressions_are_public(
                [*value.filters, *value.default_filters, *value.default_order_by], value.model, graph
            )
        )

    base_model = ""
    if value.explore:
        explore = graph.explores.get(value.explore)
        if explore is None or not consumption_contract_is_public(explore, graph):
            return False
        base_model = explore.model
    if not all(_semantic_reference_is_public(reference, base_model, graph) for reference in value.dimensions):
        return False
    if not all(
        _semantic_reference_is_public(reference, base_model, graph, prefer_graph_metric=True)
        for reference in value.metrics
    ):
        return False
    if not _expressions_are_public([*value.filters, *value.order_by], base_model, graph):
        return False
    for raw_segment in value.segments:
        segment_ref = (
            raw_segment if "." in raw_segment else f"{base_model}.{raw_segment}" if base_model else raw_segment
        )
        if "." not in segment_ref:
            return False
        model_name, segment_name = segment_ref.split(".", 1)
        model = graph.models.get(model_name)
        segment = model.get_segment(segment_name) if model is not None and model.visibility == "public" else None
        if segment is None or not segment.public:
            return False
    return True


def serialize_consumption_contract(
    value: Explore | SavedQuery, graph: Any, *, enforce_visibility: bool = False
) -> dict[str, Any] | None:
    """Serialize a contract, omitting unsafe contracts and adapter metadata when visibility is enforced."""
    if enforce_visibility and not consumption_contract_is_public(value, graph):
        return None
    return value.model_dump(
        exclude={"metadata"} if enforce_visibility else None,
        exclude_none=True,
        exclude_defaults=True,
        mode="json",
    )
