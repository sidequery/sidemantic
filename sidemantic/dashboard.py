"""Portable dashboard specs and TypeScript schema generation.

The dashboard spec is intentionally semantic: charts declare metrics,
dimensions, encodings, and interactions. Rendering, SQL compilation, and
interaction pre-aggregation stay inside Sidemantic.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from sidemantic.viz import CrossfilterDashboard, CrossfilterTab

DASHBOARD_SCHEMA = "sidemantic.dashboard.v1"
TS_SCHEMA = "sidemantic.schema.v1"
VALID_CHART_TYPES = {"auto", "bar", "line", "area", "scatter", "point"}
VALID_RENDERERS = {"vega-lite", "plotly", "observable-plot", "d3", "crossfilter"}
TIME_GRANULARITIES = ["second", "minute", "hour", "day", "week", "month", "quarter", "year"]
__all__ = [
    "DASHBOARD_SCHEMA",
    "DashboardDocument",
    "DashboardSpecError",
    "build_semantic_types_schema",
    "generate_dashboard_typescript",
    "load_dashboard",
]


class DashboardSpecError(ValueError):
    """Raised when a dashboard spec cannot be loaded or built."""


@dataclass(frozen=True)
class DashboardDocument:
    """A versioned semantic dashboard definition.

    Use :meth:`from_file` for YAML/JSON specs, :meth:`from_dict` for Python
    authoring, and :meth:`to_crossfilter_dashboard` to serve it with the
    existing database-backed crossfilter runtime.
    """

    payload: dict[str, Any]

    @classmethod
    def from_file(cls, path: str | Path) -> DashboardDocument:
        """Load a dashboard spec from YAML or JSON."""
        spec_path = Path(path)
        if not spec_path.exists():
            raise DashboardSpecError(f"Dashboard spec {spec_path} does not exist")
        text = spec_path.read_text()
        if spec_path.suffix.lower() == ".json":
            payload = json.loads(text)
        else:
            payload = yaml.safe_load(text)
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> DashboardDocument:
        """Create a dashboard document from a Python mapping."""
        if not isinstance(payload, Mapping):
            raise DashboardSpecError("Dashboard spec must be a mapping")
        return cls(dict(payload))

    @property
    def title(self) -> str:
        return str(self.payload.get("title") or "Sidemantic Dashboard")

    @property
    def tabs(self) -> list[dict[str, Any]]:
        return list(self.payload.get("tabs") or [])

    def to_dict(self) -> dict[str, Any]:
        """Return the portable dashboard payload."""
        return dict(self.payload)

    def validate(self, layer) -> list[str]:
        """Return validation errors for this dashboard against a semantic layer."""
        schema = build_semantic_types_schema(layer)
        metrics = set(schema["metrics"])
        dimensions = set(schema["dimensions"])
        fields = set(schema["fields"])
        errors: list[str] = []

        declared_schema = self.payload.get("schema")
        if declared_schema not in (None, DASHBOARD_SCHEMA):
            errors.append(f"schema must be {DASHBOARD_SCHEMA!r}")

        if not isinstance(self.payload.get("title"), str) or not self.payload.get("title"):
            errors.append("title is required")

        defaults = self.payload.get("defaults") or {}
        if defaults and not isinstance(defaults, Mapping):
            errors.append("defaults must be a mapping")
        elif isinstance(defaults, Mapping) and "renderer" in defaults:
            default_renderer = _normalize_renderer(str(defaults.get("renderer")))
            if default_renderer not in VALID_RENDERERS:
                errors.append(f"defaults.renderer must be one of: {', '.join(sorted(VALID_RENDERERS))}")

        tabs = self.payload.get("tabs")
        if not isinstance(tabs, list) or not tabs:
            errors.append("tabs must be a non-empty list")
            return errors

        tab_ids: set[str] = set()
        for tab_index, tab in enumerate(tabs):
            path = f"tabs[{tab_index}]"
            if not isinstance(tab, Mapping):
                errors.append(f"{path} must be a mapping")
                continue
            tab_id = tab.get("id")
            if not isinstance(tab_id, str) or not tab_id:
                errors.append(f"{path}.id is required")
            elif tab_id in tab_ids:
                errors.append(f"{path}.id duplicates {tab_id!r}")
            else:
                tab_ids.add(tab_id)

            charts = tab.get("charts")
            if not isinstance(charts, list) or not charts:
                errors.append(f"{path}.charts must be a non-empty list")
                continue
            if len(charts) != 1:
                errors.append(f"{path}.charts currently supports exactly one chart for dashboard serve")

            chart_ids: set[str] = set()
            for chart_index, chart in enumerate(charts):
                chart_path = f"{path}.charts[{chart_index}]"
                if not isinstance(chart, Mapping):
                    errors.append(f"{chart_path} must be a mapping")
                    continue
                errors.extend(_validate_chart(chart_path, chart, metrics, dimensions, fields, layer))
                chart_id = chart.get("id")
                if isinstance(chart_id, str):
                    if chart_id in chart_ids:
                        errors.append(f"{chart_path}.id duplicates {chart_id!r}")
                    chart_ids.add(chart_id)
        return errors

    def to_crossfilter_dashboard(self, layer) -> CrossfilterDashboard:
        """Build a live crossfilter dashboard from this semantic spec."""
        errors = self.validate(layer)
        if errors:
            raise DashboardSpecError("; ".join(errors))

        tabs: list[CrossfilterTab] = []
        for tab in self.tabs:
            chart = tab["charts"][0]
            session = _build_chart(layer, chart, self.payload).crossfilter(
                table_limit=int(_first_present(chart, "table_limit", "tableLimit") or 75),
                source_record_count=_optional_int(_first_present(chart, "source_record_count", "sourceRecordCount")),
                interaction_preaggregations=_interaction_preaggregations(self.payload, chart),
                renderer=_dashboard_renderer(self.payload, chart),
            )
            tabs.append(
                CrossfilterTab(
                    str(tab["id"]),
                    str(tab.get("label") or tab["id"]),
                    session,
                    source_record_count=_optional_int(_first_present(tab, "source_record_count", "sourceRecordCount")),
                )
            )
        return CrossfilterDashboard(
            self.title,
            tabs,
            query_endpoint=str(self.payload.get("query_endpoint") or "/crossfilter/query"),
        )


def load_dashboard(path: str | Path) -> DashboardDocument:
    """Load a dashboard spec from YAML or JSON."""
    return DashboardDocument.from_file(path)


def build_semantic_types_schema(layer) -> dict[str, Any]:
    """Return the semantic fields needed for generated JS/TS dashboard typing."""
    models: dict[str, Any] = {}
    metric_fields: list[str] = []
    dimension_fields: list[str] = []
    field_types: dict[str, str] = {}

    for model_name, model in sorted(layer.graph.models.items()):
        model_payload = {"metrics": {}, "dimensions": {}}
        for metric in sorted(model.metrics, key=lambda item: item.name):
            field = f"{model_name}.{metric.name}"
            metric_fields.append(field)
            field_types[field] = "number"
            model_payload["metrics"][metric.name] = {
                "field": field,
                "type": "number",
                "agg": metric.agg,
                "description": getattr(metric, "description", None),
            }

        for dimension in sorted(model.dimensions, key=lambda item: item.name):
            base_field = f"{model_name}.{dimension.name}"
            fields = [base_field]
            if dimension.type == "time":
                granularities = dimension.supported_granularities or TIME_GRANULARITIES
                fields.extend(f"{base_field}__{granularity}" for granularity in granularities)
            for field in fields:
                dimension_fields.append(field)
                field_types[field] = _ts_scalar_for_dimension(dimension.type)
            model_payload["dimensions"][dimension.name] = {
                "field": base_field,
                "fields": fields,
                "type": dimension.type,
                "description": dimension.description,
                "label": dimension.label,
            }
        models[model_name] = model_payload

    for metric_name, metric in sorted(layer.graph.metrics.items()):
        if "." in metric_name:
            field = metric_name
        else:
            field = metric_name
        if field not in metric_fields:
            metric_fields.append(field)
            field_types[field] = "number"

    metric_fields = sorted(set(metric_fields))
    dimension_fields = sorted(set(dimension_fields))
    return {
        "schema": TS_SCHEMA,
        "models": models,
        "metrics": metric_fields,
        "dimensions": dimension_fields,
        "fields": sorted({*metric_fields, *dimension_fields}),
        "fieldTypes": {field: field_types[field] for field in sorted(field_types)},
    }


def generate_dashboard_typescript(layer, *, schema_name: str = "sidemanticSchema") -> str:
    """Generate self-contained TypeScript types for dashboard authoring."""
    schema = build_semantic_types_schema(layer)
    value_map = "\n".join(
        f"  {json.dumps(field)}: {_ts_value_type(field_type)};"
        for field, field_type in sorted(schema["fieldTypes"].items())
    )
    schema_json = json.dumps(schema, indent=2, default=str)
    return f"""/* Generated by `sidemantic dashboard types`. Do not edit by hand. */
export const {schema_name} = {schema_json} as const;

export type SidemanticSchema = typeof {schema_name};
export type Metric = typeof {schema_name}.metrics[number];
export type Dimension = typeof {schema_name}.dimensions[number];
export type SemanticField = typeof {schema_name}.fields[number];
export type DashboardRenderer = "vega-lite" | "plotly" | "observable-plot" | "d3" | "crossfilter";
export type ChartType = "auto" | "bar" | "line" | "area" | "scatter" | "point";

export interface FieldValueMap {{
{value_map}
}}

export type FieldValue<F extends SemanticField> = F extends keyof FieldValueMap ? FieldValueMap[F] : never;

export type DashboardConfig = {{
  schema?: "sidemantic.dashboard.v1";
  title: string;
  defaults?: DashboardDefaults;
  tabs: readonly DashboardTab[];
}};

export type DashboardDefaults = {{
  renderer?: DashboardRenderer;
  query?: {{
    interactionPreaggregations?: boolean;
    interaction_preaggregations?: boolean;
    usePreaggregations?: boolean;
    use_preaggregations?: boolean;
  }};
  interactions?: {{
    scope?: "chart" | "tab" | "dashboard";
  }};
}};

export type DashboardTab = {{
  id: string;
  label?: string;
  sourceRecordCount?: number;
  source_record_count?: number;
  charts: readonly DashboardChart[];
}};

export type DashboardChart = {{
  id: string;
  title?: string;
  type?: ChartType;
  renderer?: DashboardRenderer;
  tableLimit?: number;
  table_limit?: number;
  sourceRecordCount?: number;
  source_record_count?: number;
  query: ChartQuery;
  encoding?: ChartEncoding;
  interactions?: ChartInteractions;
}};

export type ChartQuery = {{
  metrics: readonly Metric[];
  dimensions?: readonly Dimension[];
  filters?: readonly string[];
  segments?: readonly string[];
  orderBy?: readonly SemanticField[];
  order_by?: readonly SemanticField[];
  limit?: number;
  interactionPreaggregations?: boolean;
  interaction_preaggregations?: boolean;
  usePreaggregations?: boolean;
  use_preaggregations?: boolean;
}};

export type ChartEncoding = {{
  x?: SemanticField;
  y?: Metric | readonly Metric[];
  color?: Dimension;
  size?: Metric;
  facet?: Dimension;
}};

export type ChartInteractions = {{
  crossfilter?: boolean;
  brush?: boolean | {{
    fields?: readonly SemanticField[];
    channel?: "x" | "y" | "xy";
  }};
  select?: boolean | {{
    fields?: readonly SemanticField[];
  }};
}};

export function defineDashboard<const T extends DashboardConfig>(dashboard: T): T {{
  return dashboard;
}}
"""


def _validate_chart(
    path: str,
    chart: Mapping[str, Any],
    metrics: set[str],
    dimensions: set[str],
    fields: set[str],
    layer,
) -> list[str]:
    errors: list[str] = []
    chart_type = str(chart.get("type") or "auto")
    if chart_type not in VALID_CHART_TYPES:
        errors.append(f"{path}.type must be one of: {', '.join(sorted(VALID_CHART_TYPES))}")

    renderer = _normalize_renderer(str(chart.get("renderer") or "crossfilter"))
    if renderer not in VALID_RENDERERS:
        errors.append(f"{path}.renderer must be one of: {', '.join(sorted(VALID_RENDERERS))}")

    query = chart.get("query")
    if not isinstance(query, Mapping):
        errors.append(f"{path}.query is required")
        return errors

    query_metrics = _as_list(query.get("metrics"))
    query_dimensions = _as_list(query.get("dimensions"))
    if not query_metrics:
        errors.append(f"{path}.query.metrics must be a non-empty list")
    for metric in query_metrics:
        if metric not in metrics:
            errors.append(f"{path}.query.metrics contains unknown metric {metric!r}")
    for dimension in query_dimensions:
        if dimension not in dimensions:
            errors.append(f"{path}.query.dimensions contains unknown dimension {dimension!r}")
    errors.extend(_validate_order_by(path, _order_by(query), query_metrics, query_dimensions, fields))

    encoding = chart.get("encoding") or {}
    if encoding and not isinstance(encoding, Mapping):
        errors.append(f"{path}.encoding must be a mapping")
    elif isinstance(encoding, Mapping):
        errors.extend(_validate_encoding(path, encoding, query_metrics, query_dimensions, fields))

    interactions = chart.get("interactions") or {}
    if interactions and not isinstance(interactions, Mapping):
        errors.append(f"{path}.interactions must be a mapping")
    elif isinstance(interactions, Mapping):
        errors.extend(_validate_interactions(path, interactions, fields))

    if not errors:
        try:
            layer.compile(
                metrics=query_metrics,
                dimensions=query_dimensions or None,
                filters=_as_list(query.get("filters")) or None,
                segments=_as_list(query.get("segments")) or None,
                order_by=_order_by(query) or None,
                limit=_optional_int(query.get("limit")),
                use_preaggregations=_optional_bool(_first_present(query, "use_preaggregations", "usePreaggregations")),
            )
        except Exception as exc:
            errors.append(f"{path}.query cannot compile: {exc}")
    return errors


def _validate_encoding(
    path: str,
    encoding: Mapping[str, Any],
    query_metrics: list[str],
    query_dimensions: list[str],
    fields: set[str],
) -> list[str]:
    errors: list[str] = []
    query_fields = {*query_metrics, *query_dimensions}
    for key in ("x", "color", "size", "facet"):
        field = encoding.get(key)
        if field is None:
            continue
        if field not in fields:
            errors.append(f"{path}.encoding.{key} contains unknown field {field!r}")
        elif field not in query_fields:
            errors.append(f"{path}.encoding.{key} must also appear in query.metrics or query.dimensions")
    y_fields = _as_list(encoding.get("y"))
    for field in y_fields:
        if field not in fields:
            errors.append(f"{path}.encoding.y contains unknown field {field!r}")
        elif field not in query_metrics:
            errors.append(f"{path}.encoding.y must be one of query.metrics")
    return errors


def _validate_order_by(
    path: str,
    order_by: list[Any],
    query_metrics: list[str],
    query_dimensions: list[str],
    fields: set[str],
) -> list[str]:
    errors: list[str] = []
    query_fields = {*query_metrics, *query_dimensions}
    for order_field in order_by:
        field = _order_by_field_ref(order_field)
        if field is None:
            errors.append(f"{path}.query.order_by must contain field names with optional ASC or DESC")
        elif field not in fields:
            errors.append(f"{path}.query.order_by contains unknown field {field!r}")
        elif field not in query_fields:
            errors.append(
                f"{path}.query.order_by field {field!r} must also appear in query.metrics or query.dimensions"
            )
    return errors


def _order_by_field_ref(order_field: Any) -> str | None:
    if not isinstance(order_field, str) or not order_field.strip():
        return None
    parts = order_field.strip().rsplit(" ", 1)
    if len(parts) == 2 and parts[1].upper() in {"ASC", "DESC"}:
        return parts[0].strip() or None
    return order_field.strip()


def _validate_interactions(path: str, interactions: Mapping[str, Any], fields: set[str]) -> list[str]:
    errors: list[str] = []
    for key in ("brush", "select"):
        interaction = interactions.get(key)
        if interaction in (None, False, True):
            continue
        if not isinstance(interaction, Mapping):
            errors.append(f"{path}.interactions.{key} must be a boolean or mapping")
            continue
        for field in _as_list(interaction.get("fields")):
            if field not in fields:
                errors.append(f"{path}.interactions.{key}.fields contains unknown field {field!r}")
    return errors


def _build_chart(layer, chart: Mapping[str, Any], dashboard_payload: Mapping[str, Any]):
    query = chart["query"]
    encoding = chart.get("encoding") or {}
    metrics = _ordered_metrics(_as_list(query.get("metrics")), encoding)
    dimensions = _ordered_dimensions(_as_list(query.get("dimensions")), encoding)
    builder = layer.chart(
        metrics,
        by=dimensions or None,
        mark=str(chart.get("type") or "auto"),
        filters=_as_list(query.get("filters")) or None,
        segments=_as_list(query.get("segments")) or None,
        order_by=_order_by(query) or None,
        limit=_optional_int(query.get("limit")),
        title=chart.get("title") or dashboard_payload.get("title"),
        use_preaggregations=_optional_bool(_first_present(query, "use_preaggregations", "usePreaggregations")),
    )
    interactions = chart.get("interactions") or {}
    brush = interactions.get("brush") if isinstance(interactions, Mapping) else None
    if brush:
        channel = "x"
        if isinstance(brush, Mapping):
            channel = str(brush.get("channel") or channel)
        builder.brush(channel=channel if channel in {"x", "y", "xy"} else "x")
    return builder


def _ordered_metrics(metrics: list[str], encoding: Mapping[str, Any]) -> list[str]:
    ordered = list(metrics)
    y_fields = _as_list(encoding.get("y"))
    for field in reversed(y_fields):
        if field in ordered:
            ordered.remove(field)
            ordered.insert(0, field)
    return ordered


def _ordered_dimensions(dimensions: list[str], encoding: Mapping[str, Any]) -> list[str]:
    ordered = list(dimensions)
    for field, position in ((encoding.get("x"), 0), (encoding.get("color"), 1)):
        if isinstance(field, str) and field in ordered:
            ordered.remove(field)
            ordered.insert(min(position, len(ordered)), field)
    return ordered


def _interaction_preaggregations(dashboard_payload: Mapping[str, Any], chart: Mapping[str, Any]) -> bool:
    query = chart.get("query") or {}
    defaults = dashboard_payload.get("defaults") or {}
    default_query = defaults.get("query") if isinstance(defaults, Mapping) else {}
    configured = _first_present(query, "interaction_preaggregations", "interactionPreaggregations")
    if configured is None:
        configured = _first_present(chart, "interaction_preaggregations", "interactionPreaggregations")
    if configured is None:
        configured = _first_present(
            default_query or {},
            "interaction_preaggregations",
            "interactionPreaggregations",
        )
    return bool(_optional_bool(configured))


def _dashboard_renderer(dashboard_payload: Mapping[str, Any], chart: Mapping[str, Any]) -> str:
    defaults = dashboard_payload.get("defaults") or {}
    default_renderer = defaults.get("renderer") if isinstance(defaults, Mapping) else None
    return _normalize_renderer(str(chart.get("renderer") or default_renderer or "d3"))


def _order_by(query: Mapping[str, Any]) -> list[str]:
    return _as_list(_first_present(query, "order_by", "orderBy"))


def _first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _normalize_renderer(renderer: str) -> str:
    normalized = renderer.lower().replace("_", "-")
    aliases = {
        "vegalite": "vega-lite",
        "vl": "vega-lite",
        "observable": "observable-plot",
        "plot": "observable-plot",
        "linked": "crossfilter",
        "dashboard": "crossfilter",
    }
    return aliases.get(normalized, normalized)


def _ts_scalar_for_dimension(dimension_type: str) -> str:
    return {
        "categorical": "string",
        "time": "date",
        "boolean": "boolean",
        "numeric": "number",
    }.get(dimension_type, "unknown")


def _ts_value_type(field_type: str) -> str:
    return {
        "number": "number",
        "string": "string",
        "date": "string | Date",
        "boolean": "boolean",
    }.get(field_type, "unknown")
