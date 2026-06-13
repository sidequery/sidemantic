"""Headless chart builders for Sidemantic semantic queries.

This module intentionally emits plain Python dictionaries instead of depending
on renderer packages. Browser or notebook clients can feed the resulting specs
to Vega-Lite, Plotly, Observable Plot, or custom D3 code.
"""

from __future__ import annotations

import hashlib
import json
import time
from calendar import monthrange
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

from sidemantic.server.common import to_json_compatible
from sidemantic.vendor_assets import inline_vendor_scripts

ChartMark = Literal["auto", "bar", "line", "area", "scatter", "point"]
Renderer = Literal["vega-lite", "plotly", "observable-plot", "d3", "crossfilter"]
CROSSFILTER_CHART_RENDERERS = ("vega-lite", "plotly", "observable-plot", "d3")
PALETTE = ["#2E5EAA", "#CB4B16", "#2F7D32", "#7B3F98", "#008C95", "#B85C00"]
__all__ = [
    "BrushSelection",
    "ChartBuilder",
    "ChartData",
    "CrossfilterDashboard",
    "CrossfilterPlanner",
    "CrossfilterQueryResponse",
    "CrossfilterSelection",
    "CrossfilterSession",
    "CrossfilterTab",
    "DimensionEquals",
    "InteractionPreaggTable",
    "MetricRange",
    "TimeRange",
    "crossfilter_tabs_html",
]


def crossfilter_tabs_html(title: str, tabs: list[dict[str, Any]]) -> str:
    """Return crossfilter HTML with multiple selectable chart specs."""
    if not tabs:
        raise ValueError("At least one tab is required")
    safe_json = json.dumps({"renderer": "sidemantic-crossfilter-tabs", "tabs": tabs}, default=str).replace(
        "<", "\\u003c"
    )
    return _html_shell(title, _crossfilter_body(safe_json))


def _lazy_tab_spec_endpoint(base_endpoint: str, tab_id: str) -> str:
    return f"{base_endpoint}?tab={quote(tab_id, safe='')}"


@dataclass(frozen=True)
class ChartData:
    """Materialized chart query result."""

    sql: str
    columns: list[str]
    rows: list[dict[str, Any]]
    dimension_columns: list[str]
    metric_columns: list[str]


@dataclass(frozen=True)
class CompiledField:
    """One semantic field compiled to one renderer/runtime column."""

    id: str
    semantic_ref: str
    alias: str
    kind: Literal["dimension", "metric"]
    source_model: str | None
    roles: tuple[str, ...] = ()
    metric_agg: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.id,
            "semantic_ref": self.semantic_ref,
            "alias": self.alias,
            "label": _label(self.alias),
            "kind": self.kind,
            "source_model": self.source_model,
            "roles": list(self.roles),
        }
        if self.kind == "metric":
            payload["metric_agg"] = self.metric_agg
        return payload


@dataclass(frozen=True)
class CompiledChartPlan:
    """Canonical field lineage and interaction plan for a chart runtime."""

    fields: tuple[CompiledField, ...]
    encodings: dict[str, Any]
    interactions: dict[str, Any]
    fingerprint: str

    @classmethod
    def build(cls, chart: ChartBuilder, data: ChartData) -> CompiledChartPlan:
        aliases = [*data.dimension_columns, *data.metric_columns]
        duplicate_aliases = _duplicates(aliases)
        if duplicate_aliases:
            duplicate_list = ", ".join(sorted(duplicate_aliases))
            raise ValueError(
                f"Compiled chart plan received duplicate output alias(es): {duplicate_list}. "
                "Chart SQL aliases must be unique before plan compilation."
            )

        x_alias = chart._x_column(data)
        y_aliases = chart._y_columns(data)
        series_alias = chart._series_column(data)
        metric_aggs = _metric_aggs(chart, data.metric_columns)

        fields: list[CompiledField] = []
        for ref, alias in zip(chart.dimensions, data.dimension_columns):
            roles = ["dimension"]
            if alias == x_alias:
                roles.append("x")
            if alias == series_alias:
                roles.append("series")
            if alias != x_alias:
                roles.append("breakdown")
            fields.append(
                CompiledField(
                    id=ref,
                    semantic_ref=ref,
                    alias=alias,
                    kind="dimension",
                    source_model=_ref_model(ref),
                    roles=tuple(roles),
                )
            )

        for ref, alias in zip(chart.metrics, data.metric_columns):
            roles = ["metric"]
            if alias in y_aliases:
                roles.append("y")
            fields.append(
                CompiledField(
                    id=ref,
                    semantic_ref=ref,
                    alias=alias,
                    kind="metric",
                    source_model=_ref_model(ref),
                    roles=tuple(roles),
                    metric_agg=metric_aggs.get(alias),
                )
            )

        field_tuple = tuple(fields)
        encodings = _compiled_encodings(field_tuple, x_alias, y_aliases, series_alias)
        interactions = _compiled_interactions(chart, data, field_tuple, encodings)
        fingerprint = _chart_plan_fingerprint(chart, field_tuple, encodings, interactions)
        return cls(field_tuple, encodings, interactions, fingerprint)

    def field_plan(self) -> dict[str, Any]:
        return {
            "protocol": "sidemantic-field-plan-v1",
            "fingerprint": self.fingerprint,
            "fields": [field.to_dict() for field in self.fields],
            "aliases": {field.alias: field.id for field in self.fields},
            "encodings": self.encodings,
        }

    def interaction_plan(self) -> dict[str, Any]:
        return {
            "protocol": "sidemantic-interaction-plan-v1",
            "fingerprint": self.fingerprint,
            **self.interactions,
        }

    def legacy_interactions(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        brush = self.interactions.get("brush")
        if brush:
            payload["brush"] = {
                "fields": [field["alias"] for field in brush.get("fields", [])],
                "channel": brush.get("channel") or "x",
            }
        select = self.interactions.get("select")
        if select:
            payload["select"] = {"fields": [field["alias"] for field in select.get("fields", [])]}
        return payload


@dataclass(frozen=True)
class BrushSelection:
    """Portable interval selection metadata."""

    name: str = "brush"
    channel: Literal["x", "y", "xy"] = "x"

    def to_dict(self) -> dict[str, Any]:
        return {"type": "interval", "name": self.name, "channel": self.channel}


@dataclass(frozen=True)
class DimensionEquals:
    """A categorical crossfilter predicate on a semantic dimension."""

    field: str
    value: Any
    key: str | None = None

    @property
    def ignore_key(self) -> str:
        return self.key or f"category:{self.field}"

    def to_expressions(self, context: _FilterContext) -> list[str]:
        return [f"{context.dimension_ref(self.field)} = {_literal(self.value)}"]

    def to_table_expressions(self, context: _FilterContext) -> list[str]:
        return [f"{_identifier(context.dimension_alias(self.field))} = {_literal(self.value)}"]

    def to_dict(self) -> dict[str, Any]:
        return {"type": "category", "field": self.field, "value": self.value}


@dataclass(frozen=True)
class TimeRange:
    """A range predicate on a semantic time dimension."""

    field: str
    min: Any
    max: Any
    key: str = "xRange"

    @property
    def ignore_key(self) -> str:
        return self.key

    def to_expressions(self, context: _FilterContext) -> list[str]:
        return _range_filter_expressions(context.dimension_ref(self.field), self.min, self.max)

    def to_table_expressions(self, context: _FilterContext) -> list[str]:
        return _range_table_filter_expressions(context.dimension_alias(self.field), self.min, self.max)

    def to_dict(self) -> dict[str, Any]:
        return {"type": "xRange", "field": self.field, "min": self.min, "max": self.max}


@dataclass(frozen=True)
class MetricRange:
    """A range predicate on one or two semantic metrics."""

    x_field: str
    x_min: float
    x_max: float
    y_field: str | None = None
    y_min: float | None = None
    y_max: float | None = None
    key: str = "metricRange"

    @property
    def ignore_key(self) -> str:
        return self.key

    def to_expressions(self, context: _FilterContext) -> list[str]:
        expressions = [
            f"{context.metric_ref(self.x_field)} >= {float(self.x_min)}",
            f"{context.metric_ref(self.x_field)} <= {float(self.x_max)}",
        ]
        if self.y_field is not None and self.y_min is not None and self.y_max is not None:
            expressions.extend(
                [
                    f"{context.metric_ref(self.y_field)} >= {float(self.y_min)}",
                    f"{context.metric_ref(self.y_field)} <= {float(self.y_max)}",
                ]
            )
        return expressions

    def to_table_expressions(self, context: _FilterContext) -> list[str]:
        x_field = _identifier(context.metric_alias(self.x_field))
        expressions = [
            f"{x_field} >= {float(self.x_min)}",
            f"{x_field} <= {float(self.x_max)}",
        ]
        if self.y_field is not None and self.y_min is not None and self.y_max is not None:
            y_field = _identifier(context.metric_alias(self.y_field))
            expressions.extend(
                [
                    f"{y_field} >= {float(self.y_min)}",
                    f"{y_field} <= {float(self.y_max)}",
                ]
            )
        return expressions

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "metricRange",
            "xField": self.x_field,
            "xMin": self.x_min,
            "xMax": self.x_max,
        }
        if self.y_field is not None:
            payload.update({"yField": self.y_field, "yMin": self.y_min, "yMax": self.y_max})
        return payload


CrossfilterFilter = DimensionEquals | TimeRange | MetricRange


@dataclass(frozen=True)
class CrossfilterSelection:
    """Browser-neutral crossfilter request state."""

    filters: tuple[CrossfilterFilter, ...] = ()
    event: str | None = None
    active: dict[str, Any] | None = None
    interaction_preaggregations: bool | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> CrossfilterSelection:
        return cls.from_filters(
            payload.get("filters") or (),
            event=payload.get("event"),
            active=payload.get("active"),
            interaction_preaggregations=payload.get("interaction_preaggregations"),
        )

    @classmethod
    def from_filters(
        cls,
        filters: list[dict[str, Any] | CrossfilterFilter] | tuple[dict[str, Any] | CrossfilterFilter, ...] | None,
        *,
        event: str | None = None,
        active: dict[str, Any] | None = None,
        interaction_preaggregations: bool | None = None,
    ) -> CrossfilterSelection:
        return cls(
            tuple(_coerce_crossfilter_filter(filter_def) for filter_def in (filters or ())),
            event=event,
            active=active,
            interaction_preaggregations=interaction_preaggregations,
        )

    def expressions(self, context: _FilterContext, *, ignore: str | None = None) -> list[str]:
        expressions: list[str] = []
        for filter_def in self.filters:
            if _filter_ignore_key(filter_def, context) == ignore:
                continue
            expressions.extend(filter_def.to_expressions(context))
        return expressions

    def table_expressions(self, context: _FilterContext, *, ignore: str | None = None) -> list[str]:
        expressions: list[str] = []
        for filter_def in self.filters:
            if _filter_ignore_key(filter_def, context) == ignore:
                continue
            expressions.extend(filter_def.to_table_expressions(context))
        return expressions

    def to_dict(self) -> dict[str, Any]:
        return {
            "event": self.event,
            "active": self.active,
            "interaction_preaggregations": self.interaction_preaggregations,
            "filters": [filter_def.to_dict() for filter_def in self.filters],
        }


@dataclass(frozen=True)
class InteractionPreaggTable:
    """Materialized interaction aggregate table for a crossfilter session."""

    table_name: str
    cache_key: str
    model_name: str
    dimensions: list[str]
    dimension_columns: list[str]
    metrics: list[str]
    metric_columns: list[str]
    source_sql: str
    create_sql: str
    row_count: int
    build_ms: float
    built_at: str | None = None
    model_version: str | None = None
    source_watermark: dict[str, Any] | None = None

    def to_dict(self, *, reused: bool, reason: str | None = None) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "cache_key": self.cache_key,
            "model_version": self.model_version,
            "model": self.model_name,
            "dimensions": self.dimensions,
            "dimension_columns": self.dimension_columns,
            "metrics": self.metrics,
            "metric_columns": self.metric_columns,
            "row_count": self.row_count,
            "build_ms": round(self.build_ms, 2),
            "built_at": self.built_at,
            "source_watermark": self.source_watermark,
            "reused": reused,
            "reason": reason,
        }


@dataclass(frozen=True)
class ResolvedFreshnessPolicy:
    """Freshness policy resolved from chart overrides or semantic model metadata."""

    source_watermark_sql: str | None = None
    ttl_seconds: int | None = None
    source: str = "none"
    source_model: str | None = None
    watermark: str | None = None
    reason: str | None = None

    @property
    def configured(self) -> bool:
        return bool(self.source_watermark_sql or self.ttl_seconds is not None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol": "sidemantic-freshness-policy-v1",
            "configured": self.configured,
            "source": self.source,
            "source_model": self.source_model,
            "watermark": self.watermark,
            "source_watermark_configured": bool(self.source_watermark_sql),
            "source_watermark_sql": self.source_watermark_sql,
            "ttl_seconds": self.ttl_seconds,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CrossfilterQueryResponse:
    """Stable response protocol for database-backed crossfilter views."""

    rows: list[dict[str, Any]]
    total_groups: int
    total_source_rows: int
    sql: str
    filter_expressions: list[str]
    views: dict[str, Any]
    sqls: dict[str, str]
    used_interaction_preagg: bool = False
    interaction_preagg: dict[str, Any] | None = None
    timings_ms: dict[str, float] | None = None
    updated_at: str | None = None
    freshness: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        updated_at = self.updated_at or datetime.now(UTC).isoformat()
        freshness = self.freshness or {
            "protocol": "sidemantic-freshness-v1",
            "computed_at": updated_at,
            "updated_at": updated_at,
            "data_as_of": updated_at,
            "status": "direct",
            "stale": False,
        }
        upper_sql = self.sql.upper()
        diagnostics = {
            "protocol": "sidemantic-crossfilter-v1",
            "mode": "database",
            "sql": self.sql,
            "sqls": self.sqls,
            "filter_expressions": self.filter_expressions,
            "has_where": "WHERE" in upper_sql,
            "has_having": "HAVING" in upper_sql,
            "used_interaction_preagg": self.used_interaction_preagg,
            "interaction_preagg": self.interaction_preagg,
            "timings_ms": self.timings_ms or {},
            "updated_at": updated_at,
            "freshness": freshness,
        }
        return {
            "protocol": "sidemantic-crossfilter-v1",
            "updated_at": updated_at,
            "freshness": freshness,
            "rows": self.rows,
            "row_count": len(self.rows),
            "total_groups": self.total_groups,
            "total_source_rows": self.total_source_rows,
            "sql": self.sql,
            "filter_expressions": self.filter_expressions,
            "views": self.views,
            "sqls": self.sqls,
            "used_interaction_preagg": self.used_interaction_preagg,
            "interaction_preagg": self.interaction_preagg,
            "timings_ms": self.timings_ms or {},
            "diagnostics": diagnostics,
        }


class CrossfilterSession:
    """Reusable database-backed crossfilter query session for a chart."""

    def __init__(
        self,
        chart: ChartBuilder,
        *,
        table_limit: int = 75,
        source_record_count: int | None = None,
        interaction_preaggregations: bool = False,
        renderer: Renderer = "d3",
        source_watermark_sql: str | None = None,
        freshness_ttl_seconds: int | None = None,
    ):
        self.chart = chart
        self.table_limit = table_limit
        self.chart_renderer = _normalize_crossfilter_chart_renderer(renderer)
        self._spec: dict[str, Any] | None = None
        self._metadata_spec: dict[str, Any] | None = None
        self._source_record_count = source_record_count
        self._source_group_count: int | None = None
        self.interaction_preaggregations = interaction_preaggregations
        self._interaction_preaggregations_active = interaction_preaggregations
        self._interaction_preagg_cache = InteractionPreaggCache(self) if interaction_preaggregations else None
        self._last_interaction_preagg: dict[str, Any] | None = None
        self._freshness_policy = chart._resolve_freshness_policy(
            source_watermark_sql=source_watermark_sql,
            freshness_ttl_seconds=freshness_ttl_seconds,
        )
        self.source_watermark_sql = self._freshness_policy.source_watermark_sql
        self.freshness_ttl_seconds = self._freshness_policy.ttl_seconds

    def to_spec(self, *, query_endpoint: str | None = None) -> dict[str, Any]:
        spec = dict(self.spec)
        sidemantic = dict(spec.get("sidemantic") or {})
        sidemantic["protocol"] = "sidemantic-crossfilter-v1"
        sidemantic["interaction_preaggregations"] = self.interaction_preaggregations
        sidemantic["chart_renderer"] = self.chart_renderer
        sidemantic["chart_renderer_options"] = list(CROSSFILTER_CHART_RENDERERS)
        sidemantic["table_limit"] = self.table_limit
        sidemantic["freshness_policy"] = self.freshness_policy()
        spec["sidemantic"] = sidemantic
        spec["protocol"] = "sidemantic-crossfilter-v1"
        spec["chart_renderer"] = self.chart_renderer
        spec["chart_renderer_options"] = list(CROSSFILTER_CHART_RENDERERS)
        spec["interaction_preaggregations"] = self.interaction_preaggregations
        spec["table_limit"] = self.table_limit
        spec["freshness_policy"] = self.freshness_policy()
        if query_endpoint:
            spec["query_endpoint"] = query_endpoint
        return spec

    def to_metadata_spec(self, *, query_endpoint: str | None = None) -> dict[str, Any]:
        if self._metadata_spec is None:
            self._metadata_spec = self.chart.to_crossfilter_metadata()
        spec = dict(self._metadata_spec)
        sidemantic = dict(spec.get("sidemantic") or {})
        sidemantic["protocol"] = "sidemantic-crossfilter-v1"
        sidemantic["interaction_preaggregations"] = self.interaction_preaggregations
        sidemantic["chart_renderer"] = self.chart_renderer
        sidemantic["chart_renderer_options"] = list(CROSSFILTER_CHART_RENDERERS)
        sidemantic["table_limit"] = self.table_limit
        sidemantic["freshness_policy"] = self.freshness_policy()
        spec["sidemantic"] = sidemantic
        spec["protocol"] = "sidemantic-crossfilter-v1"
        spec["chart_renderer"] = self.chart_renderer
        spec["chart_renderer_options"] = list(CROSSFILTER_CHART_RENDERERS)
        spec["interaction_preaggregations"] = self.interaction_preaggregations
        spec["table_limit"] = self.table_limit
        spec["freshness_policy"] = self.freshness_policy()
        if query_endpoint:
            spec["query_endpoint"] = query_endpoint
        return spec

    def to_tab(
        self,
        tab_id: str,
        *,
        label: str | None = None,
        query_endpoint: str | None = "/crossfilter/query",
        source_record_count: int | None = None,
    ) -> dict[str, Any]:
        """Return a browser tab spec for this session.

        This is the portable shape consumed by the bundled crossfilter HTML and
        by JS clients that want to supply their own renderer.
        """
        tab: dict[str, Any] = {
            "id": tab_id,
            "label": label or tab_id.replace("_", " ").replace("-", " ").title(),
            "source_record_count": source_record_count if source_record_count is not None else self.source_record_count,
            "spec": self.to_spec(query_endpoint=query_endpoint),
        }
        if query_endpoint:
            tab["query_endpoint"] = query_endpoint
        return tab

    def to_html(
        self,
        *,
        title: str | None = None,
        tab_id: str = "default",
        label: str | None = None,
        query_endpoint: str | None = "/crossfilter/query",
    ) -> str:
        """Return standalone HTML for a single database-backed crossfilter session."""
        resolved_title = title or str(self.spec.get("title") or "Crossfilter")
        return crossfilter_tabs_html(
            resolved_title,
            [self.to_tab(tab_id, label=label or resolved_title, query_endpoint=query_endpoint)],
        )

    @property
    def spec(self) -> dict[str, Any]:
        if self._spec is None:
            self._spec = self.chart.to_crossfilter()
        return self._spec

    @property
    def source_record_count(self) -> int:
        if self._source_record_count is None:
            self._source_record_count = _source_record_count_from_spec(self.spec)
        return self._source_record_count

    @property
    def source_group_count(self) -> int | None:
        if self._source_group_count is not None:
            return self._source_group_count
        if self._spec is not None and self._spec.get("data"):
            self._source_group_count = len(self._spec["data"])
        return self._source_group_count

    def query(
        self,
        filters: list[dict[str, Any] | CrossfilterFilter]
        | tuple[dict[str, Any] | CrossfilterFilter, ...]
        | None = None,
        *,
        event: str | None = None,
        active: dict[str, Any] | None = None,
        interaction_preaggregations: bool | None = None,
    ) -> dict[str, Any]:
        selection = CrossfilterSelection.from_filters(
            filters,
            event=event,
            active=active,
            interaction_preaggregations=interaction_preaggregations,
        )
        return CrossfilterPlanner(self).query(selection).to_dict()

    def handle_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        selection = CrossfilterSelection.from_payload(payload)
        return CrossfilterPlanner(self).query(selection).to_dict()

    def ensure_interaction_preaggregation(self) -> dict[str, Any]:
        """Build the session's interaction aggregate table now, if supported."""
        if not self._interaction_preagg_cache:
            self._last_interaction_preagg = {
                "enabled": False,
                "used": False,
                "reason": "interaction preaggregations are not enabled for this session",
            }
            return self._last_interaction_preagg
        _info, diagnostics = self._interaction_preagg_cache.ensure()
        self._last_interaction_preagg = diagnostics
        return diagnostics

    def set_interaction_preaggregations(self, enabled: bool) -> None:
        self._interaction_preaggregations_active = bool(enabled) and self._interaction_preagg_cache is not None

    def interaction_preagg_for(self, selection: CrossfilterSelection) -> InteractionPreaggTable | None:
        self._last_interaction_preagg = None
        preaggregations_active = (
            bool(selection.interaction_preaggregations)
            if selection.interaction_preaggregations is not None
            else self._interaction_preaggregations_active
        )
        if not preaggregations_active:
            self._last_interaction_preagg = {
                "enabled": False,
                "used": False,
                "reason": "disabled by session toggle",
            }
            return None
        if not self._interaction_preagg_cache:
            self._last_interaction_preagg = {
                "enabled": False,
                "used": False,
                "reason": "interaction preaggregations are not enabled for this session",
            }
            return None

        existing = self._interaction_preagg_cache.info
        should_build = (
            existing is not None
            or bool(selection.filters)
            or (selection.event is not None and selection.event != "tab")
        )
        if not should_build:
            restored, diagnostics = self._interaction_preagg_cache.restore_existing()
            if restored is not None:
                self._last_interaction_preagg = diagnostics
                return restored
            return None

        info, diagnostics = self._interaction_preagg_cache.ensure()
        self._last_interaction_preagg = diagnostics
        return info

    @property
    def interaction_preagg_diagnostics(self) -> dict[str, Any] | None:
        return self._last_interaction_preagg

    def freshness_policy(self) -> dict[str, Any]:
        return self._freshness_policy.to_dict()

    def source_watermark_payload(self, *, checked_at: str | None = None) -> dict[str, Any]:
        checked_at = checked_at or datetime.now(UTC).isoformat()
        if not self.source_watermark_sql:
            return {
                "protocol": "sidemantic-source-watermark-v1",
                "configured": False,
                "status": "not_configured",
                "checked_at": checked_at,
                "sql": None,
                "value": None,
                "source": self._freshness_policy.source,
                "watermark": self._freshness_policy.watermark,
                "reason": self._freshness_policy.reason,
            }
        try:
            row = self.chart.layer.adapter.execute(self.source_watermark_sql).fetchone()
            if row is None:
                return {
                    "protocol": "sidemantic-source-watermark-v1",
                    "configured": True,
                    "status": "unavailable",
                    "checked_at": checked_at,
                    "sql": self.source_watermark_sql,
                    "value": None,
                    "error": "query returned no rows",
                    "source": self._freshness_policy.source,
                    "watermark": self._freshness_policy.watermark,
                }
            value = to_json_compatible(row[0])
            if value is None:
                return {
                    "protocol": "sidemantic-source-watermark-v1",
                    "configured": True,
                    "status": "unavailable",
                    "checked_at": checked_at,
                    "sql": self.source_watermark_sql,
                    "value": None,
                    "error": "query returned NULL",
                    "source": self._freshness_policy.source,
                    "watermark": self._freshness_policy.watermark,
                }
        except Exception as exc:
            return {
                "protocol": "sidemantic-source-watermark-v1",
                "configured": True,
                "status": "unavailable",
                "checked_at": checked_at,
                "sql": self.source_watermark_sql,
                "value": None,
                "error": str(exc),
                "source": self._freshness_policy.source,
                "watermark": self._freshness_policy.watermark,
            }
        return {
            "protocol": "sidemantic-source-watermark-v1",
            "configured": True,
            "status": "available",
            "checked_at": checked_at,
            "sql": self.source_watermark_sql,
            "value": value,
            "source": self._freshness_policy.source,
            "watermark": self._freshness_policy.watermark,
        }


@dataclass(frozen=True)
class CrossfilterTab:
    """A named crossfilter session in a multi-tab interactive explorer."""

    id: str
    label: str
    session: CrossfilterSession
    source_record_count: int | None = None
    query_endpoint: str | None = None

    def to_dict(self, *, query_endpoint: str | None = "/crossfilter/query") -> dict[str, Any]:
        return self.session.to_tab(
            self.id,
            label=self.label,
            query_endpoint=self.query_endpoint if self.query_endpoint is not None else query_endpoint,
            source_record_count=self.source_record_count,
        )


class CrossfilterDashboard:
    """Reusable route/spec wrapper for one or more crossfilter sessions."""

    def __init__(
        self,
        title: str,
        tabs: Sequence[CrossfilterTab],
        *,
        query_endpoint: str | None = "/crossfilter/query",
    ):
        if not tabs:
            raise ValueError("At least one crossfilter tab is required")
        duplicate_ids = _duplicates(tab.id for tab in tabs)
        if duplicate_ids:
            raise ValueError(f"Duplicate crossfilter tab id(s): {', '.join(sorted(duplicate_ids))}")
        self.title = title
        self.tabs = list(tabs)
        for tab in self.tabs:
            if tab.source_record_count is not None and tab.session._source_record_count is None:
                tab.session._source_record_count = tab.source_record_count
        self.query_endpoint = query_endpoint
        self._sessions = {tab.id: tab.session for tab in self.tabs}

    @classmethod
    def from_sessions(
        cls,
        title: str,
        sessions: Mapping[str, CrossfilterSession],
        *,
        labels: Mapping[str, str] | None = None,
        source_record_counts: Mapping[str, int] | None = None,
        query_endpoint: str | None = "/crossfilter/query",
    ) -> CrossfilterDashboard:
        """Build a dashboard from an id -> session mapping."""
        labels = labels or {}
        source_record_counts = source_record_counts or {}
        return cls(
            title,
            [
                CrossfilterTab(
                    tab_id,
                    labels.get(tab_id, tab_id.replace("_", " ").replace("-", " ").title()),
                    session,
                    source_record_count=source_record_counts.get(tab_id),
                )
                for tab_id, session in sessions.items()
            ],
            query_endpoint=query_endpoint,
        )

    @property
    def sessions(self) -> Mapping[str, CrossfilterSession]:
        return dict(self._sessions)

    def to_spec(self) -> dict[str, Any]:
        return {
            "renderer": "sidemantic-crossfilter-tabs",
            "protocol": "sidemantic-crossfilter-v1",
            "tabs": [tab.to_dict(query_endpoint=self.query_endpoint) for tab in self.tabs],
        }

    def tab_spec(self, tab_id: str, *, include_data: bool = True) -> dict[str, Any]:
        """Return a fully materialized spec for one tab."""
        try:
            tab = next(tab for tab in self.tabs if tab.id == tab_id)
        except StopIteration as exc:
            expected = ", ".join(self._sessions)
            raise ValueError(f"Unknown crossfilter tab {tab_id!r}. Expected one of: {expected}") from exc
        query_endpoint = tab.query_endpoint if tab.query_endpoint is not None else self.query_endpoint
        spec = (
            tab.session.to_spec(query_endpoint=query_endpoint)
            if include_data
            else tab.session.to_metadata_spec(query_endpoint=query_endpoint)
        )
        tab_payload: dict[str, Any] = {
            "id": tab.id,
            "label": tab.label,
            "source_record_count": tab.source_record_count,
            "spec": spec,
        }
        if query_endpoint:
            tab_payload["query_endpoint"] = query_endpoint
        return tab_payload

    def to_lazy_spec(
        self, *, initial_tab: str | None = None, spec_endpoint: str = "/crossfilter/spec"
    ) -> dict[str, Any]:
        """Return tab metadata plus the initial tab spec.

        Hosted dashboards use this shape so loading the shell does not run the
        initial queries for every available tab.
        """
        initial_tab_id = initial_tab or self.tabs[0].id
        tabs: list[dict[str, Any]] = []
        for tab in self.tabs:
            source_record_count = (
                tab.source_record_count if tab.source_record_count is not None else tab.session._source_record_count
            )
            tab_payload: dict[str, Any] = {
                "id": tab.id,
                "label": tab.label,
                "query_endpoint": tab.query_endpoint if tab.query_endpoint is not None else self.query_endpoint,
                "spec_endpoint": _lazy_tab_spec_endpoint(spec_endpoint, tab.id),
            }
            if source_record_count is not None:
                tab_payload["source_record_count"] = source_record_count
            if tab.id == initial_tab_id:
                tab_payload["spec"] = tab.session.to_spec(query_endpoint=tab_payload["query_endpoint"])
            tabs.append(tab_payload)
        return {
            "renderer": "sidemantic-crossfilter-tabs",
            "protocol": "sidemantic-crossfilter-v1",
            "spec_endpoint": spec_endpoint,
            "tabs": tabs,
        }

    def to_html(self) -> str:
        return crossfilter_tabs_html(self.title, self.to_spec()["tabs"])

    def to_lazy_html(self, *, initial_tab: str | None = None, spec_endpoint: str = "/crossfilter/spec") -> str:
        return crossfilter_tabs_html(
            self.title,
            self.to_lazy_spec(initial_tab=initial_tab, spec_endpoint=spec_endpoint)["tabs"],
        )

    def write(self, output_dir: str | Path) -> dict[str, Path]:
        """Write dashboard HTML and JSON spec files to a directory."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        spec_path = output_path / "crossfilter.json"
        html_path = output_path / "crossfilter.html"
        spec_path.write_text(json.dumps(self.to_spec(), indent=2, default=str))
        html_path.write_text(self.to_html())
        return {"spec": spec_path, "html": html_path}

    def write_lazy(
        self,
        output_dir: str | Path,
        *,
        initial_tab: str | None = None,
        spec_endpoint: str = "/crossfilter/spec",
    ) -> dict[str, Path]:
        """Write a lazy dashboard shell for live database-backed serving."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        spec_path = output_path / "crossfilter.json"
        html_path = output_path / "crossfilter.html"
        spec_path.write_text(
            json.dumps(
                self.to_lazy_spec(initial_tab=initial_tab, spec_endpoint=spec_endpoint),
                indent=2,
                default=str,
            )
        )
        html_path.write_text(self.to_lazy_html(initial_tab=initial_tab, spec_endpoint=spec_endpoint))
        return {"spec": spec_path, "html": html_path}

    def handle_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Crossfilter request payload must be a JSON object")
        tab_id = str(payload.get("tab") or self.tabs[0].id)
        try:
            session = self._sessions[tab_id]
        except KeyError as exc:
            expected = ", ".join(self._sessions)
            raise ValueError(f"Unknown crossfilter tab {tab_id!r}. Expected one of: {expected}") from exc
        return session.handle_request(payload)

    def warm_interaction_preaggregations(self) -> dict[str, dict[str, Any]]:
        """Build all enabled interaction aggregate tables ahead of first brush."""
        return {tab.id: tab.session.ensure_interaction_preaggregation() for tab in self.tabs}

    def to_asgi_app(self):
        """Return a FastAPI app for hosted dashboard deployments.

        This does not warm interaction pre-aggregations. They remain lazy and
        are built by the first interaction that needs them.
        """
        import threading

        try:
            from fastapi import FastAPI, Query, Request
            from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
        except ImportError as exc:
            raise ImportError(
                "Dashboard ASGI support requires FastAPI. Install with: uv add 'sidemantic[api]'"
            ) from exc

        if not self.query_endpoint:
            raise ValueError("CrossfilterDashboard.to_asgi_app requires a query_endpoint")

        dashboard = self
        query_endpoint = self.query_endpoint
        spec_endpoint = "/crossfilter/spec"
        request_lock = threading.RLock()
        app = FastAPI(title=self.title)
        app.state.dashboard = dashboard

        def json_response(payload: dict[str, Any], *, status_code: int = 200) -> JSONResponse:
            return JSONResponse(json.loads(json.dumps(payload, default=str)), status_code=status_code)

        @app.get("/")
        def index():
            return RedirectResponse(url="/crossfilter.html")

        @app.get("/readyz")
        def readyz() -> dict[str, Any]:
            return {"status": "ok", "tabs": [tab.id for tab in dashboard.tabs]}

        @app.get("/crossfilter.html")
        def crossfilter_html():
            return HTMLResponse(dashboard.to_lazy_html(spec_endpoint=spec_endpoint))

        @app.get("/crossfilter.json")
        def crossfilter_json():
            return json_response(dashboard.to_lazy_spec(spec_endpoint=spec_endpoint))

        @app.get(spec_endpoint)
        def crossfilter_spec(tab: str = Query(...)):
            return json_response(dashboard.tab_spec(tab, include_data=False))

        async def crossfilter_query(request):
            try:
                payload = await request.json()
                with request_lock:
                    response = dashboard.handle_request(payload)
            except Exception as exc:
                return json_response({"error": str(exc)}, status_code=400)
            return json_response(response)

        crossfilter_query.__annotations__["request"] = Request
        app.add_api_route(query_endpoint, crossfilter_query, methods=["POST"])

        return app

    def serve(
        self,
        output_dir: str | Path,
        *,
        host: str = "127.0.0.1",
        port: int = 8877,
    ) -> None:
        """Serve dashboard assets and the database-backed crossfilter endpoint."""
        import threading
        from http import HTTPStatus
        from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
        from urllib.parse import parse_qs, urlparse

        output_path = Path(output_dir)
        if not self.query_endpoint:
            raise ValueError("CrossfilterDashboard.serve requires a query_endpoint")
        dashboard = self
        query_endpoint = self.query_endpoint
        spec_endpoint = "/crossfilter/spec"
        self.write_lazy(output_path, spec_endpoint=spec_endpoint)
        request_lock = threading.Lock()

        class Handler(SimpleHTTPRequestHandler):
            def __init__(self, *handler_args, **handler_kwargs):
                super().__init__(*handler_args, directory=str(output_path), **handler_kwargs)

            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                if parsed.path != spec_endpoint:
                    super().do_GET()
                    return
                try:
                    tab_id = (parse_qs(parsed.query).get("tab") or [None])[0]
                    if not tab_id:
                        raise ValueError("Missing required tab query parameter")
                    response = dashboard.tab_spec(tab_id, include_data=False)
                    body = json.dumps(response, default=str).encode("utf-8")
                except Exception as exc:  # pragma: no cover - exercised by browser/CLI integration
                    body = json.dumps({"error": str(exc)}).encode("utf-8")
                    self.send_response(HTTPStatus.BAD_REQUEST)
                else:
                    self.send_response(HTTPStatus.OK)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:
                if urlparse(self.path).path != query_endpoint:
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return
                try:
                    content_length = int(self.headers.get("content-length", "0"))
                    payload = json.loads(self.rfile.read(content_length) or "{}")
                    with request_lock:
                        response = dashboard.handle_request(payload)
                    body = json.dumps(response, default=str).encode("utf-8")
                except Exception as exc:  # pragma: no cover - exercised by browser/CLI integration
                    body = json.dumps({"error": str(exc)}).encode("utf-8")
                    self.send_response(HTTPStatus.BAD_REQUEST)
                else:
                    self.send_response(HTTPStatus.OK)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer((host, port), Handler)
        server.daemon_threads = True
        print(f"Serving database-backed chart at http://{host}:{port}/crossfilter.html")
        server.serve_forever()


class InteractionPreaggCache:
    """Builds and reuses semantic interaction preaggregates for a session."""

    def __init__(self, session: CrossfilterSession, *, table_prefix: str = "sidemantic_ipreagg"):
        self.session = session
        self.chart = session.chart
        self.table_prefix = table_prefix
        self.info: InteractionPreaggTable | None = None
        self.unsupported_reason: str | None = None

    def ensure(self) -> tuple[InteractionPreaggTable | None, dict[str, Any]]:
        if self.info is not None:
            return self.info, {
                "enabled": True,
                "used": True,
                "reused": True,
                "table": self.info.to_dict(reused=True),
            }
        if self.unsupported_reason:
            return None, {"enabled": True, "used": False, "reason": self.unsupported_reason}

        reason = self._unsupported_reason()
        if reason:
            self.unsupported_reason = reason
            return None, {"enabled": True, "used": False, "reason": reason}

        candidate = self._candidate_table()
        existing = self._existing_table_info(**candidate)
        if existing is not None:
            self.info = existing
            return self.info, {
                "enabled": True,
                "used": True,
                "reused": True,
                "table": self.info.to_dict(reused=True),
            }

        started = time.perf_counter()
        self.chart.layer.adapter.execute(candidate["create_sql"])
        row_count_result = self.chart.layer.adapter.execute(
            f"SELECT COUNT(*) FROM {_identifier(candidate['table_name'])}"
        ).fetchone()
        built_at = datetime.now(UTC).isoformat()
        source_watermark = self.session.source_watermark_payload(checked_at=built_at)
        self.chart.layer.adapter.execute(
            self._metadata_create_sql(candidate, built_at=built_at, source_watermark=source_watermark)
        )
        build_ms = (time.perf_counter() - started) * 1000

        self.info = InteractionPreaggTable(
            table_name=candidate["table_name"],
            cache_key=candidate["cache_key"],
            model_name=candidate["model_name"],
            dimensions=list(self.chart.dimensions),
            dimension_columns=self.chart._dimension_aliases(),
            metrics=list(self.chart.metrics),
            metric_columns=self.chart._metric_aliases(),
            source_sql=candidate["source_sql"],
            create_sql=candidate["create_sql"],
            row_count=int(row_count_result[0] if row_count_result else 0),
            build_ms=build_ms,
            built_at=built_at,
            model_version=candidate["model_version"],
            source_watermark=source_watermark,
        )
        return self.info, {
            "enabled": True,
            "used": True,
            "reused": False,
            "table": self.info.to_dict(reused=False),
        }

    def restore_existing(self) -> tuple[InteractionPreaggTable | None, dict[str, Any] | None]:
        """Reuse a persisted interaction aggregate without creating it."""
        if self.info is not None:
            return self.info, {
                "enabled": True,
                "used": True,
                "reused": True,
                "table": self.info.to_dict(reused=True),
            }
        if self.unsupported_reason:
            return None, {"enabled": True, "used": False, "reason": self.unsupported_reason}

        reason = self._unsupported_reason()
        if reason:
            self.unsupported_reason = reason
            return None, {"enabled": True, "used": False, "reason": reason}

        candidate = self._candidate_table()
        existing = self._existing_table_info(**candidate)
        if existing is None:
            return None, None
        self.info = existing
        return self.info, {
            "enabled": True,
            "used": True,
            "reused": True,
            "table": self.info.to_dict(reused=True),
        }

    def _candidate_table(self) -> dict[str, Any]:
        source_sql = self.chart.layer.compile(
            metrics=self.chart.metrics,
            dimensions=self.chart.dimensions,
            filters=self.chart.filters or None,
            segments=self.chart.segments or None,
            order_by=None,
            limit=None,
            use_preaggregations=self.chart.use_preaggregations,
            aliases=self.chart._output_aliases(),
        )
        source_sql_clean = _strip_sidemantic_comment(source_sql).strip()
        cache_key = self._cache_key()
        table_name = f"{self.table_prefix}_{cache_key[:16]}"
        return {
            "table_name": table_name,
            "metadata_table_name": f"{table_name}__meta",
            "cache_key": cache_key,
            "model_version": _chart_model_version(self.chart),
            "model_name": _single_model_name([*self.chart.metrics, *self.chart.dimensions]) or "",
            "source_sql": source_sql_clean,
            "create_sql": f"CREATE OR REPLACE TABLE {_identifier(table_name)} AS\n{source_sql_clean}",
        }

    def _metadata_create_sql(
        self,
        candidate: Mapping[str, Any],
        *,
        built_at: str,
        source_watermark: Mapping[str, Any],
    ) -> str:
        ttl_value = (
            "NULL" if self.session.freshness_ttl_seconds is None else str(int(self.session.freshness_ttl_seconds))
        )
        return f"""CREATE OR REPLACE TABLE {_identifier(str(candidate["metadata_table_name"]))} AS
SELECT
  {_literal("sidemantic-interaction-preagg-v1")} AS protocol,
  {_literal(candidate["cache_key"])} AS cache_key,
  {_literal(candidate["model_version"])} AS model_version,
  {_literal(built_at)} AS built_at,
  {_literal(json.dumps(source_watermark, sort_keys=True, default=str))} AS source_watermark,
  {ttl_value} AS freshness_ttl_seconds"""

    def _existing_table_info(
        self,
        *,
        table_name: str,
        metadata_table_name: str,
        cache_key: str,
        model_version: str,
        model_name: str,
        source_sql: str,
        create_sql: str,
    ) -> InteractionPreaggTable | None:
        try:
            tables = self.chart.layer.adapter.get_tables()
        except Exception:
            return None
        if not any(table.get("table_name") == table_name for table in tables):
            return None

        started = time.perf_counter()
        expected_columns = [
            *self.chart._dimension_aliases(),
            *self.chart._metric_aliases(),
        ]
        try:
            schema_result = self.chart.layer.adapter.execute(f"SELECT * FROM {_identifier(table_name)} LIMIT 0")
            columns = [desc[0] for desc in schema_result.description]
            if columns != expected_columns:
                return None
            row_count_result = self.chart.layer.adapter.execute(
                f"SELECT COUNT(*) FROM {_identifier(table_name)}"
            ).fetchone()
        except Exception:
            return None
        metadata = self._read_metadata(metadata_table_name)
        if metadata and metadata.get("cache_key") != cache_key:
            return None
        lookup_ms = (time.perf_counter() - started) * 1000
        return InteractionPreaggTable(
            table_name=table_name,
            cache_key=cache_key,
            model_name=model_name,
            dimensions=list(self.chart.dimensions),
            dimension_columns=self.chart._dimension_aliases(),
            metrics=list(self.chart.metrics),
            metric_columns=self.chart._metric_aliases(),
            source_sql=source_sql,
            create_sql=create_sql,
            row_count=int(row_count_result[0] if row_count_result else 0),
            build_ms=lookup_ms,
            built_at=metadata.get("built_at"),
            model_version=metadata.get("model_version") or model_version,
            source_watermark=metadata.get("source_watermark"),
        )

    def _read_metadata(self, metadata_table_name: str) -> dict[str, Any]:
        try:
            tables = self.chart.layer.adapter.get_tables()
        except Exception:
            return {}
        if not any(table.get("table_name") == metadata_table_name for table in tables):
            return {}
        try:
            result = self.chart.layer.adapter.execute(
                f"SELECT cache_key, model_version, built_at, source_watermark "
                f"FROM {_identifier(metadata_table_name)} LIMIT 1"
            )
            row = result.fetchone()
        except Exception:
            return {}
        if not row:
            return {}
        source_watermark = {}
        if row[3]:
            try:
                source_watermark = json.loads(row[3])
            except Exception:
                source_watermark = {}
        return {"cache_key": row[0], "model_version": row[1], "built_at": row[2], "source_watermark": source_watermark}

    def _unsupported_reason(self) -> str | None:
        dialect = _layer_dialect(self.chart.layer)
        if dialect not in {"duckdb", "motherduck"}:
            return "interaction preaggregations currently require DuckDB or MotherDuck"

        model_name = _single_model_name([*self.chart.metrics, *self.chart.dimensions])
        if not model_name:
            return "interaction preaggregations currently require one semantic model"
        try:
            model = self.chart.layer.get_model(model_name)
        except Exception as exc:
            return f"model {model_name!r} could not be resolved: {exc}"

        for metric_ref in self.chart.metrics:
            metric_name = _bare_ref(metric_ref)
            metric = model.get_metric(metric_name)
            if metric is None:
                return f"metric {metric_ref!r} could not be resolved"
            if metric.type is not None:
                return f"metric {metric_ref!r} is a complex metric; interaction preaggregation supports simple metrics"
            if metric.agg not in {"sum", "count", "min", "max"}:
                return f"metric {metric_ref!r} uses unsupported aggregate {metric.agg!r}"
        return None

    def _cache_key(self) -> str:
        model_name = _single_model_name([*self.chart.metrics, *self.chart.dimensions]) or ""
        model_payload = None
        if model_name:
            try:
                model = self.chart.layer.get_model(model_name)
                model_payload = model.model_dump(mode="json", exclude_none=True)
            except Exception:
                model_payload = {"name": model_name}
        payload = {
            "protocol": "sidemantic-interaction-preagg-v1",
            "model": model_payload,
            "metrics": self.chart.metrics,
            "dimensions": self.chart.dimensions,
            "filters": self.chart.filters,
            "segments": self.chart.segments,
            "use_preaggregations": self.chart.use_preaggregations,
        }
        encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class CrossfilterPlanner:
    """Compile crossfilter selections into semantic SQL view queries."""

    def __init__(self, session: CrossfilterSession):
        self.session = session
        self.chart = session.chart
        self.spec = session._spec if session._spec is not None else session.chart.to_crossfilter_metadata()
        self.context = _filter_context_from_spec(self.spec)
        self.metric_aggs = _metric_aggs(self.chart, self.spec["fields"]["metrics"])

    def query(self, selection: CrossfilterSelection) -> CrossfilterQueryResponse:
        query_started = time.perf_counter()
        timings: dict[str, float] = {}

        def mark(name: str) -> None:
            timings[name] = round((time.perf_counter() - query_started) * 1000, 2)

        preagg = self.session.interaction_preagg_for(selection)
        mark("preagg")
        full_filter_exprs = selection.expressions(self.context)
        current = self._query_chart(
            self.chart.dimensions,
            self._filter_expressions(selection, preagg=preagg),
            preagg=preagg,
            limit=self.chart.limit,
        )
        mark("current")
        metric_aliases = self.spec["fields"]["metrics"]

        trend_dimensions = (
            self.spec["fields"]["dimensions"][:2]
            if len(self.spec["fields"]["dimensions"]) > 1
            else self.spec["fields"]["dimensions"][:1]
        )
        if preagg is not None:
            trend = self._aggregate_interaction_preagg(
                trend_dimensions,
                self._filter_expressions(selection, ignore="xRange", preagg=preagg),
                metric_aliases,
                preagg,
                order_by=self.spec["fields"]["x"],
                limit=self.chart.limit,
            )
        else:
            trend = self._query_chart(
                self._semantic_dimensions(trend_dimensions),
                self._filter_expressions(selection, ignore="xRange", preagg=preagg),
                preagg=preagg,
                order_by=[],
                limit=self.chart.limit,
            )
        mark("trend")
        if self._selection_has(selection, "metricRange"):
            scatter = self._query_chart(
                self.chart.dimensions,
                self._filter_expressions(selection, ignore="metricRange", preagg=preagg),
                preagg=preagg,
                limit=self.chart.limit,
            )
        else:
            # Without a metric-range brush the scatter view is the current grid.
            scatter = current
        mark("scatter")
        if preagg is not None:
            # Interaction preaggs guarantee additive metrics, so KPI totals are a
            # rollup of the current grid we already fetched—derive, don't rescan.
            kpis = {
                "rows": [self._aggregate_rows(current["rows"], metric_aliases)] if current["rows"] else [],
                "sql": current["sql"],
            }
        else:
            kpis = self._query_chart(
                [],
                self._filter_expressions(selection, preagg=preagg),
                preagg=preagg,
                order_by=None,
            )
        mark("kpis")
        bars: dict[str, list[dict[str, Any]]] = {}
        bar_sqls: dict[str, str] = {}
        for field in self._select_dimension_aliases():
            if preagg is not None:
                result = self._aggregate_interaction_preagg(
                    [field],
                    self._filter_expressions(selection, ignore=f"category:{field}", preagg=preagg),
                    metric_aliases,
                    preagg,
                    limit=self.chart.limit,
                )
            else:
                result = self._query_chart(
                    [self.context.dimension_ref(field)],
                    self._filter_expressions(selection, ignore=f"category:{field}", preagg=preagg),
                    preagg=preagg,
                    order_by=[],
                    limit=self.chart.limit,
                )
            bars[field] = result["rows"]
            bar_sqls[field] = result["sql"]
        mark("bars")

        sqls = {
            "current": current["sql"],
            "trend": trend["sql"],
            "scatter": scatter["sql"],
            "kpis": kpis["sql"],
            **{f"bar:{field}": sql for field, sql in bar_sqls.items()},
        }
        total_groups = self._total_groups(selection, current, preagg)
        updated_at = datetime.now(UTC).isoformat()
        return CrossfilterQueryResponse(
            rows=current["rows"],
            total_groups=total_groups,
            total_source_rows=self._total_source_records(selection, current, preagg),
            sql=current["sql"],
            filter_expressions=full_filter_exprs,
            views={
                "trend": trend["rows"],
                "scatter": scatter["rows"],
                "table": current["rows"][: self.session.table_limit],
                "kpis": kpis["rows"][0] if kpis["rows"] else {},
                "bars": bars,
            },
            sqls=sqls,
            used_interaction_preagg=preagg is not None,
            interaction_preagg=self.session.interaction_preagg_diagnostics,
            timings_ms=timings,
            updated_at=updated_at,
            freshness=self._freshness_payload(updated_at, preagg),
        )

    def _select_dimension_aliases(self) -> list[str]:
        interaction_plan = self.spec.get("interaction_plan") or {}
        select = interaction_plan.get("select") if isinstance(interaction_plan, Mapping) else None
        if isinstance(select, Mapping):
            fields = select.get("fields") or []
            return [
                str(field.get("alias"))
                for field in fields
                if isinstance(field, Mapping) and field.get("kind") == "dimension" and field.get("alias")
            ]
        return list(self.spec["fields"]["dimensions"][1:])

    def _freshness_payload(
        self,
        computed_at: str,
        preagg: InteractionPreaggTable | None,
    ) -> dict[str, Any]:
        field_plan = self.spec.get("field_plan") or {}
        diagnostics = self.session.interaction_preagg_diagnostics or {}
        reused = bool(diagnostics.get("reused"))
        source_watermark = self.session.source_watermark_payload(checked_at=computed_at)
        source_watermark_value = (
            source_watermark.get("value") if source_watermark.get("status") == "available" else None
        )
        if source_watermark_value is not None:
            data_as_of = source_watermark_value
        elif preagg is not None:
            data_as_of = preagg.built_at
        elif self.session.freshness_ttl_seconds is not None:
            data_as_of = None
        else:
            data_as_of = computed_at
        stale, stale_reason = _freshness_state(
            computed_at=computed_at,
            data_as_of=data_as_of,
            source_watermark=source_watermark,
            preagg=preagg,
            reused=reused,
            ttl_seconds=self.session.freshness_ttl_seconds,
        )
        status = "direct"
        interaction_preagg = None
        if preagg is not None:
            status = "preaggregated"
            interaction_preagg = {
                "table_name": preagg.table_name,
                "cache_key": preagg.cache_key,
                "model_version": preagg.model_version,
                "built_at": preagg.built_at,
                "reused": reused,
                "source_watermark": preagg.source_watermark,
            }
        return {
            "protocol": "sidemantic-freshness-v1",
            "computed_at": computed_at,
            "updated_at": computed_at,
            "data_as_of": data_as_of,
            "status": status,
            "stale": stale,
            "stale_reason": stale_reason,
            "policy": self.session.freshness_policy(),
            "plan_fingerprint": field_plan.get("fingerprint"),
            "source_watermark": source_watermark,
            "interaction_preagg": interaction_preagg,
        }

    def _total_groups(
        self,
        selection: CrossfilterSelection,
        current: dict[str, Any],
        preagg: InteractionPreaggTable | None,
    ) -> int:
        cached = self.session.source_group_count
        if cached is not None:
            return cached
        spec_rows = self.spec.get("data") or []
        if spec_rows:
            self.session._source_group_count = len(spec_rows)
            return self.session._source_group_count
        if preagg is not None:
            self.session._source_group_count = preagg.row_count
            return self.session._source_group_count
        if not selection.filters:
            self.session._source_group_count = len(current["rows"])
            return self.session._source_group_count
        self.session._source_group_count = self._query_unfiltered_group_count()
        return self.session._source_group_count

    def _total_source_records(
        self,
        selection: CrossfilterSelection,
        current: dict[str, Any],
        preagg: InteractionPreaggTable | None,
    ) -> int:
        cached = self.session._source_record_count
        if cached is not None:
            return cached
        if not selection.filters:
            self.session._source_record_count = _source_record_count_from_rows(self.spec, current["rows"])
            return self.session._source_record_count

        count_field = _count_metric_field_from_spec(self.spec)
        if count_field is None:
            self.session._source_record_count = self._query_unfiltered_group_count()
            return self.session._source_record_count

        if preagg is not None:
            result = self._aggregate_interaction_preagg([], [], [count_field], preagg)
        else:
            result = self._query_unfiltered_metric(count_field)
        rows = result["rows"]
        self.session._source_record_count = int(rows[0].get(count_field, 0) if rows else 0)
        return self.session._source_record_count

    def _query_unfiltered_metric(self, metric_field: str) -> dict[str, Any]:
        sql = self.chart.layer.compile(
            metrics=[self.context.metric_ref(metric_field)],
            dimensions=[],
            filters=self.chart.filters or None,
            segments=self.chart.segments or None,
            order_by=None,
            limit=None,
            use_preaggregations=self.chart.use_preaggregations,
            aliases=self.chart._output_aliases(),
        )
        return _execute_rows(self.chart.layer, sql)

    def _query_unfiltered_group_count(self) -> int:
        grouped_sql = self.chart.layer.compile(
            metrics=self.chart.metrics,
            dimensions=self.chart.dimensions,
            filters=self.chart.filters or None,
            segments=self.chart.segments or None,
            order_by=None,
            limit=None,
            use_preaggregations=self.chart.use_preaggregations,
            aliases=self.chart._output_aliases(),
        )
        grouped_sql = _strip_sidemantic_comment(grouped_sql).strip()
        result = self.chart.layer.adapter.execute(
            f"SELECT COUNT(*) AS total_groups FROM (\n{grouped_sql}\n) AS sidemantic_group_count"
        ).fetchone()
        return int(result[0] if result else 0)

    def _filter_expressions(
        self,
        selection: CrossfilterSelection,
        *,
        ignore: str | None = None,
        preagg: InteractionPreaggTable | None = None,
    ) -> list[str]:
        if preagg is not None:
            return selection.table_expressions(self.context, ignore=ignore)
        return selection.expressions(self.context, ignore=ignore)

    def _selection_has(self, selection: CrossfilterSelection, ignore_key: str) -> bool:
        """Whether any active filter would be dropped by ``ignore=ignore_key``."""
        return any(_filter_ignore_key(filter_def, self.context) == ignore_key for filter_def in selection.filters)

    def _aggregate_rows(self, rows: list[dict[str, Any]], metric_aliases: list[str]) -> dict[str, Any]:
        """Roll up already-fetched grid rows to a single total per additive metric."""
        result: dict[str, Any] = {}
        for metric in metric_aliases:
            agg = self.metric_aggs.get(metric)
            values = [row[metric] for row in rows if row.get(metric) is not None]
            if not values:
                result[metric] = None
            elif agg == "min":
                result[metric] = min(values)
            elif agg == "max":
                result[metric] = max(values)
            else:
                result[metric] = sum(values)
        return result

    def _query_chart(
        self,
        dimensions: list[str],
        filters: list[str],
        *,
        preagg: InteractionPreaggTable | None = None,
        order_by: list[str] | str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if preagg is not None:
            return self._query_interaction_preagg(dimensions, filters, preagg, limit=limit)
        resolved_order_by = (
            _as_list(order_by) if order_by is not None else _order_by_for_dimensions(dimensions, self.chart)
        )
        sql = self.chart.layer.compile(
            metrics=self.chart.metrics,
            dimensions=dimensions,
            filters=[*self.chart.filters, *filters] or None,
            segments=self.chart.segments or None,
            order_by=resolved_order_by,
            limit=limit,
            use_preaggregations=self.chart.use_preaggregations,
            aliases=self.chart._output_aliases(),
        )
        return _execute_rows(self.chart.layer, sql)

    def _semantic_dimensions(self, fields: list[str]) -> list[str]:
        return [self.context.dimension_ref(field) for field in fields]

    def _query_interaction_preagg(
        self,
        dimensions: list[str],
        filters: list[str],
        preagg: InteractionPreaggTable,
        *,
        limit: int | None = None,
    ) -> dict[str, Any]:
        dimension_aliases = [self.context.dimension_alias(dimension) for dimension in dimensions]
        dimension_selects = [f"{_identifier(dimension)} AS {_identifier(dimension)}" for dimension in dimension_aliases]
        metric_selects = [
            f"{_identifier(metric)} AS {_identifier(metric)}" for metric in self.spec["fields"]["metrics"]
        ]
        select_clause = ",\n  ".join([*dimension_selects, *metric_selects])
        sql = f"""SELECT
  {select_clause}
FROM {_identifier(preagg.table_name)}"""
        if filters:
            sql += "\nWHERE\n  " + "\n  AND ".join(filters)
        if dimension_aliases:
            positions = ",\n  ".join(str(index) for index in range(1, len(dimension_aliases) + 1))
            sql += f"\nORDER BY\n  {positions}"
        if limit is not None:
            sql += f"\nLIMIT {int(limit)}"
        return _execute_rows(self.chart.layer, sql)

    def _aggregate_interaction_preagg(
        self,
        dimensions: list[str],
        filters: list[str],
        metrics: list[str],
        preagg: InteractionPreaggTable,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        dimension_aliases = [self.context.dimension_alias(dimension) for dimension in dimensions]
        dimension_selects = [f"{_identifier(dimension)} AS {_identifier(dimension)}" for dimension in dimension_aliases]
        metric_selects = [
            f"{_aggregate_metric_sql(metric, self.metric_aggs.get(metric))} AS {_identifier(metric)}"
            for metric in metrics
        ]
        select_clause = ",\n  ".join([*dimension_selects, *metric_selects])
        sql = f"""SELECT
  {select_clause}
FROM {_identifier(preagg.table_name)}"""
        if filters:
            sql += "\nWHERE\n  " + "\n  AND ".join(filters)
        if dimension_aliases:
            positions = ",\n  ".join(str(index) for index in range(1, len(dimension_aliases) + 1))
            sql += f"\nGROUP BY\n  {positions}"
        if order_by:
            sql += f"\nORDER BY\n  {_identifier(self.context.dimension_alias(order_by))} NULLS FIRST"
        if limit is not None:
            sql += f"\nLIMIT {int(limit)}"
        return _execute_rows(self.chart.layer, sql)


@dataclass(frozen=True)
class _FilterContext:
    dimension_refs: dict[str, str]
    metric_refs: dict[str, str]

    def dimension_ref(self, field: str) -> str:
        return self._resolve_field(field, self.dimension_refs, "dimension")[1]

    def metric_ref(self, field: str) -> str:
        return self._resolve_field(field, self.metric_refs, "metric")[1]

    def metric_alias(self, field: str) -> str:
        return self._resolve_field(field, self.metric_refs, "metric")[0]

    def dimension_alias(self, field: str) -> str:
        return self._resolve_field(field, self.dimension_refs, "dimension")[0]

    @staticmethod
    def _resolve_field(field: str, refs: dict[str, str], kind: str) -> tuple[str, str]:
        if field in refs:
            return field, refs[field]
        for alias, ref in refs.items():
            if field == ref:
                return alias, ref
        expected = ", ".join(sorted({*refs.keys(), *refs.values()}))
        raise ValueError(f"Unknown crossfilter {kind} field {field!r}. Expected one of: {expected}")


def _filter_context_from_spec(spec: Mapping[str, Any]) -> _FilterContext:
    field_plan = spec.get("field_plan") or {}
    planned_fields = field_plan.get("fields") if isinstance(field_plan, Mapping) else None
    if isinstance(planned_fields, list) and planned_fields:
        dimension_refs: dict[str, str] = {}
        metric_refs: dict[str, str] = {}
        for field in planned_fields:
            if not isinstance(field, Mapping):
                continue
            alias = field.get("alias")
            ref = field.get("semantic_ref") or field.get("id")
            kind = field.get("kind")
            if not alias or not ref:
                continue
            if kind == "dimension":
                dimension_refs[str(alias)] = str(ref)
            elif kind == "metric":
                metric_refs[str(alias)] = str(ref)
        if dimension_refs or metric_refs:
            return _FilterContext(dimension_refs=dimension_refs, metric_refs=metric_refs)

    fields = spec.get("fields") or {}
    query = spec.get("query") or {}
    return _FilterContext(
        dimension_refs=dict(zip(fields.get("dimensions") or [], query.get("dimensions") or [])),
        metric_refs=dict(zip(fields.get("metrics") or [], query.get("metrics") or [])),
    )


def _freshness_state(
    *,
    computed_at: str,
    data_as_of: Any,
    source_watermark: Mapping[str, Any],
    preagg: InteractionPreaggTable | None,
    reused: bool,
    ttl_seconds: int | None,
) -> tuple[bool | None, str | None]:
    stale: bool | None = False
    reason: str | None = None
    watermark_status = str(source_watermark.get("status") or "")

    if source_watermark.get("configured") and watermark_status == "unavailable":
        stale = None
        reason = f"source watermark unavailable: {source_watermark.get('error') or 'query failed'}"

    if preagg is not None:
        if watermark_status == "available" and source_watermark.get("value") is not None:
            watermark_at = _freshness_datetime(source_watermark.get("value"))
            preagg_built_at = _freshness_datetime(preagg.built_at)
            if watermark_at is None or preagg_built_at is None:
                stale = None
                reason = "source watermark could not be compared with interaction preaggregation build time"
            elif watermark_at > preagg_built_at:
                stale = True
                reason = "source watermark is newer than interaction preaggregation"
        elif reused and ttl_seconds is None:
            stale = None
            reason = "source watermark unavailable for reused interaction preaggregation"

    ttl_stale, ttl_reason = _ttl_freshness_state(computed_at, data_as_of, ttl_seconds)
    if ttl_stale is True:
        return True, ttl_reason
    if ttl_stale is None and stale is False:
        return None, ttl_reason
    return stale, reason


def _ttl_freshness_state(computed_at: str, data_as_of: Any, ttl_seconds: int | None) -> tuple[bool | None, str | None]:
    if ttl_seconds is None:
        return False, None
    computed_dt = _freshness_datetime(computed_at)
    data_dt = _freshness_datetime(data_as_of)
    if computed_dt is None or data_dt is None:
        return None, "freshness TTL could not be evaluated"
    if computed_dt - data_dt > timedelta(seconds=int(ttl_seconds)):
        return True, f"data_as_of is older than freshness TTL ({int(ttl_seconds)}s)"
    return False, None


def _freshness_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = _parse_temporal_value(value)
    except (TypeError, ValueError):
        return None
    if isinstance(parsed, datetime):
        parsed_dt = parsed
    else:
        parsed_dt = datetime.combine(parsed, datetime.min.time())
    if parsed_dt.tzinfo is None:
        return parsed_dt.replace(tzinfo=UTC)
    return parsed_dt.astimezone(UTC)


class ChartBuilder:
    """Chainable, renderer-neutral chart builder.

    Users author charts with semantic fields. The builder compiles and executes
    a Sidemantic query only when data or a renderer output is requested.
    """

    def __init__(
        self,
        layer,
        metrics: str | list[str],
        *,
        by: str | list[str] | None = None,
        mark: ChartMark = "auto",
        filters: list[str] | None = None,
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        title: str | None = None,
        use_preaggregations: bool | None = None,
    ):
        self.layer = layer
        self.metrics = _as_list(metrics)
        self.dimensions = _as_list(by)
        self.mark: ChartMark = mark
        self.filters = list(filters or [])
        self.segments = list(segments or [])
        self.order_by = list(order_by) if order_by is not None else None
        self.limit = limit
        self.title = title
        self.use_preaggregations = use_preaggregations
        self.selection: BrushSelection | None = None
        self.interactions: dict[str, Any] = {}
        self._data: ChartData | None = None

        if not self.metrics:
            raise ValueError("At least one metric is required")

    def bar(self) -> ChartBuilder:
        self.mark = "bar"
        return self

    def line(self) -> ChartBuilder:
        self.mark = "line"
        return self

    def area(self) -> ChartBuilder:
        self.mark = "area"
        return self

    def scatter(self) -> ChartBuilder:
        self.mark = "scatter"
        return self

    def point(self) -> ChartBuilder:
        self.mark = "point"
        return self

    def brush(
        self,
        channel: Literal["x", "y", "xy"] = "x",
        name: str = "brush",
        fields: str | list[str] | None = None,
    ) -> ChartBuilder:
        self.selection = BrushSelection(name=name, channel=channel)
        self.interactions["brush"] = {
            "fields": _as_list(fields) if fields is not None else [],
            "channel": channel,
        }
        return self

    def interactive(self, enabled: bool = True) -> ChartBuilder:
        self.selection = BrushSelection() if enabled else None
        if enabled:
            self.interactions.setdefault("brush", {"fields": [], "channel": "x"})
        else:
            self.interactions.pop("brush", None)
        return self

    def select(self, fields: str | list[str] | None = None) -> ChartBuilder:
        self.interactions["select"] = {
            "fields": _as_list(fields) if fields is not None else [],
        }
        return self

    def where(self, filter_expr: str) -> ChartBuilder:
        self.filters.append(filter_expr)
        self._data = None
        return self

    def crossfilter(
        self,
        *,
        table_limit: int = 75,
        source_record_count: int | None = None,
        interaction_preaggregations: bool = False,
        renderer: Renderer = "d3",
        source_watermark_sql: str | None = None,
        freshness_ttl_seconds: int | None = None,
    ) -> CrossfilterSession:
        """Create a database-backed crossfilter session for this chart."""
        return CrossfilterSession(
            self,
            table_limit=table_limit,
            source_record_count=source_record_count,
            interaction_preaggregations=interaction_preaggregations,
            renderer=renderer,
            source_watermark_sql=source_watermark_sql,
            freshness_ttl_seconds=freshness_ttl_seconds,
        )

    def data(self) -> ChartData:
        """Compile, execute, and return renderer-neutral data."""
        if self._data is None:
            self._data = self._execute()
        return self._data

    @property
    def sql(self) -> str:
        return self.data().sql

    def to_vegalite(self) -> dict[str, Any]:
        data = self.data()
        mark = self._resolved_mark(data)
        x_col = self._x_column(data)
        y_cols = self._y_columns(data)
        series_col = self._series_column(data)

        spec: dict[str, Any] = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {"values": data.rows},
            "mark": _vegalite_mark(mark),
            "encoding": {},
            "usermeta": {"sidemantic": self._metadata(data)},
        }

        if self.title:
            spec["title"] = self.title

        if len(y_cols) == 1:
            y_col = y_cols[0]
            spec["encoding"] = {
                "x": {
                    "field": x_col,
                    "type": _vegalite_type(data.rows, x_col),
                    "title": _label(x_col),
                },
                "y": {"field": y_col, "type": "quantitative", "title": _label(y_col)},
                "tooltip": [
                    {"field": x_col, "type": _vegalite_type(data.rows, x_col), "title": _label(x_col)},
                    {"field": y_col, "type": "quantitative", "title": _label(y_col), "format": ",.2f"},
                ],
            }
            if series_col:
                spec["encoding"]["color"] = {"field": series_col, "type": "nominal", "title": _label(series_col)}
                spec["encoding"]["tooltip"].insert(
                    1,
                    {"field": series_col, "type": "nominal", "title": _label(series_col)},
                )
        else:
            spec["transform"] = [{"fold": y_cols, "as": ["metric", "value"]}]
            spec["encoding"] = {
                "x": {
                    "field": x_col,
                    "type": _vegalite_type(data.rows, x_col),
                    "title": _label(x_col),
                },
                "y": {"field": "value", "type": "quantitative", "title": "Value"},
                "color": {"field": "metric", "type": "nominal", "title": "Metric"},
                "tooltip": [
                    {"field": x_col, "type": _vegalite_type(data.rows, x_col), "title": _label(x_col)},
                    {"field": "metric", "type": "nominal", "title": "Metric"},
                    {"field": "value", "type": "quantitative", "title": "Value", "format": ",.2f"},
                ],
            }
            if series_col:
                spec["encoding"]["color"] = {"field": series_col, "type": "nominal", "title": _label(series_col)}
                spec["encoding"]["strokeDash"] = {"field": "metric", "type": "nominal", "title": "Metric"}
                spec["encoding"]["tooltip"].insert(
                    1,
                    {"field": series_col, "type": "nominal", "title": _label(series_col)},
                )

        if self.selection:
            encodings = ["x", "y"] if self.selection.channel == "xy" else [self.selection.channel]
            spec["params"] = [{"name": self.selection.name, "select": {"type": "interval", "encodings": encodings}}]
            spec["encoding"]["opacity"] = {
                "condition": {"param": self.selection.name, "value": 1},
                "value": 0.25,
            }

        return spec

    def to_plotly(self) -> dict[str, Any]:
        data = self.data()
        mark = self._resolved_mark(data)
        x_col = self._x_column(data)
        y_cols = self._y_columns(data)
        series_col = self._series_column(data)

        traces = []
        trace_type, mode = _plotly_mark(mark)
        row_groups = _group_rows(data.rows, series_col)
        for y_index, y_col in enumerate(y_cols):
            for series_index, (series_value, rows) in enumerate(row_groups):
                name = _trace_name(y_col, series_value, multiple_metrics=len(y_cols) > 1)
                trace_color = _color(series_index if series_col else y_index)
                trace: dict[str, Any] = {
                    "type": trace_type,
                    "name": name,
                    "x": [row.get(x_col) for row in rows],
                    "y": [row.get(y_col) for row in rows],
                    "marker": {"color": trace_color, "size": 8},
                    "line": {"color": trace_color},
                    "selected": {"marker": {"opacity": 1, "size": 11}},
                    "unselected": {"marker": {"opacity": 0.22}},
                }
                if mode:
                    trace["mode"] = mode
                if trace_type == "scatter" and mark == "area":
                    trace["fill"] = "tozeroy"
                traces.append(trace)

        layout: dict[str, Any] = {
            "title": {"text": self.title or _default_title(self.metrics, self.dimensions)},
            "xaxis": {"title": {"text": _label(x_col)}},
            "yaxis": {"title": {"text": "Value" if len(y_cols) > 1 else _label(y_cols[0])}},
            "template": "plotly_white",
        }
        if self.selection:
            layout["dragmode"] = "select"

        return {
            "data": traces,
            "layout": layout,
            "config": {"responsive": True, "displayModeBar": True},
            "sidemantic": self._metadata(data),
        }

    def to_observable_plot(self) -> dict[str, Any]:
        data = self.data()
        mark = self._resolved_mark(data)
        x_col = self._x_column(data)
        y_cols = self._y_columns(data)
        series_col = self._series_column(data)

        return {
            "renderer": "observable-plot",
            "data": data.rows,
            "marks": [
                {
                    "type": _observable_mark(mark),
                    "options": _observable_options(x_col, y_col, series_col, mark, index),
                }
                for index, y_col in enumerate(y_cols)
            ],
            "options": {
                "title": self.title or _default_title(self.metrics, self.dimensions),
                "grid": True,
                "x": {"label": _label(x_col)},
                "y": {"label": "Value" if len(y_cols) > 1 else _label(y_cols[0])},
                "color": {"legend": bool(series_col)},
            },
            "sidemantic": self._metadata(data),
        }

    def to_d3(self) -> dict[str, Any]:
        data = self.data()
        return {
            "renderer": "d3",
            "data": data.rows,
            "mark": self._resolved_mark(data),
            "fields": {"x": self._x_column(data), "y": self._y_columns(data), "series": self._series_column(data)},
            "title": self.title or _default_title(self.metrics, self.dimensions),
            "sidemantic": self._metadata(data),
        }

    def to_crossfilter(self) -> dict[str, Any]:
        data = self.data()
        return self._crossfilter_spec(data)

    def to_crossfilter_metadata(self) -> dict[str, Any]:
        """Return a crossfilter spec without executing the query."""
        dimension_columns = self._dimension_aliases()
        metric_columns = self._metric_aliases()
        data = ChartData(
            sql="",
            columns=[*dimension_columns, *metric_columns],
            rows=[],
            dimension_columns=dimension_columns,
            metric_columns=metric_columns,
        )
        spec = self._crossfilter_spec(data)
        spec["data_deferred"] = True
        return spec

    def _crossfilter_spec(self, data: ChartData) -> dict[str, Any]:
        plan = self._compiled_plan(data)
        field_plan = plan.field_plan()
        interaction_plan = plan.interaction_plan()
        return {
            "renderer": "sidemantic-crossfilter",
            "protocol": "sidemantic-crossfilter-v1",
            "title": self.title or _default_title(self.metrics, self.dimensions),
            "data": data.rows,
            "fields": {
                "x": self._x_column(data),
                "y": self._y_columns(data),
                "series": self._series_column(data),
                "dimensions": data.dimension_columns,
                "metrics": data.metric_columns,
                "metric_aggs": _metric_aggs(self, data.metric_columns),
            },
            "field_plan": field_plan,
            "query": {
                "metrics": self.metrics,
                "dimensions": self.dimensions,
                "filters": self.filters,
                "segments": self.segments,
                "order_by": self.order_by or self._default_order_by(),
                "limit": self.limit,
                "use_preaggregations": self.use_preaggregations,
            },
            "interactions": plan.legacy_interactions(),
            "interaction_plan": interaction_plan,
            "views": [
                {"id": "trend", "type": "line", "title": "Trend"},
                {"id": "scatter", "type": "scatter", "title": "Metric Relationship"},
                *[
                    {"id": f"breakdown_{dimension}", "type": "bar", "field": dimension, "title": _label(dimension)}
                    for dimension in data.dimension_columns[1:]
                ],
                {"id": "rows", "type": "table", "title": "Filtered Groups"},
            ],
            "sidemantic": self._metadata(data),
        }

    def to_renderer(self, renderer: str) -> dict[str, Any]:
        normalized = _normalize_renderer(renderer)
        if normalized == "vega-lite":
            return self.to_vegalite()
        if normalized == "plotly":
            return self.to_plotly()
        if normalized == "observable-plot":
            return self.to_observable_plot()
        if normalized == "crossfilter":
            return self.to_crossfilter()
        return self.to_d3()

    def to_html(self, renderer: str = "vega-lite") -> str:
        normalized = _normalize_renderer(renderer)
        spec = self.to_renderer(normalized)
        safe_json = json.dumps(spec, default=str).replace("<", "\\u003c")
        title = self.title or _default_title(self.metrics, self.dimensions)

        if normalized == "vega-lite":
            selection_name = self.selection.name if self.selection else ""
            return _html_shell(
                title,
                f"""
<div id="chart"></div>
<div id="interaction-status" aria-live="polite">Drag across the chart to brush a range.</div>
<script id="chart-spec" type="application/json">{safe_json}</script>
{inline_vendor_scripts("vega", "vega_lite", "vega_embed")}
<script>
  const spec = JSON.parse(document.getElementById('chart-spec').textContent);
  spec.width = 'container';
  spec.height = 420;
  const status = document.getElementById('interaction-status');
  vegaEmbed('#chart', spec, {{ actions: true }}).then(result => {{
  window.__SIDEMANTIC_CHART_READY__ = true;
  window.__SIDEMANTIC_LAST_EVENT__ = null;
    document.documentElement.dataset.sidemanticReady = 'true';
    document.documentElement.dataset.sidemanticEvent = '';
  window.__SIDEMANTIC_VIEW__ = result.view;
  if ({json.dumps(bool(self.selection))}) {{
      try {{
        result.view.addSignalListener({json.dumps(selection_name)}, (_name, value) => {{
          window.__SIDEMANTIC_LAST_EVENT__ = {{ type: 'brush', value }};
          document.documentElement.dataset.sidemanticEvent = 'brush';
          status.textContent = value && Object.keys(value).length
            ? 'Brush selection active.'
            : 'Drag across the chart to brush a range.';
        }});
      }} catch (_err) {{
        status.textContent = 'Chart ready. Drag across the chart to brush a range.';
      }}
  }}
  }});
</script>
""",
            )

        if normalized == "plotly":
            return _html_shell(
                title,
                f"""
<div id="chart"></div>
<div id="interaction-status" aria-live="polite">Drag to select points or hover for details.</div>
<script id="chart-spec" type="application/json">{safe_json}</script>
{inline_vendor_scripts("plotly")}
<script>
  const spec = JSON.parse(document.getElementById('chart-spec').textContent);
  const chart = document.getElementById('chart');
  const status = document.getElementById('interaction-status');
  Plotly.newPlot(chart, spec.data, spec.layout, spec.config).then(() => {{
    window.__SIDEMANTIC_CHART_READY__ = true;
    window.__SIDEMANTIC_LAST_EVENT__ = null;
    document.documentElement.dataset.sidemanticReady = 'true';
    document.documentElement.dataset.sidemanticEvent = '';
    chart.on('plotly_hover', event => {{
      const point = event.points?.[0];
      window.__SIDEMANTIC_LAST_EVENT__ = {{ type: 'hover', curve: point?.curveNumber, point: point?.pointNumber }};
      document.documentElement.dataset.sidemanticEvent = 'hover';
      status.textContent = point ? `Hover: ${{point.data.name}} at ${{point.x}}` : 'Hover active.';
    }});
    chart.on('plotly_selected', event => {{
      const count = event?.points?.length || 0;
      window.__SIDEMANTIC_LAST_EVENT__ = {{ type: 'selected', points: count }};
      document.documentElement.dataset.sidemanticEvent = `selected:${{count}}`;
      status.textContent = count ? `Selected ${{count}} point${{count === 1 ? '' : 's'}}.` : 'No points selected.';
    }});
    chart.on('plotly_deselect', () => {{
      window.__SIDEMANTIC_LAST_EVENT__ = {{ type: 'deselect' }};
      document.documentElement.dataset.sidemanticEvent = 'deselect';
      status.textContent = 'Selection cleared.';
    }});
  }});
</script>
""",
            )

        if normalized == "observable-plot":
            return _html_shell(
                title,
                f"""
<div id="chart"></div>
<div id="interaction-status" aria-live="polite">Move over marks for Observable Plot tips.</div>
<script id="chart-spec" type="application/json">{safe_json}</script>
{inline_vendor_scripts("d3", "observable_plot")}
<script>
  const Plot = window.Plot;
  const spec = JSON.parse(document.getElementById('chart-spec').textContent);
  const xField = spec.marks[0]?.options?.x;
  if (xField) {{
    for (const row of spec.data) {{
      if (typeof row[xField] === 'string' && /^\\d{{4}}-\\d{{2}}-\\d{{2}}/.test(row[xField])) {{
        row[xField] = new Date(row[xField]);
      }}
    }}
  }}
  const marks = spec.marks.map(mark => Plot[mark.type](spec.data, mark.options));
  const chart = Plot.plot({{ ...spec.options, marks }});
  const root = document.getElementById('chart');
  const status = document.getElementById('interaction-status');
  root.append(chart);
  window.__SIDEMANTIC_CHART_READY__ = true;
  window.__SIDEMANTIC_LAST_EVENT__ = null;
  document.documentElement.dataset.sidemanticReady = 'true';
  document.documentElement.dataset.sidemanticEvent = '';
  chart.addEventListener('pointermove', event => {{
    window.__SIDEMANTIC_LAST_EVENT__ = {{ type: 'pointermove', x: Math.round(event.offsetX), y: Math.round(event.offsetY) }};
    document.documentElement.dataset.sidemanticEvent = 'pointermove';
    status.textContent = `Pointer at ${{Math.round(event.offsetX)}}, ${{Math.round(event.offsetY)}}.`;
  }});
</script>
""",
            )

        if normalized == "crossfilter":
            return _html_shell(title, _crossfilter_body(safe_json))

        return _html_shell(
            title,
            f"""
<svg id="chart" viewBox="0 0 800 460" role="img"></svg>
<div id="interaction-status" aria-live="polite">Drag across the chart to brush a range.</div>
<script id="chart-spec" type="application/json">{safe_json}</script>
{inline_vendor_scripts("d3")}
<script>
  const spec = JSON.parse(document.getElementById('chart-spec').textContent);
  const svg = d3.select('#chart');
  const margin = {{ top: 32, right: 24, bottom: 56, left: 72 }};
  const width = 800 - margin.left - margin.right;
  const height = 460 - margin.top - margin.bottom;
  const g = svg.append('g').attr('transform', `translate(${{margin.left}},${{margin.top}})`);
  const xField = spec.fields.x;
  const yFields = spec.fields.y;
  const yField = yFields[0];
  const seriesField = spec.fields.series;
  const status = document.getElementById('interaction-status');
  const xDomain = Array.from(new Set(spec.data.map(d => d[xField])));
  const seriesDomain = seriesField ? Array.from(new Set(spec.data.map(d => d[seriesField]))) : yFields;
  const color = d3.scaleOrdinal(seriesDomain, d3.schemeTableau10);
  const x = d3.scalePoint().domain(xDomain).range([0, width]).padding(0.5);
  const y = d3.scaleLinear()
    .domain([0, d3.max(spec.data, d => d3.max(yFields, key => Number(d[key]))) || 1])
    .nice()
    .range([height, 0]);
  g.append('g').attr('transform', `translate(0,${{height}})`).call(d3.axisBottom(x));
  g.append('g').call(d3.axisLeft(y));
  if (spec.mark === 'bar') {{
    g.selectAll('rect').data(spec.data).join('rect')
      .attr('x', d => x(d[xField]) - 12).attr('width', 24)
      .attr('y', d => y(Number(d[yField]))).attr('height', d => height - y(Number(d[yField])))
      .attr('fill', d => seriesField ? color(d[seriesField]) : color(yField));
  }} else {{
    const groups = seriesField
      ? d3.groups(spec.data, d => d[seriesField]).map(([key, values]) => [key, yField, values])
      : yFields.map(field => [field, field, spec.data]);
    for (const [series, field, values] of groups) {{
      const sortedValues = values.slice().sort((a, b) => xDomain.indexOf(a[xField]) - xDomain.indexOf(b[xField]));
      if (spec.mark === 'area') {{
        g.append('path').datum(sortedValues)
          .attr('fill', color(series)).attr('fill-opacity', 0.18)
          .attr('d', d3.area().x(d => x(d[xField])).y0(height).y1(d => y(Number(d[field]))));
      }}
      g.append('path').datum(sortedValues)
        .attr('fill', 'none')
        .attr('stroke', color(series)).attr('stroke-width', 2)
        .attr('d', d3.line().x(d => x(d[xField])).y(d => y(Number(d[field]))));
      g.append('g').selectAll('circle').data(sortedValues).join('circle')
        .attr('cx', d => x(d[xField])).attr('cy', d => y(Number(d[field])))
        .attr('r', 4).attr('fill', color(series)).attr('data-series', series);
    }}
  }}
  if (seriesDomain.length > 1) {{
    const legend = svg.append('g').attr('transform', 'translate(650,24)');
    legend.selectAll('g').data(seriesDomain).join('g')
      .attr('transform', (_d, i) => `translate(0,${{i * 20}})`)
      .call(item => {{
        item.append('rect').attr('width', 10).attr('height', 10).attr('fill', d => color(d));
        item.append('text').attr('x', 16).attr('y', 9).attr('font-size', 12).text(d => d);
      }});
  }}
  if (spec.sidemantic?.selection) {{
    const brush = d3.brushX().extent([[0, 0], [width, height]]).on('brush end', event => {{
      if (!event.selection) {{
        window.__SIDEMANTIC_LAST_EVENT__ = {{ type: 'brush', rows: 0 }};
        document.documentElement.dataset.sidemanticEvent = 'brush:0';
        status.textContent = 'Selection cleared.';
        return;
      }}
      const [left, right] = event.selection;
      const selected = spec.data.filter(d => {{
        const px = x(d[xField]);
        return px >= left && px <= right;
      }});
      window.__SIDEMANTIC_LAST_EVENT__ = {{ type: 'brush', rows: selected.length }};
      document.documentElement.dataset.sidemanticEvent = `brush:${{selected.length}}`;
      status.textContent = `Brush selected ${{selected.length}} row${{selected.length === 1 ? '' : 's'}}.`;
    }});
    g.append('g').attr('class', 'brush').call(brush);
  }}
  window.__SIDEMANTIC_CHART_READY__ = true;
  window.__SIDEMANTIC_LAST_EVENT__ = window.__SIDEMANTIC_LAST_EVENT__ || null;
  document.documentElement.dataset.sidemanticReady = 'true';
  document.documentElement.dataset.sidemanticEvent = document.documentElement.dataset.sidemanticEvent || '';
</script>
""",
        )

    def _execute(self) -> ChartData:
        order_by = self.order_by
        if order_by is None:
            order_by = self._default_order_by()

        sql = self.layer.compile(
            metrics=self.metrics,
            dimensions=self.dimensions,
            filters=self.filters or None,
            segments=self.segments or None,
            order_by=order_by,
            limit=self.limit,
            use_preaggregations=self.use_preaggregations,
            aliases=self._output_aliases(),
        )
        result = self.layer.adapter.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = [{col: to_json_compatible(value) for col, value in zip(columns, row)} for row in result.fetchall()]
        dimension_columns = columns[: len(self.dimensions)]
        metric_columns = columns[len(self.dimensions) : len(self.dimensions) + len(self.metrics)]

        return ChartData(
            sql=sql,
            columns=columns,
            rows=rows,
            dimension_columns=dimension_columns,
            metric_columns=metric_columns,
        )

    def _default_order_by(self) -> list[str] | None:
        mark = self.mark
        if mark == "auto":
            mark = "line" if self.dimensions and _looks_temporal(self.dimensions[0]) else "bar"
        if self.dimensions and mark in ("line", "area", "point", "scatter"):
            return [self.dimensions[0]]
        if self.metrics and mark == "bar":
            return [f"{self.metrics[0]} DESC"]
        return None

    def _resolved_mark(self, data: ChartData) -> Literal["bar", "line", "area", "scatter", "point"]:
        if self.mark != "auto":
            return self.mark
        if data.dimension_columns and _looks_temporal(data.dimension_columns[0]):
            return "line"
        if len(data.metric_columns) >= 2 and not data.dimension_columns:
            return "scatter"
        return "bar"

    def _x_column(self, data: ChartData) -> str:
        if data.dimension_columns:
            return data.dimension_columns[0]
        if data.metric_columns:
            return data.metric_columns[0]
        if data.columns:
            return data.columns[0]
        raise ValueError("Chart query returned no columns")

    def _y_columns(self, data: ChartData) -> list[str]:
        if data.dimension_columns:
            return data.metric_columns or data.columns[len(data.dimension_columns) :]
        if len(data.metric_columns) > 1:
            return data.metric_columns[1:]
        return data.metric_columns or data.columns[1:] or data.columns[:1]

    def _series_column(self, data: ChartData) -> str | None:
        if len(data.dimension_columns) > 1:
            return data.dimension_columns[1]
        return None

    def _metadata(self, data: ChartData) -> dict[str, Any]:
        plan = self._compiled_plan(data)
        metadata: dict[str, Any] = {
            "sql": data.sql,
            "metrics": self.metrics,
            "dimensions": self.dimensions,
            "row_count": len(data.rows),
            "field_plan": plan.field_plan(),
            "interaction_plan": plan.interaction_plan(),
        }
        if self.selection:
            metadata["selection"] = self.selection.to_dict()
        interaction_spec = plan.legacy_interactions()
        if interaction_spec:
            metadata["interactions"] = interaction_spec
        return metadata

    def _compiled_plan(self, data: ChartData) -> CompiledChartPlan:
        return CompiledChartPlan.build(self, data)

    def _interaction_spec(self, data: ChartData) -> dict[str, Any]:
        return self._compiled_plan(data).legacy_interactions()

    def _output_aliases(self) -> dict[str, str]:
        return _unique_field_aliases([*self.dimensions, *self.metrics])

    def _dimension_aliases(self) -> list[str]:
        aliases = self._output_aliases()
        return [aliases[dimension] for dimension in self.dimensions]

    def _metric_aliases(self) -> list[str]:
        aliases = self._output_aliases()
        return [aliases[metric] for metric in self.metrics]

    def _resolve_freshness_policy(
        self,
        *,
        source_watermark_sql: str | None = None,
        freshness_ttl_seconds: int | None = None,
    ) -> ResolvedFreshnessPolicy:
        if source_watermark_sql:
            return ResolvedFreshnessPolicy(
                source_watermark_sql=source_watermark_sql,
                ttl_seconds=freshness_ttl_seconds,
                source="chart_override_sql",
                reason="explicit crossfilter source_watermark_sql override",
            )

        policy = _model_freshness_policy(self.layer, [*self.metrics, *self.dimensions])
        if freshness_ttl_seconds is not None:
            return ResolvedFreshnessPolicy(
                source_watermark_sql=policy.source_watermark_sql,
                ttl_seconds=freshness_ttl_seconds,
                source=policy.source if policy.source != "none" else "chart_override_ttl",
                source_model=policy.source_model,
                watermark=policy.watermark,
                reason=policy.reason or "explicit crossfilter freshness_ttl_seconds override",
            )
        return policy


def _as_list(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def _duplicates(values: Iterable[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        else:
            seen.add(value)
    return duplicates


def _unique_field_aliases(fields: Sequence[str]) -> dict[str, str]:
    """Return deterministic SQL/result aliases for semantic fields."""
    counts: dict[str, int] = {}
    for field in fields:
        base = _field_alias(field)
        counts[base] = counts.get(base, 0) + 1

    aliases: dict[str, str] = {}
    used: set[str] = set()
    for field in fields:
        base = _field_alias(field)
        if counts[base] > 1 and _ref_model(field):
            candidate = f"{_ref_model(field)}_{base}"
        else:
            candidate = base
        candidate = _safe_alias(candidate)
        unique_candidate = candidate
        index = 2
        while unique_candidate in used:
            unique_candidate = f"{candidate}_{index}"
            index += 1
        aliases[field] = unique_candidate
        used.add(unique_candidate)
    return aliases


def _safe_alias(value: str) -> str:
    alias = "".join(char if char.isalnum() or char == "_" else "_" for char in value.strip())
    alias = alias.strip("_") or "field"
    if alias[0].isdigit():
        alias = f"field_{alias}"
    return alias


def _compiled_encodings(
    fields: tuple[CompiledField, ...],
    x_alias: str,
    y_aliases: list[str],
    series_alias: str | None,
) -> dict[str, Any]:
    by_alias = {field.alias: field for field in fields}
    return {
        "x": _compiled_field_ref(by_alias[x_alias]) if x_alias in by_alias else None,
        "y": [_compiled_field_ref(by_alias[alias]) for alias in y_aliases if alias in by_alias],
        "series": _compiled_field_ref(by_alias[series_alias]) if series_alias and series_alias in by_alias else None,
    }


def _compiled_interactions(
    chart: ChartBuilder,
    data: ChartData,
    fields: tuple[CompiledField, ...],
    encodings: Mapping[str, Any],
) -> dict[str, Any]:
    interactions: dict[str, Any] = {}
    if chart.selection:
        brush = dict(chart.interactions.get("brush") or {})
        raw_fields = list(brush.get("fields") or [])
        if raw_fields:
            planned_fields = [_resolve_compiled_field(field, fields) for field in raw_fields]
        else:
            x_field = encodings.get("x")
            planned_fields = [_resolve_compiled_field(x_field["id"], fields)] if x_field else []
        channel = str(brush.get("channel") or chart.selection.channel or "x")
        supported = channel == "x" and all(field.kind == "dimension" for field in planned_fields)
        reason = None if supported else "live crossfilter brush currently supports x-channel dimension ranges"
        interactions["brush"] = {
            "channel": channel,
            "fields": [_compiled_field_ref(field) for field in planned_fields],
            "filter_type": "range",
            "request_type": "xRange",
            "supported": supported,
            "unsupported_reason": reason,
            "ignored_by": ["trend"],
        }

    if "select" in chart.interactions:
        select = dict(chart.interactions.get("select") or {})
        raw_fields = list(select.get("fields") or [])
        if raw_fields:
            planned_fields = [_resolve_compiled_field(field, fields, expected_kind="dimension") for field in raw_fields]
        else:
            planned_fields = [
                _resolve_compiled_field(alias, fields, expected_kind="dimension")
                for alias in data.dimension_columns[1:]
            ]
        interactions["select"] = {
            "fields": [_compiled_field_ref(field) for field in planned_fields],
            "filter_type": "category",
            "request_type": "category",
            "supported": True,
            "ignored_by": ["matching breakdown"],
        }
    return interactions


def _compiled_field_ref(field: CompiledField) -> dict[str, Any]:
    return {
        "id": field.id,
        "semantic_ref": field.semantic_ref,
        "alias": field.alias,
        "label": _label(field.alias),
        "kind": field.kind,
        "source_model": field.source_model,
    }


def _resolve_compiled_field(
    value: Any,
    fields: tuple[CompiledField, ...],
    *,
    expected_kind: Literal["dimension", "metric"] | None = None,
) -> CompiledField:
    if isinstance(value, Mapping):
        candidate = value.get("id") or value.get("semantic_ref") or value.get("alias")
    else:
        candidate = value
    text = str(candidate or "")
    for field in fields:
        if text in {field.id, field.semantic_ref, field.alias}:
            if expected_kind is not None and field.kind != expected_kind:
                raise ValueError(
                    f"Chart interaction field {text!r} must be a {expected_kind}; {field.id!r} is a {field.kind}"
                )
            return field
    expected = ", ".join(sorted({field.id for field in fields} | {field.alias for field in fields}))
    raise ValueError(f"Unknown chart interaction field {text!r}. Expected one of: {expected}")


def _chart_plan_fingerprint(
    chart: ChartBuilder,
    fields: tuple[CompiledField, ...],
    encodings: Mapping[str, Any],
    interactions: Mapping[str, Any],
) -> str:
    payload = {
        "protocol": "sidemantic-chart-plan-v1",
        "model_version": _chart_model_version(chart),
        "fields": [field.to_dict() for field in fields],
        "encodings": encodings,
        "interactions": interactions,
        "query": {
            "metrics": chart.metrics,
            "dimensions": chart.dimensions,
            "filters": chart.filters,
            "segments": chart.segments,
            "order_by": chart.order_by or chart._default_order_by(),
            "limit": chart.limit,
            "use_preaggregations": chart.use_preaggregations,
        },
    }
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_renderer(renderer: str) -> Renderer:
    normalized = renderer.lower().replace("_", "-")
    aliases = {
        "vegalite": "vega-lite",
        "vl": "vega-lite",
        "observable": "observable-plot",
        "plot": "observable-plot",
        "linked": "crossfilter",
        "dashboard": "crossfilter",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"vega-lite", "plotly", "observable-plot", "d3", "crossfilter"}:
        raise ValueError("renderer must be one of: vega-lite, plotly, observable-plot, d3, crossfilter")
    return normalized  # type: ignore[return-value]


def _normalize_crossfilter_chart_renderer(renderer: str) -> str:
    normalized = _normalize_renderer(renderer)
    return "d3" if normalized == "crossfilter" else normalized


def _coerce_crossfilter_filter(filter_def: dict[str, Any] | CrossfilterFilter) -> CrossfilterFilter:
    if isinstance(filter_def, (DimensionEquals, TimeRange, MetricRange)):
        return filter_def
    filter_type = filter_def.get("type")
    if filter_type == "category":
        return DimensionEquals(str(filter_def["field"]), filter_def["value"])
    if filter_type == "xRange":
        return TimeRange(str(filter_def["field"]), filter_def["min"], filter_def["max"])
    if filter_type == "metricRange":
        return MetricRange(
            str(filter_def["xField"]),
            float(filter_def["xMin"]),
            float(filter_def["xMax"]),
            str(filter_def["yField"]) if filter_def.get("yField") is not None else None,
            float(filter_def["yMin"]) if filter_def.get("yMin") is not None else None,
            float(filter_def["yMax"]) if filter_def.get("yMax") is not None else None,
        )
    raise ValueError(f"Unsupported crossfilter filter type: {filter_type}")


def _filter_ignore_key(filter_def: CrossfilterFilter, context: _FilterContext) -> str:
    if isinstance(filter_def, DimensionEquals):
        return filter_def.key or f"category:{context.dimension_alias(filter_def.field)}"
    return filter_def.ignore_key


def _execute_rows(layer: Any, sql: str) -> dict[str, Any]:
    result = layer.adapter.execute(sql)
    columns = [desc[0] for desc in result.description]
    rows = [{col: to_json_compatible(value) for col, value in zip(columns, row)} for row in result.fetchall()]
    return {"sql": sql, "rows": rows}


def _order_by_for_dimensions(dimensions: list[str], chart: ChartBuilder) -> list[str] | None:
    if not dimensions:
        return None
    if dimensions[0].endswith(("__day", "__week", "__month", "__quarter", "__year")):
        return [dimensions[0]]
    return chart.order_by


def _source_record_count_from_spec(spec: dict[str, Any]) -> int:
    return _source_record_count_from_rows(spec, spec.get("data") or [])


def _source_record_count_from_rows(spec: dict[str, Any], rows: list[dict[str, Any]]) -> int:
    count_field = _count_metric_field_from_spec(spec)
    if count_field is None:
        return len(rows)
    return int(sum(row.get(count_field, 0) for row in rows))


def _count_metric_field_from_spec(spec: dict[str, Any]) -> str | None:
    metric_aggs = _metric_aggs_from_spec(spec)
    return next(
        (field for field in _metric_fields_from_spec(spec) if str(metric_aggs.get(field)).lower() == "count"),
        None,
    )


def _metric_fields_from_spec(spec: dict[str, Any]) -> list[str]:
    fields = spec.get("fields") or {}
    metrics = fields.get("metrics") or fields.get("y") or []
    if isinstance(metrics, str):
        return [metrics]
    return [str(metric) for metric in metrics]


def _metric_aggs_from_spec(spec: dict[str, Any]) -> Mapping[str, Any]:
    fields = spec.get("fields") or {}
    sidemantic = spec.get("sidemantic") or {}
    metric_aggs = fields.get("metric_aggs") or spec.get("metric_aggs") or sidemantic.get("metric_aggs") or {}
    return metric_aggs if isinstance(metric_aggs, Mapping) else {}


def _literal(value: Any) -> str:
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _range_filter_expressions(field_ref: str, min_value: Any, max_value: Any) -> list[str]:
    if "__" not in field_ref:
        return [f"{field_ref} >= {_literal(min_value)}", f"{field_ref} <= {_literal(max_value)}"]
    base_ref, grain = field_ref.rsplit("__", 1)
    if grain in {"second", "minute", "hour", "day", "week", "month", "quarter", "year"}:
        try:
            lower = _parse_temporal_value(min_value)
            upper = _add_time_grain(_parse_temporal_value(max_value), grain)
        except ValueError:
            return [f"{field_ref} >= {_literal(min_value)}", f"{field_ref} <= {_literal(max_value)}"]
        return [f"{base_ref} >= {_temporal_literal(lower)}", f"{base_ref} < {_temporal_literal(upper)}"]
    return [f"{field_ref} >= {_literal(min_value)}", f"{field_ref} <= {_literal(max_value)}"]


def _range_table_filter_expressions(field_alias: str, min_value: Any, max_value: Any) -> list[str]:
    field = _identifier(field_alias)
    if _looks_like_iso_date(min_value) and _looks_like_iso_date(max_value):
        return [
            f"{field} >= DATE {_literal(str(min_value)[:10])}",
            f"{field} <= DATE {_literal(str(max_value)[:10])}",
        ]
    return [f"{field} >= {_literal(min_value)}", f"{field} <= {_literal(max_value)}"]


def _looks_like_iso_date(value: Any) -> bool:
    try:
        date.fromisoformat(str(value)[:10])
    except ValueError:
        return False
    return True


def _parse_temporal_value(value: Any) -> date | datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return date.fromisoformat(text)
    normalized = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed


def _temporal_literal(value: date | datetime) -> str:
    if isinstance(value, datetime):
        return f"TIMESTAMP {_literal(value.isoformat(sep=' ', timespec='seconds'))}"
    return f"DATE {_literal(value.isoformat())}"


def _add_time_grain(value: date | datetime, grain: str) -> date | datetime:
    if grain == "second":
        return _as_datetime(value) + timedelta(seconds=1)
    if grain == "minute":
        return _as_datetime(value) + timedelta(minutes=1)
    if grain == "hour":
        return _as_datetime(value) + timedelta(hours=1)
    if grain == "day":
        return value + timedelta(days=1)
    if grain == "week":
        return value + timedelta(days=7)
    if grain == "month":
        return _add_months(value, 1)
    if grain == "quarter":
        return _add_months(value, 3)
    if grain == "year":
        return _add_months(value, 12)
    raise ValueError(f"Unsupported time grain: {grain}")


def _as_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, datetime.min.time())


def _add_months(value: date | datetime, months: int) -> date | datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _strip_sidemantic_comment(sql: str) -> str:
    lines = sql.rstrip().splitlines()
    while lines and lines[-1].lstrip().startswith("-- sidemantic:"):
        lines.pop()
    return "\n".join(f"  {line}" for line in lines)


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _bare_ref(ref: str) -> str:
    return ref.rsplit(".", 1)[-1].split("__", 1)[0]


def _field_alias(ref: str) -> str:
    return ref.rsplit(".", 1)[-1]


def _ref_model(ref: str) -> str | None:
    if "." not in ref:
        return None
    return ref.split(".", 1)[0]


def _single_model_name(refs: list[str]) -> str | None:
    model_names = {_ref_model(ref) for ref in refs if _ref_model(ref)}
    if len(model_names) != 1:
        return None
    return next(iter(model_names))


def _model_freshness_policy(layer: Any, refs: list[str]) -> ResolvedFreshnessPolicy:
    model_names = sorted({_ref_model(ref) for ref in refs if _ref_model(ref)})
    if not model_names:
        return ResolvedFreshnessPolicy(reason="semantic fields do not identify a source model")
    if len(model_names) != 1:
        return ResolvedFreshnessPolicy(
            source="ambiguous_models",
            reason=f"chart references multiple models without a combined freshness policy: {', '.join(model_names)}",
        )

    model_name = model_names[0]
    try:
        model = layer.get_model(model_name)
    except Exception as exc:
        return ResolvedFreshnessPolicy(source_model=model_name, reason=f"model freshness could not be resolved: {exc}")

    freshness = getattr(model, "freshness", None)
    if freshness is not None:
        if freshness.sql:
            return ResolvedFreshnessPolicy(
                source_watermark_sql=freshness.sql,
                ttl_seconds=freshness.ttl_seconds,
                source="model_freshness_sql",
                source_model=model_name,
                reason="model freshness sql",
            )
        if freshness.watermark:
            source_sql = _model_watermark_sql(model, freshness.watermark)
            if source_sql:
                return ResolvedFreshnessPolicy(
                    source_watermark_sql=source_sql,
                    ttl_seconds=freshness.ttl_seconds,
                    source="model_freshness",
                    source_model=model_name,
                    watermark=_canonical_model_watermark(model_name, freshness.watermark),
                    reason="model freshness watermark",
                )
            return ResolvedFreshnessPolicy(
                ttl_seconds=freshness.ttl_seconds,
                source="model_freshness",
                source_model=model_name,
                watermark=_canonical_model_watermark(model_name, freshness.watermark),
                reason="model freshness watermark could not be compiled",
            )
        return ResolvedFreshnessPolicy(
            ttl_seconds=freshness.ttl_seconds,
            source="model_freshness",
            source_model=model_name,
            reason="model freshness ttl without source watermark",
        )

    inferred_watermark = _infer_model_watermark(model)
    if inferred_watermark:
        source_sql = _model_watermark_sql(model, inferred_watermark)
        if source_sql:
            return ResolvedFreshnessPolicy(
                source_watermark_sql=source_sql,
                source="model_inferred_watermark",
                source_model=model_name,
                watermark=_canonical_model_watermark(model_name, inferred_watermark),
                reason="inferred model freshness watermark from time dimension metadata/name",
            )

    return ResolvedFreshnessPolicy(source_model=model_name, reason="model has no freshness policy")


def _infer_model_watermark(model: Any) -> str | None:
    preferred_names = [
        "_ingested_at",
        "ingested_at",
        "_loaded_at",
        "loaded_at",
        "_updated_at",
        "updated_at",
        "synced_at",
        "refreshed_at",
    ]
    dimensions = list(getattr(model, "dimensions", []) or [])
    for dimension in dimensions:
        role = _metadata_role(dimension)
        if getattr(dimension, "type", None) == "time" and role in {
            "freshness",
            "watermark",
            "source_watermark",
            "ingestion_time",
            "updated_at",
        }:
            return str(dimension.name)
    by_name = {
        str(dimension.name).lower(): dimension for dimension in dimensions if getattr(dimension, "type", None) == "time"
    }
    for name in preferred_names:
        if name in by_name:
            return str(by_name[name].name)
    return None


def _metadata_role(value: Any) -> str | None:
    for attr in ("meta", "metadata"):
        metadata = getattr(value, attr, None)
        if isinstance(metadata, Mapping):
            role = metadata.get("role") or metadata.get("semantic_role")
            if role:
                return str(role).lower()
    return None


def _model_watermark_sql(model: Any, watermark: str) -> str | None:
    from_clause = _model_from_clause(model)
    if not from_clause:
        return None
    expression = _model_watermark_expression(model, watermark)
    return f"SELECT MAX({expression}) FROM {from_clause}"


def _model_from_clause(model: Any) -> str | None:
    model_sql = getattr(model, "sql", None)
    if model_sql:
        return f"({model_sql}) AS t"
    model_table = getattr(model, "table", None)
    if model_table:
        return str(model_table)
    return None


def _model_watermark_expression(model: Any, watermark: str) -> str:
    field_name = _bare_ref(watermark)
    dimension = model.get_dimension(field_name) if hasattr(model, "get_dimension") else None
    expression = str(dimension.sql_expr) if dimension is not None else _identifier(field_name)
    return _replace_model_placeholder_for_freshness(expression, model_has_sql=bool(getattr(model, "sql", None)))


def _replace_model_placeholder_for_freshness(expression: str, *, model_has_sql: bool) -> str:
    if model_has_sql:
        return expression.replace("{model}", "t").replace("${TABLE}", "t")
    return expression.replace("{model}.", "").replace("${TABLE}.", "").replace("{model}", "").replace("${TABLE}", "")


def _canonical_model_watermark(model_name: str, watermark: str) -> str:
    if "." in watermark:
        return watermark
    return f"{model_name}.{watermark}"


def _chart_model_version(chart: ChartBuilder) -> str:
    model_payloads: dict[str, Any] = {}
    model_names = sorted({_ref_model(ref) for ref in [*chart.metrics, *chart.dimensions] if _ref_model(ref)})
    for model_name in model_names:
        if model_name is None:
            continue
        try:
            model = chart.layer.get_model(model_name)
            model_payloads[model_name] = model.model_dump(mode="json", exclude_none=True)
        except Exception:
            model_payloads[model_name] = {"name": model_name}
    payload = {
        "protocol": "sidemantic-chart-model-v1",
        "models": model_payloads,
        "metrics": chart.metrics,
        "dimensions": chart.dimensions,
        "filters": chart.filters,
        "segments": chart.segments,
        "use_preaggregations": chart.use_preaggregations,
    }
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _metric_aggs(chart: ChartBuilder, metric_aliases: list[str]) -> dict[str, str | None]:
    model_name = _single_model_name(chart.metrics)
    if not model_name:
        return {alias: None for alias in metric_aliases}
    try:
        model = chart.layer.get_model(model_name)
    except Exception:
        return {alias: None for alias in metric_aliases}

    aggs: dict[str, str | None] = {}
    for metric_ref, alias in zip(chart.metrics, metric_aliases):
        metric = model.get_metric(_bare_ref(metric_ref))
        aggs[alias] = metric.agg if metric else None
    return aggs


def _layer_dialect(layer: Any) -> str:
    dialect = getattr(layer, "dialect", None)
    if dialect:
        return str(dialect).lower()
    adapter = getattr(layer, "adapter", None)
    adapter_dialect = getattr(adapter, "dialect", None)
    return str(adapter_dialect).lower() if adapter_dialect else ""


def _aggregate_metric_sql(metric: str, agg: str | None) -> str:
    metric_ref = _identifier(metric)
    if agg == "min":
        return f"MIN({metric_ref})"
    if agg == "max":
        return f"MAX({metric_ref})"
    if agg == "avg":
        return f"AVG({metric_ref})"
    return f"SUM({metric_ref})"


def _vegalite_mark(mark: str) -> dict[str, Any]:
    if mark == "scatter":
        return {"type": "point", "filled": True, "tooltip": True}
    if mark == "line":
        return {"type": "line", "point": True, "tooltip": True}
    if mark == "area":
        return {"type": "area", "line": True, "tooltip": True}
    return {"type": mark, "tooltip": True}


def _plotly_mark(mark: str) -> tuple[str, str | None]:
    if mark == "bar":
        return "bar", None
    if mark == "scatter":
        return "scatter", "markers"
    if mark == "point":
        return "scatter", "markers"
    if mark == "area":
        return "scatter", "lines"
    return "scatter", "lines+markers"


def _group_rows(rows: list[dict[str, Any]], series_col: str | None) -> list[tuple[Any | None, list[dict[str, Any]]]]:
    if not series_col:
        return [(None, rows)]

    grouped: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row.get(series_col), []).append(row)
    return list(grouped.items())


def _trace_name(y_col: str, series_value: Any | None, *, multiple_metrics: bool) -> str:
    metric_label = _label(y_col)
    if series_value is None:
        return metric_label
    series_label = str(series_value)
    if multiple_metrics:
        return f"{series_label} - {metric_label}"
    return series_label


def _color(index: int) -> str:
    return PALETTE[index % len(PALETTE)]


def _observable_mark(mark: str) -> str:
    if mark == "bar":
        return "barY"
    if mark == "area":
        return "areaY"
    if mark in {"scatter", "point"}:
        return "dot"
    return "lineY"


def _observable_options(
    x_col: str,
    y_col: str,
    series_col: str | None,
    mark: str,
    index: int,
) -> dict[str, Any]:
    options: dict[str, Any] = {"x": x_col, "y": y_col, "tip": True}
    if series_col:
        options["z"] = series_col
        if mark == "bar":
            options["fill"] = series_col
        elif mark == "area":
            options["stroke"] = series_col
            options["fill"] = series_col
        elif mark in {"scatter", "point"}:
            options["stroke"] = series_col
            options["fill"] = series_col
        else:
            options["stroke"] = series_col
    else:
        color = _color(index)
        options["stroke"] = color
        if mark in {"area", "bar", "scatter", "point"}:
            options["fill"] = color
    return options


def _vegalite_type(rows: list[dict[str, Any]], field: str) -> str:
    if _looks_temporal(field):
        return "temporal"
    for row in rows:
        value = row.get(field)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return "quantitative"
        return "nominal"
    return "nominal"


def _looks_temporal(field: str) -> bool:
    lowered = field.lower()
    temporal_parts = ("date", "time", "month", "year", "day", "week", "quarter", "created", "updated", "__")
    return any(part in lowered for part in temporal_parts)


def _label(field: str) -> str:
    if "." in field:
        field = field.rsplit(".", 1)[1]
    if "__" in field:
        base, grain = field.rsplit("__", 1)
        return f"{_label(base)} ({grain.title()})"
    return " ".join(part.capitalize() for part in field.replace("_", " ").split())


def _default_title(metrics: list[str], dimensions: list[str]) -> str:
    metric_label = " + ".join(_label(metric) for metric in metrics)
    if dimensions:
        return f"{metric_label} by {_label(dimensions[0])}"
    return metric_label


def _crossfilter_body(safe_json: str) -> str:
    return """
<style>
  .cf-toolbar { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: start; gap: 10px 16px; margin-bottom: 12px; }
  .cf-toolbar button { border: 1px solid #b8c0cc; background: #fff; border-radius: 6px; padding: 6px 10px; cursor: pointer; }
  .cf-filter-summary { min-width: 0; }
  .cf-active-filters { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
  .cf-active-filters[hidden] { display: none; }
  .cf-filter-pill { display: inline-flex; align-items: center; gap: 6px; max-width: 100%; border: 1px solid #c9d3e2; border-radius: 999px; background: #f4f7fb; color: #1f2937; padding: 3px 4px 3px 10px; font-size: 12px; line-height: 1.35; }
  .cf-filter-label { color: #526071; font-weight: 650; }
  .cf-filter-value { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .cf-filter-pill button { display: inline-grid; place-items: center; width: 20px; height: 20px; border-radius: 999px; border: 0; padding: 0; background: #dbe3ee; color: #253041; font-size: 12px; line-height: 1; }
  .cf-filter-pill button:hover { background: #c8d3e2; }
  .cf-preagg-diagnostics { margin-top: 6px; color: #526071; font-size: 12px; }
  .cf-preagg-diagnostics strong { color: #253041; font-weight: 650; }
  .cf-toolbar-actions { display: flex; align-items: center; gap: 10px; }
  .cf-renderers { display: inline-flex; align-items: center; gap: 3px; border: 1px solid #d5dbe5; border-radius: 8px; padding: 3px; background: #f7f9fc; }
  .cf-renderer-button { border: 0; border-radius: 6px; padding: 5px 8px; background: transparent; color: #394455; cursor: pointer; font-size: 12px; font-weight: 650; line-height: 1; white-space: nowrap; }
  .cf-renderer-button[aria-pressed="true"] { background: #fff; color: #111827; box-shadow: 0 1px 2px rgba(17, 24, 39, 0.10); }
  .cf-toggle { display: inline-flex; align-items: center; gap: 6px; color: #394455; font-size: 12px; white-space: nowrap; }
  .cf-toggle input { inline-size: 15px; block-size: 15px; margin: 0; }
  .cf-tabs { display: inline-flex; align-items: center; gap: 4px; border: 1px solid #d5dbe5; border-radius: 8px; padding: 4px; margin-bottom: 12px; background: #f7f9fc; }
  .cf-tabs[hidden] { display: none; }
  .cf-tab { display: grid; gap: 1px; border: 0; border-radius: 6px; padding: 6px 10px; background: transparent; color: #394455; cursor: pointer; text-align: left; }
  .cf-tab[aria-selected="true"] { background: #fff; color: #111827; box-shadow: 0 1px 2px rgba(17, 24, 39, 0.10); }
  .cf-tab span { font-weight: 650; font-size: 12px; }
  .cf-tab small { color: #687386; font-size: 11px; }
  .cf-dataset-meta { margin-top: 3px; color: #687386; font-size: 12px; }
  .cf-kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin-bottom: 12px; }
  .cf-kpi { border: 1px solid #d5dbe5; border-radius: 8px; padding: 10px; background: #fbfcfe; }
  .cf-kpi span { color: #556070; display: block; font-size: 11px; margin-bottom: 4px; }
  .cf-kpi strong { display: block; font-size: 20px; line-height: 1.15; }
  .cf-grid { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.65fr); gap: 18px; align-items: start; }
  .cf-panel { border: 1px solid #d5dbe5; border-radius: 8px; padding: 12px; min-width: 0; }
  .cf-panel h2 { font-size: 14px; margin: 0 0 8px; }
  .cf-panel svg { display: block; width: 100%; height: auto; overflow: visible; }
  .cf-chart-surface { min-height: 360px; position: relative; width: 100%; }
  .cf-chart-surface > div, .cf-chart-surface .plot-container, .cf-chart-surface .js-plotly-plot { width: 100%; }
  .cf-observable-legend { display: flex; flex-wrap: wrap; gap: 6px 12px; margin: 2px 0 6px; color: #253041; font-size: 12px; font-weight: 650; line-height: 1.2; }
  .cf-observable-legend-item { display: inline-flex; align-items: center; gap: 5px; }
  .cf-observable-legend-swatch { border-radius: 999px; display: inline-block; height: 9px; width: 9px; }
  .cf-observable-plot-frame { position: relative; width: 100%; }
  .cf-chart-loading { color: #556070; font-size: 12px; padding: 120px 0; text-align: center; }
  .cf-selection-overlay { background: rgba(51, 51, 51, 0.125); border: 1px solid rgba(255, 255, 255, 0.95); border-radius: 0; bottom: 58px; box-sizing: border-box; pointer-events: none; position: absolute; top: 28px; z-index: 3; }
  .cf-brush-layer { inset: 0; pointer-events: none; position: absolute; z-index: 4; }
  .cf-brush-layer .brush { pointer-events: all; }
  .cf-brush-layer .selection { fill: #333; fill-opacity: 0.125; stroke: #fff; }
  .cf-bars { display: grid; gap: 12px; }
  .cf-bar-title { font-weight: 650; font-size: 12px; margin: 0 0 4px; }
  .cf-table-panel { grid-column: 1 / -1; }
  .cf-table-wrap { overflow: auto; max-height: 260px; border-top: 1px solid #e3e7ee; }
  .cf-table { border-collapse: collapse; width: 100%; font-size: 12px; }
  .cf-table th, .cf-table td { border-bottom: 1px solid #e3e7ee; padding: 6px 8px; text-align: left; white-space: nowrap; }
  .cf-table th { background: #f7f9fc; position: sticky; top: 0; }
  .cf-muted { fill: #a9b1be; color: #556070; }
  .cf-active-range { fill: #cfd4dc; opacity: 0.65; }
  .cf-clickable { cursor: pointer; }
  @media (max-width: 760px) { .cf-grid, .cf-kpis { grid-template-columns: 1fr; } }
</style>
<div id="crossfilter-root">
  <div class="cf-tabs" id="cf-tabs" role="tablist" hidden></div>
  <div class="cf-toolbar">
    <div class="cf-filter-summary">
      <strong id="cf-summary">Preparing cross-filter views...</strong>
      <div class="cf-dataset-meta" id="cf-dataset-meta"></div>
      <div class="cf-preagg-diagnostics" id="cf-preagg-diagnostics"></div>
      <div class="cf-active-filters" id="cf-active-filters" aria-live="polite" hidden></div>
    </div>
    <div class="cf-toolbar-actions">
      <div class="cf-renderers" id="cf-renderers" role="group" aria-label="Chart renderer"></div>
      <label class="cf-toggle" title="Build and route through database-side interaction preaggregates">
        <input id="cf-preagg-toggle" type="checkbox">
        Interaction preagg
      </label>
      <button id="cf-reset" type="button">Reset</button>
    </div>
  </div>
  <div class="cf-kpis" id="cf-kpis"></div>
  <div class="cf-grid">
    <section class="cf-panel">
      <h2>Trend</h2>
      <div id="cf-line" class="cf-chart-surface" data-chart-surface="trend"></div>
    </section>
    <section class="cf-panel">
      <h2>Metric Relationship</h2>
      <div id="cf-scatter" class="cf-chart-surface" data-chart-surface="scatter"></div>
    </section>
    <section class="cf-panel cf-table-panel">
      <h2>Breakdowns</h2>
      <div class="cf-bars" id="cf-bars"></div>
    </section>
    <section class="cf-panel cf-table-panel">
      <h2>Filtered Groups</h2>
      <div class="cf-table-wrap">
        <table class="cf-table" id="cf-table"></table>
      </div>
    </section>
  </div>
</div>
<script id="chart-spec" type="application/json">__SPEC_JSON__</script>
__VENDOR_SCRIPTS__
<script>
  window.__SIDEMANTIC_OBSERVABLE_PLOT__ = window.Plot;
  window.dispatchEvent(new Event('sidemantic:observable-plot-ready'));
</script>
<script>
(function () {
  const payload = JSON.parse(document.getElementById('chart-spec').textContent);
  const tabEntries = payload.tabs || [{ id: 'default', label: payload.title || 'Explorer', spec: payload }];
  let activeTabIndex = 0;
  let spec;
  let fields;
  let fieldPlan;
  let interactionPlan;
  let freshnessPolicy;
  let xField;
  let metrics;
  let yField;
  let scatterXField;
  let scatterYField;
  let sizeField;
  let categoryFields;
  let seriesField;
  let columns;
  let tableLimit;
  let rows;
  let domainRows;
  let xValues;
  let seriesValues;
  let metricAggs;
  let state;
  let color;
  let liveEndpoint;
  let liveViews;
  let lastUpdatedAt;
  let activeRenderer;
  let pendingRequestId = 0;
  let requestInFlight = false;
  let queuedEventName = null;
  let refreshTimer = null;
  const tabPreaggState = {};
  const tabRendererState = {};
  const tabsRoot = document.getElementById('cf-tabs');
  const summary = document.getElementById('cf-summary');
  const datasetMeta = document.getElementById('cf-dataset-meta');
  const preaggDiagnostics = document.getElementById('cf-preagg-diagnostics');
  const filterPills = document.getElementById('cf-active-filters');
  const rendererControls = document.getElementById('cf-renderers');
  const preaggToggle = document.getElementById('cf-preagg-toggle');
  const resetButton = document.getElementById('cf-reset');
  const kpis = document.getElementById('cf-kpis');
  const barsRoot = document.getElementById('cf-bars');

  document.documentElement.dataset.sidemanticReady = 'true';
  document.documentElement.dataset.sidemanticCrossfilterEvent = '';
  document.documentElement.dataset.sidemanticCrossfilterFilters = '0';
  document.documentElement.dataset.sidemanticCrossfilterRenderer = '';

  tabsRoot.addEventListener('click', event => {
    const button = event.target.closest('button[data-tab-index]');
    if (!button) return;
    activateTab(Number(button.dataset.tabIndex));
  });

  resetButton.addEventListener('click', () => {
    state.xRange = null;
    state.xBrushValue = null;
    state.metricRange = null;
    state.metricBrushValue = null;
    state.categories = {};
    setEvent('reset');
  });

  preaggToggle.addEventListener('change', () => {
    tabPreaggState[currentTabId()] = interactionPreaggEnabled();
    setEvent('preagg:toggle');
  });

  rendererControls.addEventListener('click', event => {
    const button = event.target.closest('button[data-renderer]');
    if (!button) return;
    changeRenderer(button.dataset.renderer);
  });

  filterPills.addEventListener('click', event => {
    const button = event.target.closest('button[data-filter-key]');
    if (!button) return;
    clearFilter(button.dataset.filterKey);
  });

  window.addEventListener('sidemantic:observable-plot-ready', () => {
    if (activeRenderer === 'observable-plot' && state) render();
  });

  activateTab(0);

  async function activateTab(index) {
    pendingRequestId += 1;
    activeTabIndex = index;
    const tab = tabEntries[activeTabIndex];
    renderTabs();
    if (!tab.spec) {
      summary.textContent = `Loading ${tab.label || tab.id || `tab ${index + 1}`}...`;
      datasetMeta.textContent = 'Fetching dashboard spec...';
      preaggDiagnostics.textContent = '';
      kpis.innerHTML = '';
      barsRoot.innerHTML = '';
      clearSurface('cf-line');
      clearSurface('cf-scatter');
      document.getElementById('cf-table').innerHTML = '';
      try {
        await loadTabSpec(tab);
      } catch (error) {
        window.__SIDEMANTIC_CROSSFILTER_ERROR__ = String(error);
        summary.textContent = String(error);
        document.documentElement.dataset.sidemanticCrossfilterEvent = 'error';
        return;
      }
      if (activeTabIndex !== index) return;
    }
    spec = tab.spec || tab;
    const defaultRenderer = normalizeChartRenderer(spec.chart_renderer || spec.renderer_adapter || spec.sidemantic?.chart_renderer || 'd3');
    if (tabRendererState[currentTabId()] == null) {
      tabRendererState[currentTabId()] = defaultRenderer;
    }
    activeRenderer = tabRendererState[currentTabId()];
    fields = spec.fields;
    fieldPlan = spec.field_plan || spec.sidemantic?.field_plan || null;
    interactionPlan = spec.interaction_plan || spec.sidemantic?.interaction_plan || null;
    freshnessPolicy = spec.freshness_policy || spec.sidemantic?.freshness_policy || null;
    xField = fields.x;
    metrics = fields.metrics || fields.y;
    metricAggs = fields.metric_aggs || spec.metric_aggs || spec.sidemantic?.metric_aggs || {};
    yField = metrics[0];
    scatterXField = metrics[2] || metrics[1] || metrics[0];
    scatterYField = metrics[0];
    sizeField = metrics[1] || metrics[0];
    categoryFields = ((interactionFieldList('select') || fields.dimensions || [])).filter(field => field !== xField);
    seriesField = fields.series || categoryFields[0] || null;
    columns = Array.from(new Set([...(fields.dimensions || []), ...metrics].filter(Boolean)));
    tableLimit = Number(spec.table_limit || spec.sidemantic?.table_limit || 75);
    domainRows = (spec.data || []).map((row, rowIndex) => ({ ...row, __index: rowIndex }));
    rows = domainRows.slice();
    liveViews = null;
    lastUpdatedAt = null;
    liveEndpoint = tab.query_endpoint || spec.query_endpoint || spec.sidemantic?.query_endpoint || null;
    if (tabPreaggState[currentTabId()] == null) {
      tabPreaggState[currentTabId()] = Boolean(spec.interaction_preaggregations);
    }
    preaggToggle.checked = tabPreaggState[currentTabId()];
    preaggToggle.disabled = !liveEndpoint;
    xValues = valuesFrom(domainSourceRows(), xField);
    seriesValues = seriesField ? valuesFor(seriesField) : [label(yField)];
    state = { xRange: null, xBrushValue: null, metricRange: null, metricBrushValue: null, categories: {}, lastEvent: 'tab' };
    color = d3.scaleOrdinal(seriesValues, d3.schemeTableau10);
    color.domain(seriesValues);
    window.__SIDEMANTIC_CROSSFILTER__ = {
      state,
      activeTab: tab.id || String(activeTabIndex),
      totalRows: rows.length,
      totalSourceRows: recordCount(rows),
      filteredRows: rows.length,
      filteredSourceRows: recordCount(rows),
      lastEvent: null,
      renderer: activeRenderer,
      filters: {}
    };
    window.__SIDEMANTIC_CROSSFILTER_API__ = headlessApi();
    document.documentElement.dataset.sidemanticCrossfilterTab = tab.id || String(activeTabIndex);
    document.documentElement.dataset.sidemanticCrossfilterRenderer = activeRenderer;
    document.documentElement.dataset.sidemanticCrossfilterSourceRows = String(recordCount(rows));
    renderTabs();
    renderRendererControls();
    render();
    if (liveEndpoint && spec.data_deferred) {
      scheduleRefresh('tab');
    }
  }

  async function loadTabSpec(tab) {
    if (tab.spec) return tab.spec;
    if (tab._specPromise) return tab._specPromise;
    const endpoint = tab.spec_endpoint || payload.spec_endpoint;
    if (!endpoint) throw new Error(`No spec endpoint for ${tab.id || 'tab'}`);
    tab._specPromise = fetch(endpoint)
      .then(response => {
        if (!response.ok) throw new Error(`Dashboard spec query failed: ${response.status}`);
        return response.json();
      })
      .then(tabPayload => {
        tab.spec = tabPayload.spec || tabPayload;
        tab.query_endpoint = tabPayload.query_endpoint || tab.query_endpoint;
        tab.source_record_count = tabPayload.source_record_count ?? tab.source_record_count;
        tab.label = tabPayload.label || tab.label;
        return tab.spec;
      })
      .finally(() => {
        tab._specPromise = null;
      });
    return tab._specPromise;
  }

  function setEvent(eventName) {
    state.lastEvent = eventName;
    if (liveEndpoint) {
      scheduleRefresh(eventName);
    } else {
      render();
    }
  }

  function scheduleRefresh(eventName) {
    queuedEventName = eventName;
    if (requestInFlight) return;
    if (refreshTimer) clearTimeout(refreshTimer);
    const debounceMs = eventName && (eventName.includes('brush') || eventName.includes('scatter')) ? 140 : 0;
    refreshTimer = setTimeout(runQueuedRefresh, debounceMs);
  }

  function runQueuedRefresh() {
    refreshTimer = null;
    if (requestInFlight) return;
    const eventName = queuedEventName;
    queuedEventName = null;
    if (eventName) refreshFromServer(eventName);
  }

  function passFilters(row, ignoreField = null) {
    if (ignoreField !== '__xRange' && state.xRange && xRangeIsSemanticDimension()) {
      const xIndex = xValues.indexOf(row[xField]);
      if (xIndex < state.xRange[0] || xIndex > state.xRange[1]) return false;
    }
    if (ignoreField !== '__metricRange' && state.metricRange) {
      const metricX = Number(row[scatterXField]);
      const metricY = Number(row[scatterYField]);
      if (!Number.isFinite(metricX) || !Number.isFinite(metricY)) return false;
      if (metricX < state.metricRange.x[0] || metricX > state.metricRange.x[1]) return false;
      if (metricY < state.metricRange.y[0] || metricY > state.metricRange.y[1]) return false;
    }
    for (const [field, value] of Object.entries(state.categories)) {
      if (field !== ignoreField && value != null && row[field] !== value) return false;
    }
    return true;
  }

  function rowsForView(ignoreField = null) {
    return rows.filter(row => passFilters(row, ignoreField));
  }

  function filteredRows() {
    return rowsForView();
  }

  function domainSourceRows() {
    return domainRows && domainRows.length ? domainRows : rows;
  }

  function valuesFrom(data, field) {
    return Array.from(new Set((data || []).map(row => row[field]).filter(value => value != null))).sort();
  }

  function refreshDomainsFromLiveViews() {
    const bars = liveViews?.bars || {};
    const liveDomainRows = [
      ...(liveViews?.trend || []),
      ...(liveViews?.scatter || []),
      ...Object.values(bars).flat(),
      ...rows
    ];
    const nextXValues = valuesFrom((liveViews?.trend || []).length ? liveViews.trend : liveDomainRows, xField);
    if (nextXValues.length) xValues = nextXValues;
    const nextSeriesValues = seriesField
      ? valuesFrom(liveDomainRows.length ? liveDomainRows : domainSourceRows(), seriesField)
      : [label(yField)];
    if (nextSeriesValues.length || !seriesValues?.length) {
      seriesValues = nextSeriesValues;
      color = d3.scaleOrdinal(seriesValues, d3.schemeTableau10);
      color.domain(seriesValues);
    }
  }

  function valuesFor(field) {
    const barRows = liveViews?.bars?.[field];
    return valuesFrom(barRows?.length ? barRows : domainSourceRows(), field);
  }

  function filterCount() {
    return (state.xRange && xRangeIsSemanticDimension() ? 1 : 0) + (state.metricRange ? 1 : 0) + Object.values(state.categories).filter(Boolean).length;
  }

  function render() {
    const filtered = liveEndpoint ? rows : filteredRows();
    const filters = activeFilters();
    const filteredSourceRows = recordCount(filtered);
    const totalSourceRows = liveViews?.total_source_rows ?? recordCount(domainSourceRows());
    const totalGroups = liveViews?.total_groups ?? domainSourceRows().length;
    const updatedAt = liveViews?.updated_at || liveViews?.diagnostics?.updated_at || lastUpdatedAt;
    const freshness = liveViews?.freshness || liveViews?.diagnostics?.freshness || null;
    window.__SIDEMANTIC_CROSSFILTER__.filteredRows = filtered.length;
    window.__SIDEMANTIC_CROSSFILTER__.totalRows = totalGroups;
    window.__SIDEMANTIC_CROSSFILTER__.filteredSourceRows = filteredSourceRows;
    window.__SIDEMANTIC_CROSSFILTER__.totalSourceRows = totalSourceRows;
    window.__SIDEMANTIC_CROSSFILTER__.lastEvent = state.lastEvent;
    window.__SIDEMANTIC_CROSSFILTER__.renderer = activeRenderer;
    window.__SIDEMANTIC_CROSSFILTER__.filters = {
      xRange: state.xRange,
      metricRange: state.metricRange,
      categories: { ...state.categories },
      active: filters
    };
    const currentSql = liveViews?.sql || '';
    const diagnostics = liveViews?.diagnostics || {};
    window.__SIDEMANTIC_CROSSFILTER__.sql = currentSql || null;
    window.__SIDEMANTIC_CROSSFILTER__.viewSql = liveViews?.sqls || {};
    window.__SIDEMANTIC_CROSSFILTER__.interactionPreagg = diagnostics.interaction_preagg || null;
    window.__SIDEMANTIC_CROSSFILTER__.updatedAt = updatedAt || null;
    window.__SIDEMANTIC_CROSSFILTER__.freshness = freshness;
    window.__SIDEMANTIC_CROSSFILTER__.freshnessPolicy = freshnessPolicy;
    window.__SIDEMANTIC_CROSSFILTER__.fieldPlan = fieldPlan;
    window.__SIDEMANTIC_CROSSFILTER__.interactionPlan = interactionPlan;
    document.documentElement.dataset.sidemanticCrossfilterEvent = state.lastEvent || '';
    document.documentElement.dataset.sidemanticCrossfilterRenderer = activeRenderer;
    document.documentElement.dataset.sidemanticCrossfilterMode = liveEndpoint ? 'database' : 'static';
    document.documentElement.dataset.sidemanticCrossfilterRows = String(filtered.length);
    document.documentElement.dataset.sidemanticCrossfilterSourceRows = String(filteredSourceRows);
    document.documentElement.dataset.sidemanticCrossfilterFilters = String(filterCount());
    document.documentElement.dataset.sidemanticCrossfilterSemanticFilters = String(liveViews?.filter_expressions?.length || 0);
    document.documentElement.dataset.sidemanticCrossfilterUsedPreagg = diagnostics.used_interaction_preagg ? 'true' : 'false';
    document.documentElement.dataset.sidemanticCrossfilterPreaggTable = diagnostics.interaction_preagg?.table?.table_name || '';
    document.documentElement.dataset.sidemanticCrossfilterUpdatedAt = updatedAt || '';
    document.documentElement.dataset.sidemanticCrossfilterFreshnessStatus = freshness?.status || '';
    document.documentElement.dataset.sidemanticCrossfilterStale = freshness?.stale == null ? 'unknown' : String(Boolean(freshness.stale));
    document.documentElement.dataset.sidemanticCrossfilterSourceWatermarkStatus = freshness?.source_watermark?.status || '';
    document.documentElement.dataset.sidemanticCrossfilterFreshnessTtlSeconds = freshness?.policy?.ttl_seconds == null ? '' : String(freshness.policy.ttl_seconds);
    const upperSql = currentSql.toUpperCase();
    document.documentElement.dataset.sidemanticCrossfilterSqlHasWhere = upperSql.includes('WHERE') ? 'true' : 'false';
    document.documentElement.dataset.sidemanticCrossfilterSqlHasHaving = upperSql.includes('HAVING') ? 'true' : 'false';
    summary.textContent = summaryText(filtered.length, totalGroups);
    const freshnessText = updatedAt ? ` | Updated ${formatTimestamp(updatedAt)}${freshnessLabel(freshness)}` : '';
    datasetMeta.textContent = `${formatMetric(filteredSourceRows, 'count')} of ${formatMetric(totalSourceRows, 'count')} source records | ${formatMetric(filtered.length, 'count')} of ${formatMetric(totalGroups, 'count')} groups${freshnessText}`;
    renderRendererControls();
    renderPreaggDiagnostics(diagnostics.interaction_preagg);
    renderFilterPills(filters);
    renderKpis(filtered);
    renderLine();
    renderScatter();
    renderBars();
    renderTable(filtered);
  }

  async function refreshFromServer(eventName) {
    const requestId = ++pendingRequestId;
    const requestedTabId = currentTabId();
    requestInFlight = true;
    datasetMeta.textContent = 'Querying database...';
    try {
      const body = {
        tab: requestedTabId,
        event: eventName,
        active: activePayload(eventName),
        filters: requestFilters(),
        interaction_preaggregations: interactionPreaggEnabled()
      };
      document.documentElement.dataset.sidemanticCrossfilterLastRequest = JSON.stringify(body).slice(0, 1500);
      const response = await fetch(liveEndpoint, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body)
      });
      if (!response.ok) throw new Error(`Crossfilter query failed: ${response.status}`);
      const payload = await response.json();
      if (requestId !== pendingRequestId || requestedTabId !== currentTabId()) return;
      rows = (payload.rows || []).map((row, rowIndex) => ({ ...row, __index: rowIndex }));
      liveViews = payload.views || {};
      const diagnostics = payload.diagnostics || {};
      liveViews.sql = payload.sql || diagnostics.sql || null;
      liveViews.sqls = payload.sqls || diagnostics.sqls || {};
      liveViews.total_source_rows = payload.total_source_rows;
      liveViews.total_groups = payload.total_groups;
      liveViews.updated_at = payload.updated_at || diagnostics.updated_at || new Date().toISOString();
      liveViews.freshness = payload.freshness || diagnostics.freshness || null;
      liveViews.filter_expressions = payload.filter_expressions || diagnostics.filter_expressions || [];
      liveViews.diagnostics = diagnostics;
      lastUpdatedAt = liveViews.updated_at;
      refreshDomainsFromLiveViews();
      state.lastEvent = eventName;
      render();
    } catch (error) {
      if (requestId !== pendingRequestId || requestedTabId !== currentTabId()) return;
      window.__SIDEMANTIC_CROSSFILTER_ERROR__ = String(error);
      datasetMeta.textContent = String(error);
      document.documentElement.dataset.sidemanticCrossfilterEvent = 'error';
    } finally {
      requestInFlight = false;
      if (queuedEventName) scheduleRefresh(queuedEventName);
    }
  }

  async function activatePreagg(type, field) {
    if (!liveEndpoint || !interactionPreaggEnabled()) return;
    if (type === 'xRange' && !xRangeIsSemanticDimension()) return;
    try {
      const response = await fetch(liveEndpoint, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          tab: currentTab().id || String(activeTabIndex),
          event: 'activate',
          active: { type, field },
          filters: requestFilters(),
          interaction_preaggregations: interactionPreaggEnabled()
        })
      });
      if (!response.ok) return;
      const payload = await response.json();
      liveViews = liveViews || {};
      liveViews.diagnostics = payload.diagnostics || {};
      liveViews.freshness = payload.freshness || liveViews.diagnostics.freshness || liveViews.freshness || null;
      renderPreaggDiagnostics(liveViews.diagnostics.interaction_preagg);
      document.documentElement.dataset.sidemanticCrossfilterUsedPreagg = liveViews.diagnostics.used_interaction_preagg ? 'true' : 'false';
      document.documentElement.dataset.sidemanticCrossfilterPreaggTable = liveViews.diagnostics.interaction_preagg?.table?.table_name || '';
    } catch (error) {
      window.__SIDEMANTIC_CROSSFILTER_PREAGG_ERROR__ = String(error);
    }
  }

  function interactionPreaggEnabled() {
    return Boolean(preaggToggle && preaggToggle.checked);
  }

  function activePayload(eventName) {
    if (eventName === 'brush' || eventName === 'brush:clear') {
      return xRangeIsSemanticDimension() ? { type: 'xRange', field: xField } : null;
    }
    if (eventName === 'scatter' || eventName === 'scatter:clear') return { type: 'metricRange', field: scatterXField };
    if (eventName && eventName.startsWith('category:')) {
      return { type: 'category', field: eventName.split(':')[1] };
    }
    return null;
  }

  function renderPreaggDiagnostics(preagg) {
    if (!preaggDiagnostics) return;
    if (!preagg) {
      preaggDiagnostics.textContent = spec.interaction_preaggregations
        ? 'Interaction preagg: waiting for selection activation.'
        : '';
      return;
    }
    if (!preagg.used) {
      preaggDiagnostics.textContent = `Interaction preagg unavailable: ${preagg.reason || 'unsupported query shape'}`;
      return;
    }
    const table = preagg.table || {};
    const reuseText = table.reused ? 'reused' : 'built';
    preaggDiagnostics.innerHTML = `<strong>Interaction preagg ${reuseText}</strong>: ${escapeHtml(table.table_name || '')} | ${formatMetric(table.row_count || 0, 'count')} groups | ${Number(table.build_ms || 0).toFixed(1)} ms`;
  }

  function freshnessLabel(freshness) {
    if (!freshness) return '';
    if (freshness.stale === true) return ' | Stale';
    if (freshness.stale == null) return ' | Freshness unknown';
    return '';
  }

  function requestFilters() {
    const filters = [];
    if (state.xRange && xRangeIsSemanticDimension()) {
      filters.push({
        type: 'xRange',
        field: xField,
        min: xValues[state.xRange[0]],
        max: xValues[state.xRange[1]]
      });
    }
    if (state.metricRange) {
      filters.push({
        type: 'metricRange',
        xField: scatterXField,
        xMin: state.metricRange.x[0],
        xMax: state.metricRange.x[1],
        yField: scatterYField,
        yMin: state.metricRange.y[0],
        yMax: state.metricRange.y[1]
      });
    }
    for (const [field, value] of Object.entries(state.categories)) {
      filters.push({ type: 'category', field, value });
    }
    return filters;
  }

  function activeFilters() {
    const filters = [];
    if (state.xRange && xRangeIsSemanticDimension()) {
      filters.push({
        key: 'xRange',
        label: label(xField),
        value: `${xValues[state.xRange[0]]} - ${xValues[state.xRange[1]]}`
      });
    }
    if (state.metricRange) {
      filters.push({
        key: 'metricRange',
        label: 'Metric Relationship',
        value: `${label(scatterXField)} ${formatRange(state.metricRange.x, 'count')}, ${label(scatterYField)} ${formatRange(state.metricRange.y, 'currency')}`
      });
    }
    for (const [field, value] of Object.entries(state.categories)) {
      filters.push({ key: `category:${field}`, label: label(field), value: String(value) });
    }
    return filters;
  }

  function renderFilterPills(filters) {
    filterPills.hidden = filters.length === 0;
    filterPills.innerHTML = filters.map(filter => `
      <span class="cf-filter-pill" data-filter-key="${escapeAttr(filter.key)}">
        <span class="cf-filter-label">${escapeHtml(filter.label)}</span>
        <span class="cf-filter-value">${escapeHtml(filter.value)}</span>
        <button type="button" data-filter-key="${escapeAttr(filter.key)}" aria-label="Clear ${escapeAttr(filter.label)} filter" title="Clear ${escapeAttr(filter.label)} filter">x</button>
      </span>
    `).join('');
  }

  function clearFilter(key) {
    if (key === 'xRange') {
      state.xRange = null;
      state.xBrushValue = null;
    } else if (key === 'metricRange') {
      state.metricRange = null;
      state.metricBrushValue = null;
    } else if (key && key.startsWith('category:')) {
      delete state.categories[key.slice('category:'.length)];
    }
    setEvent(`clear:${key}`);
  }

  function aggregateByX(data) {
    if (!seriesField) {
      return d3.rollups(
        data,
        values => ({ value: aggregateMetric(values, yField) }),
        row => row[xField]
      ).map(([xValue, value]) => ({ [xField]: xValue, [yField]: value.value }));
    }
    const grouped = d3.rollups(
      data,
      values => ({
        value: aggregateMetric(values, yField),
        series: values[0]?.[seriesField]
      }),
      row => row[xField],
      row => row[seriesField]
    );
    return grouped.flatMap(([xValue, bySeries]) =>
      bySeries.map(([series, value]) => ({ [xField]: xValue, [seriesField]: series, [yField]: value.value }))
    );
  }

  function metricAgg(metric, overrideSpec = null) {
    const source = overrideSpec
      ? overrideSpec.fields?.metric_aggs || overrideSpec.metric_aggs || overrideSpec.sidemantic?.metric_aggs || {}
      : metricAggs;
    if (source && Object.prototype.hasOwnProperty.call(source, metric)) {
      return String(source[metric] ?? 'unknown').toLowerCase();
    }
    return 'sum';
  }

  function metricNumber(row, metric) {
    const value = Number(row?.[metric]);
    return Number.isFinite(value) ? value : null;
  }

  function aggregateMetric(values, metric) {
    return aggregateMetricForSpec(null, values, metric);
  }

  function aggregateMetricForSpec(overrideSpec, values, metric) {
    const numbers = (values || [])
      .map(row => metricNumber(row, metric))
      .filter(value => value != null);
    if (!numbers.length) return null;
    const agg = metricAgg(metric, overrideSpec);
    if (agg === 'min') return d3.min(numbers);
    if (agg === 'max') return d3.max(numbers);
    if (agg === 'sum' || agg === 'count') return d3.sum(numbers);
    return numbers.length === 1 ? numbers[0] : null;
  }

  function lineChartData() {
    return liveViews?.trend || aggregateByX(rowsForView('__xRange'));
  }

  function scatterChartData() {
    return liveViews?.scatter || rowsForView('__metricRange');
  }

  function clearSurface(id) {
    const element = document.getElementById(id);
    if (window.Plotly && element?._fullLayout) {
      Plotly.purge(element);
    }
    element.classList.remove('vega-embed', 'fit-x', 'js-plotly-plot', 'plotly-graph-div');
    element.removeAttribute('style');
    element.innerHTML = '';
    return element;
  }

  function renderAdapterLoading(root, name) {
    root.innerHTML = `<div class="cf-chart-loading">${escapeHtml(name)} loading...</div>`;
  }

  function renderAdapterError(root, error) {
    root.innerHTML = `<div class="cf-chart-loading">${escapeHtml(String(error))}</div>`;
  }

  function chartRowsForVega(data, temporalFields) {
    const temporal = new Set(temporalFields);
    return data.map(row => {
      const copy = { ...row };
      for (const field of temporal) {
        if (copy[field] != null) copy[field] = String(copy[field]);
      }
      return copy;
    });
  }

  function chartRowsForObservable(data, temporalFields) {
    const temporal = new Set(temporalFields);
    return data.map(row => {
      const copy = { ...row };
      for (const field of temporal) {
        if (typeof copy[field] === 'string' && /^\\d{4}-\\d{2}-\\d{2}/.test(copy[field])) {
          copy[field] = new Date(copy[field]);
        }
      }
      return copy;
    });
  }

  function tooltipFields(fieldNames) {
    return Array.from(new Set(fieldNames.filter(Boolean))).map(field => ({
      field,
      type: field === yField || metrics.includes(field) ? 'quantitative' : vegaLiteType(field),
      title: label(field)
    }));
  }

  function vegaLiteType(field) {
    const sample = domainSourceRows().find(row => row[field] != null)?.[field];
    if (typeof sample === 'number') return 'quantitative';
    if (sample instanceof Date) return 'temporal';
    if (typeof sample === 'string' && /^\\d{4}-\\d{2}-\\d{2}/.test(sample)) return 'temporal';
    return 'nominal';
  }

  function intervalExtent(value, field) {
    if (!value || typeof value !== 'object') return null;
    const direct = value[field];
    if (Array.isArray(direct)) return direct;
    const firstArray = Object.values(value).find(Array.isArray);
    return firstArray || null;
  }

  function numericExtent(extent) {
    const values = extent.map(Number).filter(Number.isFinite);
    if (values.length < 2) return [0, 0];
    return values.sort((a, b) => a - b);
  }

  function xRangeIsSemanticDimension() {
    return (fields?.dimensions || []).includes(xField);
  }

  function xRangeFromExtent(extent) {
    if (!xRangeIsSemanticDimension()) return null;
    const comparable = extent.map(comparableValue);
    const numeric = comparable.map(Number).filter(Number.isFinite);
    const values = numeric.length === comparable.length
      ? numeric.sort((a, b) => a - b)
      : comparable.map(String).sort();
    const indices = xValues
      .map((value, index) => ({ index, value: comparableValue(value) }))
      .filter(item => {
        const comparableItem = numeric.length === comparable.length ? Number(item.value) : String(item.value);
        return comparableItem >= values[0] && comparableItem <= values[1];
      })
      .map(item => item.index);
    return indices.length ? [Math.min(...indices), Math.max(...indices)] : null;
  }

  function comparableValue(value) {
    if (value instanceof Date) return value.getTime();
    if (typeof value === 'string') {
      const parsed = Date.parse(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    const number = Number(value);
    if (Number.isFinite(number)) return number;
    return String(value);
  }

  function renderXRangeOverlay(root) {
    if (!root || !state.xRange || !xRangeIsSemanticDimension() || xValues.length < 2) return;
    const [startIndex, endIndex] = state.xRange;
    const start = xValues[startIndex];
    const end = xValues[endIndex];
    if (start == null || end == null) return;
    const plotLeftPct = 10;
    const plotWidthPct = 84;
    const denominator = Math.max(1, xValues.length - 1);
    const left = plotLeftPct + (Math.min(startIndex, endIndex) / denominator) * plotWidthPct;
    const right = plotLeftPct + (Math.max(startIndex, endIndex) / denominator) * plotWidthPct;
    const overlay = document.createElement('div');
    overlay.className = 'cf-selection-overlay';
    overlay.dataset.activeXRange = `${start} - ${end}`;
    overlay.title = `${label(xField)}: ${start} - ${end}`;
    overlay.style.left = `${left}%`;
    overlay.style.width = `${Math.max(1.5, right - left)}%`;
    root.append(overlay);
  }

  function vegaLiteXSelectionValue() {
    if (state.xBrushValue) return state.xBrushValue;
    if (!state.xRange) return null;
    const [startIndex, endIndex] = state.xRange;
    const start = xValues[Math.min(startIndex, endIndex)];
    const end = xValues[Math.max(startIndex, endIndex)];
    if (start == null || end == null) return null;
    return { [xField]: [start, end] };
  }

  function vegaLiteMetricSelectionValue() {
    if (state.metricBrushValue) return state.metricBrushValue;
    if (!state.metricRange) return null;
    return {
      [scatterXField]: state.metricRange.x,
      [scatterYField]: state.metricRange.y
    };
  }

  function chartBrushBounds(root, kind = 'line') {
    const width = Math.max(320, root.clientWidth || root.getBoundingClientRect().width || 720);
    const height = Math.max(320, root.clientHeight || root.getBoundingClientRect().height || 360);
    const left = kind === 'scatter' ? width * 0.15 : width * 0.10;
    const right = kind === 'scatter' ? width * 0.94 : width * 0.94;
    const top = 28;
    const bottom = Math.max(top + 40, height - 58);
    return { width, height, left, right, top, bottom };
  }

  function appendBrushLayer(root, kind) {
    const bounds = chartBrushBounds(root, kind);
    const svg = d3.select(root)
      .append('svg')
      .attr('class', 'cf-brush-layer')
      .attr('viewBox', `0 0 ${bounds.width} ${bounds.height}`)
      .attr('preserveAspectRatio', 'none')
      .attr('aria-hidden', 'true');
    return { bounds, svg };
  }

  function renderObservableXBrushLayer(root) {
    if (!root || !xRangeIsSemanticDimension() || xValues.length < 2) return;
    const { bounds, svg } = appendBrushLayer(root, 'line');
    const brush = d3.brushX()
      .extent([[bounds.left, bounds.top], [bounds.right, bounds.bottom]])
      .on('start', () => activatePreagg('xRange', xField))
      .on('end', event => {
        if (!event.selection) {
          state.xRange = null;
          state.xBrushValue = null;
          setEvent('brush:clear');
          return;
        }
        const denominator = Math.max(1, xValues.length - 1);
        const [left, right] = event.selection;
        const startRatio = Math.max(0, Math.min(1, (left - bounds.left) / (bounds.right - bounds.left)));
        const endRatio = Math.max(0, Math.min(1, (right - bounds.left) / (bounds.right - bounds.left)));
        const startIndex = Math.round(startRatio * denominator);
        const endIndex = Math.round(endRatio * denominator);
        state.xRange = [Math.min(startIndex, endIndex), Math.max(startIndex, endIndex)];
        state.xBrushValue = null;
        setEvent('brush');
      });
    svg.append('g').attr('class', 'brush').call(brush);
  }

  function renderObservableMetricBrushLayer(root) {
    if (!root) return;
    const { bounds, svg } = appendBrushLayer(root, 'scatter');
    const scatterDomainRows = domainSourceRows();
    const xDomain = paddedDomain(scatterDomainRows.map(row => Number(row[scatterXField])));
    const yDomain = paddedDomain(scatterDomainRows.map(row => Number(row[scatterYField])));
    const x = d3.scaleLinear().domain(xDomain).range([bounds.left, bounds.right]);
    const y = d3.scaleLinear().domain(yDomain).range([bounds.bottom, bounds.top]);
    const brush = d3.brush()
      .extent([[bounds.left, bounds.top], [bounds.right, bounds.bottom]])
      .on('start', () => activatePreagg('metricRange', scatterXField))
      .on('end', event => {
        if (!event.selection) {
          state.metricRange = null;
          state.metricBrushValue = null;
          setEvent('scatter:clear');
          return;
        }
        const [[left, top], [right, bottom]] = event.selection;
        state.metricRange = {
          x: [x.invert(left), x.invert(right)].sort((a, b) => a - b),
          y: [y.invert(bottom), y.invert(top)].sort((a, b) => a - b)
        };
        state.metricBrushValue = null;
        setEvent('scatter');
      });
    svg.append('g').attr('class', 'brush').call(brush);
  }

  function observablePlotWidth(root) {
    return Math.max(320, Math.floor(root.clientWidth || root.getBoundingClientRect().width || 520));
  }

  function renderObservableLegend(root) {
    if (!seriesField) return;
    const legend = document.createElement('div');
    legend.className = 'cf-observable-legend';
    legend.setAttribute('aria-label', label(seriesField));
    legend.innerHTML = seriesValues.map(series => `
      <span class="cf-observable-legend-item">
        <span class="cf-observable-legend-swatch" style="background:${escapeAttr(color(series))}"></span>
        <span>${escapeHtml(series)}</span>
      </span>
    `).join('');
    root.append(legend);
  }

  function observablePlotFrame(root) {
    const frame = document.createElement('div');
    frame.className = 'cf-observable-plot-frame';
    root.append(frame);
    return frame;
  }

  function renderKpis(data) {
    const kpiRow = liveViews?.kpis || null;
    const metricCards = metrics.slice(0, 3).map(metric => ({
      label: label(metric),
      value: kpiRow ? metricNumber(kpiRow, metric) : aggregateMetric(data, metric),
      kind: metric.toLowerCase().includes('count') ? 'count' : 'currency'
    }));
    metricCards.push({ label: 'Groups', value: data.length, kind: 'count' });
    kpis.innerHTML = metricCards.map(card => `
      <div class="cf-kpi"><span>${escapeHtml(card.label)}</span><strong>${formatMetric(card.value, card.kind)}</strong></div>
    `).join('');
  }

  function renderLine() {
    if (activeRenderer === 'vega-lite') return renderVegaLiteLine();
    if (activeRenderer === 'plotly') return renderPlotlyLine();
    if (activeRenderer === 'observable-plot') return renderObservablePlotLine();
    return renderD3Line();
  }

  function renderScatter() {
    if (activeRenderer === 'vega-lite') return renderVegaLiteScatter();
    if (activeRenderer === 'plotly') return renderPlotlyScatter();
    if (activeRenderer === 'observable-plot') return renderObservablePlotScatter();
    return renderD3Scatter();
  }

  function renderD3Line() {
    const root = d3.select('#cf-line');
    root.selectAll('*').remove();
    const svg = root.append('svg').attr('viewBox', '0 0 760 360').attr('role', 'img');
    const margin = { top: 18, right: 28, bottom: 62, left: 72 };
    const width = 760 - margin.left - margin.right;
    const height = 360 - margin.top - margin.bottom;
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
    const x = d3.scalePoint().domain(xValues).range([0, width]).padding(0.5);
    const lineData = liveViews?.trend || aggregateByX(rowsForView('__xRange'));
    const maxY = d3.max(lineData, row => metricNumber(row, yField)) || d3.max(domainSourceRows(), row => metricNumber(row, yField)) || 1;
    const y = d3.scaleLinear().domain([0, maxY]).nice().range([height, 0]);

    if (state.xRange && xRangeIsSemanticDimension()) {
      const left = x(xValues[state.xRange[0]]);
      const right = x(xValues[state.xRange[1]]);
      g.append('rect')
        .attr('class', 'cf-active-range')
        .attr('x', Math.min(left, right) - 28)
        .attr('y', 0)
        .attr('width', Math.abs(right - left) + 56)
        .attr('height', height);
    }

    g.append('g').attr('transform', `translate(0,${height})`).call(d3.axisBottom(x)).selectAll('text')
      .attr('transform', 'rotate(-35)').attr('text-anchor', 'end');
    g.append('g').call(d3.axisLeft(y).ticks(6));

    const grouped = d3.groups(lineData, row => row[seriesField]);
    for (const [series, values] of grouped) {
      const sorted = values.slice().sort((a, b) => xValues.indexOf(a[xField]) - xValues.indexOf(b[xField]));
      g.append('path')
        .datum(sorted)
        .attr('fill', 'none')
        .attr('stroke', color(series))
        .attr('stroke-width', 2.5)
        .attr('d', d3.line().defined(row => metricNumber(row, yField) != null).x(row => x(row[xField])).y(row => y(metricNumber(row, yField))));
      g.append('g').selectAll('circle').data(sorted.filter(row => metricNumber(row, yField) != null)).join('circle')
        .attr('cx', row => x(row[xField]))
        .attr('cy', row => y(metricNumber(row, yField)))
        .attr('r', 3.5)
        .attr('fill', color(series));
    }

    axisLabels(g, width, height, label(xField), label(yField));

    if (xRangeIsSemanticDimension()) {
      const brush = d3.brushX().extent([[0, 0], [width, height]])
      .on('start', () => activatePreagg('xRange', xField))
      .on('end', event => {
        if (!event.selection) {
          state.xRange = null;
          state.xBrushValue = null;
          setEvent('brush:clear');
          return;
        }
        const [left, right] = event.selection;
        const indices = xValues
          .map((value, index) => ({ index, px: x(value) }))
          .filter(point => point.px >= left && point.px <= right)
          .map(point => point.index);
        state.xRange = indices.length ? [Math.min(...indices), Math.max(...indices)] : null;
        state.xBrushValue = null;
        setEvent('brush');
      });
      g.append('g').attr('class', 'brush').call(brush);
    }
  }

  function renderD3Scatter() {
    const root = d3.select('#cf-scatter');
    root.selectAll('*').remove();
    const svg = root.append('svg').attr('viewBox', '0 0 420 360').attr('role', 'img');
    const margin = { top: 18, right: 18, bottom: 58, left: 64 };
    const width = 420 - margin.left - margin.right;
    const height = 360 - margin.top - margin.bottom;
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
    const scatterDomainRows = domainSourceRows();
    const xDomain = paddedDomain(scatterDomainRows.map(row => Number(row[scatterXField])));
    const yDomain = paddedDomain(scatterDomainRows.map(row => Number(row[scatterYField])));
    const maxSize = d3.max(scatterDomainRows, row => Number(row[sizeField])) || 1;
    const maxRadius = 12;
    const brushPad = maxRadius + 4;
    const x = d3.scaleLinear().domain(xDomain).nice().range([0, width]);
    const y = d3.scaleLinear().domain(yDomain).nice().range([height, 0]);
    const size = d3.scaleSqrt().domain([0, maxSize]).range([3, maxRadius]);
    const data = liveViews?.scatter || rowsForView('__metricRange');

    if (state.metricRange) {
      g.append('rect')
        .attr('class', 'cf-active-range')
        .attr('x', x(state.metricRange.x[0]))
        .attr('y', y(state.metricRange.y[1]))
        .attr('width', Math.max(0, x(state.metricRange.x[1]) - x(state.metricRange.x[0])))
        .attr('height', Math.max(0, y(state.metricRange.y[0]) - y(state.metricRange.y[1])));
    }

    g.append('g').attr('transform', `translate(0,${height})`).call(d3.axisBottom(x).ticks(5));
    g.append('g').call(d3.axisLeft(y).ticks(6));
    g.selectAll('circle').data(data).join('circle')
      .attr('cx', row => x(Number(row[scatterXField])))
      .attr('cy', row => y(Number(row[scatterYField])))
      .attr('r', row => size(Number(row[sizeField])))
      .attr('fill', row => color(row[seriesField]))
      .attr('opacity', 0.72)
      .attr('stroke', '#fff')
      .attr('stroke-width', 1);
    axisLabels(g, width, height, label(scatterXField), label(scatterYField));

    const brush = d3.brush().extent([[-brushPad, -brushPad], [width + brushPad, height + brushPad]])
    .on('start', () => activatePreagg('metricRange', scatterXField))
    .on('end', event => {
      if (!event.selection) {
        state.metricRange = null;
        state.metricBrushValue = null;
        setEvent('scatter:clear');
        return;
      }
      const [[left, top], [right, bottom]] = event.selection;
      const xRange = [x.invert(left), x.invert(right)].sort((a, b) => a - b);
      const yRange = [y.invert(bottom), y.invert(top)].sort((a, b) => a - b);
      state.metricRange = { x: xRange, y: yRange };
      state.metricBrushValue = null;
      setEvent('scatter');
    });
    g.append('g').attr('class', 'brush').call(brush);
  }

  function renderVegaLiteLine() {
    const root = clearSurface('cf-line');
    const xBrushParam = xRangeIsSemanticDimension()
      ? { name: 'sidemanticXBrush', select: { type: 'interval', encodings: ['x'] } }
      : null;
    const xBrushValue = vegaLiteXSelectionValue();
    if (xBrushValue && xBrushParam) xBrushParam.value = xBrushValue;
    const vlSpec = {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
      width: 'container',
      height: 300,
      config: { selection: { interval: { fill: '#333', fillOpacity: 0.125, stroke: '#fff' } } },
      data: { values: chartRowsForVega(lineChartData(), [xField]) },
      params: xBrushParam ? [xBrushParam] : [],
      mark: { type: 'line', point: true, tooltip: true },
      encoding: {
        x: { field: xField, type: vegaLiteType(xField), title: label(xField) },
        y: { field: yField, type: 'quantitative', title: label(yField) },
        tooltip: tooltipFields([xField, seriesField, yField])
      }
    };
    if (seriesField) {
      vlSpec.encoding.color = { field: seriesField, type: 'nominal', title: label(seriesField) };
    }
    vegaEmbed(root, vlSpec, { actions: false }).then(result => {
      if (!xBrushParam) return;
      result.view.addSignalListener('sidemanticXBrush', (_name, value) => {
        const extent = intervalExtent(value, xField);
        if (!extent) {
          if (state.xRange) {
            state.xRange = null;
            state.xBrushValue = null;
            setEvent('brush:clear');
          }
          return;
        }
        activatePreagg('xRange', xField);
        state.xBrushValue = value;
        state.xRange = xRangeFromExtent(extent);
        setEvent('brush');
      });
    }).catch(error => renderAdapterError(root, error));
  }

  function renderVegaLiteScatter() {
    const root = clearSurface('cf-scatter');
    const metricBrushParam = { name: 'sidemanticMetricBrush', select: { type: 'interval', encodings: ['x', 'y'] } };
    const metricBrushValue = vegaLiteMetricSelectionValue();
    if (metricBrushValue) metricBrushParam.value = metricBrushValue;
    const vlSpec = {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
      width: 'container',
      height: 300,
      config: { selection: { interval: { fill: '#333', fillOpacity: 0.125, stroke: '#fff' } } },
      data: { values: chartRowsForVega(scatterChartData(), []) },
      params: [metricBrushParam],
      mark: { type: 'point', filled: true, opacity: 0.75, tooltip: true },
      encoding: {
        x: { field: scatterXField, type: 'quantitative', title: label(scatterXField) },
        y: { field: scatterYField, type: 'quantitative', title: label(scatterYField) },
        size: { field: sizeField, type: 'quantitative', legend: null },
        color: seriesField ? { field: seriesField, type: 'nominal', title: label(seriesField) } : { value: '#2E5EAA' },
        tooltip: tooltipFields([seriesField, scatterXField, scatterYField, sizeField])
      }
    };
    vegaEmbed(root, vlSpec, { actions: false }).then(result => {
      result.view.addSignalListener('sidemanticMetricBrush', (_name, value) => {
        const xExtent = intervalExtent(value, scatterXField);
        const yExtent = intervalExtent(value, scatterYField);
        if (!xExtent || !yExtent) {
          if (state.metricRange) {
            state.metricRange = null;
            state.metricBrushValue = null;
            setEvent('scatter:clear');
          }
          return;
        }
        activatePreagg('metricRange', scatterXField);
        state.metricBrushValue = value;
        state.metricRange = { x: numericExtent(xExtent), y: numericExtent(yExtent) };
        setEvent('scatter');
      });
    }).catch(error => renderAdapterError(root, error));
  }

  function renderPlotlyLine() {
    const root = clearSurface('cf-line');
    const data = lineChartData();
    const traces = d3.groups(data, row => row[seriesField]).map(([series, values]) => {
      const sorted = values.slice().sort((a, b) => xValues.indexOf(a[xField]) - xValues.indexOf(b[xField]));
      return {
        type: 'scatter',
        mode: 'lines+markers',
        name: String(series ?? label(yField)),
        x: sorted.map(row => row[xField]),
        y: sorted.map(row => metricNumber(row, yField)),
        line: { color: color(series) },
        marker: { color: color(series), size: 7 }
      };
    });
    Plotly.react(root, traces, {
      margin: { t: 18, r: 18, b: 58, l: 64 },
      xaxis: { title: label(xField) },
      yaxis: { title: label(yField) },
      dragmode: xRangeIsSemanticDimension() ? 'select' : false,
      showlegend: Boolean(seriesField),
      template: 'plotly_white'
    }, { responsive: true, displayModeBar: false }).then(() => {
      renderXRangeOverlay(root);
      if (!xRangeIsSemanticDimension()) return;
      root.on('plotly_selected', event => {
        const selectedValues = Array.from(new Set((event?.points || []).map(point => point.x)));
        if (!selectedValues.length) return;
        activatePreagg('xRange', xField);
        state.xBrushValue = null;
        state.xRange = xRangeFromExtent(d3.extent(selectedValues, comparableValue));
        setEvent('brush');
      });
      root.on('plotly_deselect', () => {
        state.xRange = null;
        state.xBrushValue = null;
        setEvent('brush:clear');
      });
    });
  }

  function renderPlotlyScatter() {
    const root = clearSurface('cf-scatter');
    const data = scatterChartData();
    const traces = d3.groups(data, row => row[seriesField]).map(([series, values]) => ({
      type: 'scatter',
      mode: 'markers',
      name: String(series ?? label(scatterYField)),
      x: values.map(row => Number(row[scatterXField])),
      y: values.map(row => Number(row[scatterYField])),
      text: values.map(row => String(row[seriesField] ?? '')),
      marker: {
        color: color(series),
        size: values.map(row => Math.max(6, Math.sqrt(Math.max(0, Number(row[sizeField]))) * 1.2)),
        opacity: 0.72,
        line: { color: '#fff', width: 1 }
      }
    }));
    Plotly.react(root, traces, {
      margin: { t: 18, r: 18, b: 58, l: 64 },
      xaxis: { title: label(scatterXField) },
      yaxis: { title: label(scatterYField) },
      dragmode: 'select',
      showlegend: Boolean(seriesField),
      template: 'plotly_white'
    }, { responsive: true, displayModeBar: false }).then(() => {
      root.on('plotly_selected', event => {
        const points = event?.points || [];
        if (!points.length) return;
        activatePreagg('metricRange', scatterXField);
        state.metricBrushValue = null;
        state.metricRange = {
          x: numericExtent(d3.extent(points, point => point.x)),
          y: numericExtent(d3.extent(points, point => point.y))
        };
        setEvent('scatter');
      });
      root.on('plotly_deselect', () => {
        state.metricRange = null;
        state.metricBrushValue = null;
        setEvent('scatter:clear');
      });
    });
  }

  function renderObservablePlotLine() {
    const root = clearSurface('cf-line');
    const Plot = window.__SIDEMANTIC_OBSERVABLE_PLOT__;
    if (!Plot) return renderAdapterLoading(root, 'Observable Plot');
    renderObservableLegend(root);
    const frame = observablePlotFrame(root);
    const data = chartRowsForObservable(lineChartData(), [xField]);
    const width = observablePlotWidth(root);
    const chart = Plot.plot({
      width,
      height: 330,
      marginTop: 18,
      marginRight: 18,
      marginBottom: 56,
      marginLeft: 64,
      grid: true,
      color: { legend: false, domain: seriesValues, range: seriesValues.map(series => color(series)) },
      x: { label: label(xField) },
      y: { label: label(yField) },
      marks: [
        Plot.ruleY([0]),
        Plot.lineY(data, {
          x: xField,
          y: yField,
          stroke: seriesField,
          tip: true
        }),
        Plot.dot(data, {
          x: xField,
          y: yField,
          fill: seriesField,
          r: 3,
          tip: true
        })
      ]
    });
    frame.append(chart);
    renderXRangeOverlay(frame);
    if (xRangeIsSemanticDimension()) renderObservableXBrushLayer(frame);
  }

  function renderObservablePlotScatter() {
    const root = clearSurface('cf-scatter');
    const Plot = window.__SIDEMANTIC_OBSERVABLE_PLOT__;
    if (!Plot) return renderAdapterLoading(root, 'Observable Plot');
    renderObservableLegend(root);
    const frame = observablePlotFrame(root);
    const data = scatterChartData();
    const scatterDomainRows = domainSourceRows();
    const width = observablePlotWidth(root);
    const chart = Plot.plot({
      width,
      height: 330,
      marginTop: 18,
      marginRight: 18,
      marginBottom: 56,
      marginLeft: 64,
      grid: true,
      color: { legend: false, domain: seriesValues, range: seriesValues.map(series => color(series)) },
      x: { label: label(scatterXField), domain: paddedDomain(scatterDomainRows.map(row => Number(row[scatterXField]))), nice: true },
      y: { label: label(scatterYField), domain: paddedDomain(scatterDomainRows.map(row => Number(row[scatterYField]))), nice: true },
      marks: [
        Plot.dot(data, {
          x: scatterXField,
          y: scatterYField,
          fill: seriesField,
          r: row => Math.max(3, Math.sqrt(Math.max(0, Number(row[sizeField]))) * 0.9),
          opacity: 0.72,
          tip: true
        })
      ]
    });
    frame.append(chart);
    renderObservableMetricBrushLayer(frame);
  }

  function renderBars() {
    barsRoot.innerHTML = '';
    for (const field of categoryFields) {
      renderBar(field);
    }
  }

  function renderBar(field) {
    const wrapper = document.createElement('div');
    wrapper.dataset.field = field;
    wrapper.innerHTML = `<div class="cf-bar-title">${escapeHtml(label(field))}</div><svg id="cf-bar-${safeId(field)}" viewBox="0 0 900 210" role="img"></svg>`;
    barsRoot.append(wrapper);
    const svg = d3.select(wrapper).select('svg');
    const data = liveViews?.bars?.[field] || rowsForView(field);
    const values = valuesFor(field);
    const totals = values.map(value => ({
      value,
      total: aggregateMetric(data.filter(row => row[field] === value), yField)
    }));
    const margin = { top: 8, right: 18, bottom: 48, left: 68 };
    const width = 900 - margin.left - margin.right;
    const height = 210 - margin.top - margin.bottom;
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
    const x = d3.scaleBand().domain(values).range([0, width]).padding(0.2);
    const y = d3.scaleLinear().domain([0, d3.max(totals, row => row.total) || 1]).nice().range([height, 0]);
    g.append('g').attr('transform', `translate(0,${height})`).call(d3.axisBottom(x)).selectAll('text')
      .attr('transform', 'rotate(-25)').attr('text-anchor', 'end');
    g.append('g').call(d3.axisLeft(y).ticks(4));
    g.selectAll('rect').data(totals).join('rect')
      .attr('class', 'cf-clickable')
      .attr('data-field', field)
      .attr('data-value', row => row.value)
      .attr('x', row => x(row.value))
      .attr('y', row => row.total == null ? height : y(row.total))
      .attr('width', x.bandwidth())
      .attr('height', row => row.total == null ? 0 : height - y(row.total))
      .attr('fill', row => field === seriesField ? color(row.value) : '#5B789D')
      .attr('opacity', row => !state.categories[field] || state.categories[field] === row.value ? 1 : 0.25)
      .attr('stroke', row => state.categories[field] === row.value ? '#1f2937' : 'transparent')
      .attr('stroke-width', row => state.categories[field] === row.value ? 2 : 0)
      .on('click', (_event, row) => {
        state.categories[field] = state.categories[field] === row.value ? null : row.value;
        if (state.categories[field] == null) delete state.categories[field];
        setEvent(`category:${field}:${row.value}`);
      });
  }

  function renderTable(data) {
    const table = d3.select('#cf-table');
    table.selectAll('*').remove();
    const tableData = liveViews?.table || data;
    const header = table.append('thead').append('tr');
    header.selectAll('th').data(columns).join('th').text(label);
    const body = table.append('tbody');
    body.selectAll('tr').data(tableData.slice(0, tableLimit)).join('tr')
      .selectAll('td')
      .data(row => columns.map(column => formatValue(row[column])))
      .join('td')
      .text(value => value);
  }

  function summaryText(count, totalGroups) {
    return `Showing ${count} of ${totalGroups} groups`;
  }

  function renderTabs() {
    tabsRoot.hidden = tabEntries.length <= 1;
    tabsRoot.innerHTML = tabEntries.map((tab, index) => `
      <button class="cf-tab" type="button" role="tab" aria-selected="${index === activeTabIndex}" data-tab-index="${index}">
        <span>${escapeHtml(tab.label || tab.id || `Tab ${index + 1}`)}</span>
        <small>${formatMetric(tabSourceRecordCount(tab), 'count')} records</small>
      </button>
    `).join('');
  }

  function renderRendererControls() {
    const options = chartRendererOptions();
    rendererControls.hidden = options.length <= 1;
    rendererControls.innerHTML = options.map(renderer => `
      <button class="cf-renderer-button" type="button" data-renderer="${escapeAttr(renderer)}" aria-pressed="${renderer === activeRenderer}" title="${escapeAttr(rendererLabel(renderer))}">
        ${escapeHtml(rendererLabel(renderer))}
      </button>
    `).join('');
  }

  function chartRendererOptions() {
    const options = spec.chart_renderer_options || spec.renderer_options || spec.sidemantic?.chart_renderer_options || payload.chart_renderer_options || ['vega-lite', 'plotly', 'observable-plot', 'd3'];
    const normalized = options.map(normalizeChartRenderer);
    return Array.from(new Set([activeRenderer, ...normalized])).filter(renderer => ['vega-lite', 'plotly', 'observable-plot', 'd3'].includes(renderer));
  }

  function changeRenderer(renderer) {
    const normalized = normalizeChartRenderer(renderer);
    if (!chartRendererOptions().includes(normalized)) return;
    tabRendererState[currentTabId()] = normalized;
    activeRenderer = normalized;
    state.lastEvent = `renderer:${normalized}`;
    render();
  }

  function rendererLabel(renderer) {
    return {
      'vega-lite': 'Vega-Lite',
      plotly: 'Plotly',
      'observable-plot': 'Observable Plot',
      d3: 'D3'
    }[renderer] || String(renderer);
  }

  function currentTab() {
    return tabEntries[activeTabIndex] || tabEntries[0];
  }

  function currentTabId() {
    return currentTab().id || String(activeTabIndex);
  }

  function interactionFieldList(kind) {
    const planned = interactionPlan?.[kind];
    if (planned && Array.isArray(planned.fields)) {
      return planned.fields
        .map(field => field?.alias || aliasField(field?.id || field?.semantic_ref || field))
        .filter(field => availableFieldAliases().has(field));
    }
    const interaction = spec.interactions?.[kind] || spec.sidemantic?.interactions?.[kind];
    if (!interaction || interaction === true) return null;
    const configuredFields = Array.isArray(interaction.fields) ? interaction.fields : [];
    if (!configuredFields.length) return [];
    return configuredFields
      .map(aliasField)
      .filter(field => availableFieldAliases().has(field));
  }

  function availableFieldAliases() {
    const planned = fieldPlan?.fields;
    if (Array.isArray(planned) && planned.length) {
      return new Set(planned.map(field => field?.alias).filter(Boolean));
    }
    return new Set([...(fields?.dimensions || []), ...(metrics || [])]);
  }

  function aliasField(field) {
    const text = String(field || '');
    return text.includes('.') ? text.split('.').pop() : text;
  }

  function normalizeChartRenderer(renderer) {
    const normalized = String(renderer || 'd3').toLowerCase().replace(/_/g, '-');
    const aliases = {
      vegalite: 'vega-lite',
      vl: 'vega-lite',
      observable: 'observable-plot',
      plot: 'observable-plot',
      crossfilter: 'd3',
      'sidemantic-crossfilter': 'd3'
    };
    const resolved = aliases[normalized] || normalized;
    return ['vega-lite', 'plotly', 'observable-plot', 'd3'].includes(resolved) ? resolved : 'd3';
  }

  function headlessApi() {
    return {
      get spec() { return spec; },
      get rows() { return rows; },
      get views() { return liveViews; },
      get fieldPlan() { return fieldPlan; },
      get interactionPlan() { return interactionPlan; },
      get freshnessPolicy() { return freshnessPolicy; },
      get freshness() { return liveViews?.freshness || liveViews?.diagnostics?.freshness || null; },
      get state() { return state; },
      get renderer() { return activeRenderer; },
      get rendererOptions() { return chartRendererOptions(); },
      get filters() { return requestFilters(); },
      setRenderer(renderer) {
        changeRenderer(renderer);
      },
      setCategory(field, value) {
        if (value == null) delete state.categories[field];
        else state.categories[field] = value;
        setEvent(`category:${field}:${value}`);
      },
      setXRange(min, max) {
        if (!xRangeIsSemanticDimension()) return;
        state.xRange = xRangeFromExtent([min, max]);
        state.xBrushValue = null;
        setEvent(state.xRange ? 'brush' : 'brush:clear');
      },
      setMetricRange(xMin, xMax, yMin, yMax) {
        state.metricRange = { x: [Number(xMin), Number(xMax)], y: [Number(yMin), Number(yMax)] };
        state.metricBrushValue = null;
        setEvent('scatter');
      },
      clear(key) {
        clearFilter(key);
      },
      reset() {
        state.xRange = null;
        state.xBrushValue = null;
        state.metricRange = null;
        state.metricBrushValue = null;
        state.categories = {};
        setEvent('reset');
      },
      async query(filters = requestFilters(), event = 'custom', active = null) {
        if (!liveEndpoint) return { rows: filteredRows(), views: liveViews };
        const response = await fetch(liveEndpoint, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ tab: currentTabId(), event, active, filters, interaction_preaggregations: interactionPreaggEnabled() })
        });
        if (!response.ok) throw new Error(`Crossfilter query failed: ${response.status}`);
        return response.json();
      }
    };
  }

  function countMetricForSpec(overrideSpec = null, candidateMetrics = null) {
    const sourceMetrics = candidateMetrics || (overrideSpec ? overrideSpec.fields?.metrics || overrideSpec.fields?.y || [] : metrics);
    return sourceMetrics.find(metric => metricAgg(metric, overrideSpec) === 'count');
  }

  function countMetric() {
    return countMetricForSpec();
  }

  function recordCount(data) {
    const metric = countMetric();
    if (!metric) return data.length;
    return aggregateMetric(data, metric) ?? data.length;
  }

  function tabSourceRecordCount(tab) {
    if (tab.source_record_count != null) return Number(tab.source_record_count);
    const tabSpec = tab.spec || tab;
    const tabMetrics = tabSpec.fields?.metrics || tabSpec.fields?.y || [];
    const metric = countMetricForSpec(tabSpec, tabMetrics);
    if (!metric) return tabSpec.data?.length || 0;
    return aggregateMetricForSpec(tabSpec, tabSpec.data || [], metric) ?? tabSpec.data?.length ?? 0;
  }

  function axisLabels(g, width, height, xLabel, yLabel) {
    g.append('text')
      .attr('x', width / 2)
      .attr('y', height + 50)
      .attr('text-anchor', 'middle')
      .attr('font-size', 12)
      .text(xLabel);
    g.append('text')
      .attr('transform', 'rotate(-90)')
      .attr('x', -height / 2)
      .attr('y', -48)
      .attr('text-anchor', 'middle')
      .attr('font-size', 12)
      .text(yLabel);
  }

  function formatMetric(value, kind) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return 'n/a';
    if (kind === 'count') return Math.round(numeric).toLocaleString();
    return numeric.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }

  function formatTimestamp(value) {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return String(value);
    return parsed.toLocaleString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      second: '2-digit'
    });
  }

  function formatValue(value) {
    if (typeof value === 'number') return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    return value == null ? '' : String(value);
  }

  function formatRange(range, kind) {
    return `${formatMetric(range[0], kind)} - ${formatMetric(range[1], kind)}`;
  }

  function safeId(value) {
    return String(value).replace(/[^a-z0-9]+/gi, '-').replace(/^-|-$/g, '').toLowerCase();
  }

  function paddedDomain(values) {
    const finite = values.filter(Number.isFinite);
    const [minValue, maxValue] = d3.extent(finite.length ? finite : [0, 1]);
    const span = maxValue - minValue || Math.max(1, Math.abs(maxValue || 1));
    const pad = span * 0.08;
    return [Math.max(0, minValue - pad), maxValue + pad];
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }

  function label(field) {
    return String(field)
      .split('.').pop()
      .replace(/__/g, ' ')
      .replace(/_/g, ' ')
      .replace(/\\b\\w/g, char => char.toUpperCase());
  }
})();
</script>
""".replace("__SPEC_JSON__", safe_json).replace(
        "__VENDOR_SCRIPTS__",
        inline_vendor_scripts("d3", "vega", "vega_lite", "vega_embed", "plotly", "observable_plot"),
    )


def _html_shell(title: str, body: str) -> str:
    safe_title = title.replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    main {{ width: min(960px, calc(100vw - 32px)); margin: 24px auto; }}
    h1 {{ font-size: 20px; font-weight: 650; margin: 0 0 16px; }}
    #chart {{ width: 100%; min-height: 420px; }}
    #interaction-status {{ margin-top: 12px; color: #3f4652; font-size: 13px; }}
  </style>
</head>
<body>
  <main>
    <h1>{safe_title}</h1>
    {body}
  </main>
</body>
</html>
"""
