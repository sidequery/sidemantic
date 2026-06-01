"""Test-only adapter that drives pure Rust from existing Python test bodies."""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import yaml

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.validation import MetricValidationError, ModelValidationError, QueryValidationError

ROOT = Path(__file__).resolve().parents[1]
RUST_MANIFEST = ROOT / "sidemantic-rs" / "Cargo.toml"
RUST_TARGET_DIR = Path("/tmp/sidemantic-rs-parity-target")
RUST_ADAPTER_BIN = RUST_TARGET_DIR / "debug" / "examples" / f"parity_adapter{'.exe' if os.name == 'nt' else ''}"


class RustSemanticLayerAdapter:
    """Minimal SemanticLayer-compatible test adapter backed by sidemantic-rs.

    This is intentionally test-only. It lets selected Python tests execute their
    original bodies against pure Rust load/compile behavior without introducing
    a product-level Rust-backed Python runtime path.
    """

    def __init__(
        self,
        connection: Any = "duckdb:///:memory:",
        dialect: str | None = None,
        auto_register: bool = False,
        use_preaggregations: bool = False,
        preagg_database: str | None = None,
        preagg_schema: str | None = None,
        init_sql: list[str] | None = None,
    ):
        self._graph = SemanticGraph()
        self.graph = RustSemanticGraphFacade(self)
        self.connection_string = connection if isinstance(connection, str) else "duckdb://custom"
        self.dialect = dialect or "duckdb"
        self.use_preaggregations = use_preaggregations
        self.preagg_database = preagg_database
        self.preagg_schema = preagg_schema
        self.init_sql = init_sql
        self._owns_conn = False
        self._conn = self._initial_connection(connection)
        self.auto_register = auto_register

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, value):
        self._conn = value

    def add_model(self, model: Model) -> None:
        existing = self._graph.models.get(model.name)
        if existing is not None:
            if existing is model or existing.model_dump() == model.model_dump():
                return
            existing_dump = existing.model_dump()
            new_dump = model.model_dump()
            if existing.auto_dimensions and model.auto_dimensions:
                existing_dump.pop("dimensions", None)
                new_dump.pop("dimensions", None)
                if existing_dump == new_dump:
                    return

        snapshot = dict(self._graph.models)
        self._graph.add_model(model)
        try:
            self._rust_validate()
        except Exception as exc:
            self._graph.models = snapshot
            if isinstance(exc, ModelValidationError):
                raise
            raise ModelValidationError(str(exc)) from exc

    def add_metric(self, measure: Metric) -> None:
        snapshot = dict(self._graph.metrics)
        self._graph.add_metric(measure)
        response = self._rust_request({"action": "validate", "models_yaml": self._models_yaml()})
        if response["status"] == "error":
            self._graph.metrics = snapshot
            raise MetricValidationError(response["error"])

    def compile(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        dialect: str | None = None,
        ungrouped: bool = False,
        parameters: dict[str, Any] | None = None,
        use_preaggregations: bool | None = None,
        skip_default_time_dimensions: bool = False,
        **_kwargs: Any,
    ) -> str:
        if _kwargs:
            unsupported = ", ".join(sorted(_kwargs))
            raise NotImplementedError(f"pure Rust test adapter does not support compile kwargs: {unsupported}")

        active_dialect = dialect or self.dialect
        if active_dialect not in {"duckdb", "bigquery"}:
            raise NotImplementedError(f"pure Rust test adapter does not support dialect '{active_dialect}' yet")
        if parameters:
            raise NotImplementedError("pure Rust test adapter does not support template parameters yet")
        effective_preaggregations = self.use_preaggregations if use_preaggregations is None else use_preaggregations
        if effective_preaggregations:
            raise NotImplementedError("pure Rust test adapter does not support pre-aggregation routing yet")

        response = self._rust_request(
            {
                "action": "compile",
                "models_yaml": self._models_yaml(),
                "metrics": metrics or [],
                "dimensions": dimensions or [],
                "filters": filters or [],
                "segments": segments or [],
                "order_by": order_by or [],
                "limit": limit,
                "offset": offset,
                "ungrouped": ungrouped,
                "skip_default_time_dimensions": skip_default_time_dimensions,
                "dialect": active_dialect,
            }
        )
        if response["status"] == "error":
            if "unsupported_source_uri_query" in response["error"]:
                raise ValueError(response["error"].replace("Rust SQL generation", "Python SQL generation"))
            raise QueryValidationError(response["error"])
        return response["sql"]

    def query(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        **kwargs: Any,
    ):
        if self._conn is None:
            raise NotImplementedError("pure Rust test adapter query execution requires layer.conn to be set")

        sql = self.compile(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            segments=segments,
            order_by=order_by,
            limit=limit,
            **kwargs,
        )
        return self._conn.execute(sql)

    def sql(self, sql: str, strict: bool = True, **_kwargs: Any):
        if _kwargs:
            unsupported = ", ".join(sorted(_kwargs))
            raise NotImplementedError(f"pure Rust test adapter does not support sql kwargs: {unsupported}")
        if not strict:
            raise NotImplementedError("pure Rust test adapter does not support non-strict SQL rewriting yet")
        if self._conn is None:
            raise NotImplementedError("pure Rust test adapter SQL rewriting requires layer.conn to be set")

        response = self._rust_request(
            {
                "action": "rewrite_sql",
                "models_yaml": self._models_yaml(),
                "sql": sql,
            }
        )
        if response["status"] == "error":
            raise ValueError(response["error"])
        return self._conn.execute(response["sql"])

    def explain(self, **_kwargs: Any):
        raise NotImplementedError("pure Rust test adapter does not support explain plans yet")

    def get_catalog_metadata(self, schema: str = "public") -> dict[str, Any]:
        response = self._rust_request(
            {
                "action": "catalog_metadata",
                "models_yaml": self._models_yaml(),
                "schema": schema,
            }
        )
        if response["status"] == "error":
            raise ValueError(response["error"])
        return response["catalog"]

    def get_model(self, name: str) -> Model:
        return self._graph.get_model(name)

    def list_models(self) -> list[str]:
        return list(self._graph.models.keys())

    def get_metric(self, name: str) -> Metric:
        return self._graph.get_metric(name)

    def list_metrics(self) -> list[str]:
        return list(self._graph.metrics.keys())

    def close(self) -> None:
        if self._owns_conn and self._conn is not None:
            self._conn.close()
            self._conn = None

    def _rust_validate(self) -> None:
        response = self._rust_request({"action": "validate", "models_yaml": self._models_yaml()})
        if response["status"] == "error":
            raise ModelValidationError(response["error"])

    def _initial_connection(self, connection: Any):
        if not isinstance(connection, str):
            return connection
        if not connection.startswith("duckdb://"):
            return None
        database = connection.removeprefix("duckdb://")
        if database in {"", "/:memory:", ":memory:"}:
            database = ":memory:"
        conn = duckdb.connect(database)
        for statement in self.init_sql or []:
            conn.execute(statement)
        self._owns_conn = True
        return conn

    def _rust_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _rust_request(payload)

    def _models_yaml(self) -> str:
        return yaml.safe_dump(
            {
                "models": [_model_to_rust_dict(model) for model in self.graph.models.values()],
                "metrics": [_metric_to_rust_dict(metric) for metric in self.graph.metrics.values()],
            },
            sort_keys=False,
        )

    def _rust_join_path(self, from_model: str, to_model: str) -> list[RustJoinPath]:
        response = self._rust_request(
            {
                "action": "join_path",
                "models_yaml": self._models_yaml(),
                "from_model": from_model,
                "to_model": to_model,
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        return [
            RustJoinPath(
                from_model=step["from_model"],
                to_model=step["to_model"],
                from_columns=step["from_columns"],
                to_columns=step["to_columns"],
                relationship=step["relationship"],
            )
            for step in response["path"]
        ]


class RustSemanticGraphDirectAdapter:
    """SemanticGraph-compatible test adapter that delegates graph behavior to Rust."""

    def __init__(self):
        self._models: dict[str, Model] = {}
        self._metrics: dict[str, Metric] = {}
        self.table_calculations: dict[str, object] = {}
        self.parameters: dict[str, object] = {}
        self._adjacency_dirty = True

    @property
    def models(self) -> dict[str, Model]:
        return self._models

    @property
    def metrics(self) -> dict[str, Metric]:
        return self._metrics

    def add_model(self, model: Model) -> None:
        response = _rust_request(
            {
                "action": "graph_add_model",
                "models_yaml": self._models_yaml(),
                "model_yaml": _single_model_yaml(model),
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        self._models[model.name] = model
        for metric in model.metrics:
            if metric.type != "time_comparison":
                continue
            metric_response = _rust_request(
                {
                    "action": "graph_get_metric",
                    "models_yaml": self._models_yaml(),
                    "name": metric.name,
                }
            )
            if metric_response["status"] == "error":
                _raise_graph_error(metric_response["error"])
            self._metrics[metric.name] = metric
        self._adjacency_dirty = True

    def add_metric(self, measure: Metric) -> None:
        response = _rust_request(
            {
                "action": "graph_add_metric",
                "models_yaml": self._models_yaml(),
                "metric_yaml": _single_metric_yaml(measure),
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        self._metrics[measure.name] = measure

    def add_table_calculation(self, calc) -> None:
        response = _rust_request(
            {
                "action": "graph_add_table_calculation",
                "models_yaml": self._models_yaml(),
                "table_calculations_json": self._table_calculations_json(),
                "table_calculation_json": _table_calculation_to_rust_dict(calc),
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        self.table_calculations[calc.name] = calc

    def get_model(self, name: str) -> Model:
        response = _rust_request(
            {
                "action": "graph_get_model",
                "models_yaml": self._models_yaml(),
                "name": name,
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        return self._models[name]

    def get_metric(self, name: str) -> Metric:
        response = _rust_request(
            {
                "action": "graph_get_metric",
                "models_yaml": self._models_yaml(),
                "name": name,
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        return self._metrics[name]

    def get_table_calculation(self, name: str):
        response = _rust_request(
            {
                "action": "graph_get_table_calculation",
                "models_yaml": self._models_yaml(),
                "table_calculations_json": self._table_calculations_json(),
                "name": name,
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        return self.table_calculations[name]

    def build_adjacency(self) -> None:
        response = _rust_request(
            {
                "action": "graph_build_adjacency",
                "models_yaml": self._models_yaml(),
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        self._adjacency_dirty = False

    def find_relationship_path(self, from_model: str, to_model: str) -> list[RustJoinPath]:
        response = _rust_request(
            {
                "action": "join_path",
                "models_yaml": self._models_yaml(),
                "from_model": from_model,
                "to_model": to_model,
            }
        )
        if response["status"] == "error":
            _raise_graph_error(response["error"])
        self._adjacency_dirty = False
        return [
            RustJoinPath(
                from_model=step["from_model"],
                to_model=step["to_model"],
                from_columns=step["from_columns"],
                to_columns=step["to_columns"],
                relationship=step["relationship"],
            )
            for step in response["path"]
        ]

    def _models_yaml(self) -> str:
        return _graph_yaml(self._models, self._metrics)

    def _table_calculations_json(self) -> list[dict[str, Any]]:
        return [_table_calculation_to_rust_dict(calc) for calc in self.table_calculations.values()]


@dataclass(frozen=True)
class RustJoinPath:
    from_model: str
    to_model: str
    from_columns: list[str]
    to_columns: list[str]
    relationship: str

    @property
    def from_entity(self) -> str:
        return self.from_columns[0] if self.from_columns else ""

    @property
    def to_entity(self) -> str:
        return self.to_columns[0] if self.to_columns else ""


class RustSemanticGraphFacade:
    def __init__(self, adapter: RustSemanticLayerAdapter):
        self._adapter = adapter

    @property
    def models(self):
        return self._adapter._graph.models

    @property
    def metrics(self):
        return self._adapter._graph.metrics

    def get_model(self, name: str) -> Model:
        return self._adapter._graph.get_model(name)

    def get_metric(self, name: str) -> Metric:
        return self._adapter._graph.get_metric(name)

    def find_relationship_path(self, from_model: str, to_model: str) -> list[RustJoinPath]:
        return self._adapter._rust_join_path(from_model, to_model)


class RustSQLGeneratorAdapter:
    """SQLGenerator-compatible test wrapper backed by the pure Rust adapter."""

    def __init__(
        self,
        graph,
        dialect: str = "duckdb",
        preagg_database: str | None = None,
        preagg_schema: str | None = None,
    ):
        self.graph = graph
        self.dialect = dialect
        self.preagg_database = preagg_database
        self.preagg_schema = preagg_schema

    def generate(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        parameters: dict[str, Any] | None = None,
        ungrouped: bool = False,
        use_preaggregations: bool = False,
        aliases: dict[str, str] | None = None,
        skip_default_time_dimensions: bool = False,
    ) -> str:
        if parameters:
            raise NotImplementedError("pure Rust test adapter does not support template parameters yet")
        if use_preaggregations:
            raise NotImplementedError("pure Rust test adapter does not support pre-aggregation routing yet")
        if aliases:
            raise NotImplementedError("pure Rust test adapter does not support custom aliases yet")

        response = _rust_request(
            {
                "action": "compile",
                "models_yaml": _graph_yaml_from_graph(self.graph),
                "metrics": metrics or [],
                "dimensions": dimensions or [],
                "filters": filters or [],
                "segments": segments or [],
                "order_by": order_by or [],
                "limit": limit,
                "offset": offset,
                "ungrouped": ungrouped,
                "skip_default_time_dimensions": skip_default_time_dimensions,
                "dialect": self.dialect,
            }
        )
        if response["status"] == "error":
            raise QueryValidationError(response["error"])
        return response["sql"]

    def generate_view(
        self,
        view_name: str,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
    ) -> str:
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", view_name):
            raise ValueError(f"Invalid view name: {view_name}")
        query_sql = self.generate(metrics, dimensions, filters, order_by=order_by, limit=limit)
        return f"CREATE VIEW {view_name} AS\n{query_sql}"


class RustQueryRewriterAdapter:
    """QueryRewriter-compatible test wrapper backed by the pure Rust adapter."""

    def __init__(self, graph: RustSemanticGraphFacade, dialect: str = "duckdb"):
        if not isinstance(graph, RustSemanticGraphFacade):
            raise TypeError("RustQueryRewriterAdapter requires RustSemanticGraphFacade")
        if dialect != "duckdb":
            raise NotImplementedError(f"pure Rust test adapter does not support dialect '{dialect}' yet")
        self._adapter = graph._adapter

    def rewrite(self, sql: str, strict: bool = True) -> str:
        stripped = sql.strip()
        upper = stripped.upper()

        if not strict:
            if "AGGREGATE(" in upper:
                return sql
            try:
                return self.rewrite(sql, strict=True)
            except ValueError:
                return sql

        if not upper.startswith("SELECT"):
            raise ValueError("Only SELECT queries are supported")
        if upper == "SELECT *":
            raise ValueError("SELECT * requires a FROM clause")
        if upper.startswith("SELECT FROM WHERE"):
            raise ValueError("Failed to parse SQL")
        if upper.startswith("SELECT FROM"):
            raise ValueError("Query must select at least one metric or dimension")

        response = self._adapter._rust_request(
            {
                "action": "rewrite_sql",
                "models_yaml": self._adapter._models_yaml(),
                "sql": sql,
            }
        )
        if response["status"] == "error":
            error = response["error"]
            if "parse" in error.lower():
                raise ValueError(f"Failed to parse SQL: {error}") from None
            raise ValueError(error)
        return _python_style_rewrite_sql(response["sql"])


def rust_build_symmetric_aggregate_sql(
    measure_expr: str,
    primary_key: str,
    agg_type: str,
    model_alias: str | None = None,
    dialect: str = "duckdb",
) -> str:
    response = _rust_request(
        {
            "action": "symmetric_aggregate_sql",
            "measure_expr": measure_expr,
            "primary_key": primary_key,
            "agg_type": agg_type,
            "model_alias": model_alias,
            "dialect": dialect,
        }
    )
    if response["status"] == "error":
        raise ValueError(response["error"])
    return response["sql"]


def rust_needs_symmetric_aggregate(relationship: str, is_base_model: bool) -> bool:
    response = _rust_request(
        {
            "action": "needs_symmetric_aggregate",
            "relationship": relationship,
            "is_base_model": is_base_model,
        }
    )
    if response["status"] == "error":
        raise ValueError(response["error"])
    return bool(response["value"])


def _model_to_rust_dict(model: Model) -> dict[str, Any]:
    return _drop_none(
        {
            "name": model.name,
            "table": model.table,
            "sql": model.sql,
            "source_uri": model.source_uri,
            "extends": model.extends,
            "primary_key": model.primary_key,
            "primary_key_columns": model.primary_key_columns,
            "unique_keys": model.unique_keys,
            "description": model.description,
            "metadata": model.metadata,
            "meta": model.meta,
            "default_time_dimension": model.default_time_dimension,
            "default_grain": model.default_grain,
            "dimensions": [_dimension_to_rust_dict(dimension) for dimension in model.dimensions],
            "metrics": [_metric_to_rust_dict(metric) for metric in model.metrics],
            "relationships": [_relationship_to_rust_dict(relationship) for relationship in model.relationships],
            "segments": [_segment_to_rust_dict(segment) for segment in model.segments],
            "pre_aggregations": [_pre_aggregation_to_rust_dict(preagg) for preagg in model.pre_aggregations],
        }
    )


def _dimension_to_rust_dict(dimension) -> dict[str, Any]:
    return _drop_none(
        {
            "name": dimension.name,
            "type": dimension.type,
            "sql": dimension.sql,
            "granularity": dimension.granularity,
            "supported_granularities": dimension.supported_granularities,
            "description": dimension.description,
            "label": dimension.label,
            "metadata": dimension.metadata,
            "meta": dimension.meta,
            "format": dimension.format,
            "value_format_name": dimension.value_format_name,
            "parent": dimension.parent,
            "window": dimension.window,
            "public": dimension.public,
        }
    )


def _metric_to_rust_dict(metric: Metric) -> dict[str, Any]:
    return _drop_none(
        {
            "name": metric.name,
            "type": metric.type,
            "agg": metric.agg,
            "sql": metric.sql,
            "numerator": metric.numerator,
            "denominator": metric.denominator,
            "offset_window": metric.offset_window,
            "base_metric": metric.base_metric,
            "window": metric.window,
            "grain_to_date": metric.grain_to_date,
            "window_expression": metric.window_expression,
            "window_frame": metric.window_frame,
            "window_order": metric.window_order,
            "comparison_type": metric.comparison_type,
            "time_offset": metric.time_offset,
            "calculation": metric.calculation,
            "entity": metric.entity,
            "base_event": metric.base_event,
            "conversion_event": metric.conversion_event,
            "conversion_window": metric.conversion_window,
            "steps": metric.steps,
            "cohort_event": metric.cohort_event,
            "activity_event": metric.activity_event,
            "periods": metric.periods,
            "retention_granularity": metric.retention_granularity,
            "inner_metrics": metric.inner_metrics,
            "entity_dimensions": metric.entity_dimensions,
            "having": metric.having,
            "filters": metric.filters,
            "fill_nulls_with": metric.fill_nulls_with,
            "description": metric.description,
            "label": metric.label,
            "metadata": metric.metadata,
            "meta": metric.meta,
            "format": metric.format,
            "value_format_name": metric.value_format_name,
            "drill_fields": metric.drill_fields,
            "non_additive_dimension": metric.non_additive_dimension,
            "public": metric.public,
        }
    )


def _relationship_to_rust_dict(relationship) -> dict[str, Any]:
    payload = {
        "name": relationship.name,
        "type": relationship.type,
        "through": getattr(relationship, "through", None),
        "through_foreign_key": getattr(relationship, "through_foreign_key", None),
        "through_foreign_key_columns": getattr(relationship, "through_foreign_key_columns", None),
        "related_foreign_key": getattr(relationship, "related_foreign_key", None),
        "related_foreign_key_columns": getattr(relationship, "related_foreign_key_columns", None),
        "sql": getattr(relationship, "sql", None),
        "metadata": relationship.metadata,
    }
    if isinstance(relationship.foreign_key, list):
        payload["foreign_key_columns"] = relationship.foreign_key
    else:
        payload["foreign_key"] = relationship.foreign_key
    if isinstance(relationship.primary_key, list):
        payload["primary_key_columns"] = relationship.primary_key
    else:
        payload["primary_key"] = relationship.primary_key
    return _drop_none(payload)


def _segment_to_rust_dict(segment) -> dict[str, Any]:
    return _drop_none(
        {
            "name": segment.name,
            "sql": segment.sql,
            "description": segment.description,
            "public": segment.public,
        }
    )


def _pre_aggregation_to_rust_dict(preagg) -> dict[str, Any]:
    return preagg.model_dump(mode="json", exclude_none=True)


def _table_calculation_to_rust_dict(calc) -> dict[str, Any]:
    return _drop_none(
        {
            "name": calc.name,
            "type": calc.type,
            "description": calc.description,
            "expression": calc.expression,
            "field": calc.field,
            "partition_by": calc.partition_by,
            "order_by": calc.order_by,
            "window_size": calc.window_size,
        }
    )


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _python_style_rewrite_sql(sql: str) -> str:
    sql = re.sub(r"\s+(LIMIT\s+\d+\b)", r"\n\1", sql, flags=re.IGNORECASE)
    return re.sub(r"\s+(OFFSET\s+\d+\b)", r"\n\1", sql, flags=re.IGNORECASE)


def _single_model_yaml(model: Model) -> str:
    return _graph_yaml({model.name: model}, {})


def _single_metric_yaml(metric: Metric) -> str:
    return _graph_yaml({}, {metric.name: metric})


def _graph_yaml(models: dict[str, Model], metrics: dict[str, Metric]) -> str:
    return yaml.safe_dump(
        {
            "models": [_model_to_rust_dict(model) for model in models.values()],
            "metrics": [_metric_to_rust_dict(metric) for metric in metrics.values()],
        },
        sort_keys=False,
    )


def _graph_yaml_from_graph(graph) -> str:
    return _graph_yaml(graph.models, graph.metrics)


def _rust_request(payload: dict[str, Any]) -> dict[str, Any]:
    _ensure_rust_adapter_binary()
    result = subprocess.run(
        [str(RUST_ADAPTER_BIN)],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)
    return json.loads(result.stdout)


def _raise_graph_error(error: str) -> None:
    normalized = _normalize_graph_error(error)
    if (
        re.search(r"^Model .+ not found$", normalized)
        or re.search(r"^Measure .+ not found$", normalized)
        or re.search(r"^Table calculation .+ not found$", normalized)
    ):
        raise KeyError(normalized)
    raise ValueError(normalized)


def _normalize_graph_error(error: str) -> str:
    message = error.removeprefix("Validation error: ")

    if match := re.search(r"Model not found: '([^']+)'", message):
        return f"Model {match.group(1)} not found"
    if match := re.search(r"Model '([^']+)' already exists", message):
        return f"Model {match.group(1)} already exists"
    if match := re.search(r"Metric not found: '([^']+)'", message):
        return f"Measure {match.group(1)} not found"
    if match := re.search(r"Measure not found: '([^']+)'", message):
        return f"Measure {match.group(1)} not found"
    if match := re.search(r"Table calculation '([^']+)' already exists", message):
        return f"Table calculation {match.group(1)} already exists"
    if match := re.search(r"Table calculation '([^']+)' not found", message):
        return f"Table calculation {match.group(1)} not found"

    return message.replace("'", "")


def _ensure_rust_adapter_binary() -> None:
    if RUST_ADAPTER_BIN.exists() and not _rust_sources_newer_than_binary():
        return
    result = subprocess.run(
        [
            "cargo",
            "build",
            "--quiet",
            "--manifest-path",
            str(RUST_MANIFEST),
            "--example",
            "parity_adapter",
        ],
        text=True,
        capture_output=True,
        cwd=ROOT,
        env={**os.environ, "CARGO_TARGET_DIR": str(RUST_TARGET_DIR)},
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)


def _rust_sources_newer_than_binary() -> bool:
    binary_mtime = RUST_ADAPTER_BIN.stat().st_mtime
    rust_root = ROOT / "sidemantic-rs"
    candidates = [RUST_MANIFEST, *(rust_root / "src").rglob("*.rs"), *(rust_root / "examples").rglob("*.rs")]
    return any(path.stat().st_mtime > binary_mtime for path in candidates)
