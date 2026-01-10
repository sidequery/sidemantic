"""Sidemantic anywidget for interactive metrics exploration."""

from __future__ import annotations

import io
import pathlib
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import anywidget
import traitlets

if TYPE_CHECKING:
    from sidemantic import SemanticLayer
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator


def _table_to_ipc(table, *, decimal_mode: str = "float") -> str:
    """Serialize Arrow table to IPC format (base64 for widget transport).

    Args:
        table: PyArrow table
        decimal_mode: "float" to cast decimals to float64, "string" to preserve precision as strings
    """
    import base64

    import pyarrow as pa
    import pyarrow.compute as pc

    if any(pa.types.is_decimal(field.type) for field in table.schema):
        arrays = []
        fields = []
        for field in table.schema:
            column = table[field.name]
            if pa.types.is_decimal(field.type):
                if decimal_mode == "string":
                    cast_type = pa.string()
                else:
                    cast_type = pa.float64()
                arrays.append(pc.cast(column, cast_type))
                fields.append(pa.field(field.name, cast_type))
            else:
                arrays.append(column)
                fields.append(field)
        table = pa.table(arrays, schema=pa.schema(fields))

    sink = io.BytesIO()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return base64.b64encode(sink.getvalue()).decode("ascii")


class MetricsExplorer(anywidget.AnyWidget):
    """Interactive metrics explorer widget.

    Works in two modes:
    1. Raw DataFrame mode - pass any dataframe, auto-infers dimensions and metrics
    2. Semantic Model mode - use a sidemantic SemanticLayer with defined metrics/dimensions

    Examples:
        # Mode 1: Raw DataFrame
        import polars as pl
        from sidemantic.widget import MetricsExplorer

        df = pl.read_parquet("sales.parquet")
        widget = MetricsExplorer(df)

        # Mode 2: Semantic Model
        from sidemantic import SemanticLayer

        layer = SemanticLayer.from_yaml("models.yaml")
        widget = MetricsExplorer(
            layer=layer,
            metrics=["orders.revenue", "orders.order_count"],
            dimensions=["orders.region", "orders.category"],
        )
    """

    _esm = pathlib.Path(__file__).parent / "static" / "widget.js"
    _css = pathlib.Path(__file__).parent / "static" / "widget.css"

    # Configuration (Python → JS)
    config = traitlets.Dict({}).tag(sync=True)
    metrics_config = traitlets.List([]).tag(sync=True)
    dimensions_config = traitlets.List([]).tag(sync=True)

    # UI State (bidirectional)
    filters = traitlets.Dict({}).tag(sync=True)  # {dimension_key: [values]}
    date_range = traitlets.List([]).tag(sync=True)  # [start_iso, end_iso]
    selected_metric = traitlets.Unicode("").tag(sync=True)
    comparison_mode = traitlets.Unicode("wow").tag(sync=True)
    brush_selection = traitlets.List([]).tag(sync=True)  # [start_iso, end_iso] or []
    time_grain = traitlets.Unicode("").tag(sync=True)
    time_grain_options = traitlets.List([]).tag(sync=True)
    active_dimension = traitlets.Unicode("").tag(sync=True)

    # Data (Python → JS, as Arrow IPC bytes)
    metric_series_data = traitlets.Unicode("").tag(sync=True)
    dimension_data = traitlets.Dict({}).tag(sync=True)  # {dim_key: base64 arrow ipc}
    metric_totals = traitlets.Dict({}).tag(sync=True)

    # Status
    status = traitlets.Unicode("loading").tag(sync=True)
    error = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        data: Any = None,
        *,
        layer: SemanticLayer | None = None,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        time_dimension: str | None = None,
        max_dimension_cardinality: int | None = None,
        auto_preaggregations: bool = False,
        auto_preagg_min_count: int = 3,
        auto_preagg_min_score: float = 0.2,
        auto_preagg_max: int = 5,
        **kwargs,
    ):
        """Initialize MetricsExplorer widget.

        Args:
            data: DataFrame-like object OR SemanticLayer instance
            layer: Existing SemanticLayer instance (deprecated, use data)
            metrics: List of metric references (e.g., ["orders.revenue"])
            dimensions: List of dimension references (e.g., ["orders.region"])
            time_dimension: Override the time dimension for sparklines
            max_dimension_cardinality: Skip dimensions with cardinality above this
            auto_preaggregations: If True, auto-materialize preaggregations from widget query intents
            auto_preagg_min_count: Min query count before auto-materializing a preaggregation
            auto_preagg_min_score: Min benefit score required to auto-materialize
            auto_preagg_max: Max auto-materialized preaggregations per widget instance
        """
        super().__init__(**kwargs)

        # Detect if data is a SemanticLayer
        from sidemantic import SemanticLayer as SemanticLayerClass

        if isinstance(data, SemanticLayerClass):
            layer = data
            data = None

        self._layer: SemanticLayer | None = None
        self._graph: SemanticGraph | None = None
        self._conn = None
        self._generator: SQLGenerator | None = None
        self._use_preaggregations: bool = False
        self._table_name: str = "widget_data"
        self._model_name: str = "widget_data"
        self._time_dimension: str | None = time_dimension
        self._metrics: list[str] = metrics or []
        self._dimensions: list[str] = dimensions or []
        self._pending_refresh: str | None = None  # Track pending refresh type
        self._last_active_dimension: str | None = None
        self._auto_preaggregations = auto_preaggregations
        self._auto_preagg_min_count = auto_preagg_min_count
        self._auto_preagg_min_score = auto_preagg_min_score
        self._auto_preagg_max = auto_preagg_max
        self._auto_preagg_materialized: set[str] = set()
        self._auto_preagg_recommender = None
        self._metric_error = ""
        self._dimension_error = ""

        if layer is not None:
            # Mode 2: Use existing SemanticLayer
            self._layer = layer
            self._graph = layer.graph
            self._conn = layer.adapter.raw_connection
            self._use_preaggregations = getattr(layer, "use_preaggregations", False)
            self._setup_from_layer()
        elif data is not None:
            # Mode 1: Raw DataFrame - auto-generate model
            self._setup_from_dataframe(data, max_dimension_cardinality)
        else:
            raise ValueError("Either 'data' or 'layer' must be provided")

        # Create SQLGenerator once (reused for all queries)
        from sidemantic.sql.generator import SQLGenerator

        preagg_database = None
        preagg_schema = None
        if self._layer is not None:
            preagg_database = getattr(self._layer, "preagg_database", None)
            preagg_schema = getattr(self._layer, "preagg_schema", None)

        self._generator = SQLGenerator(
            self._graph,
            dialect="duckdb",
            preagg_database=preagg_database,
            preagg_schema=preagg_schema,
        )

        # Set up observers with specific handlers
        self.observe(self._on_filters_change, names=["filters"])
        self.observe(self._on_date_range_change, names=["date_range"])
        self.observe(self._on_brush_change, names=["brush_selection"])
        self.observe(self._on_metric_change, names=["selected_metric"])
        self.observe(self._on_comparison_change, names=["comparison_mode"])
        self.observe(self._on_time_grain_change, names=["time_grain"])
        self.observe(self._on_active_dimension_change, names=["active_dimension"])

        if self._auto_preaggregations and not self._use_preaggregations:
            self._use_preaggregations = True

        if self._auto_preaggregations:
            from sidemantic.core.preagg_recommender import PreAggregationRecommender

            self._auto_preagg_recommender = PreAggregationRecommender(
                min_query_count=self._auto_preagg_min_count,
                min_benefit_score=self._auto_preagg_min_score,
            )

        # Initial data load
        self._refresh_all()

    def _setup_from_layer(self):
        """Configure widget from existing SemanticLayer."""
        # Find first model to use as default
        model_names = list(self._graph.models.keys())
        if not model_names:
            raise ValueError("SemanticLayer has no models")

        self._model_name = model_names[0]
        model = self._graph.get_model(self._model_name)

        # Set table name from model for date range queries
        self._table_name = model.table

        # Auto-detect time dimension from model if not provided
        if self._time_dimension is None:
            # First try model's default_time_dimension
            if model.default_time_dimension:
                self._time_dimension = model.default_time_dimension
            else:
                # Fall back to first time-type dimension
                time_dims = [d for d in (model.dimensions or []) if d.type == "time"]
                if time_dims:
                    self._time_dimension = time_dims[0].name

        # If no metrics/dimensions specified, use all from model
        if not self._metrics:
            self._metrics = [f"{self._model_name}.{m.name}" for m in (model.metrics or [])]

        if not self._dimensions:
            # For leaderboards, only include categorical and boolean dimensions (not time or numeric)
            self._dimensions = [
                f"{self._model_name}.{d.name}" for d in (model.dimensions or []) if d.type in ("categorical", "boolean")
            ]

        self._build_config()

    def _setup_from_dataframe(self, data: Any, max_cardinality: int | None):
        """Configure widget from raw DataFrame."""
        import duckdb

        from sidemantic.widget._auto_model import build_auto_model, compute_cardinality
        from sidemantic.widget._data_registry import get_schema, register_data

        # Create in-memory DuckDB connection
        self._conn = duckdb.connect(":memory:")

        # Register data
        register_data(data, self._conn, self._table_name)

        # Get schema
        schema = get_schema(data)

        # Compute cardinality if threshold is set
        cardinality_map = None
        if max_cardinality is not None:
            columns = [field.name for field in schema]
            cardinality_map = compute_cardinality(self._conn, self._table_name, columns)

        # Build auto model
        self._graph, detected_time_dim = build_auto_model(
            schema,
            table_name=self._table_name,
            max_dimension_cardinality=max_cardinality,
            cardinality_map=cardinality_map,
        )

        self._model_name = self._table_name
        model = self._graph.get_model(self._model_name)

        # Set time dimension from auto-detection (if not already set)
        if self._time_dimension is None:
            self._time_dimension = detected_time_dim

        # Auto-select metrics and dimensions
        if not self._metrics:
            self._metrics = [f"{self._model_name}.{m.name}" for m in (model.metrics or [])]

        if not self._dimensions:
            # For leaderboards, only include categorical and boolean dimensions (not time or numeric)
            self._dimensions = [
                f"{self._model_name}.{d.name}" for d in (model.dimensions or []) if d.type in ("categorical", "boolean")
            ]

        self._build_config()

    def _build_config(self):
        """Build configuration for JS side."""
        model = self._graph.get_model(self._model_name)

        # Build metrics config
        metrics_config = []
        for metric_ref in self._metrics:
            metric_name = metric_ref.split(".")[-1]
            metric = model.get_metric(metric_name) if model else None
            metrics_config.append(
                {
                    "key": metric_name,
                    "ref": metric_ref,
                    "label": metric_name.replace("_", " ").title() if metric else metric_name,
                    "format": "number",  # Could infer from metric definition
                }
            )

        # Build dimensions config
        dimensions_config = []
        for dim_ref in self._dimensions:
            dim_name = dim_ref.split(".")[-1]
            dim = model.get_dimension(dim_name) if model else None
            dimensions_config.append(
                {
                    "key": dim_name,
                    "ref": dim_ref,
                    "label": dim_name.replace("_", " ").title() if dim else dim_name,
                }
            )

        self.metrics_config = metrics_config
        self.dimensions_config = dimensions_config

        # Set default selected metric
        if metrics_config and not self.selected_metric:
            self.selected_metric = metrics_config[0]["key"]

        # Set config
        self.config = {
            "model_name": self._model_name,
            "time_dimension": self._time_dimension,
            "time_dimension_ref": f"{self._model_name}.{self._time_dimension}" if self._time_dimension else None,
        }

        # Configure time grain options
        if self._time_dimension:
            time_dim = model.get_dimension(self._time_dimension) if model else None
            supported = time_dim.supported_granularities if time_dim else None
            time_grains = supported or ["day", "week", "month", "quarter", "year"]
            default_grain = model.default_grain or (time_dim.granularity if time_dim else None) or "day"
            if default_grain not in time_grains:
                time_grains = [default_grain] + [g for g in time_grains if g != default_grain]
            self.time_grain_options = time_grains
            if not self.time_grain:
                self.time_grain = default_grain

        # Compute date range
        self._compute_date_range()

    def _compute_date_range(self):
        """Compute available date range from data."""
        if not self._time_dimension:
            return

        try:
            query = f'SELECT MIN("{self._time_dimension}") as min_date, MAX("{self._time_dimension}") as max_date FROM "{self._table_name}"'
            result = self._execute(query).fetchone()
            if result and result[0] and result[1]:
                min_date = result[0]
                max_date = result[1]

                min_date = self._stringify_time_value(min_date)
                max_date = self._stringify_time_value(max_date)

                self.date_range = [min_date, max_date]
        except Exception as e:
            # Fall back to a pre-aggregation table when the base table doesn't exist.
            if self._use_preaggregations:
                model = self._graph.get_model(self._model_name)
                if model and model.pre_aggregations:
                    preagg = next(
                        (p for p in model.pre_aggregations if p.time_dimension and p.granularity),
                        None,
                    )
                    if preagg:
                        preagg_db = None
                        preagg_schema = None
                        if self._layer is not None:
                            preagg_db = getattr(self._layer, "preagg_database", None)
                            preagg_schema = getattr(self._layer, "preagg_schema", None)
                        table_name = preagg.get_table_name(
                            model.name,
                            database=preagg_db,
                            schema=preagg_schema,
                        )
                        time_col = f"{preagg.time_dimension}_{preagg.granularity}"
                        try:
                            result = self._execute(
                                f'SELECT MIN("{time_col}") as min_date, MAX("{time_col}") as max_date FROM {table_name}'
                            ).fetchone()
                            if result and result[0] and result[1]:
                                min_date = self._stringify_time_value(result[0])
                                max_date = self._stringify_time_value(result[1])
                                self.date_range = [min_date, max_date]
                                return
                        except Exception:
                            pass
            self._metric_error = f"Failed to compute date range: {e}"

    def _on_filters_change(self, change):
        """Handle dimension filter changes - refresh all."""
        if self.active_dimension:
            self._refresh_dimensions()
            return
        self._refresh_all()

    def _on_date_range_change(self, change):
        """Handle date range changes - refresh all."""
        self._refresh_all()

    def _on_brush_change(self, change):
        """Handle brush selection changes - refresh all data to apply date filter."""
        self._refresh_all()

    def _on_metric_change(self, change):
        """Handle selected metric change - refresh dimension leaderboards."""
        self._refresh_dimensions()

    def _on_comparison_change(self, change):
        """Handle comparison mode change."""
        self._refresh_metrics()

    def _on_time_grain_change(self, change):
        """Handle time grain change - refresh metric series."""
        self._refresh_metrics()

    def _on_active_dimension_change(self, change):
        """Handle active dimension changes."""
        if change["new"]:
            self._last_active_dimension = change["new"]
            return
        if change["old"]:
            self._last_active_dimension = change["old"]
            self._refresh_all()

    def _build_filters(self, exclude_dimension: str | None = None) -> list[str]:
        """Build filter expressions for sidemantic query."""
        filter_exprs = []

        # Date range filter
        if self.brush_selection and len(self.brush_selection) == 2:
            start, end = self.brush_selection
            filter_exprs.append(self._format_time_range_filter(start, end))
        elif self.date_range and len(self.date_range) == 2:
            start, end = self.date_range
            filter_exprs.append(self._format_time_range_filter(start, end))

        # Dimension filters
        for dim_key, values in self.filters.items():
            if exclude_dimension and dim_key == exclude_dimension:
                continue
            if values:
                if len(values) == 1:
                    value = self._escape_sql_literal(str(values[0]))
                    filter_exprs.append(f"{self._model_name}.{dim_key} = '{value}'")
                else:
                    clauses = " OR ".join(
                        f"{self._model_name}.{dim_key} = '{self._escape_sql_literal(str(v))}'" for v in values
                    )
                    filter_exprs.append(f"({clauses})")

        return filter_exprs

    def _record_query_intent(
        self,
        metrics: list[str],
        dimensions: list[str],
        granularity: str | None,
        filters: list[str] | None = None,
    ) -> None:
        if not self._auto_preaggregations or not self._auto_preagg_recommender:
            return
        if not self._graph:
            return

        model = self._graph.get_model(self._model_name)
        if not model:
            return

        metric_names: list[str] = []
        count_metric_name: str | None = None
        for metric_ref in metrics:
            metric_name = metric_ref.split(".", 1)[1] if "." in metric_ref else metric_ref
            metric = model.get_metric(metric_name)
            if not metric or not metric.agg:
                continue
            agg = metric.agg.lower()
            if agg not in ("sum", "count", "min", "max", "avg"):
                continue
            metric_names.append(metric_name)
            if agg == "count":
                count_metric_name = metric_name

        if not metric_names:
            return

        # Ensure AVG metrics can be derived by including a count measure.
        if any(model.get_metric(name).agg == "avg" for name in metric_names):
            if not count_metric_name:
                count_metric_name = self._find_count_metric_name(model, metric_names)
            if count_metric_name and count_metric_name not in metric_names:
                metric_names.append(count_metric_name)

        dim_names = []
        for dim_ref in dimensions:
            dim_name = dim_ref.split(".", 1)[1] if "." in dim_ref else dim_ref
            if "__" in dim_name:
                dim_name = dim_name.rsplit("__", 1)[0]
            if self._time_dimension and dim_name == self._time_dimension:
                continue
            dim_names.append(dim_name)

        if filters:
            filter_dims = self._extract_filter_columns(filters)
            for dim_name in filter_dims:
                if self._time_dimension and dim_name == self._time_dimension:
                    continue
                if dim_name not in dim_names:
                    dim_names.append(dim_name)

        from sidemantic.core.preagg_recommender import QueryPattern

        pattern = QueryPattern(
            model=self._model_name,
            metrics=frozenset(metric_names),
            dimensions=frozenset(dim_names),
            granularities=frozenset([granularity] if granularity else []),
        )
        self._auto_preagg_recommender.patterns[pattern] += 1
        count = self._auto_preagg_recommender.patterns[pattern]

        if count < self._auto_preagg_min_count:
            return
        if len(self._auto_preagg_materialized) >= self._auto_preagg_max:
            return

        benefit_score = self._auto_preagg_recommender._calculate_benefit_score(pattern, count)
        if benefit_score < self._auto_preagg_min_score:
            return

        self._materialize_preagg(model, metric_names, dim_names, granularity)

    def _materialize_preagg(
        self,
        model,
        metric_names: list[str],
        dim_names: list[str],
        granularity: str | None,
    ) -> None:
        from sidemantic.core.pre_aggregation import PreAggregation

        time_dimension = self._time_dimension
        if time_dimension and not granularity:
            granularity = "day"

        name_parts = []
        if granularity:
            name_parts.append(granularity)
        if dim_names:
            name_parts.append("_".join(dim_names[:2]))
        if len(metric_names) == 1:
            name_parts.append(metric_names[0])
        else:
            name_parts.append(f"{len(metric_names)}metrics")

        name = "auto_" + "_".join(name_parts) if name_parts else "auto_rollup"
        if name in self._auto_preagg_materialized:
            return
        if any(preagg.name == name for preagg in (model.pre_aggregations or [])):
            return

        preagg = PreAggregation(
            name=name,
            type="rollup",
            measures=metric_names,
            dimensions=dim_names,
            time_dimension=time_dimension,
            granularity=granularity,
        )

        try:
            preagg_db = self._layer.preagg_database if self._layer else None
            preagg_schema = self._layer.preagg_schema if self._layer else None

            if preagg_schema:
                try:
                    self._execute(f"CREATE SCHEMA IF NOT EXISTS {preagg_schema}")
                except Exception:
                    pass

            table_name = preagg.get_table_name(model.name, database=preagg_db, schema=preagg_schema)
            source_sql = preagg.generate_materialization_sql(model)
            self._execute(f"CREATE OR REPLACE TABLE {table_name} AS {source_sql}")
        except Exception as e:
            self._metric_error = f"Auto pre-aggregation failed: {e}"
            return

        model.pre_aggregations.append(preagg)
        self._auto_preagg_materialized.add(name)

    def _stringify_time_value(self, value) -> str:
        if isinstance(value, datetime):
            return value.isoformat(sep=" ")
        if isinstance(value, date):
            return value.isoformat()
        return str(value)

    def _is_date_only(self, value: str) -> bool:
        return len(value) == 10 and value[4] == "-" and value[7] == "-"

    def _format_time_range_filter(self, start, end) -> str:
        start_str = self._stringify_time_value(start)
        end_str = self._stringify_time_value(end)

        start_literal = self._escape_sql_literal(start_str)
        end_literal = self._escape_sql_literal(end_str)

        if self._is_date_only(end_str):
            end_exclusive = (date.fromisoformat(end_str) + timedelta(days=1)).isoformat()
            end_literal = self._escape_sql_literal(end_exclusive)
            return (
                f"{self._model_name}.{self._time_dimension} >= '{start_literal}' AND "
                f"{self._model_name}.{self._time_dimension} < '{end_literal}'"
            )

        return (
            f"{self._model_name}.{self._time_dimension} >= '{start_literal}' AND "
            f"{self._model_name}.{self._time_dimension} <= '{end_literal}'"
        )

    def _escape_sql_literal(self, value: str) -> str:
        return value.replace("'", "''")

    def _find_count_metric_name(self, model, metric_names: list[str]) -> str | None:
        """Pick a count metric to support AVG rollups."""
        for name in metric_names:
            if name.startswith("avg_"):
                candidate = f"count_{name[4:]}"
                if model.get_metric(candidate):
                    return candidate
            if "_avg" in name:
                candidate = name.replace("_avg", "_count")
                if model.get_metric(candidate):
                    return candidate
        if model.get_metric("count"):
            return "count"
        for metric in model.metrics or []:
            if metric.agg == "count":
                return metric.name
        return None

    def _extract_filter_columns(self, filters: list[str]) -> list[str]:
        """Extract column names referenced in filter expressions."""
        import re

        columns: list[str] = []
        for filter_expr in filters:
            matches = re.findall(r"(?:\\w+\\.)?(\\w+)\\s*[=<>!]", filter_expr)
            for col in matches:
                if col not in columns:
                    columns.append(col)
        return columns

    def _execute(self, sql: str):
        """Execute SQL through adapter when layer is available, otherwise raw connection.

        Uses layer.adapter.execute() when in Semantic Layer mode, which ensures
        queries go through the adapter interface for consistent behavior across
        database backends.
        """
        if self._layer is not None:
            return self._layer.adapter.execute(sql)
        return self._conn.execute(sql)

    def _execute_arrow(self, sql: str):
        """Execute SQL and return result as PyArrow Table.

        Uses adapter.execute().fetch_record_batch() in Semantic Layer mode for
        cross-adapter compatibility (works with Snowflake, Databricks, ClickHouse, etc.).
        Falls back to raw DuckDB .arrow() in DataFrame mode.
        """
        if self._layer is not None:
            result = self._layer.adapter.execute(sql)
            reader = result.fetch_record_batch()
            return reader.read_all()
        return self._conn.execute(sql).arrow()

    def _sync_status(self) -> None:
        error = self._metric_error or self._dimension_error
        self.error = error
        self.status = "error" if error else "ready"

    def _refresh_all(self):
        """Refresh all data (metrics and dimensions)."""
        self.status = "loading"
        self._refresh_metrics(sync_status=False)
        self._refresh_dimensions(sync_status=False)
        self._sync_status()

    def _refresh_metrics(self, *, sync_status: bool = True):
        """Refresh metric series data."""
        if not self._time_dimension:
            self._metric_error = "No time dimension available for metrics."
            if sync_status:
                self._sync_status()
            return

        # Clear existing data to show skeletons while loading
        self.metric_series_data = ""
        self.metric_totals = {}
        self._metric_error = ""

        filters = self._build_filters()

        # Query all metrics as time series
        metric_refs = [m["ref"] for m in self.metrics_config]
        grain = self.time_grain or "day"
        time_dim_ref = f"{self._model_name}.{self._time_dimension}__{grain}"

        try:
            sql = self._generator.generate(
                metrics=metric_refs,
                dimensions=[time_dim_ref],
                filters=filters,
                order_by=[time_dim_ref],
                limit=500,
                use_preaggregations=self._use_preaggregations,
                skip_default_time_dimensions=True,
            )
            result = self._execute_arrow(sql)
            self.metric_series_data = _table_to_ipc(result, decimal_mode="float")

            totals_sql = self._generator.generate(
                metrics=metric_refs,
                dimensions=[],
                filters=filters,
                skip_default_time_dimensions=True,
                use_preaggregations=self._use_preaggregations,
            )
            totals_row = self._execute(totals_sql).fetchone()
            totals = {}
            if totals_row:
                for i, metric_ref in enumerate(metric_refs):
                    value = totals_row[i]
                    try:
                        from decimal import Decimal

                        if isinstance(value, Decimal):
                            value = str(value)
                    except Exception:
                        pass
                    totals[metric_ref.split(".")[-1]] = value
            self.metric_totals = totals
            self._record_query_intent(metric_refs, [time_dim_ref], grain, filters)
        except Exception as e:
            self._metric_error = f"Metric query failed: {e}"
        if sync_status:
            self._sync_status()

    def _refresh_dimensions(self, *, sync_status: bool = True):
        """Refresh dimension leaderboard data."""
        selected_metric_ref = f"{self._model_name}.{self.selected_metric}"
        self._dimension_error = ""

        # Preserve existing data so panels stay interactive while refreshing
        existing = self.dimension_data or {}
        dimension_data = dict(existing)
        for dim in self.dimensions_config:
            dim_key = dim["key"]
            if dim_key not in dimension_data:
                dimension_data[dim_key] = ""

        if self.active_dimension:
            dim_config = next((d for d in self.dimensions_config if d["key"] == self.active_dimension), None)
            if not dim_config:
                return

            dim_key = dim_config["key"]
            dim_ref = dim_config["ref"]

            # Exclude this dimension from its own filters (crossfilter pattern)
            filters = self._build_filters(exclude_dimension=dim_key)

            try:
                sql = self._generator.generate(
                    metrics=[selected_metric_ref],
                    dimensions=[dim_ref],
                    filters=filters,
                    order_by=[f"{selected_metric_ref} DESC"],
                    limit=6,
                    skip_default_time_dimensions=True,
                    use_preaggregations=self._use_preaggregations,
                )
                result = self._execute_arrow(sql)
                dimension_data[dim_key] = _table_to_ipc(result, decimal_mode="string")
                self.dimension_data = dict(dimension_data)
                self._record_query_intent(
                    [selected_metric_ref],
                    [dim_ref],
                    self.time_grain or "day",
                    filters,
                )
            except Exception as e:
                self._dimension_error = f"Dimension query failed for {dim_key}: {e}"
            if sync_status:
                self._sync_status()
            return

        # Full refresh: show skeletons until each panel completes
        preserve_dim = self._last_active_dimension
        dimension_data = {d["key"]: "" for d in self.dimensions_config}
        if preserve_dim and preserve_dim in existing:
            dimension_data[preserve_dim] = existing[preserve_dim]
        self.dimension_data = dimension_data

        for dim_config in self.dimensions_config:
            dim_key = dim_config["key"]
            dim_ref = dim_config["ref"]

            # Exclude this dimension from its own filters (crossfilter pattern)
            filters = self._build_filters(exclude_dimension=dim_key)

            try:
                sql = self._generator.generate(
                    metrics=[selected_metric_ref],
                    dimensions=[dim_ref],
                    filters=filters,
                    order_by=[f"{selected_metric_ref} DESC"],
                    limit=6,
                    skip_default_time_dimensions=True,
                    use_preaggregations=self._use_preaggregations,
                )
                result = self._execute_arrow(sql)
                dimension_data[dim_key] = _table_to_ipc(result, decimal_mode="string")
                self.dimension_data = dict(dimension_data)
                self._record_query_intent(
                    [selected_metric_ref],
                    [dim_ref],
                    self.time_grain or "day",
                    filters,
                )
            except Exception as e:
                self._dimension_error = f"Dimension query failed for {dim_key}: {e}"

        self.dimension_data = dimension_data
        self._last_active_dimension = None
        if sync_status:
            self._sync_status()
