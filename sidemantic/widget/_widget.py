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


def _table_to_ipc(table) -> bytes:
    """Serialize Arrow table to IPC format."""
    import pyarrow as pa

    sink = io.BytesIO()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


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
    metric_series_data = traitlets.Bytes(b"").tag(sync=True)
    dimension_data = traitlets.Dict({}).tag(sync=True)  # {dim_key: arrow_ipc_bytes}
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

        self._generator = SQLGenerator(self._graph, dialect="duckdb")

        # Set up observers with specific handlers
        self.observe(self._on_filters_change, names=["filters"])
        self.observe(self._on_date_range_change, names=["date_range"])
        self.observe(self._on_brush_change, names=["brush_selection"])
        self.observe(self._on_metric_change, names=["selected_metric"])
        self.observe(self._on_comparison_change, names=["comparison_mode"])
        self.observe(self._on_time_grain_change, names=["time_grain"])
        self.observe(self._on_active_dimension_change, names=["active_dimension"])

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
            result = self._conn.execute(query).fetchone()
            if result and result[0] and result[1]:
                min_date = result[0]
                max_date = result[1]

                min_date = self._stringify_time_value(min_date)
                max_date = self._stringify_time_value(max_date)

                self.date_range = [min_date, max_date]
        except Exception as e:
            self.error = f"Failed to compute date range: {e}"

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
        """Handle brush selection changes - only refresh dimensions (sparklines stay the same)."""
        self._refresh_dimensions()

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

    def _refresh_all(self):
        """Refresh all data (metrics and dimensions)."""
        self.status = "loading"
        try:
            self._refresh_metrics()
            self._refresh_dimensions()
            self.status = "ready"
        except Exception as e:
            self.error = str(e)
            self.status = "error"

    def _refresh_metrics(self):
        """Refresh metric series data."""
        if not self._time_dimension:
            return

        # Clear existing data to show skeletons while loading
        self.metric_series_data = b""
        self.metric_totals = {}

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
            )
            result = self._conn.execute(sql).arrow()
            self.metric_series_data = _table_to_ipc(result)

            totals_sql = self._generator.generate(
                metrics=metric_refs,
                dimensions=[],
                filters=filters,
                skip_default_time_dimensions=True,
                use_preaggregations=self._use_preaggregations,
            )
            totals_row = self._conn.execute(totals_sql).fetchone()
            totals = {}
            if totals_row:
                for i, metric_ref in enumerate(metric_refs):
                    totals[metric_ref.split(".")[-1]] = totals_row[i]
            self.metric_totals = totals
        except Exception as e:
            self.error = f"Metric query failed: {e}"

    def _refresh_dimensions(self):
        """Refresh dimension leaderboard data."""
        selected_metric_ref = f"{self._model_name}.{self.selected_metric}"

        # Preserve existing data so panels stay interactive while refreshing
        existing = self.dimension_data or {}
        dimension_data = dict(existing)
        for dim in self.dimensions_config:
            dim_key = dim["key"]
            if dim_key not in dimension_data:
                dimension_data[dim_key] = b""

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
                result = self._conn.execute(sql).arrow()
                dimension_data[dim_key] = _table_to_ipc(result)
                self.dimension_data = dict(dimension_data)
            except Exception as e:
                self.error = f"Dimension query failed for {dim_key}: {e}"
            return

        # Full refresh: show skeletons until each panel completes
        preserve_dim = self._last_active_dimension
        dimension_data = {d["key"]: b"" for d in self.dimensions_config}
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
                result = self._conn.execute(sql).arrow()
                dimension_data[dim_key] = _table_to_ipc(result)
                self.dimension_data = dict(dimension_data)
            except Exception as e:
                self.error = f"Dimension query failed for {dim_key}: {e}"

        self.dimension_data = dimension_data
        self._last_active_dimension = None
