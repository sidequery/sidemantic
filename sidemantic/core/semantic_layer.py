"""Semantic layer main API."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import yaml

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.rust_bridge import get_rust_module, graph_to_rust_yaml
from sidemantic.rust_parity import is_strict_for
from sidemantic.sql.generator import SQLGenerator

_RUST_SQL_OUTPUT_DIALECT = "duckdb"


class PreaggregationStrictError(RuntimeError):
    """Raised in strict (rollup-only) mode when a query cannot be served from a pre-aggregation."""


class UnsupportedMetricError(RuntimeError):
    """Raised when a queried metric uses a feature the generator cannot serve correctly.

    Currently raised for metrics with ``non_additive_dimension`` set, which would
    otherwise be silently over-aggregated (wrong results). Suppress with
    ``SemanticLayer(allow_non_additive_unsafe=True)``.
    """


# Substrings that identify a "missing relation/table" execution error across
# database adapters (DuckDB, Postgres, BigQuery, Snowflake, ClickHouse, ...).
# Used to decide whether a routed-but-unbuilt pre-aggregation table should fall
# back to the raw tables (which always produce correct results).
_MISSING_RELATION_MARKERS = (
    "does not exist",
    "doesn't exist",
    "no such table",
    "unknown table",
    "table or view",
    "not found",  # BigQuery surfaces missing tables as "404 Not found: Table ..."
)


class SemanticLayer:
    """Main semantic layer interface.

    Provides a high-level API for defining models and querying data.
    """

    def __init__(
        self,
        connection: str | BaseDatabaseAdapter = "duckdb:///:memory:",  # type: ignore # noqa: F821
        dialect: str | None = None,
        auto_register: bool = True,
        use_preaggregations: bool = False,
        preagg_strict: bool = False,
        preagg_database: str | None = None,
        preagg_schema: str | None = None,
        init_sql: list[str] | None = None,
        engine: str | None = None,
        fallback: bool | None = None,
        default_limit: int | None = None,
        max_limit: int | None = None,
        allow_non_additive_unsafe: bool = False,
    ):
        """Initialize semantic layer.

        Args:
            connection: Database connection string or adapter instance (default: in-memory DuckDB)
                Supported URLs:
                - duckdb:///:memory: (default)
                - duckdb:///path/to/db.duckdb
                - duckdb://md:database_name (MotherDuck)
                - postgres://user:pass@host:port/dbname
                - bigquery://project_id/dataset_id
                - snowflake://user:password@account/database/schema
                - clickhouse://user:password@host:port/database
                - databricks://token@server-hostname/http-path
                - spark://host:port/database
                - adbc://driver/uri (e.g., adbc://postgresql/postgresql://localhost/mydb)
            dialect: SQL dialect for query generation (optional, inferred from adapter)
            auto_register: Set as current layer for auto-registration (default: True)
            use_preaggregations: Enable automatic pre-aggregation routing (default: False)
            preagg_strict: Rollup-only mode. When True, queries must be served from a
                pre-aggregation; if none matches or its table is not built, raise
                PreaggregationStrictError instead of falling back to raw tables (default: False)
            preagg_database: Optional database name for pre-aggregation tables
            preagg_schema: Optional schema name for pre-aggregation tables
            init_sql: SQL statements to run after connecting (DuckDB only, e.g.,
                loading extensions, attaching catalogs, creating secrets)
            engine: Runtime engine for native query validation/compilation.
                Supported values are "python", "rust", and "auto". If omitted,
                legacy SIDEMANTIC_RS_* environment flags are honored.
            fallback: Whether explicit Rust/auto engine mode may fall back to Python.
                Defaults to False for engine="rust" and True for engine="auto".
            default_limit: Opt-in row limit applied when a query specifies no explicit
                limit (default: None, i.e. unlimited). Safety cap to avoid accidental
                full-table scans.
            max_limit: Opt-in maximum row limit; an explicit (or defaulted) limit larger
                than this is capped to it (default: None, i.e. uncapped).
            allow_non_additive_unsafe: When True, suppress the UnsupportedMetricError raised
                at query time for metrics that declare a non_additive_dimension. The generator
                has no semi-additive handling, so such metrics are over-aggregated (wrong)
                results; this flag opts into that behavior explicitly (default: False).
        """
        from sidemantic.db.base import BaseDatabaseAdapter

        if engine is not None:
            engine = engine.lower()
            if engine not in {"python", "rust", "auto"}:
                raise ValueError("engine must be one of: python, rust, auto")

        self.graph = SemanticGraph()
        self._sql_rewrite_cache: dict[tuple[object, ...], str] = {}
        self._sql_rewrite_cache_limit = 256
        self.use_preaggregations = use_preaggregations
        self.preagg_strict = preagg_strict
        self.preagg_database = preagg_database
        self.preagg_schema = preagg_schema
        self.engine = engine or "python"
        self.default_limit = default_limit
        self.max_limit = max_limit
        self.allow_non_additive_unsafe = allow_non_additive_unsafe
        self._strict_rust_sql_generator_entrypoint = is_strict_for("sql_generator_entrypoint")
        self._strict_rust_query_validation = is_strict_for("semantic_core_query_validation")
        if engine == "python":
            self._use_rust_sql_generator = False
            self._use_rust_query_validation = False
            self._rust_sql_verify = False
            self._rust_no_fallback = fallback is False
        elif engine == "rust":
            self._use_rust_sql_generator = True
            self._use_rust_query_validation = True
            self._rust_sql_verify = False
            self._rust_no_fallback = not (fallback if fallback is not None else False)
        elif engine == "auto":
            self._use_rust_sql_generator = True
            self._use_rust_query_validation = True
            self._rust_sql_verify = False
            self._rust_no_fallback = not (fallback if fallback is not None else True)
        else:
            self._use_rust_sql_generator = (
                os.getenv("SIDEMANTIC_RS_SQL_GENERATOR", "0") == "1" or self._strict_rust_sql_generator_entrypoint
            )
            self._use_rust_query_validation = (
                os.getenv("SIDEMANTIC_RS_QUERY_VALIDATION", "0") == "1" or self._strict_rust_query_validation
            )
            self._rust_sql_verify = (
                os.getenv("SIDEMANTIC_RS_SQL_GENERATOR_VERIFY", "1") == "1"
                and not self._strict_rust_sql_generator_entrypoint
            )
            self._rust_no_fallback = os.getenv("SIDEMANTIC_RS_NO_FALLBACK", "0") == "1"
        self._rust_module = None
        if self._use_rust_sql_generator:
            try:
                self._rust_module = get_rust_module()
            except Exception:
                if self._rust_no_fallback or self._strict_rust_sql_generator_entrypoint:
                    raise

        # Initialize adapter from connection string or use provided adapter
        if isinstance(connection, BaseDatabaseAdapter):
            self.adapter = connection
            self.dialect = dialect or connection.dialect
            self.connection_string = f"{connection.dialect}://custom"
        elif isinstance(connection, str):
            self.connection_string = connection

            if connection.startswith("duckdb://md:"):
                from sidemantic.db.motherduck import MotherDuckAdapter

                self.adapter = MotherDuckAdapter.from_url(connection)
                self.dialect = dialect or "duckdb"
                # Run init_sql after MotherDuck connection
                if init_sql:
                    for stmt in init_sql:
                        self.adapter.execute(stmt)
            elif connection.startswith("duckdb://"):
                try:
                    from sidemantic.db.duckdb import DuckDBAdapter
                except ModuleNotFoundError as exc:
                    if exc.name != "duckdb":
                        raise
                    from sidemantic.db.unavailable import UnavailableDatabaseAdapter

                    self.adapter = UnavailableDatabaseAdapter(
                        dialect="duckdb",
                        package="duckdb",
                        install_hint="Install with `pip install duckdb` or use a database adapter available in this environment.",
                    )
                    if init_sql:
                        for stmt in init_sql:
                            self.adapter.execute(stmt)
                else:
                    self.adapter = DuckDBAdapter.from_url(connection, init_sql=init_sql)
                self.dialect = dialect or "duckdb"
            elif connection.startswith(("postgres://", "postgresql://")):
                from sidemantic.db.postgres import PostgreSQLAdapter

                self.adapter = PostgreSQLAdapter.from_url(connection)
                self.dialect = dialect or "postgres"
            elif connection.startswith("bigquery://"):
                from sidemantic.db.bigquery import BigQueryAdapter

                self.adapter = BigQueryAdapter.from_url(connection)
                self.dialect = dialect or "bigquery"
            elif connection.startswith("snowflake://"):
                from sidemantic.db.snowflake import SnowflakeAdapter

                self.adapter = SnowflakeAdapter.from_url(connection)
                self.dialect = dialect or "snowflake"
            elif connection.startswith("clickhouse://"):
                from sidemantic.db.clickhouse import ClickHouseAdapter

                self.adapter = ClickHouseAdapter.from_url(connection)
                self.dialect = dialect or "clickhouse"
            elif connection.startswith("databricks://"):
                from sidemantic.db.databricks import DatabricksAdapter

                self.adapter = DatabricksAdapter.from_url(connection)
                self.dialect = dialect or "databricks"
            elif connection.startswith("spark://"):
                from sidemantic.db.spark import SparkAdapter

                self.adapter = SparkAdapter.from_url(connection)
                self.dialect = dialect or "spark"
            elif connection.startswith("adbc://"):
                from sidemantic.db.adbc import ADBCAdapter

                self.adapter = ADBCAdapter.from_url(connection)
                self.dialect = dialect or self.adapter.dialect
            else:
                raise ValueError(
                    f"Unsupported connection URL: {connection}. "
                    "Supported: duckdb:///, duckdb://md:, postgres://, bigquery://, snowflake://, clickhouse://, databricks://, spark://, adbc://, or BaseDatabaseAdapter instance"
                )
        else:
            raise TypeError(f"connection must be a string URL or BaseDatabaseAdapter instance, got {type(connection)}")

        # Set as current layer for auto-registration
        if auto_register:
            from .registry import set_current_layer

            set_current_layer(self)

    def __enter__(self):
        """Context manager entry - set as current layer."""
        from .registry import set_current_layer

        set_current_layer(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - clear current layer and close adapter."""
        from .registry import set_current_layer

        set_current_layer(None)
        if hasattr(self.adapter, "close"):
            self.adapter.close()

    @property
    def adapter(self):
        """Database adapter accessor with legacy _adapter compatibility."""
        return self._adapter

    @adapter.setter
    def adapter(self, value):
        self._adapter = value

    @property
    def conn(self):
        """Get raw database connection for backward compatibility."""
        return self.adapter.raw_connection

    @conn.setter
    def conn(self, value):
        """Set raw database connection for backward compatibility.

        This updates the adapter's internal connection.
        Used by tests that directly assign connections.
        """
        # Update the adapter's connection
        self.adapter.conn = value

    def add_model(self, model: Model) -> None:
        """Add a model to the semantic layer.

        Args:
            model: Model to add

        Raises:
            ModelValidationError: If model validation fails
        """
        from sidemantic.validation import ModelValidationError, validate_model

        # Skip registration if an identical model instance is already present.
        #
        # This can happen when models are created while a semantic layer context
        # is active. The model auto-registers itself with the current layer and
        # later user code (or tests) may explicitly call ``add_model`` with the
        # same instance. Treating that call as a no-op keeps ``add_model``
        # idempotent without weakening the duplicate detection performed by the
        # underlying graph (which will still raise when a different definition
        # with the same name is added).
        existing = self.graph.models.get(model.name)
        if existing is not None:
            if existing is model:
                return
            existing_dump = existing.model_dump()
            new_dump = model.model_dump()
            if existing_dump == new_dump:
                return
            # When both models use auto_dimensions, the existing model has
            # introspected dimensions that the new model doesn't yet. Compare
            # excluding dimensions to preserve idempotent add_model behavior.
            if existing.auto_dimensions and model.auto_dimensions:
                existing_dump.pop("dimensions", None)
                new_dump.pop("dimensions", None)
                if existing_dump == new_dump:
                    return

        self._normalize_model_table(model)

        # Auto-introspect dimensions from DB schema if requested
        if model.auto_dimensions:
            self._introspect_dimensions(model)

        errors = validate_model(model)
        if errors:
            raise ModelValidationError(
                f"Model '{model.name}' validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        self.graph.add_model(model)
        self._sql_rewrite_cache.clear()

    def _normalize_model_table(self, model: Model) -> None:
        """Normalize model.table for the active dialect when needed."""
        if not model.table or model.sql:
            return

        if self.dialect != "duckdb":
            return

        if not self._is_simple_table_reference(model.table):
            return

        normalized = self._normalize_duckdb_table_reference(model.table)
        if normalized != model.table:
            model.table = normalized

    @staticmethod
    def _is_simple_table_reference(table: str) -> bool:
        """Return True if table looks like a plain table reference (not a subquery)."""
        return not any(ch in table for ch in (" ", "\n", "\t", "(", ")"))

    def _normalize_duckdb_table_reference(self, table: str) -> str:
        """Drop unattached catalog qualifiers for DuckDB."""
        import sqlglot
        from sqlglot import exp

        try:
            parsed = sqlglot.parse_one(table, into=exp.Table, dialect="duckdb")
        except Exception:
            return table

        catalog_expr = parsed.args.get("catalog")
        if not catalog_expr:
            return table

        catalog = getattr(catalog_expr, "name", None) or getattr(catalog_expr, "this", None)
        if not catalog:
            return table

        if catalog in self._duckdb_catalogs():
            return table

        parsed.set("catalog", None)
        return parsed.sql(dialect="duckdb")

    def _duckdb_catalogs(self) -> set[str]:
        """Return attached DuckDB catalog names."""
        try:
            result = self.adapter.execute("PRAGMA database_list")
            rows = result.fetchall()
        except Exception:
            return set()

        catalogs = set()
        for row in rows:
            if len(row) >= 2:
                catalogs.add(row[1])
            elif row:
                catalogs.add(row[0])
        return catalogs

    def _introspect_dimensions(self, model: Model) -> None:
        """Auto-discover dimensions from database schema.

        Queries the database for column metadata and creates Dimension objects
        for columns that don't already have explicit definitions.

        Args:
            model: Model to introspect dimensions for
        """
        from sidemantic.core.dimension import Dimension

        existing_dim_names = {dim.name for dim in model.dimensions}
        pk_columns = set(model.primary_key_columns)

        columns = self._get_model_columns(model)
        if not columns:
            return

        for col in columns:
            col_name = col["column_name"]

            # Skip columns that already have explicit dimensions
            if col_name in existing_dim_names:
                continue

            # Skip primary key columns
            if col_name in pk_columns:
                continue

            dim_type, granularity = self._map_db_type(col["data_type"])
            dim = Dimension(name=col_name, type=dim_type, granularity=granularity)
            model.dimensions.append(dim)

    def _get_model_columns(self, model: Model) -> list[dict]:
        """Get column metadata for a model's backing table or SQL.

        Returns:
            List of dicts with 'column_name' and 'data_type' keys
        """
        if model.table:
            # Parse table reference: "table", "schema.table", or "catalog.schema.table"
            parts = model.table.split(".")
            if len(parts) >= 3:
                # catalog.schema.table -- use last two parts
                schema, table_name = parts[-2], parts[-1]
            elif len(parts) == 2:
                schema, table_name = parts
            else:
                schema, table_name = None, parts[-1]
            try:
                return self.adapter.get_columns(table_name, schema=schema)
            except Exception:
                return []
        elif model.sql:
            # For SQL-based models, run a LIMIT 0 query to get column types
            try:
                result = self.adapter.execute(f"SELECT * FROM ({model.sql}) AS _introspect LIMIT 0")
                # DuckDB returns column info via .description
                if hasattr(result, "description") and result.description:
                    return [
                        {
                            "column_name": desc[0],
                            "data_type": str(desc[1]) if len(desc) > 1 and desc[1] is not None else "VARCHAR",
                        }
                        for desc in result.description
                    ]
            except Exception:
                return []
        return []

    @staticmethod
    def _map_db_type(db_type: str) -> tuple[str, str | None]:
        """Map a database column type to a sidemantic dimension type and granularity.

        Args:
            db_type: Database column type string (e.g., 'VARCHAR', 'TIMESTAMP', 'INTEGER')

        Returns:
            Tuple of (dimension_type, granularity). Granularity is only set for time types.
        """
        upper = db_type.upper()

        # Strip precision/length info: "VARCHAR(255)" -> "VARCHAR", "DECIMAL(10,2)" -> "DECIMAL"
        base_type = upper.split("(")[0].strip()

        # Also handle array/complex types: "INTEGER[]" -> "INTEGER"
        base_type = base_type.rstrip("[]")

        time_types = {
            "DATE": "day",
            "TIMESTAMP": "second",
            "TIMESTAMPTZ": "second",
            "TIMESTAMP WITH TIME ZONE": "second",
            "TIMESTAMP WITHOUT TIME ZONE": "second",
            "DATETIME": "second",
            "TIME": "second",
            "TIMETZ": "second",
        }
        if base_type in time_types:
            return "time", time_types[base_type]

        boolean_types = {"BOOLEAN", "BOOL"}
        if base_type in boolean_types:
            return "boolean", None

        numeric_types = {
            "INTEGER",
            "INT",
            "INT2",
            "INT4",
            "INT8",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "HUGEINT",
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
            "DOUBLE",
            "DECIMAL",
            "NUMERIC",
            "REAL",
            "NUMBER",
        }
        if base_type in numeric_types:
            return "numeric", None

        # Everything else (VARCHAR, TEXT, CHAR, STRING, ENUM, BLOB, JSON, etc.)
        return "categorical", None

    def add_metric(self, measure: Metric) -> None:
        """Add a measure to the semantic layer.

        Args:
            measure: Metric to add

        Raises:
            MetricValidationError: If measure validation fails
        """
        from sidemantic.validation import MetricValidationError, validate_metric

        existing = self.graph.metrics.get(measure.name)
        if existing is not None:
            if existing is measure:
                return
            if existing.model_dump() == measure.model_dump():
                return

        errors = validate_metric(measure, self.graph)
        if errors:
            raise MetricValidationError(
                f"Measure '{measure.name}' validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        self.graph.add_metric(measure)
        self._sql_rewrite_cache.clear()

    def query(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        ungrouped: bool = False,
        parameters: dict[str, any] | None = None,
        use_preaggregations: bool | None = None,
        preagg_strict: bool | None = None,
        post_process: str | None = None,
        timezone: str | None = None,
        with_totals: bool = False,
    ):
        """Execute a query against the semantic layer.

        Args:
            metrics: List of metric references (e.g., ["orders.revenue"])
            dimensions: List of dimension references (e.g., ["orders.status", "orders.order_date__month"])
            filters: List of filter expressions (e.g., ["orders.status = 'completed'"])
            segments: List of segment references (e.g., ["orders.active_users"])
            order_by: List of fields to order by
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            ungrouped: If True, return raw rows without aggregation (no GROUP BY)
            parameters: Template parameters for Jinja2 rendering
            use_preaggregations: Override pre-aggregation routing setting for this query
            preagg_strict: Override rollup-only mode for this query. When True, raise
                PreaggregationStrictError if no rollup matches or its table is missing,
                instead of falling back to raw tables.
            post_process: Optional SQL to wrap around the semantic query result.
                Use {inner} as a placeholder for the compiled semantic query, e.g.:
                "SELECT *, revenue / count AS avg_value FROM ({inner})"
            timezone: Optional query timezone applied to time-dimension truncation
            with_totals: If True, add a grand-total row via GROUPING SETS, marked with a
                trailing _is_total column (1 for the grand total, 0 for detail rows) so it
                is distinguishable from a real all-NULL dimension group. Cannot be combined
                with ungrouped, limit, or offset

        Returns:
            DuckDB relation object (can convert to DataFrame with .df() or .to_df())
        """
        use_preaggs = use_preaggregations if use_preaggregations is not None else self.use_preaggregations
        strict = preagg_strict if preagg_strict is not None else self.preagg_strict

        # Detect pre-aggregation routing from the un-post-processed compile: post_process
        # wraps the query and strips the `-- sidemantic ... used_preagg=true` marker.
        routing_sql = self.compile(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            segments=segments,
            order_by=order_by,
            limit=limit,
            offset=offset,
            ungrouped=ungrouped,
            parameters=parameters,
            use_preaggregations=use_preaggregations,
            timezone=timezone,
            with_totals=with_totals,
        )
        used_preagg = "used_preagg=true" in routing_sql

        if post_process:
            sql = self.compile(
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                segments=segments,
                order_by=order_by,
                limit=limit,
                ungrouped=ungrouped,
                parameters=parameters,
                use_preaggregations=use_preaggregations,
                post_process=post_process,
                timezone=timezone,
            )
        else:
            sql = routing_sql

        def recompile_raw():
            return self.compile(
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                segments=segments,
                order_by=order_by,
                limit=limit,
                ungrouped=ungrouped,
                parameters=parameters,
                use_preaggregations=False,
                post_process=post_process,
                timezone=timezone,
            )

        return self._execute_with_preagg_fallback(
            sql, recompile_raw, use_preaggs=use_preaggs, strict=strict, used_preagg=used_preagg
        )

    def _execute_with_preagg_fallback(
        self, primary_sql, recompile_raw, *, use_preaggs: bool, strict: bool, used_preagg: bool
    ):
        """Execute primary_sql, falling back to raw tables when a routed rollup is missing.

        Shared by query() and sql() so both the Python API and the SQL/CLI path get
        identical missing-rollup behavior. ``used_preagg`` says whether routing selected
        a rollup (the caller detects it before any post-processing strips the marker):
        - if no rollup matched and strict is set, raise (rollup-only mode);
        - if the routed rollup table does not exist, fall back to recompile_raw() (or
          raise in strict mode). Any other error surfaces unchanged.
        """
        if not use_preaggs:
            return self.adapter.execute(primary_sql)

        # Rollup-only: a query no rollup can serve must error rather than scan raw tables.
        if strict and not used_preagg:
            raise PreaggregationStrictError(
                "Strict pre-aggregation mode: no pre-aggregation matched this query "
                "(its metrics/dimensions/granularity are not covered by any rollup)."
            )

        try:
            return self.adapter.execute(primary_sql)
        except Exception as exc:
            # Only intervene when a routed pre-aggregation table is missing; every
            # other error surfaces unchanged.
            if not used_preagg or not self._is_missing_relation_error(exc):
                raise
            if strict:
                raise PreaggregationStrictError(
                    "Strict pre-aggregation mode: the matching pre-aggregation table is not built. "
                    "Materialize it (e.g. `sidemantic preagg refresh`) before querying."
                ) from exc
            # A pure optimization fell through: recompile against raw tables so the
            # query still returns correct results.
            return self.adapter.execute(recompile_raw())

    @staticmethod
    def _is_missing_relation_error(error: Exception) -> bool:
        """Heuristic: does this execution error indicate a missing table/relation?"""
        message = str(error).lower()
        return any(marker in message for marker in _MISSING_RELATION_MARKERS)

    def get_import_warnings(self) -> list[dict[str, object]]:
        """Return structured warnings produced while importing model definitions."""
        return list(getattr(self.graph, "import_warnings", []) or [])

    def describe_models(self, model_names: list[str] | None = None) -> dict[str, object]:
        """Return UI/FFI-friendly model metadata, including source and DAX/TMDL state."""
        from sidemantic.core.introspection import describe_graph

        return describe_graph(self.graph, model_names=model_names)

    def chart(
        self,
        metric: str | list[str],
        *,
        by: str | list[str] | None = None,
        mark: str = "auto",
        filters: list[str] | None = None,
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        title: str | None = None,
        use_preaggregations: bool | None = None,
    ):
        """Create a headless chart builder from semantic fields.

        Examples:
            >>> chart = layer.chart("orders.revenue", by="orders.created_at__month").line().brush("x")
            >>> chart.to_vegalite()
            >>> chart.to_plotly()
        """
        from sidemantic.viz import ChartBuilder

        return ChartBuilder(
            self,
            metric,
            by=by,
            mark=mark,
            filters=filters,
            segments=segments,
            order_by=order_by,
            limit=limit,
            title=title,
            use_preaggregations=use_preaggregations,
        )

    def _resolve_row_limit(self, limit: int | None) -> int | None:
        """Apply opt-in default/max row-limit safety caps.

        - When no explicit limit is given, fall back to ``default_limit`` (if set).
        - When an explicit (or defaulted) limit exceeds ``max_limit`` (if set), cap it.

        Both ``default_limit`` and ``max_limit`` default to ``None`` so behavior is
        unchanged unless configured on the ``SemanticLayer``.
        """
        if limit is None and self.default_limit is not None:
            limit = self.default_limit
        if self.max_limit is not None and limit is not None and limit > self.max_limit:
            limit = self.max_limit
        return limit

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
        parameters: dict[str, any] | None = None,
        use_preaggregations: bool | None = None,
        aliases: dict[str, str] | None = None,
        post_process: str | None = None,
        timezone: str | None = None,
        with_totals: bool = False,
    ) -> str:
        """Compile a query to SQL without executing.

        Args:
            metrics: List of metric references
            dimensions: List of dimension references
            filters: List of filter expressions
            segments: List of segment references (e.g., ["orders.active_users"])
            order_by: List of fields to order by
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            dialect: SQL dialect override (defaults to layer's dialect)
            ungrouped: If True, return raw rows without aggregation (no GROUP BY)
            use_preaggregations: Override pre-aggregation routing setting for this query
            aliases: Custom output aliases keyed by semantic field reference
            post_process: Optional SQL to wrap around the semantic query result.
                Use {inner} as a placeholder for the compiled semantic query, e.g.:
                "SELECT *, revenue / count AS avg_value FROM ({inner})"
            timezone: Optional query timezone. When set, time-dimension expressions are
                converted to this timezone before truncation. Most meaningful on
                TIMESTAMPTZ columns. Truncation-side only: time-dimension filter
                comparisons are not timezone-shifted.
            with_totals: If True, add a grand-total row via GROUPING SETS, marked with a
                trailing _is_total column (1 for the grand total, 0 for detail rows) so it
                is distinguishable from a real all-NULL dimension group. Cannot be combined
                with ungrouped, limit, or offset

        Returns:
            SQL query string

        Raises:
            QueryValidationError: If query validation fails
        """
        from sidemantic.validation import QueryValidationError, validate_query

        metrics = metrics or []
        dimensions = dimensions or []

        if with_totals and (limit is not None or offset is not None):
            raise ValueError(
                "with_totals cannot be combined with limit/offset: the grand-total row shares "
                "the grouped result set with the detail rows, so pagination could page it out. "
                "Paginate in a wrapper (post_process) or omit with_totals."
            )

        # Apply opt-in default/max row-limit caps before engine dispatch so both the Rust and
        # Python compile paths see the resolved limit. Skip the caps when with_totals is set so
        # a configured default_limit/max_limit cannot page out the grand-total row.
        if not with_totals:
            limit = self._resolve_row_limit(limit)

        # Validate query
        errors = self._validate_query(metrics, dimensions, validate_query)
        if errors:
            raise QueryValidationError("Query validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

        # Determine if pre-aggregations should be used
        use_preaggs = use_preaggregations if use_preaggregations is not None else self.use_preaggregations

        inner_sql = None
        # The Rust generator implements neither query-timezone bucketing nor with_totals
        # GROUPING SETS, so use the Python path when either is requested.
        # (Pre-agg bypass for timezone queries is enforced inside SQLGenerator.generate.)
        if self._use_rust_sql_generator and not timezone and not with_totals:
            inner_sql = self._compile_with_rust(
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                segments=segments,
                order_by=order_by,
                limit=limit,
                offset=offset,
                dialect=dialect,
                ungrouped=ungrouped,
                parameters=parameters,
                use_preaggregations=use_preaggs,
                aliases=aliases,
            )
            if inner_sql is None and self._strict_rust_sql_generator_entrypoint:
                raise ValueError("Rust SQL generator returned no SQL in strict mode")
            if inner_sql is not None and self._rust_sql_verify:
                python_sql = self._compile_with_python(
                    metrics=metrics,
                    dimensions=dimensions,
                    filters=filters,
                    segments=segments,
                    order_by=order_by,
                    limit=limit,
                    offset=offset,
                    dialect=dialect,
                    ungrouped=ungrouped,
                    parameters=parameters,
                    use_preaggregations=use_preaggs,
                    aliases=aliases,
                )
                if inner_sql.strip() != python_sql.strip():
                    if self._rust_no_fallback or self._strict_rust_sql_generator_entrypoint:
                        raise ValueError("Rust SQL generator output mismatch with Python SQL generator")
                    inner_sql = python_sql

        if inner_sql is None:
            inner_sql = self._compile_with_python(
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                segments=segments,
                order_by=order_by,
                limit=limit,
                offset=offset,
                dialect=dialect,
                ungrouped=ungrouped,
                parameters=parameters,
                use_preaggregations=use_preaggs,
                aliases=aliases,
                timezone=timezone,
                with_totals=with_totals,
            )

        return self._apply_post_process(inner_sql, post_process)

    def _validate_query(
        self,
        metrics: list[str],
        dimensions: list[str],
        python_validate_query: Callable[[list[str], list[str], SemanticGraph], list[str]],
    ) -> list[str]:
        from sidemantic.validation import QueryValidationError

        if self._use_rust_query_validation:
            try:
                from sidemantic.rust_bridge import validate_query_with_rust

                return validate_query_with_rust(self.graph, metrics, dimensions)
            except Exception as e:
                if self._strict_rust_query_validation or self._rust_no_fallback:
                    raise QueryValidationError(f"Rust query validation failed: {e}") from e

        return python_validate_query(metrics, dimensions, self.graph)

    def _compile_with_python(
        self,
        metrics: list[str] | None,
        dimensions: list[str] | None,
        filters: list[str] | None,
        segments: list[str] | None,
        order_by: list[str] | None,
        limit: int | None,
        offset: int | None,
        dialect: str | None,
        ungrouped: bool,
        parameters: dict[str, any] | None,
        use_preaggregations: bool,
        aliases: dict[str, str] | None,
        timezone: str | None = None,
        with_totals: bool = False,
    ) -> str:
        generator = SQLGenerator(
            self.graph,
            dialect=dialect or self.dialect,
            preagg_database=self.preagg_database,
            preagg_schema=self.preagg_schema,
            timezone=timezone,
            allow_non_additive_unsafe=self.allow_non_additive_unsafe,
        )

        return generator.generate(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            segments=segments,
            order_by=order_by,
            limit=limit,
            offset=offset,
            ungrouped=ungrouped,
            parameters=parameters,
            use_preaggregations=use_preaggregations,
            aliases=aliases,
            with_totals=with_totals,
        )

    def _compile_with_rust(
        self,
        metrics: list[str] | None,
        dimensions: list[str] | None,
        filters: list[str] | None,
        segments: list[str] | None,
        order_by: list[str] | None,
        limit: int | None,
        offset: int | None,
        dialect: str | None,
        ungrouped: bool,
        parameters: dict[str, any] | None,
        use_preaggregations: bool,
        aliases: dict[str, str] | None,
    ) -> str | None:
        if not self._rust_module:
            if self._rust_no_fallback or self._strict_rust_sql_generator_entrypoint:
                raise ValueError("Rust SQL generator backend is not initialized")
            return None
        if aliases:
            if self._rust_no_fallback or self._strict_rust_sql_generator_entrypoint:
                raise ValueError("Rust SQL generator backend does not support compile aliases")
            return None

        payload = {
            "metrics": metrics or [],
            "dimensions": dimensions or [],
            "filters": list(filters or []),
            "parameter_values": parameters or {},
            "segments": segments or [],
            "order_by": order_by or [],
            "limit": limit,
            "offset": offset,
            "ungrouped": ungrouped,
            "use_preaggregations": bool(use_preaggregations),
            "preagg_database": self.preagg_database,
            "preagg_schema": self.preagg_schema,
        }

        try:
            models_yaml = graph_to_rust_yaml(self.graph)
            query_yaml = yaml.safe_dump(payload, sort_keys=False)
            sql = self._rust_module.compile_with_yaml(models_yaml, query_yaml)

            target_dialect = dialect or self.dialect
            if target_dialect != _RUST_SQL_OUTPUT_DIALECT:
                import sqlglot

                sql = sqlglot.transpile(sql, read=_RUST_SQL_OUTPUT_DIALECT, write=target_dialect)[0]
                if target_dialect == "bigquery":
                    sql = sql.replace("TIMESTAMP_TRUNC(", "DATE_TRUNC(")

            if "-- sidemantic:" not in sql:
                generator = SQLGenerator(
                    self.graph,
                    dialect=dialect or self.dialect,
                    preagg_database=self.preagg_database,
                    preagg_schema=self.preagg_schema,
                )
                segment_filters = generator._resolve_segments(segments or [])
                all_filters = list(filters or []) + segment_filters
                model_names = generator._find_required_models(metrics or [], dimensions or [], all_filters)
                sql = (
                    sql
                    + "\n"
                    + generator._generate_instrumentation_comment(
                        model_names,
                        metrics or [],
                        dimensions or [],
                        used_preagg=False,
                    )
                )

            return sql
        except Exception as e:
            if self._rust_no_fallback or self._strict_rust_sql_generator_entrypoint:
                raise ValueError(f"Rust SQL generator failed: {e}") from e
            return None

    def _apply_post_process(self, inner_sql: str, post_process: str | None) -> str:
        if post_process is not None:
            if "{inner}" not in post_process:
                raise ValueError("post_process must contain a {inner} placeholder")

            # Strip sidemantic instrumentation comment
            stripped = inner_sql.rstrip()
            last_line = stripped.split("\n")[-1].strip()
            if last_line.startswith("-- sidemantic:"):
                stripped = "\n".join(stripped.split("\n")[:-1])

            # Inner SQL (including any CTEs) is placed directly in the
            # subquery position. CTEs inside subqueries are valid SQL in
            # all target databases and naturally scoped, avoiding name
            # collisions with CTEs in the post_process SQL.
            return post_process.replace("{inner}", stripped)

        return inner_sql

    def explain(
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
        parameters: dict[str, any] | None = None,
        use_preaggregations: bool | None = None,
    ):
        """Explain query routing, showing whether pre-aggregations are used and why.

        Same parameters as compile(). Returns a QueryPlan with structured
        information about the routing decision and per-candidate check details.

        Example::

            plan = layer.explain(
                metrics=["events.event_count"],
                dimensions=["events.event_type"],
            )
            print(plan)
        """
        from sidemantic.core.preagg_matcher import PreAggregationMatcher
        from sidemantic.core.query_plan import QueryPlan

        metrics = metrics or []
        dimensions = dimensions or []
        filters = list(filters) if filters else []
        segments = segments or []

        # Compile the actual SQL (respects use_preaggregations setting)
        sql = self.compile(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            segments=segments,
            order_by=order_by,
            limit=limit,
            offset=offset,
            dialect=dialect,
            ungrouped=ungrouped,
            parameters=parameters,
            use_preaggregations=use_preaggregations,
        )

        use_preaggs = use_preaggregations if use_preaggregations is not None else self.use_preaggregations

        generator = SQLGenerator(
            self.graph,
            dialect=dialect or self.dialect,
            preagg_database=self.preagg_database,
            preagg_schema=self.preagg_schema,
        )
        segment_filters = generator._resolve_segments(segments)
        all_filters = filters + segment_filters
        model_names = generator._find_required_models(metrics, dimensions, all_filters)

        # Strip model prefixes from metrics and dimensions for matcher
        bare_metrics = []
        for m in metrics:
            bare_metrics.append(m.split(".", 1)[1] if "." in m else m)

        bare_dims = []
        time_granularity = None
        for d in dimensions:
            dim_name = d.split(".", 1)[1] if "." in d else d
            # Check for granularity suffix (e.g., "order_date__month")
            if "__" in dim_name:
                base, gran = dim_name.rsplit("__", 1)
                bare_dims.append(base)
                time_granularity = gran
            else:
                bare_dims.append(dim_name)

        bare_filters = []
        for f in all_filters:
            for mn in model_names:
                f = f.replace(f"{mn}.", "")
                f = f.replace(f"{mn}_cte.", "")
            bare_filters.append(f)

        # Check preconditions for preagg routing
        if not use_preaggs:
            return QueryPlan(
                sql=sql,
                model=model_names[0] if model_names else None,
                metrics=bare_metrics,
                dimensions=bare_dims,
                used_preaggregation=False,
                routing_reason="pre-aggregations not enabled (use_preaggregations=False)",
            )

        if len(model_names) != 1:
            return QueryPlan(
                sql=sql,
                model=None,
                metrics=bare_metrics,
                dimensions=bare_dims,
                used_preaggregation=False,
                routing_reason=f"multi-model query ({', '.join(sorted(model_names))}), preaggs only work for single-model queries",
            )

        model_name = model_names[0]
        try:
            model = self.get_model(model_name)
        except KeyError:
            return QueryPlan(
                sql=sql,
                model=model_name,
                metrics=bare_metrics,
                dimensions=bare_dims,
                used_preaggregation=False,
                routing_reason=f"model '{model_name}' not found",
            )

        if not model.pre_aggregations:
            return QueryPlan(
                sql=sql,
                model=model_name,
                metrics=bare_metrics,
                dimensions=bare_dims,
                used_preaggregation=False,
                routing_reason="model has no pre-aggregations defined",
            )

        # Run the matcher explanation
        matcher = PreAggregationMatcher(model)
        candidates = matcher.explain_matching(
            metrics=bare_metrics,
            dimensions=bare_dims,
            time_granularity=time_granularity,
            filters=bare_filters,
        )

        if ungrouped:
            # Drill-to-detail can only be served from a rollup that stores the
            # full primary key (rows are unique) and only for metrics whose raw
            # column is the per-row value. Mirror the routing gate in
            # _try_use_preaggregation so explain reflects actual routing.
            non_derivable = [
                m
                for m in bare_metrics
                if (metric := model.get_metric(m)) is None
                or metric.type in {"ratio", "derived"}
                or metric.agg in {"avg", "count_distinct", "approx_count_distinct"}
            ]
            if non_derivable:
                return QueryPlan(
                    sql=sql,
                    model=model_name,
                    metrics=bare_metrics,
                    dimensions=bare_dims,
                    used_preaggregation=False,
                    routing_reason=(
                        f"ungrouped query, metric(s) {', '.join(non_derivable)} are not derivable from stored rows"
                    ),
                    candidates=candidates,
                )
            pk_columns = set(model.primary_key_columns)
            for candidate in candidates:
                preagg = next((p for p in model.pre_aggregations if p.name == candidate.name), None)
                if candidate.selected and (preagg is None or not pk_columns.issubset(set(preagg.dimensions or []))):
                    candidate.selected = False
            if not any(c.selected for c in candidates):
                return QueryPlan(
                    sql=sql,
                    model=model_name,
                    metrics=bare_metrics,
                    dimensions=bare_dims,
                    used_preaggregation=False,
                    routing_reason="ungrouped query, no rollup carries the primary key for unique rows",
                    candidates=candidates,
                )

        selected = next((c for c in candidates if c.selected), None)
        if selected:
            return QueryPlan(
                sql=sql,
                model=model_name,
                metrics=bare_metrics,
                dimensions=bare_dims,
                used_preaggregation=True,
                selected_preagg=selected.name,
                routing_reason=f"matched '{selected.name}' (score: {selected.score})",
                candidates=candidates,
            )
        else:
            return QueryPlan(
                sql=sql,
                model=model_name,
                metrics=bare_metrics,
                dimensions=bare_dims,
                used_preaggregation=False,
                routing_reason="no pre-aggregation matched the query",
                candidates=candidates,
            )

    def get_model(self, name: str) -> Model:
        """Get model by name.

        Args:
            name: Model name

        Returns:
            Model instance
        """
        return self.graph.get_model(name)

    def get_metric(self, name: str) -> Metric:
        """Get measure by name.

        Args:
            name: Metric name

        Returns:
            Measure instance
        """
        return self.graph.get_metric(name)

    def list_models(self) -> list[str]:
        """List all model names.

        Returns:
            List of model names
        """
        return list(self.graph.models.keys())

    def list_metrics(self) -> list[str]:
        """List all metric names.

        Returns:
            List of metric names
        """
        return list(self.graph.metrics.keys())

    def get_catalog_metadata(self, schema: str = "public") -> dict:
        """Export semantic layer as Postgres-compatible catalog metadata.

        Returns metadata that can be used to populate information_schema
        and pg_catalog tables, enabling Postgres protocol compatibility.

        Similar to Cube.dev's SQL API, this exposes:
        - Models as tables in information_schema.tables
        - Dimensions and metrics as columns in information_schema.columns
        - Relationships as foreign keys in table_constraints

        Args:
            schema: Schema name to use (default: 'public')

        Returns:
            Dictionary containing:
            - tables: List of table metadata
            - columns: List of column metadata
            - constraints: List of constraint metadata
            - key_column_usage: Foreign key column mappings

        Example:
            >>> catalog = layer.get_catalog_metadata()
            >>> # Use for Postgres wire protocol
            >>> for table in catalog['tables']:
            ...     print(f"{table['table_name']}: {len([c for c in catalog['columns'] if c['table_name'] == table['table_name']])} columns")
        """
        from sidemantic.core.catalog import get_catalog_metadata

        return get_catalog_metadata(self.graph, schema=schema)

    @classmethod
    def from_yaml(
        cls,
        path: str | Path,
        connection: str | BaseDatabaseAdapter | None = None,  # type: ignore # noqa: F821
    ) -> SemanticLayer:
        """Load semantic layer from a native YAML or standalone TMDL file.

        Args:
            path: Path to YAML, SQL, or standalone TMDL file
            connection: Database connection string, adapter instance, or None
                (overrides connection in YAML file). Pass an adapter instance
                when your model files don't include connection config, e.g.:
                ``SemanticLayer.from_yaml("models.yaml", connection=ADBCAdapter(...))``

        Returns:
            SemanticLayer instance
        """
        path_obj = Path(path)
        if path_obj.suffix.lower() == ".tmdl":
            from sidemantic.adapters.tmdl import TMDLAdapter

            graph = TMDLAdapter().parse(path)
        else:
            import yaml

            from sidemantic.adapters.sidemantic import SidemanticAdapter, substitute_env_vars

            adapter = SidemanticAdapter()
            graph = adapter.parse(path)
            cls._mark_loaded_file_source(graph, source_format="Sidemantic", source_file=path_obj.name)

            # If connection not provided as parameter, try to read from YAML file
            # (skip for .sql files which may have multi-document YAML frontmatter)
            if connection is None and path_obj.suffix in (".yml", ".yaml"):
                with open(path) as f:
                    content = f.read()
                # Substitute environment variables
                content = substitute_env_vars(content)
                data = yaml.safe_load(content)
                if data and "connection" in data:
                    connection = data["connection"]

        # Convert dict-style connection config to URL string
        if isinstance(connection, dict):
            connection = cls._connection_dict_to_url(connection)

        # Create layer with connection (or use default if still None)
        if connection:
            layer = cls(connection=connection)
        else:
            layer = cls()
        layer.graph = graph

        return layer

    @staticmethod
    def _mark_loaded_file_source(graph, *, source_format: str, source_file: str) -> None:
        for model in graph.models.values():
            if not hasattr(model, "_source_format"):
                model._source_format = source_format
            if not hasattr(model, "_source_file"):
                model._source_file = source_file
        for metric in graph.metrics.values():
            if not hasattr(metric, "_source_format"):
                metric._source_format = source_format
            if not hasattr(metric, "_source_file"):
                metric._source_file = source_file

    @staticmethod
    def _connection_dict_to_url(config: dict) -> str:
        """Convert dict-style connection config to URL string.

        Supports YAML connection configurations like:
            connection:
              type: duckdb
              path: data/warehouse.db

            connection:
              type: postgres
              host: localhost
              port: 5432
              database: mydb
              user: myuser
              password: mypass

            connection:
              type: adbc
              driver: postgresql
              uri: postgresql://localhost/mydb

            connection:
              type: adbc
              driver: snowflake
              account: myaccount
              database: mydb

        Args:
            config: Connection configuration dictionary

        Returns:
            Connection URL string
        """
        from urllib.parse import quote, urlencode

        def quote_userinfo(value) -> str:
            return quote(str(value), safe="")

        conn_type = config.get("type", "duckdb").lower()

        if conn_type == "duckdb":
            path = config.get("path", ":memory:")
            if path.startswith("md:"):
                return f"duckdb://{path}"
            return f"duckdb:///{path}"

        elif conn_type in ("postgres", "postgresql"):
            host = config.get("host", "localhost")
            port = config.get("port", 5432)
            database = config.get("database", "postgres")
            user = config.get("user", "")
            password = config.get("password", "")

            if user and password:
                return f"postgres://{quote_userinfo(user)}:{quote_userinfo(password)}@{host}:{port}/{database}"
            elif user:
                return f"postgres://{quote_userinfo(user)}@{host}:{port}/{database}"
            else:
                return f"postgres://{host}:{port}/{database}"

        elif conn_type == "bigquery":
            project = config.get("project")
            dataset = config.get("dataset", "")
            if project:
                return f"bigquery://{project}/{dataset}"
            raise ValueError("BigQuery connection requires 'project' field")

        elif conn_type == "snowflake":
            account = config.get("account")
            user = config.get("user", "")
            password = config.get("password", "")
            database = config.get("database", "")
            schema = config.get("schema", "")

            if not account:
                raise ValueError("Snowflake connection requires 'account' field")

            path = f"/{database}" if database else ""
            if schema:
                path += f"/{schema}"

            if user and password:
                return f"snowflake://{quote_userinfo(user)}:{quote_userinfo(password)}@{account}{path}"
            elif user:
                return f"snowflake://{quote_userinfo(user)}@{account}{path}"
            else:
                return f"snowflake://{account}{path}"

        elif conn_type == "clickhouse":
            host = config.get("host", "localhost")
            port = config.get("port", 8123)
            database = config.get("database", "default")
            user = config.get("user", "")
            password = config.get("password", "")

            if user and password:
                return f"clickhouse://{quote_userinfo(user)}:{quote_userinfo(password)}@{host}:{port}/{database}"
            elif user:
                return f"clickhouse://{quote_userinfo(user)}@{host}:{port}/{database}"
            else:
                return f"clickhouse://{host}:{port}/{database}"

        elif conn_type == "databricks":
            server = config.get("server") or config.get("host")
            http_path = config.get("http_path")
            token = config.get("token", "")

            if not server:
                raise ValueError("Databricks connection requires 'server' or 'host' field")
            if not http_path:
                raise ValueError("Databricks connection requires 'http_path' field")

            return f"databricks://{quote_userinfo(token)}@{server}/{http_path}"

        elif conn_type == "spark":
            host = config.get("host", "localhost")
            port = config.get("port", 10000)
            database = config.get("database", "default")
            return f"spark://{host}:{port}/{database}"

        elif conn_type == "adbc":
            # ADBC connection: driver + optional uri + optional params
            driver = config.get("driver")
            if not driver:
                raise ValueError("ADBC connection requires 'driver' field")

            uri = config.get("uri")

            # Build query params from remaining fields
            params = {k: v for k, v in config.items() if k not in ("type", "driver", "uri")}

            if uri:
                params["uri"] = uri

            if params:
                return f"adbc://{driver}?{urlencode(params)}"
            return f"adbc://{driver}"

        else:
            raise ValueError(
                f"Unknown connection type: {conn_type}. "
                "Supported: duckdb, postgres, bigquery, snowflake, clickhouse, databricks, spark, adbc"
            )

    def sql(self, query: str):
        """Execute a SQL query against the semantic layer.

        Rewrites the SQL to use semantic layer metrics/dimensions and executes it.

        Args:
            query: SQL query like "SELECT revenue, status FROM orders WHERE status = 'completed'"

        Returns:
            DuckDB relation object (can convert to DataFrame with .df() or .to_df())

        Raises:
            ValueError: If SQL cannot be rewritten

        Example:
            >>> layer.sql("SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'")
        """
        from sidemantic.sql.query_rewriter import QueryRewriter

        cache_key = (
            getattr(self.graph, "_version", 0),
            self.dialect,
            self.use_preaggregations,
            os.getenv("SIDEMANTIC_RS_REWRITER", "0"),
            os.getenv("SIDEMANTIC_RS_NO_FALLBACK", "0"),
            query,
        )
        rewritten_sql = self._sql_rewrite_cache.get(cache_key)
        if rewritten_sql is None:
            rewriter = QueryRewriter(self.graph, dialect=self.dialect, use_preaggregations=self.use_preaggregations)
            rewritten_sql = rewriter.rewrite(query)
            if len(self._sql_rewrite_cache) >= self._sql_rewrite_cache_limit:
                self._sql_rewrite_cache.pop(next(iter(self._sql_rewrite_cache)))
            self._sql_rewrite_cache[cache_key] = rewritten_sql

        def recompile_raw():
            return QueryRewriter(self.graph, dialect=self.dialect, use_preaggregations=False).rewrite(query)

        return self._execute_with_preagg_fallback(
            rewritten_sql,
            recompile_raw,
            use_preaggs=self.use_preaggregations,
            strict=self.preagg_strict,
            used_preagg="used_preagg=true" in rewritten_sql,
        )

    def explain_sql(self, query: str, strict: bool = True):
        """Explain semantic SQL rewrite planning without executing the query.

        Args:
            query: SQL query like "SELECT orders.revenue, orders.status FROM orders"
            strict: If True, raise errors for invalid SQL or unsupported rewrites.
                    If False, return a passthrough explanation when possible.

        Returns:
            RewriteExplanation with the chosen plan, candidate plans, and rewritten SQL.
        """
        from sidemantic.sql.query_rewriter import QueryRewriter

        rewriter = QueryRewriter(self.graph, dialect=self.dialect, use_preaggregations=self.use_preaggregations)
        return rewriter.explain(query, strict=strict)

    def to_yaml(self, path: str | Path) -> None:
        """Export semantic layer to native YAML file.

        Args:
            path: Output file path
        """
        from sidemantic.adapters.sidemantic import SidemanticAdapter

        adapter = SidemanticAdapter()
        adapter.export(self.graph, path)
