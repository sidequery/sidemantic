"""Semantic layer main API."""

from __future__ import annotations

from pathlib import Path

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


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
    ):
        """Initialize semantic layer.

        Args:
            connection: Database connection string or adapter instance (default: in-memory DuckDB)
                Supported URLs:
                - duckdb:///:memory: (default)
                - duckdb:///path/to/db.duckdb
                - postgres://user:pass@host:port/dbname
                - bigquery://project_id/dataset_id
                - snowflake://user:password@account/database/schema
                - clickhouse://user:password@host:port/database
            dialect: SQL dialect for query generation (optional, inferred from adapter)
            auto_register: Set as current layer for auto-registration (default: True)
            use_preaggregations: Enable automatic pre-aggregation routing (default: False)
        """
        from sidemantic.db.base import BaseDatabaseAdapter

        self.graph = SemanticGraph()
        self.use_preaggregations = use_preaggregations

        # Initialize adapter from connection string or use provided adapter
        if isinstance(connection, BaseDatabaseAdapter):
            self.adapter = connection
            self.dialect = dialect or connection.dialect
            self.connection_string = f"{connection.dialect}://custom"
        elif isinstance(connection, str):
            self.connection_string = connection

            if connection.startswith("duckdb://"):
                from sidemantic.db.duckdb import DuckDBAdapter

                self.adapter = DuckDBAdapter.from_url(connection)
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
            else:
                raise ValueError(
                    f"Unsupported connection URL: {connection}. "
                    "Supported: duckdb:///, postgres://, bigquery://, snowflake://, clickhouse://, or BaseDatabaseAdapter instance"
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
        """Context manager exit - clear current layer."""
        from .registry import set_current_layer

        set_current_layer(None)

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
            if existing.model_dump() == model.model_dump():
                return

        errors = validate_model(model)
        if errors:
            raise ModelValidationError(
                f"Model '{model.name}' validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        self.graph.add_model(model)

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

    def query(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        ungrouped: bool = False,
        parameters: dict[str, any] | None = None,
        use_preaggregations: bool | None = None,
    ):
        """Execute a query against the semantic layer.

        Args:
            metrics: List of metric references (e.g., ["orders.revenue"])
            dimensions: List of dimension references (e.g., ["orders.status", "orders.order_date__month"])
            filters: List of filter expressions (e.g., ["orders.status = 'completed'"])
            segments: List of segment references (e.g., ["orders.active_users"])
            order_by: List of fields to order by
            limit: Maximum number of rows to return
            ungrouped: If True, return raw rows without aggregation (no GROUP BY)
            parameters: Template parameters for Jinja2 rendering
            use_preaggregations: Override pre-aggregation routing setting for this query

        Returns:
            DuckDB relation object (can convert to DataFrame with .df() or .to_df())
        """
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
        )

        return self.adapter.execute(sql)

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

        Returns:
            SQL query string

        Raises:
            QueryValidationError: If query validation fails
        """
        from sidemantic.validation import QueryValidationError, validate_query

        metrics = metrics or []
        dimensions = dimensions or []

        # Validate query
        errors = validate_query(metrics, dimensions, self.graph)
        if errors:
            raise QueryValidationError("Query validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

        # Determine if pre-aggregations should be used
        use_preaggs = use_preaggregations if use_preaggregations is not None else self.use_preaggregations

        generator = SQLGenerator(self.graph, dialect=dialect or self.dialect)

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
            use_preaggregations=use_preaggs,
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
    def from_yaml(cls, path: str | Path, connection: str = "duckdb:///:memory:") -> SemanticLayer:
        """Load semantic layer from native YAML file.

        Args:
            path: Path to YAML file
            connection: Database connection string

        Returns:
            SemanticLayer instance
        """
        from sidemantic.adapters.sidemantic import SidemanticAdapter

        adapter = SidemanticAdapter()
        graph = adapter.parse(path)

        layer = cls(connection=connection)
        layer.graph = graph

        return layer

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

        rewriter = QueryRewriter(self.graph, dialect=self.dialect)
        rewritten_sql = rewriter.rewrite(query)

        return self.conn.execute(rewritten_sql)

    def to_yaml(self, path: str | Path) -> None:
        """Export semantic layer to native YAML file.

        Args:
            path: Output file path
        """
        from sidemantic.adapters.sidemantic import SidemanticAdapter

        adapter = SidemanticAdapter()
        adapter.export(self.graph, path)
