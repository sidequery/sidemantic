"""Semantic layer main API."""

from pathlib import Path

import duckdb

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator_v2 import SQLGenerator


class SemanticLayer:
    """Main semantic layer interface.

    Provides a high-level API for defining models and querying data.
    """

    def __init__(self, connection: str = "duckdb:///:memory:", dialect: str = "duckdb", auto_register: bool = False, use_preaggregations: bool = False):
        """Initialize semantic layer.

        Args:
            connection: Database connection string (default: in-memory DuckDB)
            dialect: SQL dialect for query generation (default: duckdb)
            auto_register: Set as current layer for auto-registration (default: True)
            use_preaggregations: Enable automatic pre-aggregation routing (default: False)
        """
        self.graph = SemanticGraph()
        self.dialect = dialect
        self.connection_string = connection
        self.use_preaggregations = use_preaggregations

        # Initialize DuckDB connection
        if connection.startswith("duckdb://"):
            db_path = connection.replace("duckdb:///", "") or ":memory:"
            self.conn = duckdb.connect(db_path)
        else:
            raise NotImplementedError(f"Connection type {connection} not yet supported")

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

    def add_model(self, model: Model) -> None:
        """Add a model to the semantic layer.

        Args:
            model: Model to add

        Raises:
            ModelValidationError: If model validation fails
        """
        from sidemantic.validation import ModelValidationError, validate_model

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
            use_preaggregations: Override pre-aggregation routing setting for this query

        Returns:
            DuckDB relation object (can convert to DataFrame with .df() or .to_df())
        """
        sql = self.compile(
            metrics=metrics, dimensions=dimensions, filters=filters, segments=segments, order_by=order_by, limit=limit, ungrouped=ungrouped, use_preaggregations=use_preaggregations
        )

        return self.conn.execute(sql)

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
            raise QueryValidationError(
                "Query validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        # Determine if pre-aggregations should be used
        use_preaggs = use_preaggregations if use_preaggregations is not None else self.use_preaggregations

        generator = SQLGenerator(self.graph, dialect=dialect or self.dialect)

        return generator.generate(
            metrics=metrics, dimensions=dimensions, filters=filters, segments=segments, order_by=order_by, limit=limit, offset=offset, ungrouped=ungrouped, parameters=parameters, use_preaggregations=use_preaggs
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

    @classmethod
    def from_yaml(cls, path: str | Path, connection: str = "duckdb:///:memory:") -> "SemanticLayer":
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
