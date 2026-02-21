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
        preagg_database: str | None = None,
        preagg_schema: str | None = None,
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
            preagg_database: Optional database name for pre-aggregation tables
            preagg_schema: Optional schema name for pre-aggregation tables
        """
        from sidemantic.db.base import BaseDatabaseAdapter

        self.graph = SemanticGraph()
        self.use_preaggregations = use_preaggregations
        self.preagg_database = preagg_database
        self.preagg_schema = preagg_schema

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
            elif connection.startswith("duckdb://"):
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

        generator = SQLGenerator(
            self.graph,
            dialect=dialect or self.dialect,
            preagg_database=self.preagg_database,
            preagg_schema=self.preagg_schema,
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
    def from_yaml(
        cls,
        path: str | Path,
        connection: str | BaseDatabaseAdapter | None = None,  # type: ignore # noqa: F821
    ) -> SemanticLayer:
        """Load semantic layer from native YAML file.

        Args:
            path: Path to YAML file
            connection: Database connection string, adapter instance, or None
                (overrides connection in YAML file). Pass an adapter instance
                when your model files don't include connection config, e.g.:
                ``SemanticLayer.from_yaml("models.yaml", connection=ADBCAdapter(...))``

        Returns:
            SemanticLayer instance
        """
        import yaml

        from sidemantic.adapters.sidemantic import SidemanticAdapter, substitute_env_vars

        adapter = SidemanticAdapter()
        graph = adapter.parse(path)

        # If connection not provided as parameter, try to read from YAML file
        # (skip for .sql files which may have multi-document YAML frontmatter)
        path_obj = Path(path)
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
                return f"postgres://{quote(user)}:{quote(password)}@{host}:{port}/{database}"
            elif user:
                return f"postgres://{quote(user)}@{host}:{port}/{database}"
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
                return f"snowflake://{quote(user)}:{quote(password)}@{account}{path}"
            elif user:
                return f"snowflake://{quote(user)}@{account}{path}"
            else:
                return f"snowflake://{account}{path}"

        elif conn_type == "clickhouse":
            host = config.get("host", "localhost")
            port = config.get("port", 8123)
            database = config.get("database", "default")
            user = config.get("user", "")
            password = config.get("password", "")

            if user and password:
                return f"clickhouse://{quote(user)}:{quote(password)}@{host}:{port}/{database}"
            elif user:
                return f"clickhouse://{quote(user)}@{host}:{port}/{database}"
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

            return f"databricks://{token}@{server}/{http_path}"

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

        rewriter = QueryRewriter(self.graph, dialect=self.dialect)
        rewritten_sql = rewriter.rewrite(query)

        return self.adapter.execute(rewritten_sql)

    def to_yaml(self, path: str | Path) -> None:
        """Export semantic layer to native YAML file.

        Args:
            path: Output file path
        """
        from sidemantic.adapters.sidemantic import SidemanticAdapter

        adapter = SidemanticAdapter()
        adapter.export(self.graph, path)
