"""Pre-aggregation definitions for query optimization."""

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class RefreshKey(BaseModel):
    """Refresh strategy configuration for pre-aggregations."""

    every: str | None = Field(None, description="Refresh interval (e.g., '1 hour', '1 day', '30 minutes')")
    sql: str | None = Field(None, description="SQL query that returns a value to trigger refresh when changed")
    incremental: bool = Field(False, description="Whether to use incremental refresh (only update changed partitions)")
    update_window: str | None = Field(
        None, description="Time window to refresh incrementally (e.g., '7 day', '1 month')"
    )


class Index(BaseModel):
    """Index definition for pre-aggregation performance."""

    name: str = Field(..., description="Index name")
    columns: list[str] = Field(..., description="Columns to index")
    type: Literal["regular", "aggregate"] = Field("regular", description="Index type")


class PreAggregation(BaseModel):
    """Pre-aggregation definition for automatic query optimization.

    Pre-aggregations are materialized rollup tables that store pre-computed
    aggregations. The query engine automatically routes queries to matching
    pre-aggregations for significant performance improvements.

    Example:
        >>> PreAggregation(
        ...     name="daily_rollup",
        ...     measures=["count", "revenue"],
        ...     dimensions=["status", "region"],
        ...     time_dimension="created_at",
        ...     granularity="day",
        ...     partition_granularity="month",
        ...     refresh_key=RefreshKey(every="1 hour", incremental=True)
        ... )
    """

    name: str = Field(..., description="Unique pre-aggregation name")

    type: Literal["rollup", "original_sql", "rollup_join", "lambda"] = Field(
        "rollup", description="Pre-aggregation type"
    )

    # Rollup configuration
    measures: list[str] | None = Field(None, description="Measures to pre-aggregate (e.g., ['count', 'revenue'])")
    dimensions: list[str] | None = Field(None, description="Dimensions to group by (e.g., ['status', 'region'])")
    time_dimension: str | None = Field(None, description="Time dimension for temporal grouping")
    granularity: Literal["hour", "day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Time granularity for aggregation"
    )

    # Partitioning
    partition_granularity: Literal["day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Partition size for incremental refresh"
    )

    # Refresh strategy
    refresh_key: RefreshKey | None = Field(None, description="Refresh strategy configuration")
    scheduled_refresh: bool = Field(True, description="Whether to enable scheduled refresh")

    # Performance
    indexes: list[Index] | None = Field(None, description="Index definitions for query performance")

    # Build range (for historical data)
    build_range_start: str | None = Field(None, description="SQL expression for start of data range to aggregate")
    build_range_end: str | None = Field(None, description="SQL expression for end of data range to aggregate")

    def get_table_name(self, model_name: str, database: str | None = None, schema: str | None = None) -> str:
        """Generate the physical table name for this pre-aggregation.

        Args:
            model_name: Name of the base model
            database: Optional database name (for cross-database queries)
            schema: Optional schema name (for schema-qualified tables)

        Returns:
            Qualified table name in format: [database.][schema.]{model_name}_preagg_{preagg_name}
        """
        table_name = f"{model_name}_preagg_{self.name}"

        # Build fully qualified name if database/schema provided
        if schema:
            table_name = f"{schema}.{table_name}"
        if database:
            table_name = f"{database}.{table_name}"

        return table_name

    def generate_materialization_sql(self, model: Any) -> str:
        """Generate SQL to materialize this pre-aggregation.

        Args:
            model: The Model object that owns this pre-aggregation

        Returns:
            SQL query to create/populate the pre-aggregation table

        Example:
            >>> preagg = PreAggregation(
            ...     name="daily",
            ...     measures=["count", "revenue"],
            ...     dimensions=["status"],
            ...     time_dimension="created_at",
            ...     granularity="day"
            ... )
            >>> sql = preagg.generate_materialization_sql(model)
            # Returns:
            # SELECT
            #   DATE_TRUNC('day', created_at) as created_at_day,
            #   status,
            #   COUNT(*) as count_raw,
            #   SUM(amount) as revenue_raw
            # FROM orders
            # GROUP BY 1, 2
        """
        select_exprs = []
        group_by_positions = []
        pos = 1

        # Add time dimension with granularity
        if self.time_dimension and self.granularity:
            time_dim = model.get_dimension(self.time_dimension)
            if time_dim:
                col_name = f"{self.time_dimension}_{self.granularity}"
                select_exprs.append(f"DATE_TRUNC('{self.granularity}', {time_dim.sql_expr}) as {col_name}")
                group_by_positions.append(str(pos))
                pos += 1

        # Add dimensions
        if self.dimensions:
            for dim_name in self.dimensions:
                dim = model.get_dimension(dim_name)
                if dim:
                    select_exprs.append(f"{dim.sql_expr} as {dim_name}")
                    group_by_positions.append(str(pos))
                    pos += 1

        # Add measures (aggregations)
        if self.measures:
            for measure_name in self.measures:
                measure = model.get_metric(measure_name)
                if measure:
                    # Generate aggregation expression
                    agg_type = measure.agg.upper()
                    if agg_type == "COUNT" and not measure.sql:
                        # COUNT(*) case
                        select_exprs.append(f"COUNT(*) as {measure_name}_raw")
                    elif agg_type == "COUNT_DISTINCT":
                        select_exprs.append(f"COUNT(DISTINCT {measure.sql_expr}) as {measure_name}_raw")
                    else:
                        select_exprs.append(f"{agg_type}({measure.sql_expr}) as {measure_name}_raw")

        # Build FROM clause
        if model.sql:
            from_clause = f"({model.sql}) AS t"
        else:
            from_clause = model.table

        # Build SQL
        select_str = ",\n  ".join(select_exprs)
        group_by_str = ", ".join(group_by_positions)

        sql = f"""SELECT
  {select_str}
FROM {from_clause}
GROUP BY {group_by_str}"""

        return sql

    def refresh(
        self,
        connection: Any,
        source_sql: str,
        table_name: str,
        mode: Literal["full", "incremental", "merge", "engine"] | None = None,
        watermark_column: str | None = None,
        lookback: str | None = None,
        from_watermark: Any | None = None,
        to_watermark: Any | None = None,
        dialect: str | None = None,
    ) -> "RefreshResult":
        """Refresh pre-aggregation (STATELESS).

        This method is designed to be called by external orchestrators (Airflow, Dagster, cron).
        Sidemantic provides HOW to refresh, not WHEN - scheduling is handled externally.

        Args:
            connection: Database connection to execute SQL
            source_sql: SQL query to populate the pre-aggregation
            table_name: Physical table name for the pre-aggregation
            mode: Refresh mode - 'full', 'incremental', or 'merge'. If None, infers from config
            watermark_column: Column to use for incremental refresh (required for incremental/merge)
            lookback: Time interval to reprocess (e.g., '7 days', '2 hours') for late-arriving data
            from_watermark: Starting watermark (optional, derived from table if None)
            to_watermark: Ending watermark (optional, uses NOW() if None)

        Returns:
            RefreshResult with refresh statistics

        Examples:
            # Full refresh (truncate and reload)
            >>> result = preagg.refresh(
            ...     connection=conn,
            ...     source_sql="SELECT date, SUM(revenue) as revenue FROM orders GROUP BY date",
            ...     table_name="orders_preagg_daily",
            ...     mode="full"
            ... )

            # Incremental append (stateless - derives watermark from table)
            >>> result = preagg.refresh(
            ...     connection=conn,
            ...     source_sql="SELECT date, SUM(revenue) as revenue FROM orders WHERE date > {WATERMARK}",
            ...     table_name="orders_preagg_daily",
            ...     mode="incremental",
            ...     watermark_column="date"
            ... )

            # Incremental with lookback (reprocess last 7 days)
            >>> result = preagg.refresh(
            ...     connection=conn,
            ...     source_sql="SELECT date, SUM(revenue) as revenue FROM orders WHERE date > {WATERMARK}",
            ...     table_name="orders_preagg_daily",
            ...     mode="incremental",
            ...     watermark_column="date",
            ...     lookback="7 days"
            ... )

            # Merge/upsert strategy (idempotent)
            >>> result = preagg.refresh(
            ...     connection=conn,
            ...     source_sql="SELECT date, region, SUM(revenue) as revenue FROM orders WHERE date > {WATERMARK} GROUP BY date, region",
            ...     table_name="orders_preagg_daily",
            ...     mode="merge",
            ...     watermark_column="date",
            ...     lookback="7 days"
            ... )

            # Airflow DAG example
            >>> from airflow import DAG
            >>> from airflow.operators.python import PythonOperator
            >>>
            >>> def refresh_preagg():
            ...     # Stateless - watermark stored in XCom or derived from table
            ...     result = preagg.refresh(
            ...         connection=get_connection(),
            ...         source_sql=sql,
            ...         table_name=table,
            ...         mode="incremental",
            ...         watermark_column="created_at"
            ...     )
            ...     return result.new_watermark  # Store for next run
            >>>
            >>> with DAG("refresh_preaggs", schedule="0 * * * *") as dag:
            ...     refresh_task = PythonOperator(
            ...         task_id="refresh_daily_rollup",
            ...         python_callable=refresh_preagg
            ...     )

            # Dagster asset example
            >>> from dagster import asset, OpExecutionContext
            >>>
            >>> @asset
            >>> def daily_orders_rollup(context: OpExecutionContext):
            ...     last_watermark = context.resources.watermark_store.get("daily_orders")
            ...     result = preagg.refresh(
            ...         connection=context.resources.db,
            ...         source_sql=sql,
            ...         table_name="orders_preagg_daily",
            ...         mode="incremental",
            ...         watermark_column="created_at",
            ...         from_watermark=last_watermark
            ...     )
            ...     context.resources.watermark_store.set("daily_orders", result.new_watermark)
            ...     return result

            # Cron script example
            >>> #!/usr/bin/env python
            >>> # Run every hour via cron: 0 * * * * /path/to/refresh_preaggs.py
            >>> import duckdb
            >>> from sidemantic.core.pre_aggregation import PreAggregation
            >>>
            >>> conn = duckdb.connect("data.db")
            >>> preagg = PreAggregation(name="daily_rollup", ...)
            >>> result = preagg.refresh(
            ...     connection=conn,
            ...     source_sql=sql,
            ...     table_name="orders_preagg_daily",
            ...     mode="incremental",
            ...     watermark_column="created_at"
            ... )
            >>> print(f"Refreshed {result.rows_inserted} rows in {result.duration_seconds}s")
        """
        start_time = time.time()

        # Infer mode from config if not specified
        if mode is None:
            if self.refresh_key and self.refresh_key.incremental:
                mode = "incremental"
            else:
                mode = "full"

        # Execute appropriate refresh strategy
        if mode == "full":
            rows_inserted, rows_updated, new_watermark = self._refresh_full(connection, source_sql, table_name)
        elif mode == "incremental":
            if not watermark_column:
                raise ValueError("watermark_column required for incremental refresh")
            rows_inserted, rows_updated, new_watermark = self._refresh_incremental(
                connection, source_sql, table_name, watermark_column, lookback, from_watermark, to_watermark
            )
        elif mode == "merge":
            if not watermark_column:
                raise ValueError("watermark_column required for merge refresh")
            rows_inserted, rows_updated, new_watermark = self._refresh_merge(
                connection, source_sql, table_name, watermark_column, lookback, from_watermark, to_watermark
            )
        elif mode == "engine":
            if not dialect:
                raise ValueError("dialect required for engine refresh mode")
            rows_inserted, rows_updated, new_watermark = self._refresh_engine(
                connection, source_sql, table_name, dialect
            )
        else:
            raise ValueError(f"Invalid refresh mode: {mode}")

        duration_seconds = time.time() - start_time

        return RefreshResult(
            mode=mode,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            new_watermark=new_watermark,
            duration_seconds=duration_seconds,
            timestamp=datetime.now(),
        )

    def _get_current_watermark(self, connection: Any, table_name: str, watermark_column: str) -> Any | None:
        """Get current watermark from table (STATELESS - no metadata table).

        Args:
            connection: Database connection
            table_name: Table to get watermark from
            watermark_column: Column to use as watermark

        Returns:
            Current max watermark value or None if table empty/doesn't exist
        """
        try:
            result = connection.execute(f"SELECT MAX({watermark_column}) as max_watermark FROM {table_name}").fetchone()
            return result[0] if result else None
        except Exception:
            # Table doesn't exist yet
            return None

    def _refresh_full(self, connection: Any, source_sql: str, table_name: str) -> tuple[int, int, Any | None]:
        """Execute full refresh (truncate and reload).

        Args:
            connection: Database connection
            source_sql: SQL query to populate table
            table_name: Target table name

        Returns:
            (rows_inserted, rows_updated, new_watermark)
        """
        # Drop and recreate table
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.execute(f"CREATE TABLE {table_name} AS {source_sql}")

        # Count rows inserted
        result = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        rows_inserted = result[0] if result else 0

        return (rows_inserted, 0, None)

    def _refresh_incremental(
        self,
        connection: Any,
        source_sql: str,
        table_name: str,
        watermark_column: str,
        lookback: str | None,
        from_watermark: Any | None,
        to_watermark: Any | None,
    ) -> tuple[int, int, Any | None]:
        """Execute incremental refresh (append new data).

        Args:
            connection: Database connection
            source_sql: SQL query with {WATERMARK} placeholder
            table_name: Target table name
            watermark_column: Column to use for watermarking
            lookback: Time interval to reprocess (e.g., '7 days')
            from_watermark: Starting watermark (if None, derived from table)
            to_watermark: Ending watermark (if None, uses current time)

        Returns:
            (rows_inserted, rows_updated, new_watermark)
        """
        # Get current watermark (stateless - from table or parameter)
        current_watermark = from_watermark
        if current_watermark is None:
            current_watermark = self._get_current_watermark(connection, table_name, watermark_column)

        # Default to beginning of time if no watermark
        if current_watermark is None:
            current_watermark = "'1970-01-01'"

        # Quote watermark if it's not already quoted
        watermark_str = str(current_watermark)
        if not watermark_str.startswith("'"):
            watermark_str = f"'{watermark_str}'"

        # Apply lookback if specified (cast to TIMESTAMP for interval arithmetic)
        if lookback:
            watermark_str = f"(CAST({watermark_str} AS TIMESTAMP) - INTERVAL '{lookback}')"

        # Substitute watermark in SQL
        incremental_sql = source_sql.replace("{WATERMARK}", watermark_str)

        # Create table if it doesn't exist
        table_exists = False
        try:
            connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            table_exists = True
        except Exception:
            pass

        if not table_exists:
            connection.execute(f"CREATE TABLE {table_name} AS {incremental_sql}")
        else:
            connection.execute(f"INSERT INTO {table_name} {incremental_sql}")

        # Get new watermark
        new_watermark = self._get_current_watermark(connection, table_name, watermark_column)

        # Count rows inserted (approximate - could get from INSERT result if supported)
        # For now, we don't have exact count from append
        rows_inserted = -1  # Unknown for append

        return (rows_inserted, 0, new_watermark)

    def _refresh_merge(
        self,
        connection: Any,
        source_sql: str,
        table_name: str,
        watermark_column: str,
        lookback: str | None,
        from_watermark: Any | None,
        to_watermark: Any | None,
    ) -> tuple[int, int, Any | None]:
        """Execute merge refresh (upsert strategy for idempotent updates).

        Args:
            connection: Database connection
            source_sql: SQL query with {WATERMARK} placeholder
            table_name: Target table name
            watermark_column: Column to use for watermarking
            lookback: Time interval to reprocess (e.g., '7 days')
            from_watermark: Starting watermark (if None, derived from table)
            to_watermark: Ending watermark (if None, uses current time)

        Returns:
            (rows_inserted, rows_updated, new_watermark)
        """
        # Get current watermark (stateless - from table or parameter)
        current_watermark = from_watermark
        if current_watermark is None:
            current_watermark = self._get_current_watermark(connection, table_name, watermark_column)

        # Default to beginning of time if no watermark
        if current_watermark is None:
            current_watermark = "'1970-01-01'"

        # Quote watermark if it's not already quoted
        watermark_str = str(current_watermark)
        if not watermark_str.startswith("'"):
            watermark_str = f"'{watermark_str}'"

        # Apply lookback if specified (cast to TIMESTAMP for interval arithmetic)
        # For merge, we'll need both the SQL watermark and DELETE watermark
        delete_watermark_str = watermark_str
        if lookback:
            watermark_str = f"(CAST({watermark_str} AS TIMESTAMP) - INTERVAL '{lookback}')"
            # DELETE should also use the lookback watermark
            delete_watermark_str = watermark_str

        # Substitute watermark in SQL
        incremental_sql = source_sql.replace("{WATERMARK}", watermark_str)

        # Create table if it doesn't exist
        table_exists = False
        try:
            connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            table_exists = True
        except Exception:
            pass

        if not table_exists:
            connection.execute(f"CREATE TABLE {table_name} AS {incremental_sql}")
            new_watermark = self._get_current_watermark(connection, table_name, watermark_column)
            return (-1, 0, new_watermark)

        # For merge, we need to know the key columns (dimensions + time_dimension)
        # Delete old data in the refresh window and insert new
        # This makes the refresh idempotent
        temp_table = f"{table_name}_merge_temp"

        # Create temp table with new data
        connection.execute(f"DROP TABLE IF EXISTS {temp_table}")
        connection.execute(f"CREATE TABLE {temp_table} AS {incremental_sql}")

        # Delete overlapping data from target table (use lookback watermark if specified)
        connection.execute(
            f"""
            DELETE FROM {table_name}
            WHERE {watermark_column} >= {delete_watermark_str}
        """
        )

        # Insert new data
        connection.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table}")

        # Clean up temp table
        connection.execute(f"DROP TABLE {temp_table}")

        # Get new watermark
        new_watermark = self._get_current_watermark(connection, table_name, watermark_column)

        return (-1, -1, new_watermark)

    def _validate_sql_for_engine(self, source_sql: str, dialect: str) -> tuple[bool, str | None]:
        """Validate SQL is compatible with database-native materialized views.

        Args:
            source_sql: SQL query to validate
            dialect: Database dialect (snowflake, clickhouse, bigquery)

        Returns:
            (is_valid, error_message)
        """
        import sqlglot

        # Parse SQL to check for unsupported features
        try:
            parsed = sqlglot.parse_one(source_sql, dialect=dialect)
        except Exception as e:
            return (False, f"Failed to parse SQL: {e}")

        # Check for window functions (not supported in most materialized views)
        for node in parsed.walk():
            if isinstance(node, sqlglot.exp.Window):
                return (False, "Window functions not supported in materialized views")

        # Dialect-specific restrictions
        if dialect == "snowflake":
            # Snowflake DYNAMIC TABLES don't support certain features
            for node in parsed.walk():
                if isinstance(node, sqlglot.exp.Join):
                    # Joins are supported but with restrictions
                    pass  # Allow for now
                if isinstance(node, sqlglot.exp.Subquery):
                    # Subqueries in SELECT are restricted
                    if node.parent and isinstance(node.parent, sqlglot.exp.Select):
                        return (False, "Scalar subqueries not fully supported in Snowflake DYNAMIC TABLES")

        elif dialect == "clickhouse":
            # ClickHouse materialized views have specific restrictions
            # They must be simple aggregations
            pass  # Allow for now

        elif dialect == "bigquery":
            # BigQuery materialized views don't support certain features
            for node in parsed.walk():
                if isinstance(node, sqlglot.exp.Join):
                    join_type = node.args.get("kind", "")
                    if join_type and join_type.upper() not in ["INNER", "LEFT", "RIGHT", "FULL"]:
                        return (False, f"BigQuery materialized views don't support {join_type} joins")

        return (True, None)

    def _refresh_engine(
        self,
        connection: Any,
        source_sql: str,
        table_name: str,
        dialect: str,
    ) -> tuple[int, int, Any | None]:
        """Execute refresh using database-native materialized views.

        Args:
            connection: Database connection
            source_sql: SQL query to materialize
            table_name: Target table/view name
            dialect: Database dialect (snowflake, clickhouse, bigquery)

        Returns:
            (rows_inserted, rows_updated, new_watermark)
        """
        # Validate SQL is compatible with engine
        is_valid, error_msg = self._validate_sql_for_engine(source_sql, dialect)
        if not is_valid:
            raise ValueError(f"SQL not compatible with {dialect} materialized views: {error_msg}")

        # Generate database-specific DDL
        if dialect == "snowflake":
            # Snowflake DYNAMIC TABLE
            # Get refresh interval from config
            refresh_interval = "1 HOUR"  # Default
            if self.refresh_key and self.refresh_key.every:
                # Convert "1 hour" to "1 HOUR"
                refresh_interval = self.refresh_key.every.replace(" ", " ").upper()

            ddl = f"""
CREATE OR REPLACE DYNAMIC TABLE {table_name}
TARGET_LAG = '{refresh_interval}'
WAREHOUSE = 'COMPUTE_WH'
AS
{source_sql}
            """.strip()

        elif dialect == "clickhouse":
            # ClickHouse MATERIALIZED VIEW
            # Extract target table from view name
            target_table = f"{table_name}_data"

            ddl = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}
TO {target_table}
AS
{source_sql}
            """.strip()

        elif dialect == "bigquery":
            # BigQuery MATERIALIZED VIEW
            # Get refresh interval from config
            enable_refresh = "true"
            refresh_interval_minutes = 60  # Default 1 hour

            if self.refresh_key and self.refresh_key.every:
                # Parse interval
                parts = self.refresh_key.every.split()
                if len(parts) == 2:
                    value, unit = parts
                    value = int(value)
                    if "minute" in unit.lower():
                        refresh_interval_minutes = value
                    elif "hour" in unit.lower():
                        refresh_interval_minutes = value * 60
                    elif "day" in unit.lower():
                        refresh_interval_minutes = value * 60 * 24

            ddl = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}
OPTIONS(
  enable_refresh = {enable_refresh},
  refresh_interval_minutes = {refresh_interval_minutes}
)
AS
{source_sql}
            """.strip()

        else:
            raise ValueError(f"Unsupported dialect for engine mode: {dialect}")

        # Execute DDL
        connection.execute(ddl)

        # Engine mode: rows are managed by database, return -1
        return (-1, -1, None)


@dataclass
class RefreshResult:
    """Result of pre-aggregation refresh (STATELESS).

    External orchestrators should store the new_watermark for the next refresh.
    """

    mode: Literal["full", "incremental", "merge", "engine"]
    rows_inserted: int  # -1 if unknown
    rows_updated: int  # -1 if unknown
    new_watermark: Any | None  # Orchestrator stores this for next run
    duration_seconds: float
    timestamp: datetime
