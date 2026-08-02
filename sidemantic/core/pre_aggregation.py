"""Pre-aggregation definitions for query optimization."""

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

# Dialects with standard CREATE INDEX support for pre-aggregation tables.
# Snowflake/BigQuery/ClickHouse manage data layout via clustering/sort keys
# instead, so index DDL is skipped for them.
_INDEX_DDL_DIALECTS = {"duckdb", "postgres"}


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
    sql: str | None = Field(None, description="SQL for original_sql or custom pre-aggregation definitions")

    # Rollup configuration
    measures: list[str] | None = Field(None, description="Measures to pre-aggregate (e.g., ['count', 'revenue'])")
    dimensions: list[str] | None = Field(None, description="Dimensions to group by (e.g., ['status', 'region'])")
    time_dimension: str | None = Field(None, description="Time dimension for temporal grouping")
    granularity: Literal["second", "minute", "hour", "day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Time granularity for aggregation"
    )

    # Lambda (rollupLambda) configuration: union of a batch rollup with fresher data.
    rollups: list[str] | None = Field(
        None,
        description=(
            "For type='lambda' (Cube rollupLambda): the constituent rollups this lambda unions. Stored for "
            "round-trip; query routing matches this lambda directly on its own measures/dimensions/granularity."
        ),
    )
    union_with_source_data: bool = Field(
        False,
        description=(
            "For type='lambda': when True (and build_range_end is set), serve a query as a UNION of the batch "
            "rollup table (buckets before build_range_end) with a fresh aggregation of source rows at/after "
            "build_range_end, re-aggregated at the query grain."
        ),
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
    meta: dict[str, Any] | None = Field(None, description="Adapter-specific metadata payload")

    def get_table_name(self, model_name: str, database: str | None = None, schema: str | None = None) -> str:
        """Generate the physical table name for this pre-aggregation.

        Args:
            model_name: Name of the base model
            database: Optional database name (for cross-database queries)
            schema: Optional schema name (for schema-qualified tables)

        Returns:
            Qualified table name in format: [database.][schema.]{model_name}_preagg_{preagg_name}
        """
        import re

        # Validate identifiers to prevent SQL injection
        for name, label in [(model_name, "model"), (self.name, "preagg")]:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
                raise ValueError(f"Invalid {label} name for table generation: {name}")
        if schema and not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", schema):
            raise ValueError(f"Invalid schema name: {schema}")
        if database and not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", database):
            raise ValueError(f"Invalid database name: {database}")

        table_name = f"{model_name}_preagg_{self.name}"

        # Build fully qualified name if database/schema provided
        if schema:
            table_name = f"{schema}.{table_name}"
        if database:
            table_name = f"{database}.{table_name}"

        return table_name

    def generate_materialization_sql(self, model: Any, partition_filter: str | None = None) -> str:
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
        # original_sql pre-aggregations stage the cube's base query verbatim (no
        # GROUP BY) so heavier rollups or ad-hoc queries can build on a
        # materialized base instead of re-running it. They are not aggregation
        # rollups, so the matcher never routes metric queries to them directly.
        if self.type == "original_sql":
            if self.sql:
                # self.sql may carry {model} placeholders (e.g. normalized from Cube's ${CUBE}),
                # used both as a FROM source and as a column qualifier ({model}.col). A custom
                # query with no placeholder is staged verbatim.
                if "{model}" not in self.sql:
                    return self.sql
                if getattr(model, "sql", None):
                    # sql-backed model: expose its query as an aliased CTE and point {model} at
                    # that alias. Inlining a bare "(SELECT ...)" instead would produce invalid
                    # "(SELECT ...).col" column qualifiers in DuckDB/Postgres-style dialects.
                    alias = f"{model.name}__base"
                    return f"WITH {alias} AS (\n{model.sql}\n)\n{self.sql.replace('{model}', alias)}"
                # table-backed model: {model} resolves directly to the table name.
                return self.sql.replace("{model}", model.table)
            base_sql = getattr(model, "sql", None)
            if base_sql:
                return base_sql
            return f"SELECT * FROM {model.table}"

        select_exprs = []
        group_by_positions = []
        pos = 1

        # Add time dimension with granularity
        if self.time_dimension and self.granularity:
            time_dim = model.get_dimension(self.time_dimension)
            if time_dim:
                if time_dim.window:
                    raise ValueError(
                        f"Cannot use window dimension '{self.time_dimension}' as time_dimension "
                        f"in pre-aggregation '{self.name}': window functions are incompatible "
                        f"with GROUP BY in rollup materialization"
                    )
                col_name = f"{self.time_dimension}_{self.granularity}"
                select_exprs.append(f"DATE_TRUNC('{self.granularity}', {time_dim.sql_expr}) as {col_name}")
                group_by_positions.append(str(pos))
                pos += 1

        # Add dimensions (reject window dimensions - incompatible with GROUP BY)
        if self.dimensions:
            for dim_name in self.dimensions:
                dim = model.get_dimension(dim_name)
                if dim:
                    if dim.window:
                        raise ValueError(
                            f"Cannot use window dimension '{dim_name}' in pre-aggregation "
                            f"'{self.name}': window functions are incompatible with "
                            f"GROUP BY in rollup materialization"
                        )
                    select_exprs.append(f"{dim.sql_expr} as {dim_name}")
                    group_by_positions.append(str(pos))
                    pos += 1

        # Add measures (aggregations)
        if self.measures:
            for measure_name in self.measures:
                measure = model.get_metric(measure_name)
                if measure:
                    if measure.agg is None:
                        # Measures without a plain aggregation are not materializable rollup
                        # columns: derived measures are computed at query time from their
                        # dependencies, and complete-expression measures (number_agg /
                        # PERCENTILE) are not re-aggregatable. Skip them (the matcher likewise
                        # never routes complete-expression measures to a rollup).
                        continue
                    # Generate aggregation expression. Metric filters belong inside
                    # the aggregate input, exactly as they do for live queries; a
                    # rollup must never materialize an unfiltered value for a
                    # filtered semantic metric.
                    agg_type = measure.agg.upper()
                    filter_sql = " AND ".join(
                        condition.replace("{model}.", "").replace("{model}", "")
                        for condition in (measure.filters or [])
                    )
                    measure_input = measure.sql_expr
                    if filter_sql:
                        if agg_type == "COUNT" and not measure.sql:
                            measure_input = f"CASE WHEN {filter_sql} THEN 1 ELSE NULL END"
                        else:
                            measure_input = f"CASE WHEN {filter_sql} THEN {measure_input} ELSE NULL END"

                    if agg_type == "COUNT" and not measure.sql and not filter_sql:
                        # COUNT(*) case
                        select_exprs.append(f"COUNT(*) as {measure_name}_raw")
                    elif agg_type == "COUNT" and not measure.sql:
                        select_exprs.append(f"COUNT({measure_input}) as {measure_name}_raw")
                    elif agg_type == "AVG":
                        # Store AVG as additive sum state. A compatible count
                        # measure must also be present before query planning can
                        # roll this up safely.
                        select_exprs.append(f"SUM({measure_input}) as {measure_name}_raw")
                    elif agg_type == "COUNT_DISTINCT":
                        select_exprs.append(f"COUNT(DISTINCT {measure_input}) as {measure_name}_raw")
                    else:
                        select_exprs.append(f"{agg_type}({measure_input}) as {measure_name}_raw")

        # A rollup that projects nothing would render "SELECT  FROM ... GROUP BY " (invalid
        # SQL). This happens when its measures are all non-materializable (agg=None: derived or
        # complete-expression like number_agg/PERCENTILE) and it declares no dimensions or time
        # dimension. Reject it explicitly so the refresh command surfaces a clear error instead
        # of a database parse failure.
        if not select_exprs:
            raise ValueError(
                f"Pre-aggregation '{self.name}' has no materializable columns: its measures are all "
                f"non-aggregatable (derived or complete-expression) and it declares no dimensions or "
                f"time dimension. Add a plain aggregate measure or a dimension, or drop this rollup."
            )

        # Build FROM clause
        if model.sql:
            from_clause = f"({model.sql}) AS t"
        else:
            from_clause = model.table

        # Build SQL. partition_filter constrains the source rows to a single time
        # bucket so partitioned builds materialize one table per partition.
        select_str = ",\n  ".join(select_exprs)
        group_by_str = ", ".join(group_by_positions)
        where_clause = f"\nWHERE {partition_filter}" if partition_filter else ""

        sql = f"""SELECT
  {select_str}
FROM {from_clause}{where_clause}"""
        if group_by_str:
            sql += f"\nGROUP BY {group_by_str}"

        return sql

    def build_partitions(
        self,
        connection: Any,
        model: Any,
        *,
        database: str | None = None,
        schema: str | None = None,
        lookback: str | None = None,
        build_range_start: Any | None = None,
        build_range_end: Any | None = None,
        full_rebuild: bool = False,
    ) -> list[str]:
        """Materialize a partitioned pre-aggregation as one table per time bucket.

        Builds (or rebuilds) a physical table for each ``partition_granularity``
        time bucket present in the source data, then (re)creates a covering view
        named like the non-partitioned table so query routing reads all partitions
        transparently and the engine prunes by the query's time filter.

        When ``full_rebuild`` is True, every bucket in range is rebuilt and any
        partition table whose bucket is no longer in the source is dropped, so the
        covering view never returns stale data. Otherwise only partitions inside the
        ``lookback`` (or a declared ``refresh_key.update_window``) are rebuilt and
        older partitions are preserved as immutable history.

        Returns the list of partition table names that were (re)built.
        """
        import re

        if not self.partition_granularity:
            raise ValueError(f"Pre-aggregation '{self.name}' has no partition_granularity to build")
        if not (self.time_dimension and self.granularity):
            raise ValueError(f"Partitioned pre-aggregation '{self.name}' requires time_dimension and granularity")

        time_dim = model.get_dimension(self.time_dimension)
        if not time_dim:
            raise ValueError(f"Unknown time_dimension '{self.time_dimension}' on model '{model.name}'")
        time_expr = time_dim.sql_expr
        from_clause = f"({model.sql}) AS t" if model.sql else model.table
        pg = self.partition_granularity

        for name, label in [(model.name, "model"), (self.name, "preagg")]:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
                raise ValueError(f"Invalid {label} name for partition table generation: {name}")
        unqualified = f"{model.name}_preagg_{self.name}"

        def qualify(unqualified_name: str) -> str:
            qualified = unqualified_name
            if schema:
                qualified = f"{schema}.{qualified}"
            if database:
                qualified = f"{database}.{qualified}"
            return qualified

        # A declared update_window is the default reprocessing window for incremental
        # builds; a full rebuild ignores it and rebuilds every bucket in range.
        if full_rebuild:
            lookback = None
        elif lookback is None and self.refresh_key and self.refresh_key.update_window:
            lookback = self.refresh_key.update_window

        range_preds = []
        if build_range_start is not None:
            range_preds.append(f"{time_expr} >= '{build_range_start}'")
        if build_range_end is not None:
            range_preds.append(f"{time_expr} < '{build_range_end}'")
        range_where = (" WHERE " + " AND ".join(range_preds)) if range_preds else ""

        # Discover the buckets present in the source data.
        bucket_rows = connection.execute(
            f"SELECT DISTINCT DATE_TRUNC('{pg}', {time_expr}) AS bucket FROM {from_clause}{range_where} ORDER BY 1"
        ).fetchall()
        buckets = [row[0] for row in bucket_rows if row[0] is not None]

        # With a lookback, only rebuild buckets at/after the cutoff (immutable history).
        cutoff = None
        if lookback is not None:
            cutoff_row = connection.execute(
                f"SELECT DATE_TRUNC('{pg}', CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL '{lookback}')"
            ).fetchone()
            cutoff = cutoff_row[0] if cutoff_row else None

        built: list[str] = []
        for bucket in buckets:
            if cutoff is not None and bucket < cutoff:
                continue
            key = re.sub(r"[^0-9]", "", str(bucket)) or "0"
            part_name = qualify(f"{unqualified}_p{key}")
            partition_filter = f"DATE_TRUNC('{pg}', {time_expr}) = TIMESTAMP '{bucket}'"
            source_sql = self.generate_materialization_sql(model, partition_filter=partition_filter)
            connection.execute(f"DROP TABLE IF EXISTS {part_name}")
            connection.execute(f"CREATE TABLE {part_name} AS {source_sql}")
            built.append(part_name)

        # Discover existing partition tables IN THE TARGET SCHEMA only (those just built
        # plus any from prior runs). Filtering by schema prevents a same-named partition
        # in another schema from being mis-qualified into this one (which would make the
        # covering view reference a nonexistent table, or a full rebuild drop the wrong one).
        if schema:
            effective_schema = schema
        else:
            try:
                effective_schema = connection.execute("SELECT current_schema()").fetchone()[0]
            except Exception:
                effective_schema = None
        prefix = f"{unqualified}_p"
        all_tables = connection.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
        existing = {
            qualify(tname)
            for tschema, tname in all_tables
            if isinstance(tname, str)
            and tname.startswith(prefix)
            and (effective_schema is None or tschema == effective_schema)
        }

        if full_rebuild:
            # A full rebuild reflects exactly the current source buckets: drop partitions
            # whose bucket is no longer present so the covering view returns no stale data.
            for stale_table in sorted(existing - set(built)):
                connection.execute(f"DROP TABLE IF EXISTS {stale_table}")
            partition_tables = sorted(built)
        else:
            # Incremental: keep immutable older partitions alongside the rebuilt ones.
            partition_tables = sorted(existing | set(built))

        view_name = qualify(unqualified)
        # Always drop the prior covering view/table first: a full rebuild may have
        # dropped partitions it referenced, and a rebuild that finds no buckets must
        # not leave a view pointing at removed tables. The base name may be a view
        # (prior partitioned build) or a table (prior non-partitioned build), and
        # some engines' DROP ... IF EXISTS errors on a type mismatch, so try both.
        for drop_stmt in (f"DROP VIEW IF EXISTS {view_name}", f"DROP TABLE IF EXISTS {view_name}"):
            try:
                connection.execute(drop_stmt)
            except Exception:
                pass
        if partition_tables:
            union_sql = "\nUNION ALL\n".join(f"SELECT * FROM {table}" for table in partition_tables)
            connection.execute(f"CREATE VIEW {view_name} AS {union_sql}")

        return built

    def refresh(
        self,
        connection: Any,
        source_sql: str,
        table_name: str,
        mode: Literal["auto", "full", "incremental", "merge", "engine"] | None = None,
        watermark_column: str | None = None,
        lookback: str | None = None,
        from_watermark: Any | None = None,
        to_watermark: Any | None = None,
        dialect: str | None = None,
        model: Any | None = None,
        database: str | None = None,
        schema: str | None = None,
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
            lookback: Time interval to reprocess (e.g., '7 days', '2 hours') for late-arriving data.
                Defaults to refresh_key.update_window when omitted.
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

        # Partitioned pre-aggregations are materialized as one table per time bucket
        # ``auto`` is the public spelling of definition-driven mode selection;
        # keep ``None`` supported for backwards-compatible Python callers.
        if mode == "auto":
            mode = None

        # plus a covering view; delegate to build_partitions (needs the model).
        if self.partition_granularity:
            if model is None:
                raise ValueError(
                    f"Pre-aggregation '{self.name}' is partitioned (partition_granularity="
                    f"'{self.partition_granularity}'); pass model= to refresh() or call build_partitions() directly."
                )
            # Honor the requested mode: a full refresh rebuilds every partition, while
            # incremental/merge only refresh recent buckets (lookback / update_window).
            effective_mode = mode
            if effective_mode is None:
                effective_mode = "incremental" if (self.refresh_key and self.refresh_key.incremental) else "full"
            self.build_partitions(
                connection,
                model,
                database=database,
                schema=schema,
                lookback=lookback,
                full_rebuild=(effective_mode == "full"),
            )
            return RefreshResult(
                mode="partitioned",
                rows_inserted=-1,
                rows_updated=-1,
                new_watermark=None,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now(),
            )

        # Infer mode from config if not specified
        if mode is None:
            if self.refresh_key and self.refresh_key.incremental:
                mode = "incremental"
            else:
                mode = "full"

        # A declared refresh_key.update_window is the window to reprocess for
        # late-arriving data. Honor it as the default lookback when the caller did
        # not pass one explicitly (an explicit lookback argument always wins).
        if lookback is None and self.refresh_key and self.refresh_key.update_window:
            lookback = self.refresh_key.update_window

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

        # Materialize declared indexes for dialects that support standard secondary
        # indexes; engine mode manages its own object and is excluded.
        if mode in ("full", "incremental", "merge"):
            self._create_indexes(connection, table_name, dialect)

        duration_seconds = time.time() - start_time

        return RefreshResult(
            mode=mode,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            new_watermark=new_watermark,
            duration_seconds=duration_seconds,
            timestamp=datetime.now(),
        )

    def _create_indexes(self, connection: Any, table_name: str, dialect: str | None) -> None:
        """Emit CREATE INDEX statements for declared indexes (dialect-gated, idempotent).

        Indexes are created only for dialects that support standard secondary
        indexes ({duckdb, postgres}); other engines are skipped silently. Index
        names are namespaced to the table to avoid collisions, and identifiers are
        validated to prevent SQL injection. Uses IF NOT EXISTS so repeated
        incremental/merge refreshes do not error.
        """
        import re

        if not self.indexes or dialect not in _INDEX_DDL_DIALECTS:
            return

        table_base = table_name.split(".")[-1]
        for index in self.indexes:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", index.name):
                raise ValueError(f"Invalid index name for DDL generation: {index.name}")
            for column in index.columns:
                if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column):
                    raise ValueError(f"Invalid index column for DDL generation: {column}")
            index_name = f"{table_base}_{index.name}"
            columns = ", ".join(index.columns)
            connection.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns})")

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
            # A lookback reprocesses an overlapping watermark range. Delete that
            # range before inserting its replacement so repeated refreshes remain
            # idempotent instead of accumulating duplicate rollup rows.
            if lookback:
                connection.execute(f"DELETE FROM {table_name} WHERE {watermark_column} >= {watermark_str}")
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

    mode: Literal["full", "incremental", "merge", "engine", "partitioned"]
    rows_inserted: int  # -1 if unknown
    rows_updated: int  # -1 if unknown
    new_watermark: Any | None  # Orchestrator stores this for next run
    duration_seconds: float
    timestamp: datetime
