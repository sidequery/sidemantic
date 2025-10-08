"""Symmetric aggregate support for handling fan-out in joins.

Symmetric aggregates prevent double-counting when joins create multiple rows
for a single entity (fan-out). This is achieved using:

    SUM(DISTINCT HASH(pk) * 1e15 + value) - SUM(DISTINCT HASH(pk) * 1e15)

This ensures each row from the left side is counted exactly once, even when
the join creates duplicates.
"""

from typing import Literal


def build_symmetric_aggregate_sql(
    measure_expr: str,
    primary_key: str,
    agg_type: Literal["sum", "avg", "count", "count_distinct"],
    model_alias: str | None = None,
    dialect: str = "duckdb",
) -> str:
    """Build SQL for symmetric aggregate to prevent double-counting in fan-out joins.

    Args:
        measure_expr: The measure expression to aggregate (e.g., "amount")
        primary_key: The primary key field to use for deduplication
        agg_type: Type of aggregation (sum, avg, count, count_distinct)
        model_alias: Optional table/CTE alias to prefix columns
        dialect: SQL dialect (duckdb, bigquery, postgres, snowflake, clickhouse, databricks, spark)

    Returns:
        SQL expression using symmetric aggregates

    Examples:
        >>> build_symmetric_aggregate_sql("amount", "order_id", "sum")
        '(SUM(DISTINCT (HASH(order_id)::HUGEINT * (1::HUGEINT << 20)) + amount) - SUM(DISTINCT (HASH(order_id)::HUGEINT * (1::HUGEINT << 20))))'

        >>> build_symmetric_aggregate_sql("amount", "order_id", "avg", "orders_cte")
        '(SUM(DISTINCT (HASH(orders_cte.order_id)::HUGEINT * (1::HUGEINT << 20)) + orders_cte.amount) - SUM(DISTINCT (HASH(orders_cte.order_id)::HUGEINT * (1::HUGEINT << 20)))) / NULLIF(COUNT(DISTINCT orders_cte.order_id), 0)'
    """
    # Add table prefix if provided
    pk_col = f"{model_alias}.{primary_key}" if model_alias else primary_key
    measure_col = f"{model_alias}.{measure_expr}" if model_alias else measure_expr

    # Dialect-specific hash and multiplier functions
    if dialect == "bigquery":

        def hash_func(col):
            return f"FARM_FINGERPRINT(CAST({col} AS STRING))"

        multiplier = "1048576"  # 2^20 as literal
    elif dialect in ("postgres", "postgresql"):
        # Use hashtext which returns int4, then cast to bigint and multiply
        # Use smaller multiplier (2^10 = 1024) to avoid overflow
        def hash_func(col):
            return f"hashtext({col}::text)::bigint"

        multiplier = "1024"  # 2^10 as literal (smaller to avoid overflow)
    elif dialect == "snowflake":
        # Snowflake HASH returns very large 64-bit integers
        # Use modulo to constrain range, then very small multiplier to avoid overflow
        def hash_func(col):
            return f"(HASH({col}) % 1000000000)"  # Modulo to constrain range

        multiplier = "100"  # Very small multiplier to avoid overflow
    elif dialect == "clickhouse":
        # ClickHouse halfMD5 returns UInt64
        def hash_func(col):
            return f"halfMD5(CAST({col} AS String))"

        multiplier = "1048576"  # 2^20 as literal
    elif dialect in ("databricks", "spark"):
        # Databricks/Spark SQL xxhash64 returns bigint
        def hash_func(col):
            return f"xxhash64(CAST({col} AS STRING))"

        multiplier = "1048576"  # 2^20 as literal
    else:  # duckdb

        def hash_func(col):
            return f"HASH({col})::HUGEINT"

        multiplier = "(1::HUGEINT << 20)"

    if agg_type == "sum":
        # SUM(DISTINCT HASH(pk) * multiplier + value) - SUM(DISTINCT HASH(pk) * multiplier)
        hash_expr = hash_func(pk_col)
        return (
            f"(SUM(DISTINCT ({hash_expr} * {multiplier}) + {measure_col}) - SUM(DISTINCT ({hash_expr} * {multiplier})))"
        )

    elif agg_type == "avg":
        # Sum divided by distinct count
        hash_expr = hash_func(pk_col)
        sum_expr = (
            f"(SUM(DISTINCT ({hash_expr} * {multiplier}) + {measure_col}) - SUM(DISTINCT ({hash_expr} * {multiplier})))"
        )
        count_expr = f"COUNT(DISTINCT {pk_col})"
        return f"{sum_expr} / NULLIF({count_expr}, 0)"

    elif agg_type == "count":
        # Count distinct primary keys
        return f"COUNT(DISTINCT {pk_col})"

    elif agg_type == "count_distinct":
        # Count distinct on the measure itself - no symmetric aggregate needed
        return f"COUNT(DISTINCT {measure_col})"

    else:
        raise ValueError(f"Unsupported aggregation type for symmetric aggregates: {agg_type}")


def needs_symmetric_aggregate(
    relationship: str,
    is_base_model: bool,
) -> bool:
    """Determine if symmetric aggregates are needed for a join.

    Symmetric aggregates are needed when:
    1. The join creates a one-to-many relationship from the base model
    2. We're aggregating measures from the "one" side while joining the "many" side

    Args:
        relationship: Join relationship type (many_to_one, one_to_many, one_to_one)
        is_base_model: Whether this is the base model in the query

    Returns:
        True if symmetric aggregates should be used
    """
    # If this model is on the "one" side of a one-to-many relationship
    # and other models with one-to-many are also in the query,
    # we need symmetric aggregates
    if relationship == "one_to_many" and is_base_model:
        return True

    return False
