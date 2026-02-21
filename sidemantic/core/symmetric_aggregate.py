"""Symmetric aggregate support for handling fan-out in joins.

Symmetric aggregates prevent double-counting when joins create multiple rows
for a single entity (fan-out). This is achieved using:

    SUM(DISTINCT HASH(pk) * 1e12 + value) - SUM(DISTINCT HASH(pk) * 1e12)

This ensures each row from the left side is counted exactly once, even when
the join creates duplicates.
"""

from typing import Literal


def build_symmetric_aggregate_sql(
    measure_expr: str,
    primary_key: str,
    agg_type: Literal["sum", "avg", "count", "count_distinct", "min", "max", "median"],
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
        '(SUM(DISTINCT (HASH(order_id)::HUGEINT * (1::HUGEINT << 40)) + amount) - SUM(DISTINCT (HASH(order_id)::HUGEINT * (1::HUGEINT << 40))))'

        >>> build_symmetric_aggregate_sql("amount", "order_id", "avg", "orders_cte")
        '(SUM(DISTINCT (HASH(orders_cte.order_id)::HUGEINT * (1::HUGEINT << 40)) + orders_cte.amount) - SUM(DISTINCT (HASH(orders_cte.order_id)::HUGEINT * (1::HUGEINT << 40)))) / NULLIF(COUNT(DISTINCT orders_cte.order_id), 0)'
    """
    # Add table prefix if provided
    pk_col = f"{model_alias}.{primary_key}" if model_alias else primary_key
    measure_col = f"{model_alias}.{measure_expr}" if model_alias else measure_expr

    # Dialect-specific hash and multiplier functions.
    # The multiplier must be larger than any possible measure value so that
    # HASH(pk) * multiplier + value produces a unique value per (pk, value) pair.
    # Using high-precision numeric types avoids integer overflow.
    if dialect == "bigquery":

        def hash_func(col):
            return f"CAST(FARM_FINGERPRINT(CAST({col} AS STRING)) AS BIGNUMERIC)"

        multiplier = "1000000000000"  # 1e12: BIGNUMERIC avoids INT64 overflow
    elif dialect in ("postgres", "postgresql"):
        # Cast to numeric (arbitrary precision) to avoid bigint overflow
        def hash_func(col):
            return f"hashtext({col}::text)::numeric"

        multiplier = "1000000000000"  # 1e12
    elif dialect == "snowflake":
        # NUMBER(38,0) supports up to 38 digits, plenty of headroom
        def hash_func(col):
            return f"HASH({col})::NUMBER(38, 0)"

        multiplier = "1000000000000"  # 1e12
    elif dialect == "clickhouse":
        # ClickHouse halfMD5 returns UInt64
        def hash_func(col):
            return f"halfMD5(CAST({col} AS String))"

        multiplier = "1000000000000"  # 1e12
    elif dialect in ("databricks", "spark"):
        # Databricks/Spark SQL xxhash64 returns bigint
        def hash_func(col):
            return f"xxhash64(CAST({col} AS STRING))"

        multiplier = "1000000000000"  # 1e12
    else:  # duckdb

        def hash_func(col):
            return f"HASH({col})::HUGEINT"

        multiplier = "(1::HUGEINT << 40)"  # ~1e12 in HUGEINT space

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

    elif agg_type in ("min", "max"):
        # MIN/MAX are idempotent to fan-out: duplicated rows don't change the result
        agg_func = agg_type.upper()
        return f"{agg_func}({measure_col})"

    elif agg_type == "median":
        raise ValueError(
            "Symmetric aggregates do not support MEDIAN. "
            "Use pre-aggregation or restructure the query to avoid fan-out joins."
        )

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
