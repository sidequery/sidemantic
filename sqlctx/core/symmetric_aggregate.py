"""Symmetric aggregate support for handling fan-out in joins.

Symmetric aggregates prevent double-counting when joins create multiple rows
for a single entity (fan-out). This is achieved using:

    SUM(DISTINCT HASH(pk) * 1e12 + value) - SUM(DISTINCT HASH(pk) * 1e12)

This ensures each row from the left side is counted exactly once, even when
the join creates duplicates.
"""

from typing import Literal

# Aggregations that cannot be expressed as a symmetric aggregate under fan-out.
# They have no SUM(DISTINCT hash * M + value) decomposition that survives
# deduplication, so they raise an actionable error rather than emit fake SQL.
_NON_SYMMETRIC_AGGS = {"median", "stddev", "stddev_pop", "variance", "variance_pop"}


def build_symmetric_aggregate_sql(
    measure_expr: str,
    primary_key: str,
    agg_type: Literal[
        "sum",
        "avg",
        "count",
        "count_distinct",
        "approx_count_distinct",
        "min",
        "max",
        "median",
        "stddev",
        "stddev_pop",
        "variance",
        "variance_pop",
    ],
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
        '(SUM(DISTINCT (HASH(order_id)::HUGEINT * (1::HUGEINT << 40)) + COALESCE(amount, 0)) - SUM(DISTINCT (HASH(order_id)::HUGEINT * (1::HUGEINT << 40))))'

        >>> build_symmetric_aggregate_sql("amount", "order_id", "avg", "orders_cte")
        '(SUM(DISTINCT (HASH(orders_cte.order_id)::HUGEINT * (1::HUGEINT << 40)) + COALESCE(orders_cte.amount, 0)) - SUM(DISTINCT (HASH(orders_cte.order_id)::HUGEINT * (1::HUGEINT << 40)))) / NULLIF(COUNT(DISTINCT CASE WHEN orders_cte.amount IS NOT NULL THEN orders_cte.order_id END), 0)'
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

    # COALESCE the measure to 0 so a NULL value doesn't drop the row from the
    # value-bearing SUM while its hash term survives in the subtractor SUM, which
    # would break the cancellation and leave a huge HASH*multiplier in the result.
    coalesced_measure = f"COALESCE({measure_col}, 0)"

    if agg_type == "sum":
        # SUM(DISTINCT HASH(pk) * multiplier + value) - SUM(DISTINCT HASH(pk) * multiplier)
        hash_expr = hash_func(pk_col)
        return f"(SUM(DISTINCT ({hash_expr} * {multiplier}) + {coalesced_measure}) - SUM(DISTINCT ({hash_expr} * {multiplier})))"

    elif agg_type == "avg":
        # Sum (NULL-coalesced for the fan-out hash trick) divided by the count of
        # entities that actually have a value. SQL AVG ignores NULLs, so a NULL
        # measure must be excluded from the denominator too — otherwise a coalesced
        # 0 in the numerator would drag the average down (e.g. {10, NULL} -> 5 not 10).
        hash_expr = hash_func(pk_col)
        sum_expr = f"(SUM(DISTINCT ({hash_expr} * {multiplier}) + {coalesced_measure}) - SUM(DISTINCT ({hash_expr} * {multiplier})))"
        count_expr = f"COUNT(DISTINCT CASE WHEN {measure_col} IS NOT NULL THEN {pk_col} END)"
        return f"{sum_expr} / NULLIF({count_expr}, 0)"

    elif agg_type == "count":
        # COUNT(*) has no per-row value to gate on, so count distinct PKs directly.
        if measure_expr == "*":
            return f"COUNT(DISTINCT {pk_col})"
        # Otherwise count distinct primary keys, honoring any metric-level filter
        # baked into the raw measure column (NULL for non-matching rows).
        return f"COUNT(DISTINCT CASE WHEN {measure_col} IS NOT NULL THEN {pk_col} END)"

    elif agg_type == "count_distinct":
        # Count distinct on the measure itself - no symmetric aggregate needed
        return f"COUNT(DISTINCT {measure_col})"

    elif agg_type == "approx_count_distinct":
        # Approximate distinct is inherently dedupe-safe under fan-out (like
        # count_distinct): duplicated rows don't change the distinct cardinality,
        # so no symmetric aggregate trick is needed.
        return f"APPROX_COUNT_DISTINCT({measure_col})"

    elif agg_type in ("min", "max"):
        # MIN/MAX are idempotent to fan-out: duplicated rows don't change the result
        agg_func = agg_type.upper()
        return f"{agg_func}({measure_col})"

    elif agg_type in _NON_SYMMETRIC_AGGS:
        # median/stddev/variance genuinely cannot be expressed as a symmetric
        # aggregate: there is no SUM(DISTINCT hash * M + value) decomposition that
        # recovers the true result after deduplicating fan-out rows. Erroring is
        # correct; the message names the agg and the concrete workarounds.
        raise ValueError(
            f"{agg_type.upper()} cannot be computed as a symmetric aggregate under a fan-out "
            f"(one-to-many) join, because deduplicating fan-out rows would change the result. "
            f"Restructure the query so '{measure_expr}' is not aggregated across a fan-out join: "
            f"either pre-aggregate the measure to its entity grain (group by the model's primary "
            f"key) before joining, or query this metric without the fan-out dimension/join "
            f"(ungrouped or grouped only by the metric's own model)."
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
