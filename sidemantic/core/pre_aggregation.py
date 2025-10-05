"""Pre-aggregation definitions for query optimization."""

from typing import Literal

from pydantic import BaseModel, Field


class RefreshKey(BaseModel):
    """Refresh strategy configuration for pre-aggregations."""

    every: str | None = Field(
        None, description="Refresh interval (e.g., '1 hour', '1 day', '30 minutes')"
    )
    sql: str | None = Field(
        None, description="SQL query that returns a value to trigger refresh when changed"
    )
    incremental: bool = Field(
        False, description="Whether to use incremental refresh (only update changed partitions)"
    )
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
    measures: list[str] | None = Field(
        None, description="Measures to pre-aggregate (e.g., ['count', 'revenue'])"
    )
    dimensions: list[str] | None = Field(
        None, description="Dimensions to group by (e.g., ['status', 'region'])"
    )
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
    build_range_start: str | None = Field(
        None, description="SQL expression for start of data range to aggregate"
    )
    build_range_end: str | None = Field(
        None, description="SQL expression for end of data range to aggregate"
    )

    def get_table_name(self, model_name: str) -> str:
        """Generate the physical table name for this pre-aggregation.

        Args:
            model_name: Name of the base model

        Returns:
            Table name in format: {model_name}_preagg_{preagg_name}
        """
        return f"{model_name}_preagg_{self.name}"
