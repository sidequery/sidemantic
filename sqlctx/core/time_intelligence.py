"""Time intelligence for semantic metrics."""

from typing import Literal

from pydantic import BaseModel, Field, field_validator

TimeComparisonType = Literal[
    "yoy",  # Year over year
    "mom",  # Month over month
    "wow",  # Week over week
    "dod",  # Day over day
    "qoq",  # Quarter over quarter
    "prior_period",  # vs previous period (dynamic)
]

TimeOffsetUnit = Literal["day", "week", "month", "quarter", "year"]


class TimeComparison(BaseModel):
    """Time comparison configuration for metrics.

    Examples:
    - YoY growth: TimeComparison(type="yoy", metric="revenue")
    - MoM change: TimeComparison(type="mom", metric="orders")
    - Custom offset: TimeComparison(type="prior_period", offset=7, offset_unit="day")
    """

    type: TimeComparisonType = Field(description="Type of time comparison")
    metric: str = Field(description="Base metric to compare")

    # For custom offsets
    offset: int | None = Field(None, description="Custom offset amount")
    offset_unit: TimeOffsetUnit | None = Field(None, description="Custom offset unit")

    # Calculation type
    calculation: Literal["difference", "percent_change", "ratio"] = Field(
        "percent_change", description="How to calculate the comparison"
    )

    @field_validator("offset")
    @classmethod
    def validate_offset_not_zero(cls, v: int | None) -> int | None:
        """Validate that offset is not zero.

        Zero offset would mean comparing a period to itself, which doesn't
        make practical sense for time comparisons. Users should explicitly
        get an error rather than having their input silently changed.
        """
        if v == 0:
            raise ValueError(
                "offset cannot be 0. Time comparisons require a non-zero offset "
                "to compare against a different time period. Use offset >= 1 for "
                "past comparisons or offset <= -1 for future comparisons."
            )
        return v

    @property
    def offset_interval(self) -> tuple[int, str]:
        """Get the offset interval for this comparison.

        Returns:
            (amount, unit) tuple for SQL INTERVAL
        """
        if self.offset is not None and self.offset_unit is not None:
            return (self.offset, self.offset_unit)

        # Default offsets for standard comparisons
        offsets = {
            "dod": (1, "day"),
            "wow": (1, "week"),
            "mom": (1, "month"),
            "qoq": (1, "quarter"),
            "yoy": (1, "year"),
            "prior_period": (1, "day"),  # Default, should be overridden
        }
        return offsets[self.type]

    def get_sql_offset(self) -> str:
        """Get SQL INTERVAL expression for this comparison."""
        amount, unit = self.offset_interval
        return f"INTERVAL '{amount} {unit}'"


class TrailingPeriod(BaseModel):
    """Trailing/rolling period configuration.

    Examples:
    - Last 7 days: TrailingPeriod(amount=7, unit="day")
    - Last 3 months: TrailingPeriod(amount=3, unit="month")
    """

    amount: int = Field(description="Number of periods")
    unit: TimeOffsetUnit = Field(description="Period unit")

    def get_sql_interval(self) -> str:
        """Get SQL INTERVAL expression."""
        return f"INTERVAL '{self.amount} {self.unit}'"


def generate_time_comparison_sql(
    comparison: TimeComparison,
    current_metric_sql: str,
    time_dimension: str,
) -> str:
    """Generate SQL for time comparison metric.

    Args:
        comparison: Time comparison configuration
        current_metric_sql: SQL for current period metric
        time_dimension: Time dimension column name

    Returns:
        SQL expression for the comparison calculation
    """
    comparison.get_sql_offset()

    # Build SQL for offset metric
    # This uses a self-join to get the prior period value
    offset_metric_sql = f"""
        LAG({current_metric_sql}) OVER (
            ORDER BY {time_dimension}
        )
    """.strip()

    # Calculate based on type
    if comparison.calculation == "difference":
        return f"({current_metric_sql} - {offset_metric_sql})"
    elif comparison.calculation == "percent_change":
        return f"(({current_metric_sql} - {offset_metric_sql}) / NULLIF({offset_metric_sql}, 0) * 100)"
    elif comparison.calculation == "ratio":
        return f"({current_metric_sql} / NULLIF({offset_metric_sql}, 0))"

    raise ValueError(f"Unknown calculation type: {comparison.calculation}")
