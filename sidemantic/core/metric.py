"""Metric definitions."""

from typing import Literal

from pydantic import BaseModel, Field

from .dependency_analyzer import extract_metric_dependencies


class Metric(BaseModel):
    """Metric (business calculation) definition.

    Metrics are high-level business calculations built from measures.
    """

    name: str = Field(..., description="Unique metric name")
    type: Literal["simple", "ratio", "derived", "cumulative", "time_comparison", "conversion"] = Field(
        ..., description="Metric type"
    )
    description: str | None = Field(None, description="Human-readable description")
    label: str | None = Field(None, description="Display label")

    # Simple metric parameters
    measure: str | None = Field(None, description="Measure reference for simple metrics")

    # Ratio metric parameters
    numerator: str | None = Field(None, description="Numerator measure/metric for ratio")
    denominator: str | None = Field(None, description="Denominator measure/metric for ratio")
    offset_window: str | None = Field(
        None, description="Time offset for denominator (e.g., '1 month' for current/previous_month)"
    )

    # Derived metric parameters
    expr: str | None = Field(None, description="Formula expression for derived metrics")

    # Cumulative metric parameters
    window: str | None = Field(None, description="Time window for cumulative metrics (e.g., '7 days')")
    grain_to_date: Literal["day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Grain for period-to-date cumulative (e.g., 'month' for MTD)"
    )

    # Time comparison metric parameters
    base_metric: str | None = Field(None, description="Base metric for time comparison")
    comparison_type: Literal["yoy", "mom", "wow", "dod", "qoq", "prior_period"] | None = Field(
        None, description="Type of time comparison"
    )
    time_offset: str | None = Field(None, description="Custom time offset (e.g., '1 month', '7 days')")
    calculation: Literal["difference", "percent_change", "ratio"] | None = Field(
        None, description="How to calculate comparison (default: percent_change)"
    )

    # Conversion metric parameters
    entity: str | None = Field(None, description="Entity to track conversions for (e.g., 'user_id')")
    base_event: str | None = Field(None, description="Starting event filter (e.g., 'signup')")
    conversion_event: str | None = Field(None, description="Target event filter (e.g., 'purchase')")
    conversion_window: str | None = Field(None, description="Time window for conversion (e.g., '7 days')")

    # Common parameters
    filters: list[str] | None = Field(None, description="Optional WHERE clause filters")
    fill_nulls_with: int | float | str | None = Field(
        None, description="Default value to use when metric result is NULL"
    )

    def __hash__(self) -> int:
        return hash(self.name)

    def get_dependencies(self, graph=None) -> set[str]:
        """Auto-detect dependencies from SQL expressions.

        Uses semantic graph to resolve ambiguous references when available.

        Args:
            graph: Optional SemanticGraph for resolving measure/metric references

        Returns:
            Set of measure/metric names this metric depends on.
        """
        return extract_metric_dependencies(self, graph)
