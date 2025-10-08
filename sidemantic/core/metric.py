"""Measure definitions - unified metric/measure abstraction."""

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from .dependency_analyzer import extract_metric_dependencies


class Metric(BaseModel):
    """Measure definition - supports simple aggregations and complex metric types.

    Measures can be:
    - Simple aggregations: SUM(amount), COUNT(*), AVG(price)
    - Ratios: revenue / order_count
    - Derived formulas: (revenue - cost) / revenue
    - Cumulative: running totals, period-to-date
    - Time comparisons: YoY, MoM growth
    - Conversion funnels: signup -> purchase rate

    Auto-registers as a graph-level metric with the current semantic layer context if available.
    """

    name: str = Field(..., description="Unique measure name")
    extends: str | None = Field(None, description="Parent metric to inherit from")

    def __init__(self, **data):
        super().__init__(**data)

        # Auto-register graph-level metrics with current layer if in context
        from .registry import auto_register_metric

        auto_register_metric(self)

    # Basic aggregation (for simple measures)
    agg: Literal["sum", "count", "count_distinct", "avg", "min", "max", "median"] | None = Field(
        None, description="Aggregation function (for simple measures)"
    )
    sql: str | None = Field(None, description="SQL expression or formula (accepts 'expr' as alias)")

    @model_validator(mode="before")
    @classmethod
    def handle_expr_and_parse_agg(cls, data):
        """Handle expr alias and parse aggregation from SQL expression.

        1. Converts expr= to sql= for backwards compatibility
        2. Parses aggregation functions from SQL (e.g., SUM(amount) -> agg=sum, sql=amount)
        """
        import re

        if isinstance(data, dict):
            # Step 1: Handle expr alias
            expr_val = data.get("expr")
            sql_val = data.get("sql")

            # If both provided, they must match
            if expr_val is not None and sql_val is not None and expr_val != sql_val:
                raise ValueError(f"Cannot specify both sql='{sql_val}' and expr='{expr_val}' with different values")

            # If only expr provided, copy to sql
            if expr_val is not None and sql_val is None:
                data["sql"] = expr_val
                sql_val = expr_val  # Update for next step

            # Remove expr from data to avoid storing it
            data.pop("expr", None)

            # Step 2: Parse aggregation from SQL if needed
            agg_val = data.get("agg")
            type_val = data.get("type")

            # Only parse if sql is provided and agg is not, and this isn't a complex metric
            if sql_val and not agg_val and not type_val:
                # Match aggregation functions at the start: SUM(expr), COUNT(expr), etc.
                agg_pattern = r"^\s*(SUM|COUNT|AVG|MIN|MAX|MEDIAN|COUNT_DISTINCT)\s*\((.*)\)\s*$"
                match = re.match(agg_pattern, sql_val, re.IGNORECASE)

                if match:
                    agg_func = match.group(1).lower()
                    inner_expr = match.group(2).strip()

                    # Extract DISTINCT for COUNT(DISTINCT col)
                    if agg_func == "count":
                        distinct_match = re.match(r"^\s*DISTINCT\s+(.+)$", inner_expr, re.IGNORECASE)
                        if distinct_match:
                            agg_func = "count_distinct"
                            inner_expr = distinct_match.group(1).strip()

                    data["agg"] = agg_func
                    data["sql"] = inner_expr

        return data

    # Metric type (if this is a complex metric, not just a simple aggregation)
    type: Literal["ratio", "derived", "cumulative", "time_comparison", "conversion"] | None = Field(
        None, description="Metric type for complex calculations"
    )

    # Ratio parameters
    numerator: str | None = Field(None, description="Numerator measure for ratio")
    denominator: str | None = Field(None, description="Denominator measure for ratio")
    offset_window: str | None = Field(None, description="Time offset for denominator (e.g., '1 month')")

    # Derived metric parameters (uses expr field)

    # Cumulative parameters
    window: str | None = Field(None, description="Time window for cumulative (e.g., '7 days')")
    grain_to_date: Literal["day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Grain for period-to-date (e.g., 'month' for MTD)"
    )

    # Time comparison parameters
    base_metric: str | None = Field(None, description="Base metric for time comparison")
    comparison_type: Literal["yoy", "mom", "wow", "dod", "qoq", "prior_period"] | None = Field(
        None, description="Type of time comparison"
    )
    time_offset: str | None = Field(None, description="Custom time offset (e.g., '1 month')")
    calculation: Literal["difference", "percent_change", "ratio"] | None = Field(
        None, description="Comparison calculation (default: percent_change)"
    )

    # Conversion parameters
    entity: str | None = Field(None, description="Entity to track (e.g., 'user_id')")
    base_event: str | None = Field(None, description="Starting event filter")
    conversion_event: str | None = Field(None, description="Target event filter")
    conversion_window: str | None = Field(None, description="Conversion time window")

    # Common parameters
    filters: list[str] | None = Field(None, description="Optional WHERE clause filters")
    fill_nulls_with: int | float | str | None = Field(None, description="Default value when result is NULL")
    description: str | None = Field(None, description="Human-readable description")
    label: str | None = Field(None, description="Display label")

    # Display formatting
    format: str | None = Field(None, description="Display format string (e.g., '$#,##0.00', '0.00%')")
    value_format_name: str | None = Field(None, description="Named format (e.g., 'usd', 'percent', 'decimal_2')")

    # Drill-down configuration
    drill_fields: list[str] | None = Field(None, description="Fields to show when drilling into this metric")

    # Non-additivity
    non_additive_dimension: str | None = Field(
        None,
        description="Dimension across which this metric cannot be summed (e.g., time for averages)",
    )

    # Defaults
    default_time_dimension: str | None = Field(None, description="Default time dimension for this metric")
    default_grain: Literal["hour", "day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Default time granularity for this metric"
    )

    def __hash__(self) -> int:
        return hash((self.name, self.agg, self.sql))

    @property
    def sql_expr(self) -> str:
        """Get SQL expression for the measure."""
        if self.agg == "count" and not self.sql:
            return "*"
        return self.sql or self.name

    @property
    def is_simple_aggregation(self) -> bool:
        """Check if this is a simple aggregation (not a complex metric)."""
        return self.agg is not None and self.type is None

    def to_sql(self) -> str:
        """Convert simple measure to SQL aggregation expression.

        Returns:
            SQL aggregation expression (e.g., "SUM(amount)", "COUNT(*)")
        """
        if not self.agg:
            raise ValueError(f"Cannot convert complex metric '{self.name}' to SQL - use type-specific logic")

        agg_func = self.agg.upper()
        if agg_func == "COUNT_DISTINCT":
            agg_func = "COUNT(DISTINCT"
            return f"{agg_func} {self.sql_expr})"
        return f"{agg_func}({self.sql_expr})"

    def get_dependencies(self, graph=None) -> set[str]:
        """Auto-detect dependencies from SQL expressions.

        Uses semantic graph to resolve ambiguous references when available.

        Args:
            graph: Optional SemanticGraph for resolving measure/metric references

        Returns:
            Set of measure/metric names this depends on.
        """
        return extract_metric_dependencies(self, graph)
