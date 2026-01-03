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

        Uses sqlglot to properly parse expressions and handle nested parentheses.
        Only extracts aggregation from SIMPLE expressions (single aggregation function).
        Complex expressions like SUM(x) / SUM(y) are preserved as-is.
        """
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

            # Parse if sql is provided and agg is not set
            # Allow parsing for simple metrics (no type) OR cumulative metrics (to support AVG/COUNT windows)
            if sql_val and not agg_val and (not type_val or type_val == "cumulative"):
                try:
                    import sqlglot
                    from sqlglot import expressions as exp

                    parsed = sqlglot.parse_one(sql_val, read="duckdb")

                    # Only extract if the TOP-LEVEL expression is a simple aggregation
                    # This prevents breaking expressions like SUM(x) / SUM(y)
                    agg_map = {
                        exp.Sum: "sum",
                        exp.Avg: "avg",
                        exp.Min: "min",
                        exp.Max: "max",
                        exp.Median: "median",
                    }

                    agg_func = None
                    inner_expr = None

                    # Check for standard aggregations
                    for agg_class, agg_name in agg_map.items():
                        if isinstance(parsed, agg_class):
                            agg_func = agg_name
                            if parsed.this:
                                inner_expr = parsed.this.sql(dialect="duckdb")
                            break

                    # Handle COUNT specially (need to detect DISTINCT)
                    if isinstance(parsed, exp.Count):
                        # Check if the argument is a Distinct expression
                        if isinstance(parsed.this, exp.Distinct):
                            agg_func = "count_distinct"
                            # Extract all expressions from inside Distinct
                            # e.g., COUNT(DISTINCT a, b) -> "a, b"
                            if parsed.this.expressions:
                                inner_expr = ", ".join(e.sql(dialect="duckdb") for e in parsed.this.expressions)
                            else:
                                inner_expr = parsed.this.sql(dialect="duckdb")
                        else:
                            agg_func = "count"
                            if parsed.this:
                                inner_expr = parsed.this.sql(dialect="duckdb")
                            # COUNT(*) case - inner_expr stays None

                    if agg_func:
                        data["agg"] = agg_func
                        if inner_expr is not None:
                            data["sql"] = inner_expr
                        elif agg_func == "count":
                            # COUNT(*) - leave sql as None or "*"
                            data["sql"] = None

                except Exception:
                    # If sqlglot parsing fails, leave the expression as-is
                    pass

        return data

    @model_validator(mode="after")
    def validate_type_specific_fields(self):
        """Validate that required fields are present for each metric type."""
        if self.type == "time_comparison" and not self.base_metric:
            raise ValueError("time_comparison metric requires 'base_metric' field")
        if self.type == "ratio":
            if not self.numerator:
                raise ValueError("ratio metric requires 'numerator' field")
            if not self.denominator:
                raise ValueError("ratio metric requires 'denominator' field")
        if self.type == "derived" and not self.sql:
            raise ValueError("derived metric requires 'sql' field")
        if self.type == "cumulative" and not self.sql and not self.window_expression:
            raise ValueError("cumulative metric requires 'sql' or 'window_expression' field")
        if self.type == "conversion":
            if not self.entity:
                raise ValueError("conversion metric requires 'entity' field")
            if not self.base_event:
                raise ValueError("conversion metric requires 'base_event' field")
            if not self.conversion_event:
                raise ValueError("conversion metric requires 'conversion_event' field")
        return self

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

    # Advanced window function parameters (for arbitrary window expressions)
    window_expression: str | None = Field(
        None, description="Raw SQL expression for window function (e.g., 'AVG(total_bids) FILTER (WHERE active)')"
    )
    window_frame: str | None = Field(
        None,
        description="Window frame clause (e.g., 'RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW')",
    )
    window_order: str | None = Field(
        None, description="Window ORDER BY column (defaults to model's default_time_dimension)"
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

    def get_dependencies(self, graph=None, model_context=None) -> set[str]:
        """Auto-detect dependencies from SQL expressions.

        Uses semantic graph to resolve ambiguous references when available.

        Args:
            graph: Optional SemanticGraph for resolving measure/metric references
            model_context: Optional model name to prefer when resolving ambiguous references

        Returns:
            Set of measure/metric names this depends on.
        """
        return extract_metric_dependencies(self, graph, model_context)
