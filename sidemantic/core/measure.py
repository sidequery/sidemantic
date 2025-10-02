"""Measure definitions."""

from typing import Literal

from pydantic import BaseModel, Field


class Measure(BaseModel):
    """Measure (aggregation) definition.

    Measures are aggregated in queries to produce metrics.
    """

    name: str = Field(..., description="Unique measure name within model")
    agg: Literal["sum", "count", "count_distinct", "avg", "min", "max", "median"] = Field(
        ..., description="Aggregation function"
    )
    expr: str | None = Field(None, description="SQL expression (defaults to * for count)")
    filters: list[str] | None = Field(None, description="Optional WHERE clause filters")
    description: str | None = Field(None, description="Human-readable description")
    label: str | None = Field(None, description="Display label")

    def __hash__(self) -> int:
        return hash((self.name, self.agg, self.expr))

    @property
    def sql_expr(self) -> str:
        """Get SQL expression for the measure."""
        if self.agg == "count" and not self.expr:
            return "*"
        return self.expr or self.name

    def to_sql(self) -> str:
        """Convert measure to SQL aggregation expression.

        Returns:
            SQL aggregation expression (e.g., "SUM(amount)", "COUNT(*)")
        """
        agg_func = self.agg.upper()
        if agg_func == "COUNT_DISTINCT":
            agg_func = "COUNT(DISTINCT"
            return f"{agg_func} {self.sql_expr})"
        return f"{agg_func}({self.sql_expr})"
