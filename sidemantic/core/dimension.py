"""Dimension definitions."""

from typing import Literal

from pydantic import BaseModel, Field


class Dimension(BaseModel):
    """Dimension (attribute) definition.

    Dimensions are used for grouping and filtering in queries.
    """

    name: str = Field(..., description="Unique dimension name within model")
    type: Literal["categorical", "time", "boolean", "numeric"] = Field(
        ..., description="Dimension type"
    )
    expr: str | None = Field(None, description="SQL expression (defaults to name)")
    granularity: Literal["hour", "day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Base granularity for time dimensions"
    )
    supported_granularities: list[str] | None = Field(
        None, description="Supported granularities for time dimensions"
    )
    description: str | None = Field(None, description="Human-readable description")
    label: str | None = Field(None, description="Display label")

    def __hash__(self) -> int:
        return hash((self.name, self.type, self.expr))

    @property
    def sql_expr(self) -> str:
        """Get SQL expression, defaulting to name if not specified."""
        return self.expr or self.name

    def with_granularity(self, granularity: str) -> str:
        """Get SQL expression with time granularity applied.

        Args:
            granularity: Time granularity (hour, day, week, month, quarter, year)

        Returns:
            SQL expression with DATE_TRUNC applied
        """
        if self.type != "time":
            raise ValueError(f"Cannot apply granularity to non-time dimension {self.name}")

        # Validate granularity
        supported = self.supported_granularities or [
            "hour",
            "day",
            "week",
            "month",
            "quarter",
            "year",
        ]
        if granularity not in supported:
            raise ValueError(
                f"Granularity {granularity} not supported for {self.name}. "
                f"Supported: {supported}"
            )

        return f"DATE_TRUNC('{granularity}', {self.sql_expr})"
