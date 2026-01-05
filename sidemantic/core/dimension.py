"""Dimension definitions."""

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class Dimension(BaseModel):
    """Dimension (attribute) definition.

    Dimensions are used for grouping and filtering in queries.
    """

    name: str = Field(..., description="Unique dimension name within model")
    type: Literal["categorical", "time", "boolean", "numeric"] = Field(..., description="Dimension type")
    sql: str | None = Field(None, description="SQL expression (defaults to name; accepts 'expr' as alias)")
    granularity: Literal["second", "minute", "hour", "day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Base granularity for time dimensions"
    )
    supported_granularities: list[str] | None = Field(None, description="Supported granularities for time dimensions")
    description: str | None = Field(None, description="Human-readable description")
    label: str | None = Field(None, description="Display label")

    # Display formatting
    format: str | None = Field(None, description="Display format string (e.g., '$#,##0.00', '0.00%')")
    value_format_name: str | None = Field(None, description="Named format (e.g., 'usd', 'percent', 'decimal_2')")

    # Hierarchy
    parent: str | None = Field(None, description="Parent dimension for hierarchies (e.g., 'state' parent is 'country')")

    @model_validator(mode="before")
    @classmethod
    def handle_expr_alias(cls, data):
        """Handle expr as an alias for sql.

        This allows users to specify either sql= or expr= when creating dimensions.
        Both are equivalent and will be stored as 'sql'.
        """
        if isinstance(data, dict):
            expr_val = data.get("expr")
            sql_val = data.get("sql")

            # If both provided, they must match
            if expr_val is not None and sql_val is not None and expr_val != sql_val:
                raise ValueError(f"Cannot specify both sql='{sql_val}' and expr='{expr_val}' with different values")

            # If only expr provided, copy to sql
            if expr_val is not None and sql_val is None:
                data["sql"] = expr_val

            # Remove expr from data to avoid storing it
            data.pop("expr", None)

        return data

    def __hash__(self) -> int:
        return hash((self.name, self.type, self.sql))

    @property
    def sql_expr(self) -> str:
        """Get SQL expression, defaulting to name if not specified."""
        return self.sql or self.name

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
            "second",
            "minute",
            "hour",
            "day",
            "week",
            "month",
            "quarter",
            "year",
        ]
        if granularity not in supported:
            raise ValueError(f"Granularity {granularity} not supported for {self.name}. Supported: {supported}")

        return f"DATE_TRUNC('{granularity}', {self.sql_expr})"
