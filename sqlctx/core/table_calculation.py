"""Table calculation definitions.

Table calculations are runtime calculations applied AFTER the query executes,
similar to LookML table calculations or Excel formulas.
"""

import ast
import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class TableCalculation(BaseModel):
    """Table calculation definition.

    Table calculations operate on query results, not raw data.
    They can reference other fields in the result set and perform
    calculations like running totals, percentages, rankings, etc.
    """

    name: str = Field(..., description="Unique calculation name")
    type: Literal[
        "formula",
        "percent_of_total",
        "percent_of_previous",
        "percent_of_column_total",
        "running_total",
        "rank",
        "row_number",
        "percentile",
        "moving_average",
    ] = Field(..., description="Type of table calculation")
    description: str | None = Field(None, description="Human-readable description")

    # Formula-based calculation
    expression: str | None = Field(
        None,
        description="Formula expression referencing result columns (e.g., '${revenue} / ${cost}')",
    )

    # Field reference for calculations
    field: str | None = Field(None, description="Field to calculate on (for percent_of_total, running_total, etc.)")

    # Partition by for window calculations
    partition_by: list[str] | None = Field(None, description="Fields to partition by for window calculations")

    # Order by for window calculations
    order_by: list[str] | None = Field(None, description="Fields to order by for ranked/sequential calculations")

    # Moving average window
    window_size: int | None = Field(
        None, description="Window size for moving average (e.g., 7 for 7-day moving average)"
    )

    # Percentile value (0-1)
    percentile: float | None = Field(
        None, description="Percentile value between 0 and 1 (e.g., 0.5 for median, 0.95 for p95)"
    )

    @model_validator(mode="after")
    def validate_formula_expression(self) -> "TableCalculation":
        """Validate formula expression syntax at creation time."""
        if self.type == "formula" and self.expression is not None:
            # Replace field references with placeholder numbers for syntax validation
            test_expr = re.sub(r"\$\{[^}]+\}", "1", self.expression)
            try:
                ast.parse(test_expr, mode="eval")
            except SyntaxError as e:
                raise ValueError(f"Invalid formula expression syntax: {self.expression!r}") from e
        return self

    def __hash__(self) -> int:
        return hash(self.name)
