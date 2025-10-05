"""Table calculation definitions.

Table calculations are runtime calculations applied AFTER the query executes,
similar to LookML table calculations or Excel formulas.
"""

from typing import Literal

from pydantic import BaseModel, Field


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
    field: str | None = Field(
        None, description="Field to calculate on (for percent_of_total, running_total, etc.)"
    )

    # Partition by for window calculations
    partition_by: list[str] | None = Field(
        None, description="Fields to partition by for window calculations"
    )

    # Order by for window calculations
    order_by: list[str] | None = Field(
        None, description="Fields to order by for ranked/sequential calculations"
    )

    # Moving average window
    window_size: int | None = Field(
        None, description="Window size for moving average (e.g., 7 for 7-day moving average)"
    )

    def __hash__(self) -> int:
        return hash(self.name)
