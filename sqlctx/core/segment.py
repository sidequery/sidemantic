"""Segment definitions - reusable named filters."""

from pydantic import BaseModel, Field


class Segment(BaseModel):
    """Segment definition - predefined reusable filter.

    Segments are named filters that can be applied to queries to consistently
    filter data according to business definitions.

    Example:
        active_users = Segment(
            name="active_users",
            sql="{model}.status = 'active' AND {model}.last_login > CURRENT_DATE - 30"
        )
    """

    name: str = Field(..., description="Unique segment name")
    sql: str = Field(..., description="SQL WHERE clause expression")
    description: str | None = Field(None, description="Human-readable description")
    public: bool = Field(True, description="Whether segment is visible in API/UI")

    def __hash__(self) -> int:
        return hash((self.name, self.sql))

    def get_sql(self, model_alias: str = "model") -> str:
        """Get SQL expression with model references replaced.

        Args:
            model_alias: The alias to use for {model} placeholders

        Returns:
            SQL expression with {model} replaced with actual alias
        """
        return self.sql.replace("{model}", model_alias)
