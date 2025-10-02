"""Join definitions for semantic layer models."""

from typing import Literal

from pydantic import BaseModel, Field


class Join(BaseModel):
    """Represents a join relationship between models.

    Rails-like syntax:
    - belongs_to: This model has a foreign key to another (many-to-one)
    - has_one: This model is referenced by another with unique constraint (one-to-one)
    - has_many: This model is referenced by another (one-to-many)
    """

    name: str = Field(description="Name of the related model")
    type: Literal["belongs_to", "has_one", "has_many"] = Field(
        description="Type of relationship"
    )
    foreign_key: str | None = Field(
        default=None,
        description="Foreign key column (defaults to {name}_id for belongs_to, id for has_*)"
    )
    primary_key: str | None = Field(
        default=None,
        description="Primary key column in related model (defaults to id)"
    )

    @property
    def sql_expr(self) -> str:
        """Get SQL expression for the foreign key."""
        if self.foreign_key:
            return self.foreign_key

        # Default: {name}_id for belongs_to, id for has_*
        if self.type == "belongs_to":
            return f"{self.name}_id"
        else:
            return "id"

    @property
    def related_key(self) -> str:
        """Get the key in the related model."""
        if self.primary_key:
            return self.primary_key

        # Default: id for belongs_to, {model}_id for has_*
        if self.type == "belongs_to":
            return "id"
        else:
            # This would need the source model name, handled at graph level
            return "id"
