"""Relationship definitions for semantic layer models."""

from typing import Literal

from pydantic import BaseModel, Field


class Relationship(BaseModel):
    """Represents a relationship between models.

    Relationship types:
    - many_to_one: This model has a foreign key to another
    - one_to_one: This model is referenced by another with unique constraint
    - one_to_many: This model is referenced by another
    - many_to_many: This model relates to another through a junction table
    """

    name: str = Field(description="Name of the related model")
    type: Literal["many_to_one", "one_to_one", "one_to_many", "many_to_many"] = Field(
        description="Type of relationship"
    )
    foreign_key: str | None = Field(
        default=None, description="Foreign key column (defaults to {name}_id for many_to_one)"
    )
    primary_key: str | None = Field(default=None, description="Primary key column in related model (defaults to id)")
    through: str | None = Field(default=None, description="Junction model for many_to_many relationships")
    through_foreign_key: str | None = Field(
        default=None, description="Foreign key in junction model pointing to this model"
    )
    related_foreign_key: str | None = Field(
        default=None, description="Foreign key in junction model pointing to related model"
    )

    @property
    def sql_expr(self) -> str:
        """Get SQL expression for the foreign key."""
        if self.foreign_key:
            return self.foreign_key

        # Default: {name}_id for many_to_one
        if self.type == "many_to_one":
            return f"{self.name}_id"
        else:
            return "id"

    @property
    def related_key(self) -> str:
        """Get the key in the related model."""
        if self.primary_key:
            return self.primary_key
        return "id"

    def junction_keys(self) -> tuple[str | None, str | None]:
        """Get junction keys for many_to_many relationships."""
        if self.type != "many_to_many":
            return None, None
        return self.through_foreign_key or self.foreign_key, self.related_foreign_key
