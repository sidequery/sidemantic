"""Relationship definitions for semantic layer models."""

from typing import Any, Literal

from pydantic import BaseModel, Field


class Relationship(BaseModel):
    """Represents a relationship between models.

    Relationship types:
    - many_to_one: This model has a foreign key to another
    - one_to_one: This model is referenced by another with unique constraint
    - one_to_many: This model is referenced by another
    - many_to_many: This model relates to another through a junction table
    - cross: This model should be cross joined to another
    """

    name: str = Field(description="Name of the related model")
    type: Literal["many_to_one", "one_to_one", "one_to_many", "many_to_many", "cross"] = Field(
        description="Type of relationship"
    )
    foreign_key: str | list[str] | None = Field(
        default=None, description="Foreign key column(s); required for keyed relationships unless sql is provided"
    )
    primary_key: str | list[str] | None = Field(
        default=None,
        description=(
            "Primary/unique key column(s): related model key for many_to_one, local model key for one_to_many"
        ),
    )
    through: str | None = Field(default=None, description="Junction model for many_to_many relationships")
    through_foreign_key: str | None = Field(
        default=None, description="Foreign key in junction model pointing to this model"
    )
    through_foreign_key_columns: list[str] | None = Field(
        default=None, description="Foreign key columns in junction model pointing to this model"
    )
    related_foreign_key: str | None = Field(
        default=None, description="Foreign key in junction model pointing to related model"
    )
    active: bool = Field(default=True, description="Whether the relationship is active by default")
    related_foreign_key_columns: list[str] | None = Field(
        default=None, description="Foreign key columns in junction model pointing to related model"
    )
    sql: str | None = Field(default=None, description="Custom join SQL using {from} and {to} runtime placeholders")
    metadata: dict[str, Any] | None = Field(None, description="Adapter-specific metadata payload")

    @property
    def sql_expr(self) -> str | None:
        """Get the first explicitly declared foreign-key column, if any."""
        if self.foreign_key:
            if isinstance(self.foreign_key, list):
                return self.foreign_key[0] if self.foreign_key else None
            return self.foreign_key
        return None

    @property
    def related_key(self) -> str | None:
        """Get the first explicitly declared related key, if any."""
        if self.primary_key:
            if isinstance(self.primary_key, list):
                return self.primary_key[0] if self.primary_key else None
            return self.primary_key
        return None

    @property
    def foreign_key_columns(self) -> list[str]:
        """Get foreign key as list of columns (normalizes single string to list)."""
        if self.type == "cross":
            return []
        if self.foreign_key is None:
            return []
        if isinstance(self.foreign_key, str):
            return [self.foreign_key]
        return self.foreign_key

    @property
    def primary_key_columns(self) -> list[str]:
        """Get primary key as list of columns (normalizes single string to list)."""
        if self.type == "cross":
            return []
        if self.primary_key is None:
            return []
        if isinstance(self.primary_key, str):
            return [self.primary_key]
        return self.primary_key

    def junction_keys(self) -> tuple[str | None, str | None]:
        """Get junction keys for many_to_many relationships."""
        if self.type != "many_to_many":
            return None, None
        source_keys, target_keys = self.junction_key_columns()
        return (
            source_keys[0] if source_keys else None,
            target_keys[0] if target_keys else None,
        )

    def junction_key_columns(self) -> tuple[list[str], list[str]]:
        """Get junction key columns for many_to_many relationships."""
        if self.type != "many_to_many":
            return [], []

        if self.through_foreign_key_columns:
            source_keys = self.through_foreign_key_columns
        elif self.through_foreign_key:
            source_keys = [self.through_foreign_key]
        else:
            source_keys = self.foreign_key_columns if self.foreign_key else []

        if self.related_foreign_key_columns:
            target_keys = self.related_foreign_key_columns
        elif self.related_foreign_key:
            target_keys = [self.related_foreign_key]
        else:
            target_keys = []

        return source_keys, target_keys
