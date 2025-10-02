"""Entity definitions for join relationships."""

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class EntityType(str, Enum):
    """Entity type for join relationships."""

    PRIMARY = "primary"
    FOREIGN = "foreign"
    UNIQUE = "unique"


class Entity(BaseModel):
    """Entity (join key) definition.

    Entities enable automatic join discovery between models.
    """

    name: str = Field(..., description="Unique entity name")
    type: Literal["primary", "foreign", "unique"] = Field(
        ..., description="Entity type (primary, foreign, or unique)"
    )
    expr: str = Field(..., description="SQL expression or column name")

    def __hash__(self) -> int:
        return hash((self.name, self.type, self.expr))
