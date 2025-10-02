"""Model definitions."""

from pydantic import BaseModel, Field

from sidemantic.core.dimension import Dimension
from sidemantic.core.entity import Entity
from sidemantic.core.join import Join
from sidemantic.core.measure import Measure


class Model(BaseModel):
    """Model (dataset) definition.

    Models are the foundation of the semantic layer, mapping to physical tables
    or SQL expressions.
    """

    name: str = Field(..., description="Unique model name")
    table: str | None = Field(None, description="Physical table name (schema.table)")
    sql: str | None = Field(None, description="SQL expression for derived tables")
    description: str | None = Field(None, description="Human-readable description")

    # Legacy entity-based joins (still supported)
    entities: list[Entity] = Field(default_factory=list, description="Entity (join key) definitions")

    # New Rails-like joins (preferred)
    joins: list[Join] = Field(
        default_factory=list,
        description="Join relationships (belongs_to, has_one, has_many)"
    )

    # Primary key (required if using joins)
    primary_key: str | None = Field(
        default=None,
        description="Primary key column (defaults to 'id')"
    )

    dimensions: list[Dimension] = Field(default_factory=list, description="Dimension definitions")
    measures: list[Measure] = Field(default_factory=list, description="Measure definitions")

    def __hash__(self) -> int:
        return hash(self.name)

    @property
    def primary_entity(self) -> Entity | None:
        """Get the primary entity for this model."""
        for entity in self.entities:
            if entity.type == "primary":
                return entity
        return None

    def get_entity(self, name: str) -> Entity | None:
        """Get entity by name."""
        for entity in self.entities:
            if entity.name == name:
                return entity
        return None

    def get_dimension(self, name: str) -> Dimension | None:
        """Get dimension by name."""
        for dimension in self.dimensions:
            if dimension.name == name:
                return dimension
        return None

    def get_measure(self, name: str) -> Measure | None:
        """Get measure by name."""
        for measure in self.measures:
            if measure.name == name:
                return measure
        return None
