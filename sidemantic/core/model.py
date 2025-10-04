"""Model definitions."""

from pydantic import BaseModel, Field

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment


class Model(BaseModel):
    """Model (dataset) definition.

    Models are the foundation of the semantic layer, mapping to physical tables
    or SQL expressions. Auto-registers with the current semantic layer context if available.
    """

    name: str = Field(..., description="Unique model name")
    table: str | None = Field(None, description="Physical table name (schema.table)")
    sql: str | None = Field(None, description="SQL expression for derived tables")
    description: str | None = Field(None, description="Human-readable description")

    # Relationships
    relationships: list[Relationship] = Field(
        default_factory=list,
        description="Relationships to other models"
    )

    # Primary key (required)
    primary_key: str = Field(
        default="id",
        description="Primary key column"
    )

    dimensions: list[Dimension] = Field(default_factory=list, description="Dimension definitions")
    metrics: list[Metric] = Field(default_factory=list, description="Measure definitions")
    segments: list[Segment] = Field(default_factory=list, description="Segment (named filter) definitions")

    def __init__(self, **data):
        super().__init__(**data)

        # Auto-register with current layer if in context
        from .registry import auto_register_model
        auto_register_model(self)

    def __hash__(self) -> int:
        return hash(self.name)

    def get_dimension(self, name: str) -> Dimension | None:
        """Get dimension by name."""
        for dimension in self.dimensions:
            if dimension.name == name:
                return dimension
        return None

    def get_metric(self, name: str) -> Metric | None:
        """Get metric by name."""
        for metric in self.metrics:
            if metric.name == name:
                return metric
        return None

    def get_segment(self, name: str) -> Segment | None:
        """Get segment by name."""
        for segment in self.segments:
            if segment.name == name:
                return segment
        return None
