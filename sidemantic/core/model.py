"""Model definitions."""

from typing import Literal

from pydantic import BaseModel, Field

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.pre_aggregation import PreAggregation
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
    source_uri: str | None = Field(None, description="Remote data source URI (e.g., https://, s3://, gs://)")
    description: str | None = Field(None, description="Human-readable description")
    extends: str | None = Field(None, description="Parent model to inherit from")

    # Relationships
    relationships: list[Relationship] = Field(default_factory=list, description="Relationships to other models")

    # Primary key (required)
    primary_key: str = Field(default="id", description="Primary key column")

    dimensions: list[Dimension] = Field(default_factory=list, description="Dimension definitions")
    metrics: list[Metric] = Field(default_factory=list, description="Measure definitions")
    segments: list[Segment] = Field(default_factory=list, description="Segment (named filter) definitions")
    pre_aggregations: list[PreAggregation] = Field(
        default_factory=list, description="Pre-aggregation definitions for query optimization"
    )

    # Default time dimension for all metrics in this model
    default_time_dimension: str | None = Field(
        None, description="Default time dimension for metrics (auto-included in queries)"
    )
    default_grain: Literal["hour", "day", "week", "month", "quarter", "year"] | None = Field(
        None, description="Default time granularity when using default_time_dimension"
    )

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

    def get_pre_aggregation(self, name: str) -> PreAggregation | None:
        """Get pre-aggregation by name."""
        for preagg in self.pre_aggregations:
            if preagg.name == name:
                return preagg
        return None

    def get_hierarchy_path(self, dimension_name: str) -> list[str]:
        """Get the full hierarchy path from root to given dimension.

        Args:
            dimension_name: Name of dimension to find path for

        Returns:
            List of dimension names from root to given dimension

        Examples:
            >>> model.get_hierarchy_path("city")
            ['country', 'state', 'city']
        """
        dim = self.get_dimension(dimension_name)
        if not dim:
            return []

        path = [dimension_name]

        # Walk up the parent chain
        current = dim
        while current and current.parent:
            path.insert(0, current.parent)
            current = self.get_dimension(current.parent)

        return path

    def get_drill_down(self, dimension_name: str) -> str | None:
        """Get the next dimension to drill down to from given dimension.

        Args:
            dimension_name: Current dimension name

        Returns:
            Name of child dimension, or None if no children

        Examples:
            >>> model.get_drill_down("country")
            'state'
        """
        # Find dimension that has current dimension as parent
        for dim in self.dimensions:
            if dim.parent == dimension_name:
                return dim.name
        return None

    def get_drill_up(self, dimension_name: str) -> str | None:
        """Get the parent dimension to drill up to from given dimension.

        Args:
            dimension_name: Current dimension name

        Returns:
            Name of parent dimension, or None if at top level

        Examples:
            >>> model.get_drill_up("city")
            'state'
        """
        dim = self.get_dimension(dimension_name)
        return dim.parent if dim else None
