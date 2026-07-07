"""Sidemantic: Universal semantic layer - import from Cube, dbt, LookML, Hex, and more."""

from typing import TYPE_CHECKING

__version__ = "0.10.2"

from sidemantic.core.dimension import Dimension
from sidemantic.core.freshness import Freshness
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.pre_aggregation import PreAggregation, RefreshKey, RefreshResult
from sidemantic.core.preagg_recommender import PreAggRecommendation, PreAggregationRecommender, QueryPattern
from sidemantic.core.query_plan import PreaggCandidate, PreaggCheck, QueryPlan
from sidemantic.core.relationship import Relationship
from sidemantic.core.security import SecurityPolicy
from sidemantic.core.segment import Segment

# Backwards compatibility alias
Measure = Metric

__all__ = [
    "Dimension",
    "DashboardDocument",
    "DashboardSpecError",
    "Freshness",
    "Measure",  # Backwards compatibility
    "Metric",
    "Model",
    "Parameter",
    "PreAggregation",
    "PreAggregationRecommender",
    "PreAggRecommendation",
    "PreaggCandidate",
    "PreaggCheck",
    "QueryPattern",
    "QueryPlan",
    "RefreshKey",
    "RefreshResult",
    "Relationship",
    "SecurityPolicy",
    "Segment",
    "SemanticLayer",
    "load_from_directory",
]

if TYPE_CHECKING:
    from sidemantic.core.semantic_layer import SemanticLayer as SemanticLayer
    from sidemantic.loaders import load_from_directory as load_from_directory


def __getattr__(name):  # Lazy import to avoid importing duckdb on package import
    if name == "SemanticLayer":
        from sidemantic.core.semantic_layer import SemanticLayer  # type: ignore

        return SemanticLayer
    if name == "load_from_directory":
        from sidemantic.loaders import load_from_directory  # type: ignore

        return load_from_directory
    if name == "DashboardDocument":
        from sidemantic.dashboard import DashboardDocument  # type: ignore

        return DashboardDocument
    if name == "DashboardSpecError":
        from sidemantic.dashboard import DashboardSpecError  # type: ignore

        return DashboardSpecError
    raise AttributeError(name)
