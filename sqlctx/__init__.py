"""sqlctx: Universal semantic layer - import from Cube, dbt, LookML, Hex, and more."""

from typing import TYPE_CHECKING

__version__ = "0.13.2"

from sqlctx.core.consumption import Explore, SavedQuery, View
from sqlctx.core.dimension import Dimension
from sqlctx.core.freshness import Freshness
from sqlctx.core.governance import Deprecation
from sqlctx.core.metric import Metric
from sqlctx.core.model import Model
from sqlctx.core.parameter import Parameter
from sqlctx.core.pre_aggregation import PreAggregation, RefreshKey, RefreshResult
from sqlctx.core.preagg_recommender import PreAggRecommendation, PreAggregationRecommender, QueryPattern
from sqlctx.core.query_plan import PreaggCandidate, PreaggCheck, QueryPlan
from sqlctx.core.relationship import Relationship
from sqlctx.core.schema_exposure import SchemaExposure
from sqlctx.core.security import SecurityPolicy
from sqlctx.core.segment import Segment

# Backwards compatibility alias
Measure = Metric

__all__ = [
    "Dimension",
    "Deprecation",
    "DashboardDocument",
    "DashboardSpecError",
    "Freshness",
    "Explore",
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
    "SecurityError",
    "SecurityPolicy",
    "SchemaExposure",
    "Segment",
    "SavedQuery",
    "SemanticLayer",
    "View",
    "load_from_directory",
]

if TYPE_CHECKING:
    from sqlctx.core.semantic_layer import SemanticLayer as SemanticLayer
    from sqlctx.loaders import load_from_directory as load_from_directory


def __getattr__(name):  # Lazy import to avoid importing duckdb on package import
    if name == "SemanticLayer":
        from sqlctx.core.semantic_layer import SemanticLayer

        return SemanticLayer
    if name == "load_from_directory":
        from sqlctx.loaders import load_from_directory

        return load_from_directory
    if name == "DashboardDocument":
        from sqlctx.dashboard import DashboardDocument

        return DashboardDocument
    if name == "DashboardSpecError":
        from sqlctx.dashboard import DashboardSpecError

        return DashboardSpecError
    if name == "SecurityError":
        from sqlctx.core.semantic_layer import SecurityError

        return SecurityError
    raise AttributeError(name)
