"""Planning structures for semantic SQL rewrites."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class CandidatePlan:
    """A deterministic rewrite candidate considered by the semantic SQL planner."""

    name: str
    valid: bool
    reason: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SemanticQueryPlan:
    """Structured plan extracted from a primary semantic SQL SELECT."""

    source_sql: str
    source_kind: str
    metrics: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    order_by: list[str] | None = None
    limit: int | None = None
    offset: int | None = None
    aliases: dict[str, str] = field(default_factory=dict)
    candidate_kind: str = "direct_semantic"
    candidate_plans: list[CandidatePlan] = field(default_factory=list)
    eligibility: dict[str, Any] = field(default_factory=dict)
    applied_rules: list[str] = field(default_factory=list)
    rejected_rules: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RewriteExplanation:
    """Machine-testable explanation for a semantic SQL rewrite decision."""

    input_sql: str
    chosen_plan: str
    rewritten_sql: str | None = None
    source_kind: str = "unknown"
    metrics: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    order_by: list[str] | None = None
    limit: int | None = None
    offset: int | None = None
    aliases: dict[str, str] = field(default_factory=dict)
    candidate_plans: list[CandidatePlan] = field(default_factory=list)
    semantic_scopes: list[SemanticQueryPlan] = field(default_factory=list)
    pushed_filters: list[str] = field(default_factory=list)
    post_process: str | None = None
    preaggregation: dict[str, Any] = field(default_factory=dict)
    fanout: dict[str, Any] = field(default_factory=dict)
    applied_rules: list[str] = field(default_factory=list)
    rejected_rules: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = [
    "CandidatePlan",
    "RewriteExplanation",
    "SemanticQueryPlan",
]
