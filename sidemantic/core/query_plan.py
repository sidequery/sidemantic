"""Query plan types for pre-aggregation routing explanation."""

from dataclasses import dataclass, field


@dataclass
class PreaggCheck:
    """Result of a single pre-aggregation compatibility check."""

    name: str
    passed: bool
    detail: str

    def __str__(self) -> str:
        mark = "pass" if self.passed else "FAIL"
        return f"[{mark}] {self.name}: {self.detail}"


@dataclass
class PreaggCandidate:
    """Evaluation result for a single pre-aggregation candidate."""

    name: str
    matched: bool
    score: int | None = None
    selected: bool = False
    checks: list[PreaggCheck] = field(default_factory=list)

    def __str__(self) -> str:
        if self.selected:
            label = f"{self.name} (score: {self.score}, selected)"
        elif self.matched:
            label = f"{self.name} (score: {self.score})"
        else:
            label = f"{self.name} (not matched)"
        lines = [label]
        for check in self.checks:
            lines.append(f"    {check}")
        return "\n".join(lines)


@dataclass
class QueryPlan:
    """Explain output for a semantic layer query.

    Shows whether pre-aggregations were used and why, with per-candidate
    check details. Inspired by SQL EXPLAIN but friendlier.

    Example::

        plan = layer.explain(
            metrics=["events.event_count"],
            dimensions=["events.event_type"],
        )
        print(plan)
    """

    sql: str
    model: str | None = None
    metrics: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    used_preaggregation: bool = False
    selected_preagg: str | None = None
    routing_reason: str = ""
    candidates: list[PreaggCandidate] = field(default_factory=list)

    def __str__(self) -> str:
        lines = ["Query Plan"]

        if self.model:
            lines.append(f"  Model: {self.model}")
        if self.metrics:
            lines.append(f"  Metrics: {', '.join(self.metrics)}")
        if self.dimensions:
            lines.append(f"  Dimensions: {', '.join(self.dimensions)}")
        lines.append("")

        if self.used_preaggregation:
            lines.append(f"  Routing: using pre-aggregation '{self.selected_preagg}'")
        else:
            lines.append("  Routing: scanning raw table")
        lines.append(f"  Reason: {self.routing_reason}")
        lines.append("")

        if self.candidates:
            lines.append("  Candidates:")
            for candidate in self.candidates:
                prefix = "  > " if candidate.selected else "    "
                for i, line in enumerate(str(candidate).split("\n")):
                    lines.append(f"{prefix}{line}" if i == 0 else f"      {line}")
            lines.append("")

        lines.append("  SQL:")
        for sql_line in self.sql.strip().split("\n"):
            lines.append(f"    {sql_line}")

        return "\n".join(lines)
