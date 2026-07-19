"""Shared fail-closed security gates for SQL-based transports."""

from __future__ import annotations

from typing import Any


def has_declared_security(layer: Any) -> bool:
    """Return whether any model declares an access or row-level policy."""
    return any(getattr(model, "security", None) is not None for model in layer.graph.models.values())


def has_enforced_column_restrictions(layer: Any) -> bool:
    """Return whether visibility enforcement has any restricted fields to protect."""
    if not getattr(layer, "enforce_visibility", False):
        return False
    if any(not getattr(metric, "public", True) for metric in layer.graph.metrics.values()):
        return True
    return any(
        not getattr(field, "public", True)
        for model in layer.graph.models.values()
        for field in [*model.dimensions, *model.metrics, *model.segments]
    )


def controls_are_active(layer: Any) -> bool:
    return has_declared_security(layer) or has_enforced_column_restrictions(layer)


def _reads_from_source(sql: str, dialect: str) -> bool:
    """Conservatively detect source reads in SQL that the semantic rewriter passed through."""
    import sqlglot
    from sqlglot import exp

    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        # Under active controls, an unparseable passthrough can never be proven safe.
        return True
    return any(True for _ in parsed.find_all(exp.Table))


def _unrecognized_sources(sql: str, layer: Any) -> list[str]:
    """Return source tables that are neither semantic models nor local CTEs."""
    import sqlglot
    from sqlglot import exp

    try:
        parsed = sqlglot.parse_one(sql, dialect=layer.dialect)
    except Exception:
        return ["<unparseable SQL>"]

    ctes = list(parsed.find_all(exp.CTE))
    cte_names = {cte.alias_or_name.lower() for cte in ctes}
    semantic_sources = {name.lower() for name in layer.graph.models}
    semantic_sources.add("metrics")
    unrecognized = {
        table.sql(dialect=layer.dialect)
        for table in parsed.find_all(exp.Table)
        if table.name.lower() not in semantic_sources and table.name.lower() not in cte_names
    }
    # A CTE name is not safely local inside its own body on every backend; it
    # can resolve to a physical table unless explicitly recursive. Reject that
    # ambiguous shadowing rather than treating it as a proven CTE reference.
    for cte in ctes:
        alias = cte.alias_or_name.lower()
        unrecognized.update(
            table.sql(dialect=layer.dialect)
            for table in cte.this.find_all(exp.Table)
            if table.name.lower() == alias and alias not in semantic_sources
        )
    return sorted(unrecognized)


def rewrite_transport_sql(
    layer: Any,
    query: str,
    *,
    user_attributes: dict | None,
    transport: str,
    strict: bool = True,
    use_preaggregations: bool | None = None,
) -> str:
    """Rewrite SQL with row/access/column controls and reject unsafe passthrough.

    When controls are active, SQL that reads a source must be recognized and
    regenerated as semantic SQL. A query that the rewriter leaves untouched is
    denied before execution because its policies cannot be proven to apply.
    Projection-only queries such as ``SELECT 1`` remain safe and available.
    """
    from sidemantic.core.semantic_layer import SecurityError
    from sidemantic.sql.query_rewriter import QueryRewriter

    if controls_are_active(layer):
        unrecognized = _unrecognized_sources(query, layer)
        if unrecognized:
            sources = ", ".join(unrecognized)
            raise SecurityError(
                f"{transport} refused non-semantic source(s) {sources} while security controls "
                "are active. Query semantic model fields, or use a structured query transport "
                "so access gates, row filters, and column restrictions are enforced."
            )

    requested_preaggregations = (
        getattr(layer, "use_preaggregations", False) if use_preaggregations is None else use_preaggregations
    )
    # Rollups are materialized without per-user row scope. Structured compile
    # already disables them for active row filters; SQL transports take the
    # conservative equivalent and bypass rollups for every secured graph.
    if has_declared_security(layer):
        requested_preaggregations = False

    rewriter = QueryRewriter(
        layer.graph,
        dialect=layer.dialect,
        use_preaggregations=requested_preaggregations,
        enforce_visibility=getattr(layer, "enforce_visibility", False),
    )
    # Yardstick's explicit and implicit measure paths expand directly against
    # physical model tables. They do not currently route those reads through
    # SQLGenerator, so they cannot apply per-user access checks, row filters,
    # or field visibility. Deny every query the rewriter would send through
    # either path until those controls can be enforced there.
    if controls_are_active(layer) and rewriter.would_use_yardstick_rewrite(query):
        raise SecurityError(
            f"{transport} refused Yardstick semantic SQL while security controls are active "
            "because that rewrite path cannot prove access gates, row filters, and column "
            "restrictions were enforced. Use a structured query or standard semantic SQL."
        )
    rewritten = rewriter.rewrite(query, strict=strict, user_attributes=user_attributes)

    def canonical(value: str) -> str:
        return value.strip().removesuffix(";").strip()

    if (
        controls_are_active(layer)
        and canonical(rewritten) == canonical(query)
        and _reads_from_source(query, layer.dialect)
    ):
        raise SecurityError(
            f"{transport} refused SQL that could not be proven to use the semantic layer while "
            "security controls are active. Query semantic model fields, or use a structured "
            "query transport so access gates, row filters, and column restrictions are enforced."
        )
    return rewritten


def deny_raw_sql(layer: Any, *, transport: str) -> None:
    """Disable a raw database bypass whenever a declared control needs enforcement."""
    from sidemantic.core.semantic_layer import SecurityError

    controls: list[str] = []
    if has_declared_security(layer):
        controls.append("model access/row policies")
    if has_enforced_column_restrictions(layer):
        controls.append("column visibility restrictions")
    if controls:
        raise SecurityError(
            f"{transport} is disabled because {' and '.join(controls)} are active and raw SQL "
            "bypasses semantic enforcement. Use structured queries or semantic SQL instead."
        )
