"""Shared fail-closed security gates for SQL-based transports."""

from __future__ import annotations

from typing import Any


def has_declared_security(layer: Any) -> bool:
    """Return whether any model declares an access or row-level policy."""
    return any(getattr(model, "security", None) is not None for model in layer.graph.models.values())


def has_enforced_column_restrictions(layer: Any) -> bool:
    """Return whether semantic column visibility is being enforced.

    The enforcement flag itself is the security boundary. Even when every
    declared semantic field is public, physical source columns that are absent
    from the model must not become discoverable or queryable through another
    transport.
    """
    return bool(getattr(layer, "enforce_visibility", False))


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
    from sqlglot.optimizer.scope import traverse_scope

    try:
        parsed = sqlglot.parse_one(sql, dialect=layer.dialect)
        scopes = traverse_scope(parsed)
    except Exception:
        return ["<unparseable SQL>"]

    semantic_sources = {name.lower() for name in layer.graph.models}
    semantic_sources.add("metrics")
    unrecognized: set[str] = set()
    try:
        for scope in scopes:
            for _name, (_node, source) in scope.selected_sources.items():
                # In-scope CTEs and derived tables resolve to another Scope.
                # A Table source is a real database read in this exact scope.
                if isinstance(source, exp.Table) and source.name.lower() not in semantic_sources:
                    unrecognized.add(source.sql(dialect=layer.dialect))
    except Exception:
        return ["<unparseable SQL>"]
    return sorted(unrecognized)


def _has_unsafe_subquery(sql: str, dialect: str) -> bool:
    """Return whether an expression embeds a SELECT the rewriter cannot secure."""
    import sqlglot
    from sqlglot import exp

    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        return True

    for select in parsed.find_all(exp.Select):
        query: exp.Expression = select
        while isinstance(query.parent, exp.SetOperation):
            query = query.parent

        parent = query.parent
        if parent is None or isinstance(parent, exp.CTE):
            continue
        if isinstance(parent, exp.Subquery):
            container = parent.parent
            if isinstance(container, exp.SetOperation):
                continue
            if isinstance(container, (exp.From, exp.Join)) and container.this is parent:
                continue
        return True
    return False


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
        if _has_unsafe_subquery(query, layer.dialect):
            raise SecurityError(
                f"{transport} refused a predicate subquery, projection subquery, or other expression subquery "
                "while security controls are active because nested expression reads cannot currently prove "
                "that access gates, row "
                "filters, and column restrictions were enforced. Rewrite it as structured "
                "semantic filters or a supported semantic join."
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
        # The optional Rust planner does not accept caller attributes or run
        # SQLGenerator's policy/visibility checks yet. Keep secured transports
        # on the policy-aware Python planner even when it is enabled globally.
        use_rust_rewriter=False if controls_are_active(layer) else None,
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
    if controls_are_active(layer):
        explanation = rewriter.explain(query, strict=strict, user_attributes=user_attributes)
        if explanation.chosen_plan == "passthrough_plain_sql" and _reads_from_source(query, layer.dialect):
            raise SecurityError(
                f"{transport} refused SQL that could not be proven to use the semantic layer while "
                "security controls are active. Query semantic model fields, or use a structured "
                "query transport so access gates, row filters, and column restrictions are enforced."
            )
        island_rejection = explanation.rejected_rules.get("semantic_island_optimization")
        if island_rejection and _reads_from_source(query, layer.dialect):
            raise SecurityError(
                f"{transport} refused a semantic subquery that could not be secured ({island_rejection}). "
                "Rewrite the nested query using declared semantic fields, or use a structured query "
                "transport so access gates, row filters, and column restrictions are enforced."
            )
        rewritten = explanation.rewritten_sql
    else:
        rewritten = rewriter.rewrite(query, strict=strict, user_attributes=user_attributes)
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
