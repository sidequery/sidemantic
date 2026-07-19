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
        for projection in select.expressions:
            if any(True for _ in projection.find_all(exp.Select)):
                return True
        for clause_name in ("where", "having", "qualify"):
            clause = select.args.get(clause_name)
            if clause is not None and any(True for _ in clause.find_all(exp.Select)):
                return True
        for join in select.args.get("joins") or []:
            predicate = join.args.get("on")
            if predicate is not None and any(True for _ in predicate.find_all(exp.Select)):
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
                f"{transport} refused a predicate subquery or projection subquery while security controls "
                "are active because nested expression reads cannot currently prove that access gates, row "
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
