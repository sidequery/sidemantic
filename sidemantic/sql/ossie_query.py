"""Ossie query routing hints without performing semantic expansion."""

from sqlglot import exp

from sidemantic.core.semantic_graph import SemanticGraph


def has_ossie_aggregate_query(statement: exp.Expression, graph: SemanticGraph) -> bool:
    """Identify the reserved measure operator without expanding it in Python."""
    return any((model.metadata or {}).get("ossie_scope_name") for model in graph.models.values()) and any(
        isinstance(node, getattr(exp, "Agg", ())) or isinstance(node, exp.Anonymous) and node.name.upper() == "AGG"
        for node in statement.walk()
    )
