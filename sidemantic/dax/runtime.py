"""Runtime helpers for parsing/translating DAX against a semantic graph."""

from __future__ import annotations

from typing import Any

from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.dax import RelationshipEdge, translate_dax_query


def parse_dax_query(text: str) -> Any:
    """Parse raw DAX query text into sidemantic_dax AST."""
    try:
        import sidemantic_dax
    except Exception as exc:
        raise RuntimeError(
            "sidemantic_dax is required for DAX query execution. Install DAX extras and retry (e.g. `uv sync --extra dax`)."
        ) from exc

    try:
        return sidemantic_dax.parse_query(text)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            raise RuntimeError(
                "sidemantic_dax native module is not available. Rebuild/install DAX extras (e.g. `uv sync --extra dax`)."
            ) from exc
        raise


def build_dax_translation_context(graph: SemanticGraph) -> dict[str, Any]:
    """Build query translation context from graph models/relationships."""
    column_sql_by_table: dict[str, dict[str, str]] = {}
    measure_names_by_table: dict[str, set[str]] = {}
    measure_aggs_by_table: dict[str, dict[str, str]] = {}
    measure_sql_by_table: dict[str, dict[str, str]] = {}
    measure_filters_by_table: dict[str, dict[str, list[str]]] = {}
    time_dimensions_by_table: dict[str, set[str]] = {}

    for model_name, model in graph.models.items():
        column_sql_by_table[model_name] = {dim.name: (dim.sql or dim.name) for dim in model.dimensions}
        measure_names_by_table[model_name] = {metric.name for metric in model.metrics}
        measure_aggs_by_table[model_name] = {metric.name: metric.agg for metric in model.metrics if metric.agg}
        measure_sql_by_table[model_name] = {metric.name: metric.sql for metric in model.metrics if metric.sql}
        measure_filters_by_table[model_name] = {
            metric.name: list(metric.filters or []) for metric in model.metrics if metric.filters
        }
        time_dimensions_by_table[model_name] = {dim.name for dim in model.dimensions if dim.type == "time"}

    edges: list[RelationshipEdge] = []
    seen_edges: set[tuple[str, str, str, str]] = set()
    for model_name, model in graph.models.items():
        for rel in model.relationships:
            if not rel.active:
                continue
            related_model = graph.models.get(rel.name)
            if related_model is None:
                continue

            if rel.type == "many_to_one":
                from_column = rel.foreign_key or rel.sql_expr
                to_column = rel.primary_key or related_model.primary_key
            elif rel.type in ("one_to_many", "one_to_one"):
                from_column = _relationship_tmdl_from_column(rel) or model.primary_key
                to_column = rel.foreign_key or rel.sql_expr
            elif rel.type == "many_to_many":
                from_column = rel.foreign_key or rel.sql_expr
                to_column = rel.primary_key or related_model.primary_key
            else:
                continue

            if not from_column or not to_column:
                continue

            key = (model_name.lower(), from_column.lower(), related_model.name.lower(), to_column.lower())
            reverse = (related_model.name.lower(), to_column.lower(), model_name.lower(), from_column.lower())
            if key in seen_edges or reverse in seen_edges:
                continue

            seen_edges.add(key)
            edges.append(
                RelationshipEdge(
                    from_table=model_name,
                    from_column=from_column,
                    to_table=related_model.name,
                    to_column=to_column,
                )
            )

    return {
        "column_sql_by_table": column_sql_by_table,
        "measure_names_by_table": measure_names_by_table,
        "measure_aggs_by_table": measure_aggs_by_table,
        "measure_sql_by_table": measure_sql_by_table,
        "measure_filters_by_table": measure_filters_by_table,
        "time_dimensions_by_table": time_dimensions_by_table,
        "relationship_edges": edges,
    }


def _relationship_tmdl_from_column(rel: Any) -> str | None:
    value = getattr(rel, "_tmdl_from_column", None)
    if isinstance(value, str) and value.strip():
        return value
    return None


def translate_dax_query_ast(query_ast: Any, graph: SemanticGraph) -> Any:
    """Translate parsed DAX query AST into executable SQL payloads."""
    context = build_dax_translation_context(graph)
    return translate_dax_query(query_ast, **context)
