"""Base adapter interface for importing semantic models."""

from abc import ABC, abstractmethod
from pathlib import Path

from sidemantic.core.metric import Metric
from sidemantic.core.semantic_graph import SemanticGraph


def filtered_measure_sql(metric: Metric, model_name: str) -> str | None:
    """Fold a simple aggregate's row filters into the expression it aggregates.

    Formats without a measure-level filter can still carry a filtered measure
    as ``AGG(CASE WHEN <filters> THEN <value> END)``: aggregates skip the NULLs
    produced for non-matching rows, so the result equals the filtered
    aggregate. Returns the ``CASE`` expression, or ``None`` when the filters
    cannot be evaluated on the metric's own rows (another model's columns,
    subqueries, templates) and the export must report the loss instead.
    """
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import SqlglotError

    def own_row_sql(sql: str) -> str | None:
        local = sql.replace("{model}.", "")
        try:
            parsed = sqlglot.parse_one(local)
        except SqlglotError:
            return None
        if parsed.find(exp.Select) is not None:
            return None
        qualified = [column for column in parsed.find_all(exp.Column) if column.table]
        if not qualified:
            return local
        if any(column.table != model_name for column in qualified):
            return None
        for column in qualified:
            column.set("table", None)
        return parsed.sql()

    conditions = [own_row_sql(predicate) for predicate in metric.filters or []]
    if not conditions or any(condition is None for condition in conditions):
        return None
    if metric.agg == "count" and not metric.sql:
        value = "1"
    else:
        value = own_row_sql(metric.sql or metric.name)
        if value is None:
            return None
    condition = conditions[0] if len(conditions) == 1 else " AND ".join(f"({condition})" for condition in conditions)
    return f"CASE WHEN {condition} THEN {value} END"


def filtered_aggregate_sql(metric: Metric, model_name: str) -> str | None:
    """Complete aggregate SQL for a simple aggregate, with any row filters folded in.

    Returns ``None`` when the metric has filters that cannot be folded.
    """
    if not metric.filters:
        return (
            metric.model_copy(update={"sql": metric.sql.replace("{model}.", "")}).to_sql()
            if metric.sql
            else metric.to_sql()
        )
    folded = filtered_measure_sql(metric, model_name)
    if folded is None:
        return None
    return metric.model_copy(update={"sql": folded}).to_sql()


def reject_unsupported_export(format_name: str, problems: list[tuple[str, str, str]]) -> None:
    """Report every construct the target format cannot carry, then refuse the export.

    ``problems`` holds ``(feature, location, detail)`` entries. Each becomes an
    ``unsupported`` export diagnostic, which blocks publication through the
    shared conversion path; raising also keeps direct ``adapter.export()``
    callers from receiving output that silently omits those constructs.
    """
    if not problems:
        return
    from sidemantic.fidelity import record_export_feature

    for feature, location, detail in problems:
        record_export_feature(feature, "unsupported", source=format_name, location=location, detail=detail)
    raise ValueError(f"{format_name} export cannot represent: " + "; ".join(detail for _, _, detail in problems))


def graph_only_metric_names(graph: SemanticGraph, models: dict) -> list[str]:
    """Names of graph-level metrics that no exported model owns."""
    owned = {id(metric) for model in models.values() for metric in model.metrics}
    owned |= {id(metric) for model in graph.models.values() for metric in model.metrics}
    return [name for name, metric in graph.metrics.items() if id(metric) not in owned]


class BaseAdapter(ABC):
    """Base adapter for importing/exporting semantic models from external formats."""

    @abstractmethod
    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse external format into semantic graph.

        Args:
            source: Path to file or directory containing semantic model definitions

        Returns:
            Semantic graph with imported models and metrics
        """
        raise NotImplementedError

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to external format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output file

        Raises:
            NotImplementedError: If export is not supported by this adapter
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support export")

    def validate(self, graph: SemanticGraph) -> list[str]:
        """Validate imported semantic graph.

        Args:
            graph: Semantic graph to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Basic validation
        for model_name, model in graph.models.items():
            # Check for primary key
            if not model.primary_key:
                errors.append(f"Model {model_name} has no primary key")

            # Check for table or SQL
            if not model.table and not model.sql:
                errors.append(f"Model {model_name} has neither table nor sql definition")

        return errors
