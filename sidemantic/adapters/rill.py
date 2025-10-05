"""Rill adapter for importing and exporting Rill metrics view YAML files.

Rill separates data loading (Model YAML) from semantic definitions (Metrics View YAML).
This adapter focuses on the Metrics View YAML which defines dimensions and measures.
"""

from pathlib import Path
from typing import Any

import sqlglot
import yaml
from sqlglot import expressions as exp

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


class RillAdapter:
    """Adapter for Rill metrics view YAML format."""

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Rill metrics view YAML file(s) into a SemanticGraph.

        Args:
            source: Path to a Rill metrics view YAML file or directory

        Returns:
            SemanticGraph containing the parsed models
        """
        source_path = Path(source)

        graph = SemanticGraph()
        if source_path.is_file():
            model = self._parse_file(source_path)
            if model:
                graph.add_model(model)
        else:
            for yaml_file in source_path.glob("**/*.yaml"):
                model = self._parse_file(yaml_file)
                if model:
                    graph.add_model(model)
            for yml_file in source_path.glob("**/*.yml"):
                model = self._parse_file(yml_file)
                if model:
                    graph.add_model(model)

        return graph

    def _parse_file(self, file_path: Path) -> Model | None:
        """Parse a single Rill YAML file.

        Args:
            file_path: Path to the YAML file

        Returns:
            Model if the file is a metrics_view, None otherwise
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data or data.get("type") != "metrics_view":
            return None

        model_name = data.get("name") or file_path.stem
        data.get("display_name")
        description = data.get("description")

        # Get the source table or model
        table = data.get("table") or data.get("model")

        # Parse dimensions
        dimensions: list[Dimension] = []
        timeseries_column = data.get("timeseries")
        smallest_time_grain = data.get("smallest_time_grain")

        for dim_def in data.get("dimensions", []):
            dimension = self._parse_dimension(dim_def, timeseries_column, smallest_time_grain)
            if dimension:
                dimensions.append(dimension)

        # If timeseries is specified but not found in dimensions, create it
        if timeseries_column:
            has_timeseries = any(d.sql == timeseries_column or d.name == timeseries_column for d in dimensions)
            if not has_timeseries:
                time_dim = Dimension(
                    name=timeseries_column,
                    sql=timeseries_column,
                    type="time",
                    granularity=self._map_time_grain(smallest_time_grain),
                )
                dimensions.append(time_dim)

        # Parse measures
        metrics: list[Metric] = []
        for measure_def in data.get("measures", []):
            metric = self._parse_measure(measure_def)
            if metric:
                metrics.append(metric)

        return Model(
            name=model_name,
            description=description,
            table=table,
            dimensions=dimensions,
            metrics=metrics,
        )

    def _parse_dimension(
        self,
        dim_def: dict[str, Any],
        timeseries_column: str | None,
        smallest_time_grain: str | None,
    ) -> Dimension | None:
        """Parse a Rill dimension into a Sidemantic Dimension.

        Args:
            dim_def: Dimension definition from Rill YAML
            timeseries_column: Name of the timeseries column
            smallest_time_grain: Smallest time grain for time dimensions

        Returns:
            Dimension or None if parsing fails
        """
        name = dim_def.get("name")
        if not name:
            return None

        label = dim_def.get("display_name")  # Rill uses display_name, Sidemantic uses label
        description = dim_def.get("description")
        sql = dim_def.get("expression") or dim_def.get("column")

        if not sql:
            return None

        # Determine if this is the timeseries dimension
        is_timeseries = timeseries_column and (sql == timeseries_column or name == timeseries_column)

        return Dimension(
            name=name,
            label=label,
            description=description,
            sql=sql,
            type="time" if is_timeseries else "categorical",
            granularity=self._map_time_grain(smallest_time_grain) if is_timeseries else None,
        )

    def _parse_measure(self, measure_def: dict[str, Any]) -> Metric | None:
        """Parse a Rill measure into a Sidemantic Metric.

        Args:
            measure_def: Measure definition from Rill YAML

        Returns:
            Metric or None if parsing fails
        """
        name = measure_def.get("name")
        expression = measure_def.get("expression")

        if not name or not expression:
            return None

        label = measure_def.get("display_name")  # Rill uses display_name, Sidemantic uses label
        description = measure_def.get("description")
        measure_type = measure_def.get("type", "simple")

        # Determine metric type based on Rill's type
        # "simple" = basic aggregation (None type), "derived" = calculation using other measures
        metric_type = None
        if measure_type == "derived" or measure_def.get("requires"):
            metric_type = "derived"

        # Use sqlglot to detect simple aggregations
        agg_type = None
        agg_sql = None
        try:
            parsed = sqlglot.parse_one(expression, read="duckdb")

            # Check if this is a simple aggregation function
            if isinstance(parsed, (exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max)):
                # Map sqlglot aggregation types to Sidemantic agg types
                if isinstance(parsed, exp.Sum):
                    agg_type = "sum"
                elif isinstance(parsed, exp.Avg):
                    agg_type = "avg"
                elif isinstance(parsed, exp.Count):
                    if parsed.args.get("distinct"):
                        agg_type = "count_distinct"
                    else:
                        agg_type = "count"
                elif isinstance(parsed, exp.Min):
                    agg_type = "min"
                elif isinstance(parsed, exp.Max):
                    agg_type = "max"

                # Extract the aggregated column/expression
                agg_arg = parsed.this
                if agg_arg:
                    agg_sql = agg_arg.sql(dialect="duckdb")
                elif isinstance(parsed, exp.Count):
                    # COUNT(*) case
                    agg_sql = None
        except Exception:
            # If parsing fails, treat as custom SQL expression
            pass

        return Metric(
            name=name,
            label=label,
            description=description,
            agg=agg_type,
            sql=agg_sql if agg_type else expression,
            type=metric_type,
        )

    def _map_time_grain(self, grain: str | None) -> str:
        """Map Rill time grain to Sidemantic granularity.

        Args:
            grain: Rill time grain (millisecond, second, minute, hour, day, week, month, quarter, year)

        Returns:
            Sidemantic granularity (hour, day, week, month, quarter, year)
        """
        if not grain:
            return "day"

        grain_mapping = {
            "millisecond": "hour",
            "second": "hour",
            "minute": "hour",
            "hour": "hour",
            "day": "day",
            "week": "week",
            "month": "month",
            "quarter": "quarter",
            "year": "year",
        }

        return grain_mapping.get(grain, "day")

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export a SemanticGraph to Rill metrics view YAML files.

        Creates one metrics view YAML file per model.

        Args:
            graph: The semantic graph to export
            output_path: Directory to write the YAML files to
        """
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Resolve inheritance before export
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        for model in resolved_models.values():
            self._export_model(model, output_dir, graph)

    def _export_model(self, model: Model, output_dir: Path, graph: SemanticGraph) -> None:
        """Export a single Model to a Rill metrics view YAML file.

        Args:
            model: The model to export
            output_dir: Directory to write the file to
            graph: The full semantic graph (for context)
        """
        metrics_view: dict[str, Any] = {
            "type": "metrics_view",
        }

        # Model doesn't have display_name, so we skip it

        if model.description:
            metrics_view["description"] = model.description

        # Set the model reference (could be a table or model name)
        if model.table:
            # If it looks like a model reference (no dots/schemas), use model field
            if "." not in model.table:
                metrics_view["model"] = model.table
            else:
                metrics_view["table"] = model.table
        else:
            # Default to model name + _model
            metrics_view["model"] = f"{model.name}_model"

        # Export dimensions
        dimensions = []
        timeseries_column = None
        smallest_time_grain = None

        for dim in model.dimensions:
            dim_def: dict[str, Any] = {
                "name": dim.name,
            }

            if dim.label:
                dim_def["display_name"] = dim.label

            if dim.description:
                dim_def["description"] = dim.description

            # Use column if SQL is simple column reference, otherwise use expression
            sql = dim.sql or dim.name  # Default to name if no SQL specified
            if sql and (sql.isidentifier() or sql.replace("_", "").isalnum()):
                dim_def["column"] = sql
            else:
                dim_def["expression"] = sql

            # Track timeseries dimension
            if dim.type == "time":
                if not timeseries_column:
                    timeseries_column = dim.sql
                    if dim.granularity:
                        smallest_time_grain = self._map_granularity_to_rill(dim.granularity)

            dimensions.append(dim_def)

        if dimensions:
            metrics_view["dimensions"] = dimensions

        if timeseries_column:
            metrics_view["timeseries"] = timeseries_column

        if smallest_time_grain:
            metrics_view["smallest_time_grain"] = smallest_time_grain

        # Export measures
        measures = []
        for metric in model.metrics:
            # Build expression from agg + sql or just sql
            if metric.agg and metric.sql:
                expression = f"{metric.agg.upper()}({metric.sql})"
            elif metric.agg:
                expression = f"{metric.agg.upper()}(*)"
            else:
                expression = metric.sql or ""

            measure_def: dict[str, Any] = {
                "name": metric.name,
                "expression": expression,
            }

            if metric.label:
                measure_def["display_name"] = metric.label

            if metric.description:
                measure_def["description"] = metric.description

            # Map metric type to Rill measure type
            if metric.type == "derived":
                measure_def["type"] = "derived"
            # else: default is "simple", no need to specify

            measures.append(measure_def)

        if measures:
            metrics_view["measures"] = measures

        # Write to file
        output_file = output_dir / f"{model.name}.yaml"
        with open(output_file, "w") as f:
            yaml.dump(metrics_view, f, sort_keys=False, default_flow_style=False)

    def _map_granularity_to_rill(self, granularity: str) -> str:
        """Map Sidemantic granularity to Rill time grain.

        Args:
            granularity: Sidemantic granularity

        Returns:
            Rill time grain
        """
        # Sidemantic uses: hour, day, week, month, quarter, year
        # Rill uses: millisecond, second, minute, hour, day, week, month, quarter, year
        # Direct mapping for most values
        return granularity
