"""Sidemantic native YAML adapter."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph


class SidemanticAdapter(BaseAdapter):
    """Adapter for Sidemantic native YAML format.

    Native format structure:
    ```yaml
    models:
      - name: orders
        table: public.orders
        entities: [...]
        dimensions: [...]
        measures: [...]

    metrics:
      - name: total_revenue
        type: simple
        measure: orders.revenue
    ```
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Sidemantic YAML into semantic graph.

        Args:
            source: Path to YAML file

        Returns:
            Semantic graph with imported models and metrics
        """
        graph = SemanticGraph()
        source_path = Path(source)

        with open(source_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return graph

        # Parse models
        for model_def in data.get("models", []):
            model = self._parse_model(model_def)
            if model:
                graph.add_model(model)

        # Parse metrics
        for metric_def in data.get("metrics", []):
            metric = self._parse_metric(metric_def)
            if metric:
                graph.add_metric(metric)

        return graph

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Sidemantic YAML.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        data = {
            "models": [self._export_model(model) for model in graph.models.values()],
        }

        if graph.metrics:
            data["metrics"] = [self._export_metric(metric, graph) for metric in graph.metrics.values()]

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _parse_model(self, model_def: dict) -> Model | None:
        """Parse model definition.

        Args:
            model_def: Model definition dictionary

        Returns:
            Model instance or None
        """
        name = model_def.get("name")
        if not name:
            return None

        # Parse joins
        joins = []
        for relationship_def in model_def.get("relationships", []):
            join = Relationship(
                name=relationship_def.get("name"),
                type=relationship_def.get("type"),
                foreign_key=relationship_def.get("foreign_key"),
                primary_key=relationship_def.get("primary_key"),
            )
            joins.append(join)

        # Parse dimensions
        dimensions = []
        for dim_def in model_def.get("dimensions", []):
            dimension = Dimension(
                name=dim_def.get("name"),
                type=dim_def.get("type"),
                sql=dim_def.get("sql"),
                granularity=dim_def.get("granularity"),
                description=dim_def.get("description"),
                label=dim_def.get("label"),
                format=dim_def.get("format"),
                value_format_name=dim_def.get("value_format_name"),
                parent=dim_def.get("parent"),
            )
            dimensions.append(dimension)

        # Parse measures/metrics (support both field names for backwards compatibility)
        measures = []
        for measure_def in model_def.get("metrics", model_def.get("measures", [])):
            measure = Metric(
                name=measure_def.get("name"),
                agg=measure_def.get("agg"),
                sql=measure_def.get("sql"),
                filters=measure_def.get("filters"),
                description=measure_def.get("description"),
                label=measure_def.get("label"),
                format=measure_def.get("format"),
                value_format_name=measure_def.get("value_format_name"),
                drill_fields=measure_def.get("drill_fields"),
                non_additive_dimension=measure_def.get("non_additive_dimension"),
                default_time_dimension=measure_def.get("default_time_dimension"),
                default_grain=measure_def.get("default_grain"),
            )
            measures.append(measure)

        # Parse segments
        segments = []
        for seg_def in model_def.get("segments", []):
            segment = Segment(
                name=seg_def.get("name"),
                sql=seg_def.get("sql"),
                description=seg_def.get("description"),
                public=seg_def.get("public", True),
            )
            segments.append(segment)

        return Model(
            name=name,
            table=model_def.get("table"),
            sql=model_def.get("sql"),
            description=model_def.get("description"),
            primary_key=model_def.get("primary_key", "id"),
            joins=joins,
            dimensions=dimensions,
            metrics=measures,
            segments=segments,
        )

    def _parse_metric(self, metric_def: dict) -> Metric | None:
        """Parse measure definition.

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Measure instance or None
        """
        name = metric_def.get("name")
        metric_type = metric_def.get("type")

        if not name:
            return None

        return Metric(
            name=name,
            type=metric_type,
            description=metric_def.get("description"),
            label=metric_def.get("label"),
            sql=metric_def.get("sql") or metric_def.get("measure"),
            numerator=metric_def.get("numerator"),
            denominator=metric_def.get("denominator"),
            window=metric_def.get("window"),
            filters=metric_def.get("filters"),
        )

    def _export_model(self, model: Model) -> dict:
        """Export model to dictionary.

        Args:
            model: Model to export

        Returns:
            Model definition dictionary
        """
        result = {"name": model.name}

        if model.table:
            result["table"] = model.table
        if model.sql:
            result["sql"] = model.sql
        if model.description:
            result["description"] = model.description

        # Export joins
        if model.relationships:
            result["relationships"] = [
                {
                    "name": relationship.name,
                    "type": relationship.type,
                    **({"foreign_key": relationship.foreign_key} if relationship.foreign_key else {}),
                    **({"primary_key": relationship.primary_key} if relationship.primary_key else {}),
                }
                for relationship in model.relationships
            ]

        # Export primary key
        if model.primary_key != "id":  # Only export if non-default
            result["primary_key"] = model.primary_key

        # Export dimensions
        if model.dimensions:
            result["dimensions"] = []
            for dim in model.dimensions:
                dim_def = {
                    "name": dim.name,
                    "type": dim.type,
                }
                if dim.sql:
                    dim_def["sql"] = dim.sql
                if dim.granularity:
                    dim_def["granularity"] = dim.granularity
                if dim.description:
                    dim_def["description"] = dim.description
                if dim.label:
                    dim_def["label"] = dim.label
                if dim.format:
                    dim_def["format"] = dim.format
                if dim.value_format_name:
                    dim_def["value_format_name"] = dim.value_format_name
                if dim.parent:
                    dim_def["parent"] = dim.parent
                result["dimensions"].append(dim_def)

        # Export metrics (model-level aggregations)
        if model.metrics:
            result["metrics"] = []
            for measure in model.metrics:
                measure_def = {
                    "name": measure.name,
                    "agg": measure.agg,
                }
                if measure.sql:
                    measure_def["sql"] = measure.sql
                if measure.filters:
                    measure_def["filters"] = measure.filters
                if measure.description:
                    measure_def["description"] = measure.description
                if measure.label:
                    measure_def["label"] = measure.label
                if measure.format:
                    measure_def["format"] = measure.format
                if measure.value_format_name:
                    measure_def["value_format_name"] = measure.value_format_name
                if measure.drill_fields:
                    measure_def["drill_fields"] = measure.drill_fields
                if measure.non_additive_dimension:
                    measure_def["non_additive_dimension"] = measure.non_additive_dimension
                if measure.default_time_dimension:
                    measure_def["default_time_dimension"] = measure.default_time_dimension
                if measure.default_grain:
                    measure_def["default_grain"] = measure.default_grain
                result["metrics"].append(measure_def)

        # Export segments
        if model.segments:
            result["segments"] = []
            for segment in model.segments:
                seg_def = {
                    "name": segment.name,
                    "sql": segment.sql,
                }
                if segment.description:
                    seg_def["description"] = segment.description
                if not segment.public:  # Only export if non-default (False)
                    seg_def["public"] = segment.public
                result["segments"].append(seg_def)

        return result

    def _export_metric(self, measure: Metric, graph) -> dict:
        """Export measure to dictionary.

        Args:
            measure: Metric to export

        Returns:
            Measure definition dictionary
        """
        result = {
            "name": measure.name,
        }

        if measure.type:
            result["type"] = measure.type

        if measure.description:
            result["description"] = measure.description
        if measure.label:
            result["label"] = measure.label

        # Type-specific fields
        if measure.numerator:
            result["numerator"] = measure.numerator
        if measure.denominator:
            result["denominator"] = measure.denominator
        if measure.sql:
            result["sql"] = measure.sql
            # Auto-detect and export dependencies for derived measures
            if measure.type == "derived":
                dependencies = measure.get_dependencies(graph)
                if dependencies:
                    result["metrics"] = list(dependencies)
        if measure.window:
            result["window"] = measure.window
        if measure.filters:
            result["filters"] = measure.filters

        return result
