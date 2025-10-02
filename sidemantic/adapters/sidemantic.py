"""Sidemantic native YAML adapter."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.entity import Entity
from sidemantic.core.measure import Measure
from sidemantic.core.model import Model
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

        # Parse entities
        entities = []
        for entity_def in model_def.get("entities", []):
            entity = Entity(
                name=entity_def.get("name"),
                type=entity_def.get("type"),
                expr=entity_def.get("expr"),
            )
            entities.append(entity)

        # Parse dimensions
        dimensions = []
        for dim_def in model_def.get("dimensions", []):
            dimension = Dimension(
                name=dim_def.get("name"),
                type=dim_def.get("type"),
                expr=dim_def.get("expr"),
                granularity=dim_def.get("granularity"),
                description=dim_def.get("description"),
                label=dim_def.get("label"),
            )
            dimensions.append(dimension)

        # Parse measures
        measures = []
        for measure_def in model_def.get("measures", []):
            measure = Measure(
                name=measure_def.get("name"),
                agg=measure_def.get("agg"),
                expr=measure_def.get("expr"),
                filters=measure_def.get("filters"),
                description=measure_def.get("description"),
                label=measure_def.get("label"),
            )
            measures.append(measure)

        return Model(
            name=name,
            table=model_def.get("table"),
            sql=model_def.get("sql"),
            description=model_def.get("description"),
            entities=entities,
            dimensions=dimensions,
            measures=measures,
        )

    def _parse_metric(self, metric_def: dict) -> Measure | None:
        """Parse measure definition.

        Args:
            metric_def: Measure definition dictionary

        Returns:
            Measure instance or None
        """
        name = metric_def.get("name")
        metric_type = metric_def.get("type")

        if not name:
            return None

        return Measure(
            name=name,
            type=metric_type,
            description=metric_def.get("description"),
            label=metric_def.get("label"),
            expr=metric_def.get("expr") or metric_def.get("measure"),
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

        # Export entities
        result["entities"] = [
            {
                "name": entity.name,
                "type": entity.type,
                **({"expr": entity.expr} if entity.expr else {}),
            }
            for entity in model.entities
        ]

        # Export dimensions
        if model.dimensions:
            result["dimensions"] = []
            for dim in model.dimensions:
                dim_def = {
                    "name": dim.name,
                    "type": dim.type,
                }
                if dim.expr:
                    dim_def["expr"] = dim.expr
                if dim.granularity:
                    dim_def["granularity"] = dim.granularity
                if dim.description:
                    dim_def["description"] = dim.description
                if dim.label:
                    dim_def["label"] = dim.label
                result["dimensions"].append(dim_def)

        # Export measures
        if model.measures:
            result["measures"] = []
            for measure in model.measures:
                measure_def = {
                    "name": measure.name,
                    "agg": measure.agg,
                }
                if measure.expr:
                    measure_def["expr"] = measure.expr
                if measure.filters:
                    measure_def["filters"] = measure.filters
                if measure.description:
                    measure_def["description"] = measure.description
                if measure.label:
                    measure_def["label"] = measure.label
                result["measures"].append(measure_def)

        return result

    def _export_metric(self, measure: Measure, graph) -> dict:
        """Export measure to dictionary.

        Args:
            measure: Measure to export

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
        if measure.expr:
            result["expr"] = measure.expr
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
