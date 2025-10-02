"""MetricFlow adapter for importing dbt semantic layer models."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.entity import Entity
from sidemantic.core.measure import Measure
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


class MetricFlowAdapter(BaseAdapter):
    """Adapter for importing/exporting dbt MetricFlow semantic models.

    Transforms MetricFlow definitions into Sidemantic format:
    - Semantic models → Models
    - Entities → Entities (direct mapping)
    - Dimensions → Dimensions
    - Measures → Measures
    - Metrics → Metrics (all 5 types)
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse MetricFlow YAML files into semantic graph.

        Args:
            source: Path to YAML file or directory containing semantic models

        Returns:
            Semantic graph with imported models and metrics
        """
        graph = SemanticGraph()
        source_path = Path(source)

        if source_path.is_dir():
            # Parse all YAML files in directory
            for yaml_file in source_path.rglob("*.yml"):
                self._parse_file(yaml_file, graph)
            for yaml_file in source_path.rglob("*.yaml"):
                self._parse_file(yaml_file, graph)
        else:
            # Parse single file
            self._parse_file(source_path, graph)

        return graph

    def _parse_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse a single MetricFlow YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models/metrics to
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # Parse semantic models
        for model_def in data.get("semantic_models", []):
            model = self._parse_semantic_model(model_def)
            if model:
                graph.add_model(model)

        # Parse metrics
        for metric_def in data.get("metrics", []):
            metric = self._parse_metric(metric_def)
            if metric:
                graph.add_metric(metric)

    def _parse_semantic_model(self, model_def: dict) -> Model | None:
        """Parse MetricFlow semantic model into Model.

        Args:
            model_def: Semantic model definition dictionary

        Returns:
            Model instance or None
        """
        name = model_def.get("name")
        if not name:
            return None

        # Get table from model ref or config
        model_ref = model_def.get("model", "")
        table = None

        # Extract table from config.meta.hex if present
        config = model_def.get("config", {})
        meta = config.get("meta", {})
        hex_config = meta.get("hex", {})
        table = hex_config.get("table")

        # If no table in config, use model name as fallback
        if not table:
            # Try to extract from ref()
            if "ref(" in model_ref:
                ref_model = model_ref.replace("ref('", "").replace("')", "").replace('ref("', "").replace('")', "")
                table = ref_model

        # Parse entities
        entities = []
        for entity_def in model_def.get("entities", []):
            entity = self._parse_entity(entity_def)
            if entity:
                entities.append(entity)

        # Parse dimensions
        dimensions = []
        for dim_def in model_def.get("dimensions", []):
            dim = self._parse_dimension(dim_def)
            if dim:
                dimensions.append(dim)

        # Parse measures
        measures = []
        for measure_def in model_def.get("measures", []):
            measure = self._parse_measure(measure_def)
            if measure:
                measures.append(measure)

        return Model(
            name=name,
            table=table,
            description=model_def.get("description"),
            entities=entities,
            dimensions=dimensions,
            measures=measures,
        )

    def _parse_entity(self, entity_def: dict) -> Entity | None:
        """Parse MetricFlow entity into Sidemantic entity.

        Args:
            entity_def: Entity definition dictionary

        Returns:
            Entity instance or None
        """
        name = entity_def.get("name")
        if not name:
            return None

        entity_type = entity_def.get("type", "primary")

        # Map MetricFlow entity types to Sidemantic
        # natural -> unique (close enough for our purposes)
        type_mapping = {
            "primary": "primary",
            "foreign": "foreign",
            "unique": "unique",
            "natural": "unique",
        }

        sidemantic_type = type_mapping.get(entity_type, "primary")

        return Entity(
            name=name,
            type=sidemantic_type,
            expr=entity_def.get("expr", name),
        )

    def _parse_dimension(self, dim_def: dict) -> Dimension | None:
        """Parse MetricFlow dimension into Sidemantic dimension.

        Args:
            dim_def: Dimension definition dictionary

        Returns:
            Dimension instance or None
        """
        name = dim_def.get("name")
        if not name:
            return None

        dim_type = dim_def.get("type", "categorical")

        # MetricFlow has categorical and time types
        type_mapping = {
            "categorical": "categorical",
            "time": "time",
        }

        sidemantic_type = type_mapping.get(dim_type, "categorical")

        # For time dimensions, extract granularity from type_params
        granularity = None
        if dim_type == "time":
            type_params = dim_def.get("type_params", {})
            granularity = type_params.get("time_granularity", "day")

        return Dimension(
            name=name,
            type=sidemantic_type,
            expr=dim_def.get("expr"),
            granularity=granularity,
            description=dim_def.get("description"),
            label=dim_def.get("label"),
        )

    def _parse_measure(self, measure_def: dict) -> Measure | None:
        """Parse MetricFlow measure into Sidemantic measure.

        Args:
            measure_def: Measure definition dictionary

        Returns:
            Measure instance or None
        """
        name = measure_def.get("name")
        if not name:
            return None

        agg_type = measure_def.get("agg", "sum")

        # Map MetricFlow aggregation types
        type_mapping = {
            "sum": "sum",
            "count": "count",
            "count_distinct": "count_distinct",
            "average": "avg",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "median": "median",
            "sum_boolean": "sum",
        }

        sidemantic_agg = type_mapping.get(agg_type, "sum")

        return Measure(
            name=name,
            agg=sidemantic_agg,
            expr=measure_def.get("expr"),
            description=measure_def.get("description"),
            label=measure_def.get("label"),
        )

    def _parse_metric(self, metric_def: dict) -> Metric | None:
        """Parse MetricFlow metric into Sidemantic metric.

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Metric instance or None
        """
        name = metric_def.get("name")
        if not name:
            return None

        metric_type = metric_def.get("type", "simple")

        # Map MetricFlow metric types
        type_mapping = {
            "simple": "simple",
            "ratio": "ratio",
            "derived": "derived",
            "cumulative": "cumulative",
            # conversion not yet supported
        }

        sidemantic_type = type_mapping.get(metric_type)
        if not sidemantic_type:
            return None  # Skip unsupported metric types

        # Extract type-specific parameters
        type_params = metric_def.get("type_params", {})

        # Simple metric
        measure = None
        if metric_type == "simple":
            measure_def = type_params.get("measure", {})
            if isinstance(measure_def, dict):
                measure = measure_def.get("name")
            else:
                measure = measure_def

        # Ratio metric
        numerator = None
        denominator = None
        if metric_type == "ratio":
            numerator_def = type_params.get("numerator", {})
            denominator_def = type_params.get("denominator", {})

            if isinstance(numerator_def, dict):
                numerator = numerator_def.get("name")
            else:
                numerator = numerator_def

            if isinstance(denominator_def, dict):
                denominator = denominator_def.get("name")
            else:
                denominator = denominator_def

        # Derived metric
        expr = None
        metrics = None
        if metric_type == "derived":
            expr = type_params.get("expr")
            metrics_list = type_params.get("metrics", [])
            metrics = [m.get("name") if isinstance(m, dict) else m for m in metrics_list]

        # Cumulative metric
        window = None
        if metric_type == "cumulative":
            cumulative_params = type_params.get("cumulative_type_params", {})
            window = cumulative_params.get("window")

        # Parse filter
        filter_expr = metric_def.get("filter")
        filters = [filter_expr] if filter_expr else None

        return Metric(
            name=name,
            type=sidemantic_type,
            description=metric_def.get("description"),
            label=metric_def.get("label"),
            measure=measure,
            numerator=numerator,
            denominator=denominator,
            expr=expr,
            metrics=metrics,
            window=window,
            filters=filters,
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to MetricFlow YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        # Export semantic models
        semantic_models = []
        for model in graph.models.values():
            semantic_model = self._export_semantic_model(model)
            semantic_models.append(semantic_model)

        data = {"semantic_models": semantic_models}

        # Export metrics if present
        if graph.metrics:
            data["metrics"] = [self._export_metric(metric, graph) for metric in graph.metrics.values()]

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _export_semantic_model(self, model: Model) -> dict:
        """Export model to MetricFlow semantic model.

        Args:
            model: Model to export

        Returns:
            Semantic model definition dictionary
        """
        result = {"name": model.name}

        if model.table:
            result["model"] = f"ref('{model.table.split('.')[-1]}')"
        elif model.sql:
            result["sql"] = model.sql

        if model.description:
            result["description"] = model.description

        # Export entities
        result["entities"] = []
        for entity in model.entities:
            entity_def = {
                "name": entity.name,
                "type": entity.type,
            }
            if entity.expr:
                entity_def["expr"] = entity.expr
            result["entities"].append(entity_def)

        # Export dimensions
        if model.dimensions:
            result["dimensions"] = []
            for dim in model.dimensions:
                dim_def = {"name": dim.name, "type": dim.type}

                if dim.expr:
                    dim_def["expr"] = dim.expr

                if dim.type == "time" and dim.granularity:
                    dim_def["type_params"] = {"time_granularity": dim.granularity}

                if dim.description:
                    dim_def["description"] = dim.description
                if dim.label:
                    dim_def["label"] = dim.label

                result["dimensions"].append(dim_def)

        # Export measures
        if model.measures:
            result["measures"] = []
            for measure in model.measures:
                measure_def = {"name": measure.name}

                # Map agg types
                agg_mapping = {
                    "sum": "sum",
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "avg": "average",
                    "min": "min",
                    "max": "max",
                    "median": "median",
                }
                measure_def["agg"] = agg_mapping.get(measure.agg, "sum")

                if measure.expr:
                    measure_def["expr"] = measure.expr

                if measure.description:
                    measure_def["description"] = measure.description
                if measure.label:
                    measure_def["label"] = measure.label

                result["measures"].append(measure_def)

        return result

    def _export_metric(self, metric: Metric, graph) -> dict:
        """Export metric to MetricFlow format.

        Args:
            metric: Metric to export

        Returns:
            Metric definition dictionary
        """
        result = {
            "name": metric.name,
            "type": metric.type,
        }

        if metric.description:
            result["description"] = metric.description
        if metric.label:
            result["label"] = metric.label

        # Type-specific params
        type_params = {}

        if metric.type == "simple" and metric.measure:
            type_params["measure"] = {"name": metric.measure}

        elif metric.type == "ratio":
            if metric.numerator:
                type_params["numerator"] = {"name": metric.numerator}
            if metric.denominator:
                type_params["denominator"] = {"name": metric.denominator}

        elif metric.type == "derived":
            if metric.expr:
                type_params["expr"] = metric.expr
            # Auto-detect dependencies from expression using graph for resolution
            dependencies = metric.get_dependencies(graph)
            if dependencies:
                type_params["metrics"] = [{"name": m} for m in dependencies]

        elif metric.type == "cumulative" and metric.window:
            type_params["cumulative_type_params"] = {"window": metric.window}

        if type_params:
            result["type_params"] = type_params

        if metric.filters:
            result["filter"] = metric.filters[0]  # MetricFlow uses single filter string

        return result
