"""MetricFlow adapter for importing dbt semantic layer models."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
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

        # Resolve entity names to actual model names
        # MetricFlow uses singular entity names (e.g., "customer") while models may be plural (e.g., "customers")
        self._resolve_relationship_names(graph)

        # Rebuild adjacency graph after resolving relationship names
        graph.build_adjacency()

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
        for model_def in data.get("semantic_models") or []:
            model = self._parse_semantic_model(model_def)
            if model:
                graph.add_model(model)

        # Parse metrics
        for metric_def in data.get("metrics") or []:
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

        # Parse entities to extract primary key and relationships
        primary_key = "id"  # default
        relationships = []

        for entity_def in model_def.get("entities") or []:
            entity_type = entity_def.get("type", "primary")
            entity_name = entity_def.get("name")
            entity_expr = entity_def.get("expr", entity_name)

            if entity_type == "primary":
                # Use this as the primary key
                primary_key = entity_expr
            elif entity_type == "foreign":
                # Create a many_to_one relationship
                relationships.append(Relationship(name=entity_name, type="many_to_one", foreign_key=entity_expr))

        # Parse dimensions
        dimensions = []
        for dim_def in model_def.get("dimensions") or []:
            dim = self._parse_dimension(dim_def)
            if dim:
                dimensions.append(dim)

        # Parse measures
        measures = []
        for measure_def in model_def.get("measures") or []:
            measure = self._parse_measure(measure_def)
            if measure:
                measures.append(measure)

        # Parse segments from meta
        from sidemantic.core.segment import Segment

        segments = []
        meta = model_def.get("meta", {})
        for segment_def in meta.get("segments") or []:
            segment_name = segment_def.get("name")
            segment_sql = segment_def.get("sql")
            if segment_name and segment_sql:
                segments.append(
                    Segment(
                        name=segment_name,
                        sql=segment_sql,
                        description=segment_def.get("description"),
                    )
                )

        # Parse inheritance
        extends = meta.get("extends")

        # Parse default time dimension (MetricFlow uses defaults.agg_time_dimension)
        defaults = model_def.get("defaults", {})
        default_time_dimension = defaults.get("agg_time_dimension")
        default_grain = meta.get("default_grain")

        return Model(
            name=name,
            table=table,
            description=model_def.get("description"),
            primary_key=primary_key,
            relationships=relationships,
            dimensions=dimensions,
            metrics=measures,
            segments=segments,
            extends=extends,
            default_time_dimension=default_time_dimension,
            default_grain=default_grain,
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

        # Parse metadata fields from meta
        meta = dim_def.get("meta", {})
        format_str = meta.get("format")
        value_format_name = meta.get("value_format_name")
        parent = meta.get("parent")

        # Convert expr to string if it's not None (can be various types)
        expr = dim_def.get("expr")
        sql_expr = str(expr) if expr is not None else None

        return Dimension(
            name=name,
            type=sidemantic_type,
            sql=sql_expr,
            granularity=granularity,
            description=dim_def.get("description"),
            label=dim_def.get("label"),
            format=format_str,
            value_format_name=value_format_name,
            parent=parent,
        )

    def _parse_measure(self, measure_def: dict) -> Metric | None:
        """Parse MetricFlow measure into Sidemantic measure.

        Args:
            measure_def: Metric definition dictionary

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

        # Parse metadata and filters from meta
        meta = measure_def.get("meta", {})
        filters = meta.get("filters")
        format_str = meta.get("format")
        value_format_name = meta.get("value_format_name")
        drill_fields = meta.get("drill_fields")

        # Parse non_additive_dimension
        non_additive = measure_def.get("non_additive_dimension")
        non_additive_dimension = None
        if non_additive and isinstance(non_additive, dict):
            non_additive_dimension = non_additive.get("name")

        # Convert expr to string if it's not None (can be int, like 1 for count)
        expr = measure_def.get("expr")
        sql_expr = str(expr) if expr is not None else None

        return Metric(
            name=name,
            agg=sidemantic_agg,
            sql=sql_expr,
            description=measure_def.get("description"),
            label=measure_def.get("label"),
            filters=filters,
            format=format_str,
            value_format_name=value_format_name,
            drill_fields=drill_fields,
            non_additive_dimension=non_additive_dimension,
        )

    def _parse_metric(self, metric_def: dict) -> Metric | None:
        """Parse MetricFlow metric into Sidemantic measure.

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Measure instance or None
        """
        name = metric_def.get("name")
        if not name:
            return None

        metric_type = metric_def.get("type", "simple")

        # Map MetricFlow metric types
        # Note: "simple" maps to None (untyped) since we removed the simple type
        type_mapping = {
            "simple": None,  # Untyped metric with sql expression
            "ratio": "ratio",
            "derived": "derived",
            "cumulative": "cumulative",
            # conversion not yet supported
        }

        sidemantic_type = type_mapping.get(metric_type, None)
        # Only skip if the metric type is truly unsupported (not in mapping at all)
        if metric_type not in type_mapping:
            return None  # Skip unsupported metric types

        # Extract type-specific parameters
        type_params = metric_def.get("type_params", {})

        # Simple metric
        expr = None
        if metric_type == "simple":
            measure_def = type_params.get("measure", {})
            if isinstance(measure_def, dict):
                expr = measure_def.get("name")
            else:
                expr = measure_def

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
        if metric_type == "derived":
            expr = type_params.get("expr")

        # Cumulative metric
        window = None
        grain_to_date = None
        if metric_type == "cumulative":
            # Get the base measure reference
            measure_def = type_params.get("measure", {})
            if isinstance(measure_def, dict):
                expr = measure_def.get("name")
            else:
                expr = measure_def
            # Window can be directly in type_params
            window = type_params.get("window")
            grain_to_date = type_params.get("grain_to_date")
            # Or in cumulative_type_params (alternative structure)
            if not window and not grain_to_date:
                cumulative_params = type_params.get("cumulative_type_params", {})
                window = cumulative_params.get("window")
                grain_to_date = cumulative_params.get("grain_to_date")

        # Parse filter
        filter_expr = metric_def.get("filter")
        filters = [filter_expr] if filter_expr else None

        # Parse metadata from meta
        meta = metric_def.get("meta", {})
        format_str = meta.get("format")
        value_format_name = meta.get("value_format_name")
        drill_fields = meta.get("drill_fields")
        extends = meta.get("extends")

        return Metric(
            name=name,
            type=sidemantic_type,
            description=metric_def.get("description"),
            label=metric_def.get("label"),
            sql=expr,
            numerator=numerator,
            denominator=denominator,
            window=window,
            grain_to_date=grain_to_date,
            filters=filters,
            format=format_str,
            value_format_name=value_format_name,
            drill_fields=drill_fields,
            extends=extends,
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to MetricFlow YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import (
            resolve_metric_inheritance,
            resolve_model_inheritance,
        )

        resolved_models = resolve_model_inheritance(graph.models)
        resolved_metrics = resolve_metric_inheritance(graph.metrics) if graph.metrics else {}

        # Export semantic models
        semantic_models = []
        for model in resolved_models.values():
            semantic_model = self._export_semantic_model(model)
            semantic_models.append(semantic_model)

        data = {"semantic_models": semantic_models}

        # Export metrics if present
        if resolved_metrics:
            data["metrics"] = [self._export_metric(metric, graph) for metric in resolved_metrics.values()]

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

        # Export entities (convert from relationships and primary_key)
        result["entities"] = []

        # Add primary entity
        result["entities"].append(
            {
                "name": model.name,  # Use model name as entity name
                "type": "primary",
                "expr": model.primary_key,
            }
        )

        # Add foreign entities from relationships
        for rel in model.relationships:
            if rel.type == "many_to_one":
                result["entities"].append(
                    {
                        "name": rel.name,
                        "type": "foreign",
                        "expr": rel.foreign_key or f"{rel.name}_id",
                    }
                )

        # Export dimensions
        if model.dimensions:
            result["dimensions"] = []
            for dim in model.dimensions:
                dim_def = {"name": dim.name, "type": dim.type}

                if dim.sql:
                    dim_def["expr"] = dim.sql

                if dim.type == "time" and dim.granularity:
                    dim_def["type_params"] = {"time_granularity": dim.granularity}

                if dim.description:
                    dim_def["description"] = dim.description
                if dim.label:
                    dim_def["label"] = dim.label

                # Add metadata fields
                if dim.format:
                    dim_def["meta"] = dim_def.get("meta", {})
                    dim_def["meta"]["format"] = dim.format
                if dim.value_format_name:
                    dim_def["meta"] = dim_def.get("meta", {})
                    dim_def["meta"]["value_format_name"] = dim.value_format_name

                # Add hierarchy parent info (MetricFlow doesn't have native hierarchies, use meta)
                if dim.parent:
                    dim_def["meta"] = dim_def.get("meta", {})
                    dim_def["meta"]["parent"] = dim.parent

                result["dimensions"].append(dim_def)

        # Export measures
        if model.metrics:
            result["measures"] = []
            for measure in model.metrics:
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

                if measure.sql:
                    measure_def["expr"] = measure.sql

                if measure.description:
                    measure_def["description"] = measure.description
                if measure.label:
                    measure_def["label"] = measure.label

                # Add metric-level filters
                if measure.filters:
                    # MetricFlow supports filters in create_metric, but we can put in meta for now
                    measure_def["meta"] = measure_def.get("meta", {})
                    measure_def["meta"]["filters"] = measure.filters

                # Add metadata fields
                if measure.format:
                    measure_def["meta"] = measure_def.get("meta", {})
                    measure_def["meta"]["format"] = measure.format
                if measure.value_format_name:
                    measure_def["meta"] = measure_def.get("meta", {})
                    measure_def["meta"]["value_format_name"] = measure.value_format_name
                if measure.drill_fields:
                    measure_def["meta"] = measure_def.get("meta", {})
                    measure_def["meta"]["drill_fields"] = measure.drill_fields
                if measure.non_additive_dimension:
                    measure_def["non_additive_dimension"] = {"name": measure.non_additive_dimension}

                result["measures"].append(measure_def)

        # Export model-level default_time_dimension
        if model.default_time_dimension:
            result["defaults"] = {"agg_time_dimension": model.default_time_dimension}
            if model.default_grain:
                result["meta"] = result.get("meta", {})
                result["meta"]["default_grain"] = model.default_grain

        # Export segments (as meta since MetricFlow doesn't have native segment support)
        if model.segments:
            result["meta"] = result.get("meta", {})
            result["meta"]["segments"] = []
            for segment in model.segments:
                segment_def = {"name": segment.name, "sql": segment.sql}
                if segment.description:
                    segment_def["description"] = segment.description
                result["meta"]["segments"].append(segment_def)

        # Note: inheritance is resolved before export, so extends field is not exported

        return result

    def _export_metric(self, measure: Metric, graph) -> dict:
        """Export measure to MetricFlow format.

        Args:
            measure: Metric to export

        Returns:
            Measure definition dictionary
        """
        # Determine export type - untyped metrics with sql should be exported as "simple"
        export_type = measure.type or ("simple" if not measure.agg and measure.sql else None)

        result = {
            "name": measure.name,
            "type": export_type,
        }

        if measure.description:
            result["description"] = measure.description
        if measure.label:
            result["label"] = measure.label

        # Type-specific params
        type_params = {}

        # Untyped metrics with sql are treated as simple (measure references)
        if not measure.type and not measure.agg and measure.sql:
            type_params["measure"] = {"name": measure.sql}

        elif measure.type == "ratio":
            if measure.numerator:
                type_params["numerator"] = {"name": measure.numerator}
            if measure.denominator:
                type_params["denominator"] = {"name": measure.denominator}

        elif measure.type == "derived":
            if measure.sql:
                type_params["expr"] = measure.sql
            # Auto-detect dependencies from expression using graph for resolution
            dependencies = measure.get_dependencies(graph)
            if dependencies:
                type_params["metrics"] = [{"name": m} for m in dependencies]

        elif measure.type == "cumulative" and measure.window:
            type_params["cumulative_type_params"] = {"window": measure.window}

        if type_params:
            result["type_params"] = type_params

        if measure.filters:
            result["filter"] = measure.filters[0]  # MetricFlow uses single filter string

        # Add metadata fields for graph-level metrics
        if measure.format or measure.value_format_name or measure.drill_fields:
            result["meta"] = result.get("meta", {})
            if measure.format:
                result["meta"]["format"] = measure.format
            if measure.value_format_name:
                result["meta"]["value_format_name"] = measure.value_format_name
            if measure.drill_fields:
                result["meta"]["drill_fields"] = measure.drill_fields
            # Note: inheritance is resolved before export, so extends field is not exported

        return result

    def _resolve_relationship_names(self, graph: SemanticGraph) -> None:
        """Resolve MetricFlow entity names to actual model names.

        MetricFlow uses singular entity names (e.g., "customer") while models are often plural (e.g., "customers").
        This method attempts to match entity names to actual models in the graph.

        Args:
            graph: Semantic graph with models
        """
        # Get all model names
        model_names = set(graph.models.keys())

        # For each model, check its relationships
        for model in graph.models.values():
            for rel in model.relationships:
                # If the relationship name doesn't match any model, try to resolve it
                if rel.name not in model_names:
                    resolved_name = self._resolve_entity_to_model(rel.name, model_names)
                    if resolved_name:
                        # Update the relationship name to the actual model name
                        rel.name = resolved_name

    def _resolve_entity_to_model(self, entity_name: str, model_names: set[str]) -> str | None:
        """Attempt to resolve an entity name to an actual model name.

        Uses inflect library for proper pluralization/singularization.

        Args:
            entity_name: Entity name from MetricFlow
            model_names: Set of available model names

        Returns:
            Resolved model name or None if no match found
        """
        import inflect

        p = inflect.engine()

        # Try exact match (case-sensitive)
        if entity_name in model_names:
            return entity_name

        # Try pluralization
        plural = p.plural(entity_name)
        if plural in model_names:
            return plural

        # Try singularization
        singular = p.singular_noun(entity_name)
        if singular and singular in model_names:
            return singular

        # Try case-insensitive match
        entity_lower = entity_name.lower()
        for model_name in model_names:
            if model_name.lower() == entity_lower:
                return model_name

        # No match found
        return None
