"""Hex adapter for importing/exporting Hex semantic models."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class HexAdapter(BaseAdapter):
    """Adapter for importing/exporting Hex semantic models.

    Transforms Hex definitions into Sidemantic format:
    - Models → Models
    - Dimensions → Dimensions
    - Measures → Metrics
    - Relations → Relationships
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Hex YAML files into semantic graph.

        Args:
            source: Path to Hex YAML file or directory

        Returns:
            Semantic graph with imported models
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
        """Parse a single Hex YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models to
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # Each file is a single model
        model = self._parse_model(data)
        if model:
            graph.add_model(model)

    def _parse_model(self, model_def: dict) -> Model | None:
        """Parse a Hex model definition into a Model.

        Args:
            model_def: Model definition dictionary

        Returns:
            Model instance or None if parsing fails
        """
        model_id = model_def.get("id")
        if not model_id:
            return None

        # Get table name or SQL
        table = model_def.get("base_sql_table")
        sql = model_def.get("base_sql_query")

        # Parse dimensions and find primary key
        dimensions = []
        primary_key = "id"  # default

        for dim_def in model_def.get("dimensions") or []:
            dim = self._parse_dimension(dim_def)
            if dim:
                dimensions.append(dim)

                # Check if this is unique (could be primary key)
                if dim_def.get("unique"):
                    primary_key = dim.name

        # Parse measures
        measures = []
        for measure_def in model_def.get("measures") or []:
            measure = self._parse_measure(measure_def)
            if measure:
                measures.append(measure)

        # Parse relations
        relationships = []
        for relation_def in model_def.get("relations") or []:
            relation = self._parse_relation(relation_def)
            if relation:
                relationships.append(relation)

        return Model(
            name=model_id,
            table=table,
            sql=sql,
            description=model_def.get("description"),
            primary_key=primary_key,
            relationships=relationships,
            dimensions=dimensions,
            metrics=measures,
        )

    def _parse_dimension(self, dim_def: dict) -> Dimension | None:
        """Parse Hex dimension into Sidemantic dimension.

        Args:
            dim_def: Dimension definition dictionary

        Returns:
            Dimension instance or None
        """
        dim_id = dim_def.get("id")
        if not dim_id:
            return None

        dim_type = dim_def.get("type", "string")

        # Map Hex types to Sidemantic types
        type_mapping = {
            "string": "categorical",
            "number": "numeric",
            "timestamp_tz": "time",
            "timestamp_naive": "time",
            "date": "time",
            "boolean": "categorical",
            "other": "categorical",
        }

        sidemantic_type = type_mapping.get(dim_type, "categorical")

        # Get expression - defaults to id if not specified
        expr_sql = dim_def.get("expr_sql")
        expr_calc = dim_def.get("expr_calc")

        # Prefer expr_sql, but use expr_calc if that's all we have
        expr = expr_sql or expr_calc or dim_id

        # Replace ${} interpolation with {model} placeholder
        if expr:
            # For now, keep ${} references as is - they reference other dimensions
            # Only replace ${model_name} pattern with {model}
            pass

        # Determine granularity for time dimensions
        granularity = None
        if sidemantic_type == "time":
            if dim_type == "date":
                granularity = "day"
            elif "timestamp" in dim_type:
                granularity = "hour"  # Default to hour for timestamps

        return Dimension(
            name=dim_id,
            type=sidemantic_type,
            sql=expr,
            granularity=granularity,
            description=dim_def.get("description"),
        )

    def _parse_measure(self, measure_def: dict) -> Metric | None:
        """Parse Hex measure into Sidemantic metric.

        Args:
            measure_def: Measure definition dictionary

        Returns:
            Metric instance or None
        """
        measure_id = measure_def.get("id")
        if not measure_id:
            return None

        # Check for standard func
        func = measure_def.get("func")
        func_sql = measure_def.get("func_sql")
        func_calc = measure_def.get("func_calc")

        # Map Hex func to Sidemantic aggregation type
        agg_type = None
        metric_type = None
        expr = None

        if func:
            # Standard aggregation
            type_mapping = {
                "count": "count",
                "count_distinct": "count_distinct",
                "sum": "sum",
                "avg": "avg",
                "median": "avg",  # Approximate as avg
                "min": "min",
                "max": "max",
                "stddev": "sum",  # No direct mapping
                "stddev_pop": "sum",
                "variance": "sum",
                "variance_pop": "sum",
            }
            agg_type = type_mapping.get(func, "count")

            # Get the dimension being aggregated
            of_dim = measure_def.get("of")
            if of_dim:
                expr = of_dim

        elif func_sql or func_calc:
            # Custom aggregation - treat as derived metric
            metric_type = "derived"
            expr = func_sql or func_calc

        # Parse filters
        filters = []
        filter_defs = measure_def.get("filters") or []
        for filter_def in filter_defs:
            if isinstance(filter_def, dict):
                # Inline dimension definition
                filter_expr = filter_def.get("expr_sql") or filter_def.get("expr_calc")
                if filter_expr:
                    filters.append(filter_expr)
            elif isinstance(filter_def, str):
                # Reference to existing dimension
                filters.append(filter_def)

        return Metric(
            name=measure_id,
            type=metric_type,
            agg=agg_type,
            sql=expr,
            filters=filters if filters else None,
            description=measure_def.get("description"),
        )

    def _parse_relation(self, relation_def: dict) -> Relationship | None:
        """Parse Hex relation into Sidemantic relationship.

        Args:
            relation_def: Relation definition dictionary

        Returns:
            Relationship instance or None
        """
        relation_id = relation_def.get("id")
        if not relation_id:
            return None

        target = relation_def.get("target", relation_id)
        rel_type = relation_def.get("type", "many_to_one")

        # Map Hex relation types to Sidemantic
        type_mapping = {
            "many_to_one": "many_to_one",
            "one_to_many": "one_to_many",
            "one_to_one": "many_to_one",  # Treat as many_to_one
        }

        sidemantic_type = type_mapping.get(rel_type, "many_to_one")

        # Parse join_sql to extract foreign key
        join_sql = relation_def.get("join_sql", "")
        foreign_key = None

        # Try to extract foreign key from join_sql like "customer_id = ${customers}.id"
        if "=" in join_sql:
            parts = join_sql.split("=")
            if len(parts) == 2:
                foreign_key = parts[0].strip()

        return Relationship(
            name=target,
            type=sidemantic_type,
            foreign_key=foreign_key,
            sql_expr=join_sql if join_sql else None,
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Hex YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output directory (will create one file per model)
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # If output_path is a file, export all models to that file (non-standard)
        # If it's a directory, export one file per model
        if output_path.suffix in [".yml", ".yaml"]:
            # Single file - export first model only
            if resolved_models:
                model = next(iter(resolved_models.values()))
                model_data = self._export_model(model, graph)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, "w") as f:
                    yaml.dump(model_data, f, sort_keys=False, default_flow_style=False)
        else:
            # Directory - one file per model
            output_path.mkdir(parents=True, exist_ok=True)
            for model in resolved_models.values():
                model_data = self._export_model(model, graph)
                model_file = output_path / f"{model.name}.yml"
                with open(model_file, "w") as f:
                    yaml.dump(model_data, f, sort_keys=False, default_flow_style=False)

    def _export_model(self, model: Model, graph: SemanticGraph) -> dict:
        """Export model to Hex definition.

        Args:
            model: Model to export
            graph: Semantic graph (for context)

        Returns:
            Model definition dictionary
        """
        model_def = {"id": model.name}

        if model.table:
            model_def["base_sql_table"] = model.table
        elif model.sql:
            model_def["base_sql_query"] = model.sql

        if model.description:
            model_def["description"] = model.description

        # Export dimensions
        dimensions = []
        for dim in model.dimensions:
            dim_def = {"id": dim.name}

            # Map Sidemantic types to Hex types
            type_mapping = {
                "categorical": "string",
                "numeric": "number",
                "time": "timestamp_tz",
                "boolean": "boolean",
            }

            # For time dimensions, be more specific
            if dim.type == "time":
                if dim.granularity == "day":
                    dim_def["type"] = "date"
                elif dim.granularity in ["second", "minute", "hour"]:
                    dim_def["type"] = "timestamp_tz"
                else:
                    dim_def["type"] = "timestamp_tz"
            else:
                dim_def["type"] = type_mapping.get(dim.type, "string")

            # Only add expr_sql if it's different from id
            if dim.sql and dim.sql != dim.name:
                # Replace {model} placeholder with column reference
                expr_sql = dim.sql.replace("{model}.", "")
                dim_def["expr_sql"] = expr_sql

            if dim.description:
                dim_def["description"] = dim.description

            # Mark unique dimensions
            if dim.name == model.primary_key:
                dim_def["unique"] = True

            dimensions.append(dim_def)

        if dimensions:
            model_def["dimensions"] = dimensions

        # Export measures
        measures = []
        for metric in model.metrics:
            measure_def = {"id": metric.name}

            # Handle different metric types
            if metric.type == "derived":
                # Custom SQL aggregation
                if metric.sql:
                    measure_def["func_sql"] = metric.sql
            elif metric.type == "ratio":
                # Ratio metrics - export as func_sql
                if metric.numerator and metric.denominator:
                    measure_def["func_sql"] = f"{metric.numerator} / NULLIF({metric.denominator}, 0)"
            else:
                # Standard aggregation
                if metric.agg:
                    type_mapping = {
                        "count": "count",
                        "count_distinct": "count_distinct",
                        "sum": "sum",
                        "avg": "avg",
                        "min": "min",
                        "max": "max",
                    }
                    measure_def["func"] = type_mapping.get(metric.agg, "count")

                    # Add 'of' dimension if specified
                    if metric.sql:
                        measure_def["of"] = metric.sql

            # Add filters
            if metric.filters:
                filters = []
                for filter_expr in metric.filters:
                    # Export as inline dimension with expr_sql
                    filters.append({"expr_sql": filter_expr})
                measure_def["filters"] = filters

            if metric.description:
                measure_def["description"] = metric.description

            measures.append(measure_def)

        if measures:
            model_def["measures"] = measures

        # Export relations
        relations = []
        for rel in model.relationships:
            relation_def = {
                "id": rel.name,
                "type": rel.type,
            }

            # Build join_sql
            if rel.sql_expr:
                relation_def["join_sql"] = rel.sql_expr
            elif rel.foreign_key:
                # Construct join condition
                relation_def["join_sql"] = f"{rel.foreign_key} = ${{{rel.name}}}.id"

            relations.append(relation_def)

        if relations:
            model_def["relations"] = relations

        return model_def
