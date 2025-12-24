"""Boring Semantic Layer (BSL) adapter for importing/exporting semantic models.

BSL is an Ibis-powered semantic layer with YAML configuration.
See: https://github.com/boringdata/boring-semantic-layer

Transforms BSL definitions into Sidemantic format:
- BSL models -> Models
- BSL dimensions -> Dimensions
- BSL measures -> Metrics
- BSL joins -> Relationships
"""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.adapters.bsl_expr import (
    GRANULARITY_TO_TIME_GRAIN,
    TIME_GRAIN_MAP,
    bsl_to_sql,
    is_calc_measure_expr,
    sql_to_bsl,
)
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class BSLAdapter(BaseAdapter):
    """Adapter for importing/exporting Boring Semantic Layer YAML models.

    BSL YAML format example:
    ```yaml
    profile: example_db

    orders:
      table: public.orders
      description: "Customer orders"

      dimensions:
        status: _.status
        created_at:
          expr: _.created_at
          is_time_dimension: true
          smallest_time_grain: "TIME_GRAIN_DAY"

      measures:
        count: _.count()
        revenue:
          expr: _.amount.sum()
          description: "Total revenue"
    ```
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse BSL YAML files into semantic graph.

        Args:
            source: Path to BSL YAML file or directory

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
        """Parse a single BSL YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models to
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # Store profile if present (for metadata)
        profile = data.pop("profile", None)

        # Each top-level key (except profile) is a model
        for model_name, model_def in data.items():
            if isinstance(model_def, dict) and "table" in model_def:
                model = self._parse_model(model_name, model_def, profile)
                if model:
                    graph.add_model(model)

    def _parse_model(self, name: str, model_def: dict, profile: str | None = None) -> Model | None:
        """Parse a BSL model definition into a Sidemantic Model.

        Args:
            name: Model name
            model_def: Model definition dictionary
            profile: Optional profile name for metadata

        Returns:
            Model instance or None if parsing fails
        """
        # Get table name
        table = model_def.get("table")
        description = model_def.get("description")

        # Parse dimensions
        dimensions = []
        primary_key = "id"  # default

        dims_dict = model_def.get("dimensions") or {}
        for dim_name, dim_def in dims_dict.items():
            dim = self._parse_dimension(dim_name, dim_def)
            if dim:
                dimensions.append(dim)

                # Check for entity dimension as primary key candidate
                if isinstance(dim_def, dict) and dim_def.get("is_entity"):
                    primary_key = dim_name

        # Parse measures
        metrics = []
        measures_dict = model_def.get("measures") or {}
        for measure_name, measure_def in measures_dict.items():
            metric = self._parse_measure(measure_name, measure_def)
            if metric:
                metrics.append(metric)

        # Parse joins to create relationships
        relationships = []
        joins_dict = model_def.get("joins") or {}
        for join_name, join_def in joins_dict.items():
            rel = self._parse_join(join_name, join_def)
            if rel:
                relationships.append(rel)

        return Model(
            name=name,
            table=table,
            description=description,
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            relationships=relationships,
        )

    def _parse_dimension(self, name: str, dim_def: str | dict) -> Dimension | None:
        """Parse BSL dimension (simple or extended form).

        Args:
            name: Dimension name
            dim_def: Either a simple expression string or dict with expr and metadata

        Returns:
            Dimension instance or None
        """
        # Handle simple form: dim_name: _.column
        if isinstance(dim_def, str):
            expr = dim_def
            description = None
            is_time = False
            time_grain = None
        else:
            # Extended form with metadata
            expr = dim_def.get("expr", f"_.{name}")
            description = dim_def.get("description")
            is_time = dim_def.get("is_time_dimension", False)
            time_grain = dim_def.get("smallest_time_grain")

        # Parse the expression
        sql_expr, agg_type, date_part = bsl_to_sql(expr)

        # Determine dimension type
        dim_type = "categorical"
        granularity = None

        if is_time:
            dim_type = "time"
            if time_grain:
                granularity = TIME_GRAIN_MAP.get(time_grain, "day")

        # If there's a date extraction (year, month, etc.), treat as categorical
        # since it's extracting a part, not the full timestamp
        if date_part:
            dim_type = "categorical"
            # Convert to SQL EXTRACT expression
            if sql_expr:
                sql_expr = f"EXTRACT({date_part.upper()} FROM {sql_expr})"

        return Dimension(
            name=name,
            type=dim_type,
            sql=sql_expr,
            granularity=granularity,
            description=description,
        )

    def _parse_measure(self, name: str, measure_def: str | dict) -> Metric | None:
        """Parse BSL measure (simple or extended form).

        Args:
            name: Measure name
            measure_def: Either a simple expression string or dict with expr and metadata

        Returns:
            Metric instance or None
        """
        # Handle simple form: measure_name: _.column.sum()
        if isinstance(measure_def, str):
            expr = measure_def
            description = None
        else:
            # Extended form with metadata
            expr = measure_def.get("expr", "")
            description = measure_def.get("description")

        # Check if this is a calc measure (references other measures)
        if is_calc_measure_expr(expr):
            # This is a derived metric referencing other measures by name
            return Metric(
                name=name,
                type="derived",
                sql=expr,  # Keep the expression as-is for now
                description=description,
            )

        # Parse the expression
        sql_expr, agg_type, date_part = bsl_to_sql(expr)

        # If date extraction in a measure, wrap with aggregation context
        if date_part and sql_expr:
            sql_expr = f"EXTRACT({date_part.upper()} FROM {sql_expr})"

        return Metric(
            name=name,
            agg=agg_type,
            sql=sql_expr,
            description=description,
        )

    def _parse_join(self, name: str, join_def: dict) -> Relationship | None:
        """Parse BSL join into a Relationship.

        BSL uses:
        - model: target model name (optional, defaults to join key name)
        - type: "one" (many_to_one) or other cardinality
        - left_on: foreign key in current model
        - right_on: primary key in target model

        Args:
            name: Join key name (usually target model name)
            join_def: Join definition dict

        Returns:
            Relationship instance or None
        """
        # Get target model name (defaults to join key)
        target_model = join_def.get("model", name)

        # Map BSL join types to sidemantic relationship types
        # BSL uses "one" to mean many_to_one (many flights to one carrier)
        join_type = join_def.get("type", "one")
        type_mapping = {
            "one": "many_to_one",
            "many": "one_to_many",
            "one_to_one": "one_to_one",
            "one_to_many": "one_to_many",
            "many_to_one": "many_to_one",
            "many_to_many": "many_to_many",
        }
        rel_type = type_mapping.get(join_type, "many_to_one")

        # Extract foreign key from left_on
        foreign_key = join_def.get("left_on")
        primary_key = join_def.get("right_on")

        return Relationship(
            name=target_model,
            type=rel_type,
            foreign_key=foreign_key,
            primary_key=primary_key,
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to BSL YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file or directory
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # If output_path is a directory, create one file per model
        if output_path.is_dir() or not output_path.suffix:
            output_path.mkdir(parents=True, exist_ok=True)
            for model in resolved_models.values():
                model_data = self._export_model(model)
                model_file = output_path / f"{model.name}.yml"
                with open(model_file, "w") as f:
                    yaml.dump(model_data, f, sort_keys=False, default_flow_style=False)
        else:
            # Single file with all models
            data = {}

            # Export each model
            for model in resolved_models.values():
                model_data = self._export_model(model)
                data.update(model_data)

            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _export_model(self, model: Model) -> dict:
        """Export a Sidemantic Model to BSL format.

        Args:
            model: Model to export

        Returns:
            Dictionary with model name as key and definition as value
        """
        model_def = {}

        if model.table:
            model_def["table"] = model.table

        if model.description:
            model_def["description"] = model.description

        # Export dimensions
        dimensions = {}
        for dim in model.dimensions:
            dim_data = self._export_dimension(dim, model.primary_key)
            dimensions[dim.name] = dim_data

        if dimensions:
            model_def["dimensions"] = dimensions

        # Export measures
        measures = {}
        for metric in model.metrics:
            measure_data = self._export_measure(metric)
            measures[metric.name] = measure_data

        if measures:
            model_def["measures"] = measures

        # Export joins (from relationships)
        if model.relationships:
            joins = {}
            for rel in model.relationships:
                join_def = self._export_join(rel)
                joins[rel.name] = join_def
            model_def["joins"] = joins

        return {model.name: model_def}

    def _export_dimension(self, dim: Dimension, primary_key: str | None = None) -> str | dict:
        """Export dimension to BSL format (simple or extended).

        Args:
            dim: Dimension to export
            primary_key: Model's primary key name for entity detection

        Returns:
            Simple expression string or dict with metadata
        """
        # Generate BSL expression
        bsl_expr = sql_to_bsl(dim.sql, None, None)

        # Check if we need extended form
        needs_extended = dim.description or dim.type == "time" or dim.granularity or dim.name == primary_key

        if not needs_extended:
            return bsl_expr

        # Extended form
        result = {"expr": bsl_expr}

        if dim.description:
            result["description"] = dim.description

        if dim.type == "time":
            result["is_time_dimension"] = True

        if dim.granularity:
            time_grain = GRANULARITY_TO_TIME_GRAIN.get(dim.granularity)
            if time_grain:
                result["smallest_time_grain"] = time_grain

        if dim.name == primary_key:
            result["is_entity"] = True

        return result

    def _export_measure(self, metric: Metric) -> str | dict:
        """Export metric to BSL format (simple or extended).

        Args:
            metric: Metric to export

        Returns:
            Simple expression string or dict with metadata
        """
        # Handle derived/ratio metrics
        if metric.type in ("derived", "ratio"):
            if metric.sql:
                expr = metric.sql
            elif metric.type == "ratio" and metric.numerator and metric.denominator:
                expr = f"{metric.numerator} / {metric.denominator}"
            else:
                expr = f"_.{metric.name}"

            if metric.description:
                return {"expr": expr, "description": metric.description}
            return expr

        # Generate BSL expression for regular aggregations
        bsl_expr = sql_to_bsl(metric.sql, metric.agg, None)

        # Check if we need extended form
        if not metric.description:
            return bsl_expr

        return {"expr": bsl_expr, "description": metric.description}

    def _export_join(self, rel: Relationship) -> dict:
        """Export relationship to BSL join format.

        Args:
            rel: Relationship to export

        Returns:
            Join definition dictionary
        """
        # Map sidemantic relationship types to BSL types
        type_mapping = {
            "many_to_one": "one",
            "one_to_many": "many",
            "one_to_one": "one_to_one",
            "many_to_many": "many_to_many",
        }

        join_def = {
            "model": rel.name,
            "type": type_mapping.get(rel.type, "one"),
        }

        if rel.foreign_key:
            join_def["left_on"] = rel.foreign_key

        if rel.primary_key:
            join_def["right_on"] = rel.primary_key

        return join_def
