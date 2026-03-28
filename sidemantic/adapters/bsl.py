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
    _sql_to_bsl_expr,
    bsl_filter_to_sql,
    bsl_to_sql,
    is_calc_measure_expr,
    sql_to_bsl,
)
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
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
        table = model_def.get("table")
        description = model_def.get("description")

        # Primary key: explicit field > is_entity dimension > default "id"
        primary_key = model_def.get("primary_key")

        dimensions = []
        dims_dict = model_def.get("dimensions") or {}
        for dim_name, dim_def in dims_dict.items():
            dim = self._parse_dimension(dim_name, dim_def)
            if dim:
                dimensions.append(dim)
                if not primary_key and isinstance(dim_def, dict) and dim_def.get("is_entity"):
                    primary_key = dim_name

        if not primary_key:
            primary_key = "id"

        # Model-level time dimension and grain
        default_time_dimension = None
        default_grain = None
        time_dim_name = model_def.get("time_dimension")

        if time_dim_name:
            default_time_dimension = time_dim_name
            found = False
            for i, dim in enumerate(dimensions):
                if dim.name == time_dim_name:
                    found = True
                    if dim.type != "time":
                        dimensions[i] = dim.model_copy(update={"type": "time"})
                    break
            if not found:
                dimensions.append(Dimension(name=time_dim_name, type="time", sql=time_dim_name))

        model_time_grain = model_def.get("smallest_time_grain")
        if model_time_grain:
            grain = TIME_GRAIN_MAP.get(model_time_grain, "day")
            default_grain = grain
            if time_dim_name:
                for i, dim in enumerate(dimensions):
                    if dim.name == time_dim_name and not dim.granularity:
                        dimensions[i] = dim.model_copy(update={"granularity": grain})

        # Measures and calculated measures
        metrics = []
        for measure_name, measure_def in (model_def.get("measures") or {}).items():
            metric = self._parse_measure(measure_name, measure_def)
            if metric:
                metrics.append(metric)

        for measure_name, measure_def in (model_def.get("calculated_measures") or {}).items():
            metric = self._parse_measure(measure_name, measure_def)
            if metric:
                if metric.type != "derived":
                    metric = metric.model_copy(update={"type": "derived"})
                metrics.append(metric)

        # Joins
        relationships = []
        for join_name, join_def in (model_def.get("joins") or {}).items():
            rel = self._parse_join(join_name, join_def)
            if rel:
                relationships.append(rel)

        # Model-level filter -> Segment for query engine + metadata for roundtrip
        segments = []
        metadata = None
        filter_expr = model_def.get("filter")
        if filter_expr:
            filter_str = str(filter_expr)
            segments.append(Segment(name="_default_filter", sql=bsl_filter_to_sql(filter_str)))
            metadata = {"bsl_filter": filter_str}

        return Model(
            name=name,
            table=table,
            description=description,
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            relationships=relationships,
            default_time_dimension=default_time_dimension,
            default_grain=default_grain,
            segments=segments,
            metadata=metadata,
        )

    def _parse_dimension(self, name: str, dim_def: str | dict) -> Dimension | None:
        """Parse BSL dimension (simple or extended form)."""
        if isinstance(dim_def, str):
            expr = dim_def
            description = None
            is_time = False
            time_grain = None
        else:
            expr = dim_def.get("expr", f"_.{name}")
            description = dim_def.get("description")
            is_time = dim_def.get("is_time_dimension", False)
            time_grain = dim_def.get("smallest_time_grain")

        sql_expr, agg_type, date_part = bsl_to_sql(expr)

        dim_type = "categorical"
        granularity = None

        if is_time:
            dim_type = "time"
            if time_grain:
                granularity = TIME_GRAIN_MAP.get(time_grain, "day")

        if date_part:
            dim_type = "categorical"
            if sql_expr:
                sql_expr = f"EXTRACT({date_part.upper()} FROM {sql_expr})"

        return Dimension(
            name=name,
            type=dim_type,
            sql=sql_expr,
            granularity=granularity,
            description=description,
            metadata={"bsl_expr": expr},
        )

    def _parse_measure(self, name: str, measure_def: str | dict) -> Metric | None:
        """Parse BSL measure (simple or extended form)."""
        if isinstance(measure_def, str):
            expr = measure_def
            description = None
        else:
            expr = measure_def.get("expr", "")
            description = measure_def.get("description")

        # Calc measures reference other measures by name (no _. prefix)
        if is_calc_measure_expr(expr):
            return Metric(
                name=name,
                type="derived",
                sql=expr,
                description=description,
                metadata={"bsl_expr": expr},
            )

        sql_expr, agg_type, date_part = bsl_to_sql(expr)

        if date_part and sql_expr:
            sql_expr = f"EXTRACT({date_part.upper()} FROM {sql_expr})"

        return Metric(
            name=name,
            agg=agg_type,
            sql=sql_expr,
            description=description,
            metadata={"bsl_expr": expr},
        )

    def _parse_join(self, name: str, join_def: dict) -> Relationship | None:
        """Parse BSL join into a Relationship.

        BSL supports two join key syntaxes:
        - Explicit: left_on/right_on
        - Shorthand: with: _.foreign_key_column
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

        # Extract join keys: prefer explicit left_on/right_on, fall back to with:
        foreign_key = join_def.get("left_on")
        primary_key = join_def.get("right_on")

        if not foreign_key:
            with_expr = join_def.get("with")
            if with_expr and isinstance(with_expr, str):
                if with_expr.startswith("_."):
                    foreign_key = with_expr[2:]
                else:
                    foreign_key = with_expr

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
        """Export a Sidemantic Model to BSL format."""
        model_def = {}

        if model.table:
            model_def["table"] = model.table

        if model.description:
            model_def["description"] = model.description

        if model.primary_key and model.primary_key != "id":
            model_def["primary_key"] = model.primary_key

        if model.default_time_dimension:
            model_def["time_dimension"] = model.default_time_dimension
        if model.default_grain:
            grain_key = GRANULARITY_TO_TIME_GRAIN.get(model.default_grain)
            if grain_key:
                model_def["smallest_time_grain"] = grain_key

        if model.metadata and "bsl_filter" in model.metadata:
            model_def["filter"] = model.metadata["bsl_filter"]

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
        """Export dimension to BSL format (simple or extended)."""
        # Use stored original BSL expression if available, otherwise reconstruct
        if dim.metadata and "bsl_expr" in dim.metadata:
            bsl_expr = dim.metadata["bsl_expr"]
        else:
            bsl_expr = sql_to_bsl(dim.sql, None, None)

        needs_extended = dim.description or dim.type == "time" or dim.granularity or dim.name == primary_key

        if not needs_extended:
            return bsl_expr

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
        """Export metric to BSL format (simple or extended)."""
        # Use stored original BSL expression if available
        if metric.metadata and "bsl_expr" in metric.metadata:
            bsl_expr = metric.metadata["bsl_expr"]
        elif metric.type in ("derived", "ratio"):
            if metric.sql:
                bsl_expr = metric.sql
            elif metric.type == "ratio" and metric.numerator and metric.denominator:
                bsl_expr = f"{metric.numerator} / {metric.denominator}"
            else:
                bsl_expr = f"_.{metric.name}"
        else:
            # Cross-format conversion: reconstruct BSL from SQL + aggregation
            if metric.agg == "count" and not metric.sql:
                bsl_expr = "_.count()"
            else:
                bsl_expr = _sql_to_bsl_expr(metric.sql or metric.name, metric.agg)

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

        # Use with: shorthand for single-column FK without explicit primary_key
        if rel.foreign_key and not rel.primary_key and isinstance(rel.foreign_key, str):
            join_def["with"] = f"_.{rel.foreign_key}"
        else:
            if rel.foreign_key:
                join_def["left_on"] = rel.foreign_key
            if rel.primary_key:
                join_def["right_on"] = rel.primary_key

        return join_def
