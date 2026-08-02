"""Hex adapter for importing/exporting Hex semantic models."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.yaml_compat import safe_load_all as _yaml_safe_load_all


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

        A file may contain multiple resources separated by ``---`` (multi-document
        YAML). Each document carries a top-level ``type:`` discriminator
        (``model`` or ``view``). Legacy single-document files without a ``type``
        are treated as models.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models to
        """
        with open(file_path) as f:
            documents = _yaml_safe_load_all(f)

            for data in documents:
                if not data or not isinstance(data, dict):
                    continue

                model = self._parse_resource(data)
                if model:
                    graph.add_model(model)

    def _parse_resource(self, resource_def: dict) -> Model | None:
        """Dispatch a Hex resource to the correct parser based on ``type``.

        Args:
            resource_def: Resource definition dictionary

        Returns:
            Model instance or None
        """
        # ``type`` is the resource discriminator on current Hex YAML. Legacy
        # files omit it and are always models.
        resource_type = resource_def.get("type", "model")

        if resource_type == "view":
            return self._parse_view(resource_def)
        return self._parse_model(resource_def)

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

        # Visibility: public/internal/private. Only "public" stays visible.
        visibility = model_def.get("visibility")
        meta = {}
        if visibility is not None:
            meta["visibility"] = visibility

        # Display label (Model has no `label`, so it rides on `metadata`).
        name = model_def.get("name")
        metadata = {"label": name} if name else None

        return Model(
            name=model_id,
            table=table,
            sql=sql,
            description=model_def.get("description"),
            primary_key=primary_key,
            relationships=relationships,
            dimensions=dimensions,
            metrics=measures,
            metadata=metadata,
            meta=meta or None,
        )

    def _parse_view(self, view_def: dict) -> Model | None:
        """Parse a Hex ``view`` resource into a Model.

        Views (``type: view``) are fit-for-purpose entrypoints layered on top of
        a base model. Sidemantic has no native view concept, so the view's
        structure (``base`` model reference and ``contents`` groups) is preserved
        on the model's ``meta`` payload for faithful round-tripping.

        Args:
            view_def: View definition dictionary

        Returns:
            Model instance or None if parsing fails
        """
        view_id = view_def.get("id")
        if not view_id:
            return None

        meta = {"hex_resource_type": "view"}

        base = view_def.get("base")
        if base is not None:
            meta["base"] = base

        contents = view_def.get("contents")
        if contents is not None:
            meta["contents"] = contents

        name = view_def.get("name")
        visibility = view_def.get("visibility")
        if visibility is not None:
            meta["visibility"] = visibility

        return Model(
            name=view_id,
            description=view_def.get("description"),
            metadata={"label": name} if name else None,
            meta=meta,
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

        # Visibility: public/internal/private. Only "public" stays visible.
        visibility = dim_def.get("visibility")
        meta = {"visibility": visibility} if visibility is not None else None
        public = visibility is None or visibility == "public"

        return Dimension(
            name=dim_id,
            type=sidemantic_type,
            sql=expr,
            granularity=granularity,
            description=dim_def.get("description"),
            label=dim_def.get("name"),
            public=public,
            meta=meta,
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
                "median": "median",
                "min": "min",
                "max": "max",
                "stddev": "stddev",
                "stddev_pop": "stddev_pop",
                "variance": "variance",
                "variance_pop": "variance_pop",
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

        # Semi-additive measures: non-additive across the given dimension(s).
        semi_additive = measure_def.get("semi_additive")
        non_additive_dimension = self._parse_semi_additive(semi_additive)

        # Build metadata payload.
        meta = {}

        # Visibility: public/internal/private. Only "public" stays visible.
        visibility = measure_def.get("visibility")
        if visibility is not None:
            meta["visibility"] = visibility
        public = visibility is None or visibility == "public"

        # Preserve the full object-form ``semi_additive`` config so that
        # ``pick``/``groupings`` survive a round-trip. Sidemantic only models a
        # single ``non_additive_dimension``; without stashing the original, an
        # export would drop ``pick`` and the Hex spec would default it to ``max``,
        # silently corrupting opening-balance (``pick: min``) snapshots.
        if isinstance(semi_additive, dict):
            meta["hex_semi_additive"] = semi_additive

        meta = meta or None

        return Metric(
            name=measure_id,
            type=metric_type,
            agg=agg_type,
            sql=expr,
            filters=filters if filters else None,
            description=measure_def.get("description"),
            label=measure_def.get("name"),
            non_additive_dimension=non_additive_dimension,
            public=public,
            meta=meta,
        )

    @staticmethod
    def _parse_semi_additive(semi_additive) -> str | None:
        """Extract the non-additive dimension from a Hex ``semi_additive`` config.

        Current Hex YAML uses an object form::

            semi_additive:
              over:
                - dimension: <dimension_id>
                  pick: min | max
              groupings:
                - <dimension_id>

        Legacy/shorthand string forms (e.g. ``semi_additive: last``) are also
        accepted and ignored for the dimension extraction (there is no associated
        dimension to record). Returns the first ``over`` dimension id, which maps
        to Sidemantic's single ``non_additive_dimension``.

        Args:
            semi_additive: Raw value of the ``semi_additive`` field

        Returns:
            Dimension id the measure is non-additive across, or None
        """
        if not semi_additive:
            return None

        if isinstance(semi_additive, dict):
            over = semi_additive.get("over") or []
            for entry in over:
                if isinstance(entry, dict) and entry.get("dimension") is not None:
                    dimension = entry["dimension"]
                    # The Hex spec allows ``dimension`` to be either a bare
                    # dimension id or an inline Dimension object (``{id: ...}``).
                    # Sidemantic's ``non_additive_dimension`` is a plain string,
                    # so extract the id from the object form.
                    if isinstance(dimension, dict):
                        dimension_id = dimension.get("id")
                        if isinstance(dimension_id, str):
                            return dimension_id
                        continue
                    if isinstance(dimension, str):
                        return dimension
                    continue
                if isinstance(entry, str):
                    return entry
        return None

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
        meta = model.meta or {}

        # Round-trip Hex views back to ``type: view`` resources.
        if meta.get("hex_resource_type") == "view":
            return self._export_view(model)

        # ``type`` is the resource discriminator required on current Hex YAML.
        model_def = {"id": model.name, "type": "model"}

        label = (model.metadata or {}).get("label")
        if label:
            model_def["name"] = label

        if model.sql:
            model_def["base_sql_query"] = model.sql
        elif model.table:
            model_def["base_sql_table"] = model.table

        if model.description:
            model_def["description"] = model.description

        if meta.get("visibility"):
            model_def["visibility"] = meta["visibility"]

        # Export dimensions
        dimensions = []
        for dim in model.dimensions:
            dim_def = {"id": dim.name}

            if dim.label:
                dim_def["name"] = dim.label

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

            # Visibility: prefer recorded value, otherwise derive from public flag.
            dim_visibility = (dim.meta or {}).get("visibility")
            if dim_visibility:
                dim_def["visibility"] = dim_visibility
            elif not dim.public:
                dim_def["visibility"] = "internal"

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

            if metric.label:
                measure_def["name"] = metric.label

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

            # Semi-additive: prefer the preserved object-form config (keeps
            # ``pick``/``groupings`` intact on round-trip), otherwise emit the
            # minimal form derived from ``non_additive_dimension``.
            preserved_semi_additive = (metric.meta or {}).get("hex_semi_additive")
            if preserved_semi_additive:
                measure_def["semi_additive"] = preserved_semi_additive
            elif metric.non_additive_dimension:
                measure_def["semi_additive"] = {"over": [{"dimension": metric.non_additive_dimension}]}

            # Visibility: prefer recorded value, otherwise derive from public flag.
            measure_visibility = (metric.meta or {}).get("visibility")
            if measure_visibility:
                measure_def["visibility"] = measure_visibility
            elif not metric.public:
                measure_def["visibility"] = "internal"

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

    def _export_view(self, model: Model) -> dict:
        """Export a model that was imported from a Hex ``view`` resource.

        Reconstructs the ``type: view`` resource from the metadata captured during
        import (``base`` and ``contents``).

        Args:
            model: Model carrying ``hex_resource_type == "view"`` metadata

        Returns:
            View definition dictionary
        """
        meta = model.meta or {}
        view_def = {"id": model.name, "type": "view"}

        label = (model.metadata or {}).get("label")
        if label:
            view_def["name"] = label

        if model.description:
            view_def["description"] = model.description

        if meta.get("visibility"):
            view_def["visibility"] = meta["visibility"]

        if meta.get("base") is not None:
            view_def["base"] = meta["base"]

        if meta.get("contents") is not None:
            view_def["contents"] = meta["contents"]

        return view_def
