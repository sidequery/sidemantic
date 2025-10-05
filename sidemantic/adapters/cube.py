"""Cube adapter for importing Cube.js semantic models."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class CubeAdapter(BaseAdapter):
    """Adapter for importing/exporting Cube.js YAML semantic models.

    Transforms Cube.js definitions into Sidemantic format:
    - Cubes → Models
    - Dimensions → Dimensions
    - Measures → Measures
    - Joins → Inferred from relationships
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Cube YAML files into semantic graph.

        Args:
            source: Path to Cube YAML file or directory

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
        """Parse a single Cube YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models to
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # Cube YAML has "cubes" key with list of cube definitions
        cubes = data.get("cubes", [])

        for cube_def in cubes:
            model = self._parse_cube(cube_def)
            if model:
                graph.add_model(model)

    def _parse_cube(self, cube_def: dict) -> Model | None:
        """Parse a Cube definition into a Model.

        Args:
            cube_def: Cube definition dictionary

        Returns:
            Model instance or None if parsing fails
        """
        name = cube_def.get("name")
        if not name:
            return None

        # Get table name
        table = cube_def.get("sql_table")
        sql = cube_def.get("sql")

        # Parse dimensions and find primary key
        dimensions = []
        primary_key = "id"  # default

        for dim_def in cube_def.get("dimensions", []):
            dim = self._parse_dimension(dim_def)
            if dim:
                dimensions.append(dim)

                # Check if this is a primary key
                if dim_def.get("primary_key"):
                    primary_key = dim.name

        # Parse measures
        measures = []
        for measure_def in cube_def.get("measures", []):
            measure = self._parse_measure(measure_def)
            if measure:
                measures.append(measure)

        # Parse segments
        from sidemantic.core.segment import Segment

        segments = []
        for segment_def in cube_def.get("segments", []):
            segment_name = segment_def.get("name")
            segment_sql = segment_def.get("sql")
            if segment_name and segment_sql:
                # Replace ${CUBE} with {model} placeholder
                segment_sql = segment_sql.replace("${CUBE}", "{model}")
                segments.append(
                    Segment(
                        name=segment_name,
                        sql=segment_sql,
                        description=segment_def.get("description"),
                    )
                )

        # Parse joins to create relationships
        relationships = []
        for join_def in cube_def.get("joins", []):
            join_name = join_def.get("name")
            if join_name:
                # Get relationship type from join definition, default to many_to_one
                rel_type = join_def.get("relationship", "many_to_one")
                relationships.append(Relationship(name=join_name, type=rel_type, foreign_key=f"{join_name}_id"))

        # Parse pre-aggregations
        pre_aggregations = []
        for preagg_def in cube_def.get("pre_aggregations", []):
            preagg = self._parse_preaggregation(preagg_def, name)
            if preagg:
                pre_aggregations.append(preagg)

        return Model(
            name=name,
            table=table,
            sql=sql,
            description=cube_def.get("description"),
            primary_key=primary_key,
            relationships=relationships,
            dimensions=dimensions,
            metrics=measures,
            segments=segments,
            pre_aggregations=pre_aggregations,
        )

    def _parse_dimension(self, dim_def: dict) -> Dimension | None:
        """Parse Cube dimension into Sidemantic dimension.

        Args:
            dim_def: Dimension definition dictionary

        Returns:
            Dimension instance or None
        """
        name = dim_def.get("name")
        if not name:
            return None

        dim_type = dim_def.get("type", "string")

        # Map Cube types to Sidemantic types
        type_mapping = {
            "string": "categorical",
            "number": "numeric",
            "time": "time",
            "boolean": "categorical",
        }

        sidemantic_type = type_mapping.get(dim_type, "categorical")

        # For time dimensions, extract granularity
        granularity = None
        if dim_type == "time":
            granularity = "day"  # Default granularity

        return Dimension(
            name=name,
            type=sidemantic_type,
            sql=dim_def.get("sql"),
            granularity=granularity,
            description=dim_def.get("description"),
            format=dim_def.get("format"),
        )

    def _parse_measure(self, measure_def: dict) -> Metric | None:
        """Parse Cube measure into Sidemantic measure.

        Args:
            measure_def: Metric definition dictionary

        Returns:
            Measure instance or None
        """
        name = measure_def.get("name")
        if not name:
            return None

        measure_type = measure_def.get("type", "count")

        # Map Cube measure types to Sidemantic aggregation types
        type_mapping = {
            "count": "count",
            "count_distinct": "count_distinct",
            "count_distinct_approx": "count_distinct",
            "sum": "sum",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "number": None,  # Calculated measures - determine type from context
        }

        agg_type = type_mapping.get(measure_type, "count")

        # Parse filters
        filters = []
        for filter_def in measure_def.get("filters", []):
            if isinstance(filter_def, dict):
                sql_filter = filter_def.get("sql")
                if sql_filter:
                    filters.append(sql_filter)

        # Check for rolling_window (cumulative metric)
        rolling_window = measure_def.get("rolling_window")
        metric_type = None
        window = None
        if rolling_window:
            metric_type = "cumulative"
            window = rolling_window.get("trailing")

        # For calculated measures (type=number), treat as derived with SQL expression
        if measure_type == "number" and not rolling_window:
            sql_expr = measure_def.get("sql", "")
            if sql_expr:
                metric_type = "derived"

        return Metric(
            name=name,
            type=metric_type,
            agg=agg_type,
            sql=measure_def.get("sql"),
            window=window,
            filters=filters if filters else None,
            description=measure_def.get("description"),
            format=measure_def.get("format"),
        )

    def _parse_preaggregation(self, preagg_def: dict, cube_name: str) -> PreAggregation | None:
        """Parse Cube pre-aggregation into Sidemantic pre-aggregation.

        Args:
            preagg_def: Pre-aggregation definition dictionary
            cube_name: Name of the parent cube

        Returns:
            PreAggregation instance or None
        """
        name = preagg_def.get("name")
        if not name:
            return None

        preagg_type = preagg_def.get("type", "rollup")

        # Extract measures - strip CUBE prefix if present
        measures = []
        for measure_ref in preagg_def.get("measures", []):
            if isinstance(measure_ref, str):
                # Remove CUBE. or {cube_name}. prefix
                measure_name = measure_ref.replace("CUBE.", "").replace(f"{cube_name}.", "")
                measures.append(measure_name)

        # Extract dimensions - strip CUBE prefix if present
        dimensions = []
        for dim_ref in preagg_def.get("dimensions", []):
            if isinstance(dim_ref, str):
                # Remove CUBE. or {cube_name}. prefix
                dim_name = dim_ref.replace("CUBE.", "").replace(f"{cube_name}.", "")
                dimensions.append(dim_name)

        # Parse time dimension
        time_dimension = preagg_def.get("time_dimension")
        if time_dimension:
            # Remove CUBE. prefix if present
            time_dimension = time_dimension.replace("CUBE.", "").replace(f"{cube_name}.", "")

        # Parse granularity
        granularity = preagg_def.get("granularity")

        # Parse partition granularity
        partition_granularity = preagg_def.get("partition_granularity")

        # Parse refresh key
        refresh_key_def = preagg_def.get("refresh_key")
        refresh_key = None
        if refresh_key_def:
            refresh_key = RefreshKey(
                every=refresh_key_def.get("every"),
                sql=refresh_key_def.get("sql"),
                incremental=refresh_key_def.get("incremental", False),
                update_window=refresh_key_def.get("update_window"),
            )

        # Parse scheduled refresh
        scheduled_refresh = preagg_def.get("scheduled_refresh", True)

        # Parse indexes
        indexes = []
        for index_def in preagg_def.get("indexes", []):
            if isinstance(index_def, dict):
                index_name = index_def.get("name", f"idx_{len(indexes)}")
                index_columns = index_def.get("columns", [])

                # Strip CUBE prefix from column names
                cleaned_columns = [col.replace("CUBE.", "").replace(f"{cube_name}.", "") for col in index_columns]

                indexes.append(
                    Index(
                        name=index_name,
                        columns=cleaned_columns,
                        type=index_def.get("type", "regular"),
                    )
                )

        # Parse build range
        build_range_start = preagg_def.get("build_range_start", {}).get("sql")
        build_range_end = preagg_def.get("build_range_end", {}).get("sql")

        return PreAggregation(
            name=name,
            type=preagg_type,
            measures=measures if measures else None,
            dimensions=dimensions if dimensions else None,
            time_dimension=time_dimension,
            granularity=granularity,
            partition_granularity=partition_granularity,
            refresh_key=refresh_key,
            scheduled_refresh=scheduled_refresh,
            indexes=indexes if indexes else None,
            build_range_start=build_range_start,
            build_range_end=build_range_end,
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Cube YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Convert models to cubes
        cubes = []
        for model in resolved_models.values():
            cube = self._export_cube(model, graph)
            cubes.append(cube)

        data = {"cubes": cubes}

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _export_cube(self, model: Model, graph: SemanticGraph) -> dict:
        """Export model to Cube definition.

        Args:
            model: Model to export
            graph: Semantic graph (for join discovery)

        Returns:
            Cube definition dictionary
        """
        cube = {"name": model.name}

        if model.table:
            cube["sql_table"] = model.table
        elif model.sql:
            cube["sql"] = model.sql

        if model.description:
            cube["description"] = model.description

        # Export dimensions
        dimensions = []
        drill_members = []  # Track dimensions with drill-down support

        for dim in model.dimensions:
            dim_def = {"name": dim.name}

            # Map Sidemantic types to Cube types
            type_mapping = {
                "categorical": "string",
                "numeric": "number",
                "time": "time",
                "boolean": "boolean",
            }
            dim_def["type"] = type_mapping.get(dim.type, "string")

            if dim.sql:
                dim_def["sql"] = dim.sql

            if dim.description:
                dim_def["description"] = dim.description

            # Add metadata fields
            if dim.format:
                dim_def["format"] = dim.format

            # Track hierarchies - collect all dimensions for drill_members
            if dim.parent or any(other.parent == dim.name for other in model.dimensions):
                drill_members.append(dim.name)

            dimensions.append(dim_def)

        # Add primary key dimension if not already in dimensions
        if model.primary_key:
            dim_names = [d["name"] for d in dimensions]
            if model.primary_key not in dim_names:
                dimensions.append(
                    {
                        "name": model.primary_key,
                        "type": "number",
                        "sql": model.primary_key,
                        "primary_key": True,
                    }
                )

        if dimensions:
            cube["dimensions"] = dimensions

        # Export measures
        measures = []
        for measure in model.metrics:
            measure_def = {"name": measure.name}

            # Handle different metric types
            if measure.type == "ratio":
                # Ratio metrics become calculated measures
                measure_def["type"] = "number"
                if measure.numerator and measure.denominator:
                    measure_def["sql"] = f"{measure.numerator} / NULLIF({measure.denominator}, 0)"
            elif measure.type == "derived":
                # Derived metrics become calculated measures
                measure_def["type"] = "number"
                if measure.sql:
                    measure_def["sql"] = measure.sql
            elif measure.type == "cumulative":
                # Cumulative metrics become rolling window measures (Cube has rolling_window)
                measure_def["type"] = "number"
                if measure.sql:
                    measure_def["sql"] = measure.sql
                if measure.window:
                    measure_def["rolling_window"] = {"trailing": measure.window}
            elif measure.type == "time_comparison":
                # Time comparison - use Cube's time dimension features
                measure_def["type"] = "number"
                if measure.base_metric:
                    # Add comment explaining this is a time comparison
                    measure_def["description"] = (
                        measure.description or ""
                    ) + f" (Time comparison of {measure.base_metric})"
                    measure_def["sql"] = measure.base_metric
            else:
                # Regular aggregation measure
                type_mapping = {
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "sum": "sum",
                    "avg": "avg",
                    "min": "min",
                    "max": "max",
                }
                measure_def["type"] = type_mapping.get(measure.agg, "count")

                if measure.sql:
                    measure_def["sql"] = measure.sql

            if measure.filters:
                measure_def["filters"] = [{"sql": f} for f in measure.filters]

            if measure.description:
                measure_def["description"] = measure.description

            # Add metadata fields
            if measure.format:
                measure_def["format"] = measure.format

            # Add drill fields if specified
            if measure.drill_fields and drill_members:
                # Only include drill fields that exist in this model
                valid_drill = [f for f in measure.drill_fields if f in [d.name for d in model.dimensions]]
                if valid_drill:
                    measure_def["drill_members"] = valid_drill
            elif drill_members:
                # Default to hierarchy dimensions
                measure_def["drill_members"] = drill_members

            measures.append(measure_def)

        if measures:
            cube["measures"] = measures

        # Export segments
        if model.segments:
            cube["segments"] = []
            for segment in model.segments:
                segment_def = {"name": segment.name}
                if segment.sql:
                    # Replace {model} placeholder with CUBE reference
                    segment_sql = segment.sql.replace("{model}", "${CUBE}")
                    segment_def["sql"] = segment_sql
                if segment.description:
                    segment_def["description"] = segment.description
                cube["segments"].append(segment_def)

        # Export joins (from many_to_one relationships)
        joins = []
        for relationship in model.relationships:
            if relationship.type == "many_to_one":
                # Find target model
                target_model = graph.models.get(relationship.name)
                if target_model:
                    local_key = relationship.sql_expr or relationship.foreign_key
                    remote_key = relationship.primary_key or target_model.primary_key
                    join_def = {
                        "name": relationship.name,
                        "sql": f"{model.name}.{local_key} = {relationship.name}.{remote_key}",
                        "relationship": "many_to_one",
                    }
                    joins.append(join_def)

        if joins:
            cube["joins"] = joins

        return cube
