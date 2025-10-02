"""Cube adapter for importing Cube.js semantic models."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.entity import Entity
from sidemantic.core.measure import Measure
from sidemantic.core.model import Model
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

        # Parse dimensions
        dimensions = []
        entities = []

        for dim_def in cube_def.get("dimensions", []):
            dim = self._parse_dimension(dim_def)
            if dim:
                dimensions.append(dim)

                # Check if this is a primary key (create entity)
                if dim_def.get("primary_key"):
                    entities.append(
                        Entity(name=dim.name, type="primary", expr=dim_def.get("sql", dim.name))
                    )

        # Parse measures
        measures = []
        for measure_def in cube_def.get("measures", []):
            measure = self._parse_measure(measure_def)
            if measure:
                measures.append(measure)

        # Parse joins to create foreign key entities
        for join_def in cube_def.get("joins", []):
            join_name = join_def.get("name")
            if join_name:
                # Extract entity name from join SQL
                # This is simplified - real implementation would parse SQL
                entities.append(
                    Entity(name=join_name, type="foreign", expr=f"{join_name}_id")
                )

        return Model(
            name=name,
            table=table,
            sql=sql,
            description=cube_def.get("description"),
            entities=entities,
            dimensions=dimensions,
            measures=measures,
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
            expr=dim_def.get("sql"),
            granularity=granularity,
            description=dim_def.get("description"),
        )

    def _parse_measure(self, measure_def: dict) -> Measure | None:
        """Parse Cube measure into Sidemantic measure.

        Args:
            measure_def: Measure definition dictionary

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
            "number": "sum",  # Calculated measures default to sum
        }

        agg_type = type_mapping.get(measure_type, "count")

        # Parse filters
        filters = []
        for filter_def in measure_def.get("filters", []):
            if isinstance(filter_def, dict):
                sql_filter = filter_def.get("sql")
                if sql_filter:
                    filters.append(sql_filter)

        return Measure(
            name=name,
            agg=agg_type,
            expr=measure_def.get("sql"),
            filters=filters if filters else None,
            description=measure_def.get("description"),
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Cube YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        # Convert models to cubes
        cubes = []
        for model in graph.models.values():
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

            if dim.expr:
                dim_def["sql"] = dim.expr

            if dim.description:
                dim_def["description"] = dim.description

            dimensions.append(dim_def)

        # Add primary key dimension
        if model.primary_entity and model.primary_entity.expr:
            dimensions.append(
                {
                    "name": model.primary_entity.name,
                    "type": "number",
                    "sql": model.primary_entity.expr,
                    "primary_key": True,
                }
            )

        if dimensions:
            cube["dimensions"] = dimensions

        # Export measures
        measures = []
        for measure in model.measures:
            measure_def = {"name": measure.name}

            # Map Sidemantic agg to Cube types
            type_mapping = {
                "count": "count",
                "count_distinct": "count_distinct",
                "sum": "sum",
                "avg": "avg",
                "min": "min",
                "max": "max",
            }
            measure_def["type"] = type_mapping.get(measure.agg, "count")

            if measure.expr:
                measure_def["sql"] = measure.expr

            if measure.filters:
                measure_def["filters"] = [{"sql": f} for f in measure.filters]

            if measure.description:
                measure_def["description"] = measure.description

            measures.append(measure_def)

        if measures:
            cube["measures"] = measures

        # Export joins (for foreign key entities)
        joins = []
        for entity in model.entities:
            if entity.type == "foreign":
                # Find target model
                target_model = graph.models.get(entity.name)
                if target_model:
                    join_def = {
                        "name": entity.name,
                        "sql": f"{model.name}.{entity.expr} = {entity.name}.{target_model.primary_entity.expr if target_model.primary_entity else entity.name + '_id'}",
                        "relationship": "many_to_one",
                    }
                    joins.append(join_def)

        if joins:
            cube["joins"] = joins

        return cube
