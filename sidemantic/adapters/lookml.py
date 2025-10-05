"""LookML adapter for importing Looker semantic models."""

from pathlib import Path

import lkml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class LookMLAdapter(BaseAdapter):
    """Adapter for importing/exporting LookML view definitions.

    Transforms LookML definitions into Sidemantic format:
    - Views → Models
    - Dimensions → Dimensions
    - Measures → Metrics
    - dimension_group (time) → Time dimensions
    - derived_table → Model with SQL
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse LookML files into semantic graph.

        Args:
            source: Path to .lkml file or directory

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)

        # Collect all .lkml files
        lkml_files = []
        if source_path.is_dir():
            lkml_files = list(source_path.rglob("*.lkml"))
        else:
            lkml_files = [source_path]

        # First pass: parse all views
        for lkml_file in lkml_files:
            self._parse_views_from_file(lkml_file, graph)

        # Second pass: parse explores and add relationships
        for lkml_file in lkml_files:
            self._parse_explores_from_file(lkml_file, graph)

        return graph

    def _parse_views_from_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse views from a single LookML file.

        Args:
            file_path: Path to .lkml file
            graph: Semantic graph to add models to
        """
        with open(file_path) as f:
            content = f.read()

        parsed = lkml.load(content)

        if not parsed:
            return

        # Parse views
        for view_def in parsed.get("views", []):
            model = self._parse_view(view_def)
            if model:
                graph.add_model(model)

    def _parse_explores_from_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse explores from a single LookML file and add relationships.

        Args:
            file_path: Path to .lkml file
            graph: Semantic graph to add relationships to
        """
        with open(file_path) as f:
            content = f.read()

        parsed = lkml.load(content)

        if not parsed:
            return

        # Parse explores
        for explore_def in parsed.get("explores", []):
            self._parse_explore(explore_def, graph)

    def _parse_view(self, view_def: dict) -> Model | None:
        """Parse LookML view into Sidemantic model.

        Args:
            view_def: View definition dictionary (after parsing)

        Returns:
            Model instance or None
        """
        name = view_def.get("name")
        if not name:
            return None

        # Get table name
        table = view_def.get("sql_table_name")

        # Parse derived table SQL
        sql = None
        derived_table = view_def.get("derived_table")
        if derived_table:
            sql = derived_table.get("sql")

        # Parse dimensions and find primary key
        dimensions = []
        primary_key = "id"  # default

        for dim_def in view_def.get("dimensions", []):
            dim = self._parse_dimension(dim_def)
            if dim:
                dimensions.append(dim)

                # Check for primary key
                if dim_def.get("primary_key") in ("yes", True):
                    primary_key = dim.name

        # Parse dimension_group (time dimensions)
        for dim_group_def in view_def.get("dimension_groups", []):
            dims = self._parse_dimension_group(dim_group_def)
            dimensions.extend(dims)

        # Parse measures
        measures = []
        for measure_def in view_def.get("measures", []):
            measure = self._parse_measure(measure_def)
            if measure:
                measures.append(measure)

        # Parse segments
        from sidemantic.core.segment import Segment

        segments = []
        for segment_def in view_def.get("filters", []):
            # LookML filters at view level can be used as segments
            segment_name = segment_def.get("name")
            segment_sql = segment_def.get("sql")
            if segment_name and segment_sql:
                # Replace ${TABLE} with {model} placeholder
                segment_sql = segment_sql.replace("${TABLE}", "{model}")
                segments.append(
                    Segment(
                        name=segment_name,
                        sql=segment_sql,
                        description=segment_def.get("description"),
                    )
                )

        return Model(
            name=name,
            table=table,
            sql=sql,
            description=view_def.get("description"),
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=measures,
            segments=segments,
        )

    def _parse_dimension(self, dim_def: dict) -> Dimension | None:
        """Parse LookML dimension.

        Args:
            dim_def: Dimension definition

        Returns:
            Dimension instance or None
        """
        name = dim_def.get("name")
        if not name:
            return None

        dim_type = dim_def.get("type", "string")

        # Map LookML types to Sidemantic types
        type_mapping = {
            "string": "categorical",
            "number": "numeric",
            "yesno": "categorical",
            "tier": "categorical",
        }

        sidemantic_type = type_mapping.get(dim_type, "categorical")

        # Replace ${TABLE} with {model} placeholder
        sql = dim_def.get("sql")
        if sql:
            sql = sql.replace("${TABLE}", "{model}")

        return Dimension(
            name=name,
            type=sidemantic_type,
            sql=sql,
            description=dim_def.get("description"),
        )

    def _parse_dimension_group(self, dim_group_def: dict) -> list[Dimension]:
        """Parse LookML dimension_group (time dimensions).

        Args:
            dim_group_def: Dimension group definition

        Returns:
            List of time dimensions with different granularities
        """
        name = dim_group_def.get("name")
        if not name:
            return []

        group_type = dim_group_def.get("type", "time")
        if group_type != "time":
            return []

        timeframes = dim_group_def.get("timeframes", ["date"])

        # Replace ${TABLE} with {model} placeholder
        sql = dim_group_def.get("sql")
        if sql:
            sql = sql.replace("${TABLE}", "{model}")

        # Create a dimension for each timeframe
        dimensions = []
        for timeframe in timeframes:
            if timeframe == "raw":
                continue  # Skip raw timeframe

            # Map LookML timeframe to granularity
            granularity_mapping = {
                "time": "hour",
                "date": "day",
                "week": "week",
                "month": "month",
                "quarter": "quarter",
                "year": "year",
            }

            granularity = granularity_mapping.get(timeframe, "day")

            dimensions.append(
                Dimension(
                    name=f"{name}_{timeframe}",
                    type="time",
                    sql=sql,
                    granularity=granularity,
                )
            )

        return dimensions

    def _parse_measure(self, measure_def: dict) -> Metric | None:
        """Parse LookML measure.

        Args:
            measure_def: Metric definition

        Returns:
            Metric instance or None
        """
        name = measure_def.get("name")
        if not name:
            return None

        # Check if type is explicitly set
        has_explicit_type = "type" in measure_def
        measure_type = measure_def.get("type", "count")

        # Handle period_over_period type (time comparisons)
        if measure_type == "period_over_period":
            based_on = measure_def.get("based_on")
            period = measure_def.get("period", "year")
            kind = measure_def.get("kind", "relative_change")

            # Map period to comparison_type
            period_mapping = {
                "year": "yoy",
                "month": "mom",
                "week": "wow",
                "day": "dod",
                "quarter": "qoq",
            }
            comparison_type = period_mapping.get(period, "yoy")

            # Map kind to calculation
            kind_mapping = {
                "difference": "difference",
                "relative_change": "percent_change",
                "ratio": "ratio",
            }
            calculation = kind_mapping.get(kind, "percent_change")

            return Metric(
                name=name,
                type="time_comparison",
                base_metric=based_on,
                comparison_type=comparison_type,
                calculation=calculation,
                description=measure_def.get("description"),
            )

        # Map LookML measure types
        type_mapping = {
            "count": "count",
            "count_distinct": "count_distinct",
            "sum": "sum",
            "average": "avg",
            "min": "min",
            "max": "max",
            "number": None,  # Calculated measures
        }

        agg_type = type_mapping.get(measure_type)

        # Parse filters - lkml parses these as filters__all
        filters = []
        filters_all = measure_def.get("filters__all", [])
        if filters_all:
            for filter_list in filters_all:
                for filter_dict in filter_list:
                    if isinstance(filter_dict, dict):
                        for field, value in filter_dict.items():
                            filters.append(f'{field}: "{value}"')

        # Replace ${TABLE} and ${measure_ref} placeholders in SQL
        sql = measure_def.get("sql")
        if sql:
            sql = sql.replace("${TABLE}", "{model}")
            # Keep ${measure_ref} as is for now - could be enhanced later

        # Determine if this is a derived/ratio metric
        metric_type = None
        if measure_type == "number":
            metric_type = "derived"
        # If there's SQL but no explicit type, treat as derived measure
        elif sql and not has_explicit_type:
            metric_type = "derived"
            agg_type = None  # No aggregation type for derived measures

        return Metric(
            name=name,
            type=metric_type,
            agg=agg_type,
            sql=sql,
            filters=filters if filters else None,
            description=measure_def.get("description"),
        )

    def _parse_explore(self, explore_def: dict, graph: SemanticGraph) -> None:
        """Parse LookML explore and add relationships to models.

        Args:
            explore_def: Explore definition from parsed LookML
            graph: Semantic graph to add relationships to
        """
        explore_name = explore_def.get("name")
        if not explore_name:
            return

        # Check if base model exists
        if explore_name not in graph.models:
            return

        base_model = graph.models[explore_name]

        # Parse joins
        for join_def in explore_def.get("joins", []):
            relationship = self._parse_join(join_def, explore_name)
            if relationship:
                # Add relationship to the base model
                base_model.relationships.append(relationship)

    def _parse_join(self, join_def: dict, base_model_name: str) -> Relationship | None:
        """Parse a join definition into a Relationship.

        Args:
            join_def: Join definition from explore
            base_model_name: Name of the base model in the explore

        Returns:
            Relationship or None if parsing fails
        """
        join_name = join_def.get("name")
        if not join_name:
            return None

        # Get relationship type from LookML
        # LookML uses: one_to_one, one_to_many, many_to_one, many_to_many
        lookml_relationship = join_def.get("relationship", "many_to_one")

        # Map LookML relationship types to Sidemantic types
        relationship_mapping = {
            "many_to_one": "many_to_one",
            "one_to_one": "one_to_one",
            "one_to_many": "one_to_many",
            "many_to_many": "many_to_many",
        }

        relationship_type = relationship_mapping.get(lookml_relationship, "many_to_one")

        # Extract foreign key from sql_on if possible
        # sql_on typically looks like: ${orders.customer_id} = ${customers.id}
        foreign_key = None
        sql_on = join_def.get("sql_on", "")

        # Try to extract foreign key from sql_on
        # Pattern: ${base_model.column} = ${joined_model.column}
        if sql_on:
            # Simple extraction - look for ${base_model_name.column_name}
            import re

            # Match ${model.column} patterns
            matches = re.findall(r"\$\{(\w+)\.(\w+)\}", sql_on)
            for model, column in matches:
                if model == base_model_name:
                    foreign_key = column
                    break

        return Relationship(
            name=join_name,
            type=relationship_type,
            foreign_key=foreign_key,
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to LookML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output .lkml file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Convert models to views
        views = []
        for model in resolved_models.values():
            view = self._export_view(model, graph)
            views.append(view)

        data = {"views": views}

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use lkml to dump to LookML format
        with open(output_path, "w") as f:
            lookml_str = lkml.dump(data)
            f.write(lookml_str)

    def _export_view(self, model: Model, graph: SemanticGraph) -> dict:
        """Export model to LookML view definition.

        Args:
            model: Model to export
            graph: Semantic graph (for context)

        Returns:
            View definition dictionary
        """
        view = {"name": model.name}

        if model.table:
            view["sql_table_name"] = model.table
        elif model.sql:
            view["derived_table"] = {"sql": model.sql}

        if model.description:
            view["description"] = model.description

        # Export dimensions
        dimensions = []
        for dim in model.dimensions:
            # Skip time dimensions with granularity - they'll be in dimension_groups
            if dim.type == "time" and dim.granularity:
                continue

            dim_def = {"name": dim.name}

            # Map Sidemantic types to LookML types
            type_mapping = {
                "categorical": "string",
                "numeric": "number",
                "boolean": "yesno",
            }
            dim_def["type"] = type_mapping.get(dim.type, "string")

            if dim.sql:
                # Replace {model} with ${TABLE}
                sql = dim.sql.replace("{model}", "${TABLE}")
                dim_def["sql"] = sql

            if dim.description:
                dim_def["description"] = dim.description

            # Check if primary key
            if dim.name == model.primary_key:
                dim_def["primary_key"] = "yes"

            dimensions.append(dim_def)

        if dimensions:
            view["dimensions"] = dimensions

        # Export dimension_groups (time dimensions)
        # Group time dimensions by base name
        time_dims = [d for d in model.dimensions if d.type == "time" and d.granularity]
        if time_dims:
            # Group by base name and collect all timeframes
            from collections import defaultdict

            base_name_groups = defaultdict(list)

            for dim in time_dims:
                # Extract base name (remove _date, _week, etc suffix)
                base_name = dim.name
                for suffix in ["_date", "_week", "_month", "_quarter", "_year", "_time", "_hour"]:
                    if dim.name.endswith(suffix):
                        base_name = dim.name[: -len(suffix)]
                        break
                base_name_groups[base_name].append(dim)

            dimension_groups = []
            for base_name, dims in base_name_groups.items():
                # Map granularity to timeframe
                granularity_mapping = {
                    "hour": "time",
                    "day": "date",
                    "week": "week",
                    "month": "month",
                    "quarter": "quarter",
                    "year": "year",
                }

                # Collect all timeframes for this base name
                timeframes = []
                sql = None
                for dim in dims:
                    timeframe = granularity_mapping.get(dim.granularity, "date")
                    timeframes.append(timeframe)
                    if dim.sql and not sql:
                        sql = dim.sql

                dim_group_def = {
                    "name": base_name,
                    "type": "time",
                    "timeframes": timeframes,
                }

                if sql:
                    sql = sql.replace("{model}", "${TABLE}")
                    dim_group_def["sql"] = sql

                dimension_groups.append(dim_group_def)

            if dimension_groups:
                view["dimension_groups"] = dimension_groups

        # Export measures
        measures = []
        for metric in model.metrics:
            measure_def = {"name": metric.name}

            # Handle different metric types
            if metric.type == "time_comparison":
                # Export as period_over_period measure
                measure_def["type"] = "period_over_period"

                # Add based_on (base metric)
                if metric.base_metric:
                    # Remove model prefix if present (e.g., "sales.revenue" -> "revenue")
                    based_on = metric.base_metric
                    if "." in based_on:
                        based_on = based_on.split(".")[-1]
                    measure_def["based_on"] = based_on

                # Map comparison_type to period
                if metric.comparison_type:
                    period_mapping = {
                        "yoy": "year",
                        "mom": "month",
                        "wow": "week",
                        "dod": "day",
                        "qoq": "quarter",
                    }
                    period = period_mapping.get(metric.comparison_type, "year")
                    measure_def["period"] = period

                # Map calculation to kind
                if metric.calculation:
                    kind_mapping = {
                        "difference": "difference",
                        "percent_change": "relative_change",
                        "ratio": "ratio",
                    }
                    kind = kind_mapping.get(metric.calculation, "relative_change")
                    measure_def["kind"] = kind

                if metric.description:
                    measure_def["description"] = metric.description

            elif metric.type == "derived":
                measure_def["type"] = "number"
                if metric.sql:
                    sql = metric.sql.replace("{model}", "${TABLE}")
                    measure_def["sql"] = sql
            elif metric.type == "ratio":
                measure_def["type"] = "number"
                if metric.numerator and metric.denominator:
                    measure_def["sql"] = f"1.0 * ${{{metric.numerator}}} / NULLIF(${{{metric.denominator}}}, 0)"
            else:
                # Regular aggregation measure
                type_mapping = {
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "sum": "sum",
                    "avg": "average",
                    "min": "min",
                    "max": "max",
                }
                measure_def["type"] = type_mapping.get(metric.agg, "count")

                if metric.sql:
                    sql = metric.sql.replace("{model}", "${TABLE}")
                    measure_def["sql"] = sql

            # Add filters (skip for time_comparison as they don't use filters)
            if metric.filters and metric.type != "time_comparison":
                filters_all = []
                for filter_str in metric.filters:
                    # Parse "field: value" format
                    if ":" in filter_str:
                        field, value = filter_str.split(":", 1)
                        field = field.strip()
                        value = value.strip().strip('"')
                        filters_all.append([{field: value}])
                measure_def["filters__all"] = filters_all

            if metric.description and metric.type != "time_comparison":
                measure_def["description"] = metric.description

            measures.append(measure_def)

        if measures:
            view["measures"] = measures

        # Export segments as view-level filters
        if model.segments:
            filters = []
            for segment in model.segments:
                filter_def = {"name": segment.name}
                if segment.sql:
                    sql = segment.sql.replace("{model}", "${TABLE}")
                    filter_def["sql"] = sql
                if segment.description:
                    filter_def["description"] = segment.description
                filters.append(filter_def)
            view["filters"] = filters

        return view
