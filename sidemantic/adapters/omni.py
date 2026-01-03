"""Omni adapter for importing/exporting Omni Analytics semantic models."""

from pathlib import Path
from typing import Any

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class OmniAdapter(BaseAdapter):
    """Adapter for importing/exporting Omni Analytics view definitions.

    Transforms Omni definitions into Sidemantic format:
    - Views → Models
    - Dimensions → Dimensions
    - Measures → Metrics
    - Relationships (from model file) → Relationships
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Omni view files into semantic graph.

        Args:
            source: Path to view .yaml file, views directory, or model directory

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)

        # Collect all view .yaml files
        view_files = []
        if source_path.is_dir():
            # Look for views in views/ subdirectory or all yaml files
            views_dir = source_path / "views"
            if views_dir.exists():
                view_files = list(views_dir.glob("*.yaml")) + list(views_dir.glob("*.yml"))
            else:
                view_files = list(source_path.rglob("*.yaml")) + list(source_path.rglob("*.yml"))
        else:
            view_files = [source_path]

        # Parse all views
        for view_file in view_files:
            # Skip model files (relationships defined separately)
            if "model.yaml" in str(view_file) or "model.yml" in str(view_file):
                continue

            model = self._parse_view(view_file)
            if model:
                graph.add_model(model)

        # Parse relationships from model file if present
        if source_path.is_dir():
            model_file = source_path / "model.yaml"
            if not model_file.exists():
                model_file = source_path / "model.yml"
            if model_file.exists():
                self._parse_relationships(model_file, graph)

        return graph

    def _parse_view(self, file_path: Path) -> Model | None:
        """Parse Omni view YAML into Sidemantic model.

        Args:
            file_path: Path to view YAML file

        Returns:
            Model instance or None
        """
        with open(file_path) as f:
            view = yaml.safe_load(f)

        if not view:
            return None

        # Get view name from filename or name field
        name = view.get("name") or file_path.stem

        # Get table reference
        schema = view.get("schema")
        table_name = view.get("table_name") or view.get("table")

        if schema and table_name:
            table = f"{schema}.{table_name}"
        elif table_name:
            table = table_name
        else:
            table = None

        # Get SQL for query-based views
        sql = view.get("sql")
        if not sql and "query" in view:
            # Query-based views - we'll use the base view reference
            query_def = view["query"]
            base_view = query_def.get("base_view")
            if base_view:
                # For now, treat query views as references to base view
                table = base_view

        # Parse dimensions
        dimensions = []
        primary_key = "id"  # default

        for dim_name, dim_def in (view.get("dimensions") or {}).items():
            if dim_def is None:
                dim_def = {}

            dim = self._parse_dimension(dim_name, dim_def)
            if dim:
                dimensions.append(dim)

                # Check for primary key
                if dim_def.get("primary_key") is True:
                    primary_key = dim.name

        # Parse measures
        metrics = []
        for measure_name, measure_def in (view.get("measures") or {}).items():
            if measure_def is None:
                measure_def = {}

            metric = self._parse_measure(measure_name, measure_def)
            if metric:
                metrics.append(metric)

        # Use description if available, otherwise use label
        description = view.get("description")
        if not description:
            description = view.get("label")

        return Model(
            name=name,
            table=table,
            sql=sql,
            description=description,
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
        )

    def _parse_dimension(self, name: str, dim_def: dict[str, Any]) -> Dimension | None:
        """Parse Omni dimension definition.

        Args:
            name: Dimension name
            dim_def: Dimension definition dict

        Returns:
            Dimension instance or None
        """
        # Map Omni types to Sidemantic types
        dim_type_str = dim_def.get("type", "string")
        type_mapping = {
            "string": "categorical",
            "number": "numeric",
            "date": "time",
            "timestamp": "time",
            "yesno": "boolean",
        }

        dim_type = type_mapping.get(dim_type_str, "categorical")

        # Get SQL - replace ${TABLE} or ${view.field} references with {model}
        sql = dim_def.get("sql", name)
        if sql:
            sql = sql.replace("${TABLE}", "{model}")
            # Simplify ${view.field} to just field for now
            import re

            sql = re.sub(r"\$\{[^.]+\.([^}]+)\}", r"\1", sql)

        # Handle timeframes for time dimensions
        timeframes = dim_def.get("timeframes") or []
        granularity = None
        if dim_type == "time" and timeframes:
            # Map first timeframe to granularity
            timeframe_mapping = {
                "date": "day",
                "week": "week",
                "month": "month",
                "quarter": "quarter",
                "year": "year",
                "hour": "hour",
            }
            if timeframes:
                first_timeframe = timeframes[0] if isinstance(timeframes, list) else timeframes
                granularity = timeframe_mapping.get(first_timeframe, "day")

        return Dimension(
            name=name,
            type=dim_type,
            sql=sql,
            label=dim_def.get("label"),
            granularity=granularity,
            description=dim_def.get("description"),
        )

    def _parse_measure(self, name: str, measure_def: dict[str, Any]) -> Metric | None:
        """Parse Omni measure definition.

        Args:
            name: Measure name
            measure_def: Measure definition dict

        Returns:
            Metric instance or None
        """
        # Check for time comparison pattern first (date_offset_from_query + cancel_query_filter)
        filter_defs = measure_def.get("filters", {})
        if filter_defs:
            for field, conditions in filter_defs.items():
                if isinstance(conditions, dict):
                    has_offset = "date_offset_from_query" in conditions
                    has_cancel = conditions.get("cancel_query_filter") is True

                    if has_offset and has_cancel:
                        # This is a time comparison measure
                        offset_str = conditions["date_offset_from_query"]

                        # Parse offset to determine comparison type
                        # e.g., "2 years" -> yoy, "1 month" -> mom, "1 week" -> wow
                        comparison_type = self._parse_time_offset_to_comparison(offset_str)

                        # Extract base measure name - typically the measure name minus suffix
                        # e.g., "count_signups_same_time_two_years_previously" -> "count_signups"
                        base_metric = self._extract_base_metric_name(name)

                        return Metric(
                            name=name,
                            type="time_comparison",
                            base_metric=base_metric,
                            comparison_type=comparison_type,
                            time_offset=offset_str,
                            calculation="difference",  # Omni defaults to difference
                            label=measure_def.get("label"),
                            description=measure_def.get("description"),
                        )

        # Map Omni aggregate types
        agg_type_str = measure_def.get("aggregate_type", "")
        type_mapping = {
            "count": "count",
            "count_distinct": "count_distinct",
            "sum": "sum",
            "average": "avg",
            "avg": "avg",
            "min": "min",
            "max": "max",
        }

        agg = type_mapping.get(agg_type_str)

        # Get SQL - replace ${view.field} references
        sql = measure_def.get("sql")
        if sql:
            import re

            # Replace ${view.field} with just field
            sql = re.sub(r"\$\{[^.]+\.([^}]+)\}", r"\1", sql)

        # Parse regular filters
        filters = []
        if filter_defs:
            for field, conditions in filter_defs.items():
                if isinstance(conditions, dict):
                    # Skip time comparison filters
                    if "date_offset_from_query" in conditions and conditions.get("cancel_query_filter") is True:
                        continue

                    for operator, value in conditions.items():
                        if operator == "is":
                            filters.append(f"{field} = '{value}'")
                        elif operator == "greater_than_or_equal_to":
                            filters.append(f"{field} >= {value}")
                        # Add more operators as needed

        # Determine metric type
        metric_type = None
        if not agg and sql:
            # Custom SQL without standard aggregation
            metric_type = "derived"

        return Metric(
            name=name,
            type=metric_type,
            agg=agg,
            sql=sql,
            filters=filters if filters else None,
            label=measure_def.get("label"),
            description=measure_def.get("description"),
        )

    def _parse_time_offset_to_comparison(self, offset: str) -> str:
        """Parse Omni time offset string to comparison_type.

        Args:
            offset: Offset string like "2 years", "1 month", "1 week"

        Returns:
            Comparison type: yoy, mom, wow, dod, qoq, or prior_period
        """
        offset_lower = offset.lower().strip()

        # Check for common patterns
        if "year" in offset_lower:
            return "yoy"
        elif "month" in offset_lower:
            return "mom"
        elif "week" in offset_lower:
            return "wow"
        elif "day" in offset_lower:
            return "dod"
        elif "quarter" in offset_lower:
            return "qoq"
        else:
            # Default to prior_period for custom offsets
            return "prior_period"

    def _extract_base_metric_name(self, comparison_name: str) -> str:
        """Extract base metric name from time comparison metric name.

        Args:
            comparison_name: Name like "revenue_yoy" or "count_signups_same_time_two_years_previously"

        Returns:
            Base metric name (best guess)
        """
        # Common suffixes to remove
        suffixes = [
            "_yoy",
            "_mom",
            "_wow",
            "_dod",
            "_qoq",
            "_same_time_two_years_previously",
            "_same_time_one_year_previously",
            "_same_time_last_month",
            "_same_time_last_week",
            "_previous_period",
            "_prior_period",
        ]

        name = comparison_name
        for suffix in suffixes:
            if name.endswith(suffix):
                return name[: -len(suffix)]

        # If no known suffix, return as-is and let user fix it
        return comparison_name

    def _parse_relationships(self, model_file: Path, graph: SemanticGraph) -> None:
        """Parse relationships from Omni model file.

        Args:
            model_file: Path to model.yaml file
            graph: Semantic graph to add relationships to
        """
        with open(model_file) as f:
            model_def = yaml.safe_load(f)

        if not model_def:
            return

        relationships_list = model_def.get("relationships") or []

        for rel_def in relationships_list:
            from_view = rel_def.get("join_from_view")
            to_view = rel_def.get("join_to_view")

            if not from_view or not to_view:
                continue

            # Get relationship type
            rel_type_str = rel_def.get("relationship_type", "many_to_one")
            type_mapping = {
                "one_to_one": "one_to_one",
                "many_to_one": "many_to_one",
                "one_to_many": "one_to_many",
                "many_to_many": "many_to_many",
            }
            rel_type = type_mapping.get(rel_type_str, "many_to_one")

            # Extract foreign key from on_sql
            on_sql = rel_def.get("on_sql", "")
            foreign_key = None
            if on_sql:
                import re

                # Try to extract ${from_view.column} = ${to_view.column}
                matches = re.findall(r"\$\{([^.]+)\.([^}]+)\}", on_sql)
                for view, column in matches:
                    if view == from_view:
                        foreign_key = column
                        break

            # Add relationship to from_view
            if from_view in graph.models:
                relationship = Relationship(
                    name=to_view,
                    type=rel_type,
                    foreign_key=foreign_key,
                )
                graph.models[from_view].relationships.append(relationship)

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Omni view format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output directory
        """
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Create views directory
        views_dir = output_path / "views"
        views_dir.mkdir(exist_ok=True)

        # Export each model as a view file
        for model in resolved_models.values():
            view = self._export_view(model)
            file_path = views_dir / f"{model.name}.yaml"
            with open(file_path, "w") as f:
                yaml.dump(view, f, default_flow_style=False, sort_keys=False)

        # Export relationships to model file
        if any(model.relationships for model in resolved_models.values()):
            self._export_relationships(resolved_models, output_path)

    def _export_view(self, model: Model) -> dict[str, Any]:
        """Export model to Omni view definition.

        Args:
            model: Model to export

        Returns:
            View definition dictionary
        """
        view: dict[str, Any] = {
            "name": model.name,
        }

        if model.description:
            view["label"] = model.description

        # Extract schema and table
        if model.table and "." in model.table:
            parts = model.table.split(".", 1)
            view["schema"] = parts[0]
            view["table_name"] = parts[1]
        elif model.table:
            view["table_name"] = model.table

        if model.sql:
            view["sql"] = model.sql

        # Don't add separate description field since we used it for label

        # Export dimensions
        dimensions: dict[str, Any] = {}
        for dim in model.dimensions:
            dim_def: dict[str, Any] = {}

            # Map Sidemantic types to Omni types
            type_mapping = {
                "categorical": "string",
                "numeric": "number",
                "time": "timestamp",
                "boolean": "yesno",
            }
            if dim.type in type_mapping:
                dim_def["type"] = type_mapping[dim.type]

            # Add SQL - convert {model} to ${TABLE}
            if dim.sql and dim.sql != dim.name:
                sql = dim.sql.replace("{model}", "${TABLE}")
                dim_def["sql"] = sql

            if dim.label:
                dim_def["label"] = dim.label

            if dim.description:
                dim_def["description"] = dim.description

            # Handle timeframes for time dimensions
            if dim.type == "time" and dim.granularity:
                granularity_mapping = {
                    "hour": "hour",
                    "day": "date",
                    "week": "week",
                    "month": "month",
                    "quarter": "quarter",
                    "year": "year",
                }
                timeframe = granularity_mapping.get(dim.granularity, "date")
                dim_def["timeframes"] = [timeframe]

            # Mark primary key
            if dim.name == model.primary_key:
                dim_def["primary_key"] = True

            dimensions[dim.name] = dim_def

        if dimensions:
            view["dimensions"] = dimensions

        # Export measures
        measures: dict[str, Any] = {}
        for metric in model.metrics:
            measure_def: dict[str, Any] = {}

            # Handle time_comparison metrics specially
            if metric.type == "time_comparison":
                # Get the base metric to determine aggregate type
                # For now, use count as default (would need graph context to resolve properly)
                measure_def["aggregate_type"] = "count"

                # Convert time_comparison to Omni's filter-based format
                time_offset = metric.time_offset or self._comparison_type_to_offset(metric.comparison_type)

                # Find the time dimension to apply the offset to
                # Use the model's default time dimension
                time_field = model.default_time_dimension or "created_at"

                measure_def["filters"] = {
                    time_field: {
                        "date_offset_from_query": time_offset,
                        "cancel_query_filter": True,
                    }
                }

                if metric.label:
                    measure_def["label"] = metric.label

                if metric.description:
                    measure_def["description"] = metric.description

                measures[metric.name] = measure_def
                continue

            # Map aggregation type
            if metric.agg:
                type_mapping = {
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "sum": "sum",
                    "avg": "average",
                    "min": "min",
                    "max": "max",
                }
                measure_def["aggregate_type"] = type_mapping.get(metric.agg, "sum")

            if metric.sql:
                # Convert {model} references
                sql = metric.sql.replace("{model}", "${TABLE}")
                measure_def["sql"] = sql

            if metric.label:
                measure_def["label"] = metric.label

            if metric.description:
                measure_def["description"] = metric.description

            # Export filters
            if metric.filters:
                filters_dict = {}
                for filter_str in metric.filters:
                    # Parse "field = 'value'" format
                    if "=" in filter_str:
                        parts = filter_str.split("=", 1)
                        field = parts[0].strip()
                        value = parts[1].strip().strip("'\"")
                        filters_dict[field] = {"is": value}
                    elif ">=" in filter_str:
                        parts = filter_str.split(">=", 1)
                        field = parts[0].strip()
                        value = parts[1].strip()
                        try:
                            filters_dict[field] = {"greater_than_or_equal_to": int(value)}
                        except ValueError:
                            filters_dict[field] = {"greater_than_or_equal_to": value}

                if filters_dict:
                    measure_def["filters"] = filters_dict

            measures[metric.name] = measure_def

        if measures:
            view["measures"] = measures

        return view

    def _comparison_type_to_offset(self, comparison_type: str | None) -> str:
        """Convert comparison_type to Omni time offset string.

        Args:
            comparison_type: Comparison type (yoy, mom, wow, etc.)

        Returns:
            Offset string like "1 year", "1 month", etc.
        """
        if not comparison_type:
            return "1 year"

        mapping = {
            "yoy": "1 year",
            "mom": "1 month",
            "wow": "1 week",
            "dod": "1 day",
            "qoq": "1 quarter",
        }
        return mapping.get(comparison_type, "1 year")

    def _export_relationships(self, models: dict[str, Model], output_dir: Path) -> None:
        """Export relationships to model.yaml file.

        Args:
            models: Dictionary of resolved models
            output_dir: Output directory path
        """
        relationships = []

        for model in models.values():
            for rel in model.relationships:
                rel_def = {
                    "join_from_view": model.name,
                    "join_to_view": rel.name,
                    "join_type": "always_left",
                }

                # Map relationship type
                type_mapping = {
                    "one_to_one": "one_to_one",
                    "many_to_one": "many_to_one",
                    "one_to_many": "one_to_many",
                    "many_to_many": "many_to_many",
                }
                rel_def["relationship_type"] = type_mapping.get(rel.type, "many_to_one")

                # Build on_sql
                from_key = rel.foreign_key or f"{rel.name}_id"
                to_key = rel.primary_key or "id"
                rel_def["on_sql"] = f"${{{model.name}.{from_key}}} = ${{{rel.name}.{to_key}}}"

                relationships.append(rel_def)

        if relationships:
            model_file = output_dir / "model.yaml"
            model_def = {"relationships": relationships}
            with open(model_file, "w") as f:
                yaml.dump(model_def, f, default_flow_style=False, sort_keys=False)
