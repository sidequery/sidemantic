"""Sidemantic native YAML adapter with SQL syntax support."""

import os
import re
from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.sql_definitions import parse_sql_definitions, parse_sql_file_with_frontmatter, parse_sql_model


def substitute_env_vars(content: str) -> str:
    """Substitute environment variables in YAML content.

    Supports:
    - ${ENV_VAR} - replaced with environment variable value
    - ${ENV_VAR:-default} - replaced with value or default if not set
    - $ENV_VAR - simple form without braces

    Args:
        content: YAML content string

    Returns:
        Content with environment variables substituted

    Examples:
        >>> os.environ['DB_HOST'] = 'localhost'
        >>> substitute_env_vars('host: ${DB_HOST}')
        'host: localhost'
        >>> substitute_env_vars('host: ${MISSING:-default}')
        'host: default'
    """

    # Pattern for ${ENV_VAR} or ${ENV_VAR:-default}
    def replace_var(match):
        var_expr = match.group(1)
        # Check for default value syntax: VAR_NAME:-default
        if ":-" in var_expr:
            var_name, default = var_expr.split(":-", 1)
            return os.environ.get(var_name, default)
        else:
            var_name = var_expr
            value = os.environ.get(var_name)
            if value is None:
                # Keep original if not found (don't fail, let user handle missing vars)
                return match.group(0)
            return value

    # Replace ${VAR} and ${VAR:-default}
    content = re.sub(r"\$\{([^}]+)\}", replace_var, content)

    # Replace $VAR (simple form, no braces)
    # Only match valid environment variable names (alphanumeric + underscore)
    def replace_simple_var(match):
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            return match.group(0)
        return value

    content = re.sub(r"\$([A-Z_][A-Z0-9_]*)", replace_simple_var, content)

    return content


class SidemanticAdapter(BaseAdapter):
    """Adapter for Sidemantic native YAML format.

    Native format structure:
    ```yaml
    models:
      - name: orders
        table: public.orders
        entities: [...]
        dimensions: [...]
        measures: [...]

    metrics:
      - name: total_revenue
        type: simple
        measure: orders.revenue
    ```
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Sidemantic YAML or SQL into semantic graph.

        Supports:
        - Pure YAML files (.yml, .yaml)
        - SQL files with YAML frontmatter (.sql)
        - YAML files with embedded SQL blocks (sql_metrics, sql_segments fields)

        Args:
            source: Path to YAML or SQL file

        Returns:
            Semantic graph with imported models and metrics
        """
        graph = SemanticGraph()
        source_path = Path(source)

        # Handle .sql files
        if source_path.suffix == ".sql":
            # Read file content to check if it's pure SQL or has frontmatter
            with open(source_path) as f:
                content = f.read()

            # Check if file contains MODEL() statement (pure SQL)
            if "MODEL" in content.upper() and "MODEL (" in content.upper():
                model = parse_sql_model(content)
                if model:
                    graph.add_model(model)
            else:
                # YAML frontmatter + SQL metrics/segments
                frontmatter, sql_metrics, sql_segments = parse_sql_file_with_frontmatter(source_path)

                # Parse frontmatter as model definition if present
                if frontmatter:
                    model = self._parse_model(frontmatter)
                    if model:
                        # Add SQL-defined metrics/segments to the model
                        model.metrics.extend(sql_metrics)
                        model.segments.extend(sql_segments)
                        graph.add_model(model)
                else:
                    # No frontmatter - treat as graph-level metrics/segments
                    for metric in sql_metrics:
                        graph.add_metric(metric)
                    # Segments need to be attached to models, skip if no model

            return graph

        # Handle YAML files
        with open(source_path) as f:
            content = f.read()

        # Substitute environment variables
        content = substitute_env_vars(content)

        data = yaml.safe_load(content)

        if not data:
            return graph

        # Parse models
        for model_def in data.get("models") or []:
            model = self._parse_model(model_def)
            if model:
                graph.add_model(model)

        # Parse metrics
        for metric_def in data.get("metrics") or []:
            metric = self._parse_metric(metric_def)
            if metric:
                graph.add_metric(metric)

        # Parse SQL-defined metrics/segments if present
        if "sql_metrics" in data:
            sql_metrics, _ = parse_sql_definitions(data["sql_metrics"])
            for metric in sql_metrics:
                graph.add_metric(metric)

        if "sql_segments" in data:
            _, sql_segments = parse_sql_definitions(data["sql_segments"])
            # Note: segments need to be attached to models
            # For now, skip graph-level segments

        return graph

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Sidemantic YAML.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        data = {
            "models": [self._export_model(model) for model in graph.models.values()],
        }

        if graph.metrics:
            data["metrics"] = [self._export_metric(metric, graph) for metric in graph.metrics.values()]

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _parse_model(self, model_def: dict) -> Model | None:
        """Parse model definition.

        Args:
            model_def: Model definition dictionary

        Returns:
            Model instance or None
        """
        name = model_def.get("name")
        if not name:
            return None

        # Parse joins
        joins = []
        for relationship_def in model_def.get("relationships") or []:
            join = Relationship(
                name=relationship_def.get("name"),
                type=relationship_def.get("type"),
                foreign_key=relationship_def.get("foreign_key"),
                primary_key=relationship_def.get("primary_key"),
                through=relationship_def.get("through"),
                through_foreign_key=relationship_def.get("through_foreign_key"),
                related_foreign_key=relationship_def.get("related_foreign_key"),
            )
            joins.append(join)

        # Parse dimensions
        dimensions = []
        for dim_def in model_def.get("dimensions") or []:
            dimension = Dimension(
                name=dim_def.get("name"),
                type=dim_def.get("type", "categorical"),  # Default to categorical
                sql=dim_def.get("sql"),
                granularity=dim_def.get("granularity"),
                description=dim_def.get("description"),
                label=dim_def.get("label"),
                format=dim_def.get("format"),
                value_format_name=dim_def.get("value_format_name"),
                parent=dim_def.get("parent"),
            )
            dimensions.append(dimension)

        # Parse measures/metrics (support both field names for backwards compatibility)
        measures = []
        for measure_def in model_def.get("metrics", model_def.get("measures") or []):
            measure = Metric(
                name=measure_def.get("name"),
                agg=measure_def.get("agg"),
                sql=measure_def.get("sql"),
                type=measure_def.get("type"),
                filters=measure_def.get("filters"),
                description=measure_def.get("description"),
                label=measure_def.get("label"),
                format=measure_def.get("format"),
                value_format_name=measure_def.get("value_format_name"),
                drill_fields=measure_def.get("drill_fields"),
                non_additive_dimension=measure_def.get("non_additive_dimension"),
                base_metric=measure_def.get("base_metric"),
                comparison_type=measure_def.get("comparison_type"),
                time_offset=measure_def.get("time_offset"),
                calculation=measure_def.get("calculation"),
                entity=measure_def.get("entity"),
                base_event=measure_def.get("base_event"),
                conversion_event=measure_def.get("conversion_event"),
                conversion_window=measure_def.get("conversion_window"),
                offset_window=measure_def.get("offset_window"),
                # Cumulative/window parameters
                window=measure_def.get("window"),
                grain_to_date=measure_def.get("grain_to_date"),
                window_expression=measure_def.get("window_expression"),
                window_frame=measure_def.get("window_frame"),
                window_order=measure_def.get("window_order"),
            )
            measures.append(measure)

        # Parse segments
        segments = []
        for seg_def in model_def.get("segments") or []:
            segment = Segment(
                name=seg_def.get("name"),
                sql=seg_def.get("sql"),
                description=seg_def.get("description"),
                public=seg_def.get("public", True),
            )
            segments.append(segment)

        # Parse SQL-defined metrics/segments if present
        if "sql_metrics" in model_def:
            sql_metrics, _ = parse_sql_definitions(model_def["sql_metrics"])
            measures.extend(sql_metrics)

        if "sql_segments" in model_def:
            _, sql_segments = parse_sql_definitions(model_def["sql_segments"])
            segments.extend(sql_segments)

        # Parse pre-aggregations
        from sidemantic.core.pre_aggregation import PreAggregation, RefreshKey

        pre_aggregations = []
        for preagg_def in model_def.get("pre_aggregations") or []:
            # Parse refresh_key if present
            refresh_key = None
            if "refresh_key" in preagg_def:
                refresh_key_def = preagg_def["refresh_key"]
                if isinstance(refresh_key_def, dict):
                    refresh_key = RefreshKey(
                        every=refresh_key_def.get("every"),
                        sql=refresh_key_def.get("sql"),
                    )

            preagg = PreAggregation(
                name=preagg_def.get("name"),
                measures=preagg_def.get("measures") or [],
                dimensions=preagg_def.get("dimensions") or [],
                time_dimension=preagg_def.get("time_dimension"),
                granularity=preagg_def.get("granularity"),
                refresh_key=refresh_key,
                indexes=preagg_def.get("indexes"),
                build_range_start=preagg_def.get("build_range_start"),
                build_range_end=preagg_def.get("build_range_end"),
            )
            pre_aggregations.append(preagg)

        return Model(
            name=name,
            table=model_def.get("table"),
            sql=model_def.get("sql"),
            source_uri=model_def.get("source_uri"),
            description=model_def.get("description"),
            primary_key=model_def.get("primary_key", "id"),
            relationships=joins,
            dimensions=dimensions,
            metrics=measures,
            segments=segments,
            pre_aggregations=pre_aggregations,
            default_time_dimension=model_def.get("default_time_dimension"),
            default_grain=model_def.get("default_grain"),
        )

    def _parse_metric(self, metric_def: dict) -> Metric | None:
        """Parse measure definition.

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Measure instance or None
        """
        name = metric_def.get("name")
        metric_type = metric_def.get("type")

        if not name:
            return None

        return Metric(
            name=name,
            type=metric_type,
            description=metric_def.get("description"),
            label=metric_def.get("label"),
            sql=metric_def.get("sql") or metric_def.get("measure"),
            numerator=metric_def.get("numerator"),
            denominator=metric_def.get("denominator"),
            base_metric=metric_def.get("base_metric"),
            comparison_type=metric_def.get("comparison_type"),
            time_offset=metric_def.get("time_offset"),
            calculation=metric_def.get("calculation"),
            entity=metric_def.get("entity"),
            base_event=metric_def.get("base_event"),
            conversion_event=metric_def.get("conversion_event"),
            conversion_window=metric_def.get("conversion_window"),
            offset_window=metric_def.get("offset_window"),
            window=metric_def.get("window"),
            filters=metric_def.get("filters"),
        )

    def _export_model(self, model: Model) -> dict:
        """Export model to dictionary.

        Args:
            model: Model to export

        Returns:
            Model definition dictionary
        """
        result = {"name": model.name}

        if model.table:
            result["table"] = model.table
        if model.sql:
            result["sql"] = model.sql
        if model.source_uri:
            result["source_uri"] = model.source_uri
        if model.description:
            result["description"] = model.description

        # Export joins
        if model.relationships:
            result["relationships"] = [
                {
                    "name": relationship.name,
                    "type": relationship.type,
                    **({"foreign_key": relationship.foreign_key} if relationship.foreign_key else {}),
                    **({"primary_key": relationship.primary_key} if relationship.primary_key else {}),
                    **({"through": relationship.through} if relationship.through else {}),
                    **(
                        {"through_foreign_key": relationship.through_foreign_key}
                        if relationship.through_foreign_key
                        else {}
                    ),
                    **(
                        {"related_foreign_key": relationship.related_foreign_key}
                        if relationship.related_foreign_key
                        else {}
                    ),
                }
                for relationship in model.relationships
            ]

        # Export primary key
        if model.primary_key != "id":  # Only export if non-default
            result["primary_key"] = model.primary_key

        # Export dimensions
        if model.dimensions:
            result["dimensions"] = []
            for dim in model.dimensions:
                dim_def = {
                    "name": dim.name,
                    "type": dim.type,
                }
                if dim.sql:
                    dim_def["sql"] = dim.sql
                if dim.granularity:
                    dim_def["granularity"] = dim.granularity
                if dim.description:
                    dim_def["description"] = dim.description
                if dim.label:
                    dim_def["label"] = dim.label
                if dim.format:
                    dim_def["format"] = dim.format
                if dim.value_format_name:
                    dim_def["value_format_name"] = dim.value_format_name
                if dim.parent:
                    dim_def["parent"] = dim.parent
                result["dimensions"].append(dim_def)

        # Export metrics (model-level aggregations)
        if model.metrics:
            result["metrics"] = []
            for measure in model.metrics:
                measure_def = {
                    "name": measure.name,
                    "agg": measure.agg,
                }
                if measure.sql:
                    measure_def["sql"] = measure.sql
                if measure.filters:
                    measure_def["filters"] = measure.filters
                if measure.description:
                    measure_def["description"] = measure.description
                if measure.label:
                    measure_def["label"] = measure.label
                if measure.format:
                    measure_def["format"] = measure.format
                if measure.value_format_name:
                    measure_def["value_format_name"] = measure.value_format_name
                if measure.drill_fields:
                    measure_def["drill_fields"] = measure.drill_fields
                if measure.non_additive_dimension:
                    measure_def["non_additive_dimension"] = measure.non_additive_dimension
                if measure.type:
                    measure_def["type"] = measure.type
                if measure.base_metric:
                    measure_def["base_metric"] = measure.base_metric
                if measure.comparison_type:
                    measure_def["comparison_type"] = measure.comparison_type
                if measure.time_offset:
                    measure_def["time_offset"] = measure.time_offset
                if measure.calculation:
                    measure_def["calculation"] = measure.calculation
                if measure.entity:
                    measure_def["entity"] = measure.entity
                if measure.base_event:
                    measure_def["base_event"] = measure.base_event
                if measure.conversion_event:
                    measure_def["conversion_event"] = measure.conversion_event
                if measure.conversion_window:
                    measure_def["conversion_window"] = measure.conversion_window
                if measure.offset_window:
                    measure_def["offset_window"] = measure.offset_window
                # Cumulative/window parameters
                if measure.window:
                    measure_def["window"] = measure.window
                if measure.grain_to_date:
                    measure_def["grain_to_date"] = measure.grain_to_date
                if measure.window_expression:
                    measure_def["window_expression"] = measure.window_expression
                if measure.window_frame:
                    measure_def["window_frame"] = measure.window_frame
                if measure.window_order:
                    measure_def["window_order"] = measure.window_order
                result["metrics"].append(measure_def)

        # Export model-level default_time_dimension
        if model.default_time_dimension:
            result["default_time_dimension"] = model.default_time_dimension
        if model.default_grain:
            result["default_grain"] = model.default_grain

        # Export segments
        if model.segments:
            result["segments"] = []
            for segment in model.segments:
                seg_def = {
                    "name": segment.name,
                    "sql": segment.sql,
                }
                if segment.description:
                    seg_def["description"] = segment.description
                if not segment.public:  # Only export if non-default (False)
                    seg_def["public"] = segment.public
                result["segments"].append(seg_def)

        return result

    def _export_metric(self, measure: Metric, graph) -> dict:
        """Export measure to dictionary.

        Args:
            measure: Metric to export

        Returns:
            Measure definition dictionary
        """
        result = {
            "name": measure.name,
        }

        if measure.type:
            result["type"] = measure.type

        if measure.description:
            result["description"] = measure.description
        if measure.label:
            result["label"] = measure.label

        # Type-specific fields
        if measure.numerator:
            result["numerator"] = measure.numerator
        if measure.denominator:
            result["denominator"] = measure.denominator
        if measure.base_metric:
            result["base_metric"] = measure.base_metric
        if measure.comparison_type:
            result["comparison_type"] = measure.comparison_type
        if measure.time_offset:
            result["time_offset"] = measure.time_offset
        if measure.calculation:
            result["calculation"] = measure.calculation
        if measure.entity:
            result["entity"] = measure.entity
        if measure.base_event:
            result["base_event"] = measure.base_event
        if measure.conversion_event:
            result["conversion_event"] = measure.conversion_event
        if measure.conversion_window:
            result["conversion_window"] = measure.conversion_window
        if measure.offset_window:
            result["offset_window"] = measure.offset_window
        if measure.sql:
            result["sql"] = measure.sql
            # Auto-detect and export dependencies for derived measures
            if measure.type == "derived":
                dependencies = measure.get_dependencies(graph)
                if dependencies:
                    result["metrics"] = list(dependencies)
        if measure.window:
            result["window"] = measure.window
        if measure.filters:
            result["filters"] = measure.filters

        return result
