"""Sidemantic native YAML adapter with SQL syntax support."""

import os
import re
from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.sql_definitions import (
    parse_sql_definitions,
    parse_sql_file_with_frontmatter_extended,
    parse_sql_graph_definitions,
    parse_sql_models,
)

NATIVE_FORMAT_VERSION = 1
ROOT_FIELDS = {
    "version",
    "connection",
    "models",
    "metrics",
    "parameters",
    "sql_metrics",
    "sql_segments",
}
MODEL_FIELDS = {
    "name",
    "extends",
    "table",
    "sql",
    "source_uri",
    "primary_key",
    "primary_key_columns",
    "unique_keys",
    "description",
    "label",
    "metadata",
    "meta",
    "auto_dimensions",
    "dimensions",
    "metrics",
    "measures",
    "relationships",
    "segments",
    "pre_aggregations",
    "default_time_dimension",
    "default_grain",
    "sql_metrics",
    "sql_segments",
}
DIMENSION_FIELDS = {
    "name",
    "type",
    "sql",
    "expr",
    "granularity",
    "supported_granularities",
    "description",
    "label",
    "metadata",
    "meta",
    "format",
    "value_format_name",
    "parent",
    "window",
    "public",
}
METRIC_FIELDS = {
    "name",
    "extends",
    "type",
    "agg",
    "sql",
    "expr",
    "measure",
    "metrics",
    "numerator",
    "denominator",
    "offset_window",
    "window",
    "grain_to_date",
    "window_expression",
    "window_frame",
    "window_order",
    "base_metric",
    "comparison_type",
    "time_offset",
    "calculation",
    "entity",
    "base_event",
    "conversion_event",
    "conversion_window",
    "steps",
    "cohort_event",
    "activity_event",
    "periods",
    "retention_granularity",
    "granularity",
    "inner_metrics",
    "entity_dimensions",
    "having",
    "fill_nulls_with",
    "format",
    "value_format_name",
    "drill_fields",
    "non_additive_dimension",
    "filters",
    "description",
    "label",
    "metadata",
    "meta",
    "public",
}
RELATIONSHIP_FIELDS = {
    "name",
    "type",
    "foreign_key",
    "foreign_key_columns",
    "primary_key",
    "primary_key_columns",
    "through",
    "through_foreign_key",
    "through_foreign_key_columns",
    "related_foreign_key",
    "related_foreign_key_columns",
    "sql",
    "metadata",
}
SEGMENT_FIELDS = {
    "name",
    "sql",
    "description",
    "public",
}
PRE_AGGREGATION_FIELDS = {
    "name",
    "type",
    "sql",
    "measures",
    "dimensions",
    "time_dimension",
    "granularity",
    "partition_granularity",
    "build_range_start",
    "build_range_end",
    "scheduled_refresh",
    "refresh_key",
    "indexes",
    "meta",
}
REFRESH_KEY_FIELDS = {
    "every",
    "sql",
    "incremental",
    "update_window",
}
INDEX_FIELDS = {
    "name",
    "columns",
    "type",
}
PARAMETER_FIELDS = {
    "name",
    "type",
    "description",
    "label",
    "default_value",
    "allowed_values",
    "default_to_today",
}


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


def validate_native_format_version(data: dict) -> None:
    version = data.get("version")
    if version in (None, NATIVE_FORMAT_VERSION):
        return
    raise ValueError(
        f"Unsupported native Sidemantic format version {version}; supported version is {NATIVE_FORMAT_VERSION}"
    )


def reject_unknown_fields(
    mapping: dict,
    allowed_fields: set[str],
    context: str,
    *,
    source_path: Path | None = None,
) -> None:
    """Reject misspelled native fields before constructing permissive Pydantic models."""
    if not isinstance(mapping, dict):
        location = f"{source_path}: " if source_path else ""
        raise ValueError(f"{location}{context} must be a mapping")

    unknown = sorted(set(mapping) - allowed_fields)
    if unknown:
        location = f"{source_path}: " if source_path else ""
        fields = ", ".join(unknown)
        raise ValueError(f"{location}unknown native field(s) in {context}: {fields}")


def normalize_sql_frontmatter(frontmatter: dict) -> dict:
    validate_native_format_version(frontmatter)
    normalized = dict(frontmatter)
    normalized.pop("version", None)
    normalized.pop("connection", None)
    normalized.pop("models", None)
    normalized.pop("parameters", None)
    return normalized


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

            # Pure SQL model files can use either MODEL(...) or model name from table (...).
            models = parse_sql_models(content)
            if models:
                for model in models:
                    graph.add_model(model)
                try:
                    sql_metrics, sql_segments, sql_parameters = parse_sql_graph_definitions(content)
                except Exception as exc:
                    raise ValueError(f"{source_path}: invalid SQL graph definitions: {exc}") from exc
                model_metric_names = {metric.name for model in models for metric in model.metrics}
                for metric in sql_metrics:
                    if metric.name not in model_metric_names:
                        graph.add_metric(metric)
                for param in sql_parameters:
                    graph.add_parameter(param)
            else:
                # YAML frontmatter + SQL metrics/segments
                try:
                    frontmatter, sql_metrics, sql_segments, sql_parameters, sql_preaggs = (
                        parse_sql_file_with_frontmatter_extended(source_path)
                    )
                except Exception as exc:
                    raise ValueError(f"{source_path}: invalid SQL definitions: {exc}") from exc

                # Parse frontmatter as a model only when it still contains model fields
                # after native contract metadata such as `version` is removed.
                normalized_frontmatter = normalize_sql_frontmatter(frontmatter) if frontmatter else {}
                if normalized_frontmatter:
                    model = self._parse_model(normalized_frontmatter, source_path=source_path)
                    if model:
                        # Add SQL-defined metrics/segments to the model
                        model.metrics.extend(sql_metrics)
                        model.segments.extend(sql_segments)
                        if sql_preaggs:
                            model.pre_aggregations.extend(sql_preaggs)
                        graph.add_model(model)
                else:
                    # No frontmatter - treat as graph-level metrics/segments
                    for metric in sql_metrics:
                        graph.add_metric(metric)
                    for param in sql_parameters:
                        graph.add_parameter(param)
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

        validate_native_format_version(data)
        reject_unknown_fields(data, ROOT_FIELDS, "root", source_path=source_path)

        # Parse models
        for model_def in data.get("models") or []:
            model = self._parse_model(model_def, source_path=source_path)
            if model:
                graph.add_model(model)

        # Parse metrics
        for metric_def in data.get("metrics") or []:
            metric = self._parse_metric(metric_def, source_path=source_path, context="metric")
            if metric:
                graph.add_metric(metric)
        # Parse parameters
        for parameter_def in data.get("parameters") or []:
            parameter = self._parse_parameter(parameter_def, source_path=source_path, context="parameter")
            if parameter:
                graph.add_parameter(parameter)

        # Parse SQL-defined metrics/segments if present
        if "sql_metrics" in data:
            sql_metrics, _ = self._parse_embedded_sql_definitions(
                data["sql_metrics"], source_path=source_path, block_name="sql_metrics"
            )
            for metric in sql_metrics:
                graph.add_metric(metric)

        if "sql_segments" in data:
            _, sql_segments = self._parse_embedded_sql_definitions(
                data["sql_segments"], source_path=source_path, block_name="sql_segments"
            )
            # Note: segments need to be attached to models
            # For now, skip graph-level segments

        self._resolve_inheritance(graph)

        return graph

    def _parse_embedded_sql_definitions(
        self,
        sql: str,
        *,
        source_path: Path | None = None,
        block_name: str,
        model_name: str | None = None,
    ) -> tuple[list[Metric], list[Segment]]:
        try:
            return parse_sql_definitions(sql)
        except Exception as exc:
            scope = f"model '{model_name}' {block_name}" if model_name else block_name
            location = f"{source_path}: " if source_path else ""
            raise ValueError(f"{location}invalid {scope}: {exc}") from exc

    def _resolve_inheritance(self, graph: SemanticGraph) -> None:
        from sidemantic.core.inheritance import (
            resolve_metric_inheritance,
            resolve_model_inheritance,
            resolve_model_metric_inheritance,
        )

        if any(model.extends for model in graph.models.values()):
            missing_parent = any(model.extends and model.extends not in graph.models for model in graph.models.values())
            if not missing_parent:
                graph.models = resolve_model_inheritance(graph.models)
                graph._mark_dirty()

        for model in graph.models.values():
            if model.extends and model.extends not in graph.models:
                continue
            resolve_model_metric_inheritance(model)

        if any(metric.extends for metric in graph.metrics.values()):
            graph.metrics = resolve_metric_inheritance(graph.metrics)
            graph._mark_dirty()

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Sidemantic YAML.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        data = {
            "version": NATIVE_FORMAT_VERSION,
            "models": [self._export_model(model) for model in graph.models.values()],
        }

        if graph.metrics:
            data["metrics"] = [self._export_metric(metric, graph) for metric in graph.metrics.values()]

        if graph.parameters:
            data["parameters"] = [self._export_parameter(parameter) for parameter in graph.parameters.values()]

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _parse_model(self, model_def: dict, *, source_path: Path | None = None) -> Model | None:
        """Parse model definition.

        Args:
            model_def: Model definition dictionary

        Returns:
            Model instance or None
        """
        reject_unknown_fields(model_def, MODEL_FIELDS, "model", source_path=source_path)

        name = model_def.get("name")
        if not name:
            return None

        # Parse joins
        joins = []
        for relationship_def in model_def.get("relationships") or []:
            reject_unknown_fields(
                relationship_def,
                RELATIONSHIP_FIELDS,
                f"model '{name}' relationship",
                source_path=source_path,
            )
            relationship_sql = relationship_def.get("sql")
            if relationship_sql is not None and ("{from}" not in relationship_sql or "{to}" not in relationship_sql):
                location = f"{source_path}: " if source_path else ""
                raise ValueError(
                    f"{location}model '{name}' relationship '{relationship_def.get('name')}' sql must include "
                    "both {from} and {to} placeholders"
                )
            join = Relationship(
                name=relationship_def.get("name"),
                type=relationship_def.get("type"),
                foreign_key=relationship_def.get("foreign_key_columns") or relationship_def.get("foreign_key"),
                primary_key=relationship_def.get("primary_key_columns") or relationship_def.get("primary_key"),
                metadata=relationship_def.get("metadata"),
                through=relationship_def.get("through"),
                through_foreign_key=relationship_def.get("through_foreign_key"),
                through_foreign_key_columns=relationship_def.get("through_foreign_key_columns"),
                related_foreign_key=relationship_def.get("related_foreign_key"),
                related_foreign_key_columns=relationship_def.get("related_foreign_key_columns"),
                sql=relationship_sql,
            )
            joins.append(join)

        # Parse dimensions
        dimensions = []
        for dim_def in model_def.get("dimensions") or []:
            reject_unknown_fields(
                dim_def,
                DIMENSION_FIELDS,
                f"model '{name}' dimension",
                source_path=source_path,
            )
            dimension = Dimension(
                name=dim_def.get("name"),
                type=dim_def.get("type", "categorical"),  # Default to categorical
                sql=dim_def.get("sql") or dim_def.get("expr"),
                granularity=dim_def.get("granularity"),
                supported_granularities=dim_def.get("supported_granularities"),
                description=dim_def.get("description"),
                label=dim_def.get("label"),
                format=dim_def.get("format"),
                value_format_name=dim_def.get("value_format_name"),
                parent=dim_def.get("parent"),
                metadata=dim_def.get("metadata"),
                meta=dim_def.get("meta"),
                window=dim_def.get("window"),
                public=dim_def.get("public", True),
            )
            dimensions.append(dimension)

        # Parse measures/metrics (support both field names for backwards compatibility)
        measures = []
        for measure_def in model_def.get("metrics", model_def.get("measures") or []):
            measure = self._parse_metric(
                measure_def,
                source_path=source_path,
                context=f"model '{name}' metric",
            )
            if measure:
                measures.append(measure)

        # Parse segments
        segments = []
        for seg_def in model_def.get("segments") or []:
            reject_unknown_fields(seg_def, SEGMENT_FIELDS, f"model '{name}' segment", source_path=source_path)
            segment = Segment(
                name=seg_def.get("name"),
                sql=seg_def.get("sql"),
                description=seg_def.get("description"),
                public=seg_def.get("public", True),
            )
            segments.append(segment)

        # Parse SQL-defined metrics/segments if present
        if "sql_metrics" in model_def:
            sql_metrics, _ = self._parse_embedded_sql_definitions(
                model_def["sql_metrics"], source_path=source_path, block_name="sql_metrics", model_name=name
            )
            measures.extend(sql_metrics)

        if "sql_segments" in model_def:
            _, sql_segments = self._parse_embedded_sql_definitions(
                model_def["sql_segments"], source_path=source_path, block_name="sql_segments", model_name=name
            )
            segments.extend(sql_segments)

        # Parse pre-aggregations
        from sidemantic.core.pre_aggregation import PreAggregation, RefreshKey

        pre_aggregations = []
        for preagg_def in model_def.get("pre_aggregations") or []:
            reject_unknown_fields(
                preagg_def,
                PRE_AGGREGATION_FIELDS,
                f"model '{name}' pre_aggregation",
                source_path=source_path,
            )
            # Parse refresh_key if present
            refresh_key = None
            if "refresh_key" in preagg_def:
                refresh_key_def = preagg_def["refresh_key"]
                if isinstance(refresh_key_def, dict):
                    reject_unknown_fields(
                        refresh_key_def,
                        REFRESH_KEY_FIELDS,
                        f"model '{name}' pre_aggregation refresh_key",
                        source_path=source_path,
                    )
                    refresh_key = RefreshKey(
                        every=refresh_key_def.get("every"),
                        sql=refresh_key_def.get("sql"),
                        incremental=refresh_key_def.get("incremental", False),
                        update_window=refresh_key_def.get("update_window"),
                    )

            for index_def in preagg_def.get("indexes") or []:
                if isinstance(index_def, dict):
                    reject_unknown_fields(
                        index_def,
                        INDEX_FIELDS,
                        f"model '{name}' pre_aggregation index",
                        source_path=source_path,
                    )

            preagg = PreAggregation(
                name=preagg_def.get("name"),
                type=preagg_def.get("type", "rollup"),
                sql=preagg_def.get("sql"),
                measures=preagg_def.get("measures") or [],
                dimensions=preagg_def.get("dimensions") or [],
                time_dimension=preagg_def.get("time_dimension"),
                granularity=preagg_def.get("granularity"),
                partition_granularity=preagg_def.get("partition_granularity"),
                refresh_key=refresh_key,
                scheduled_refresh=preagg_def.get("scheduled_refresh", True),
                indexes=preagg_def.get("indexes"),
                build_range_start=preagg_def.get("build_range_start"),
                build_range_end=preagg_def.get("build_range_end"),
                meta=preagg_def.get("meta"),
            )
            pre_aggregations.append(preagg)

        model_kwargs = {
            "name": name,
            "relationships": joins,
            "dimensions": dimensions,
            "metrics": measures,
            "segments": segments,
            "pre_aggregations": pre_aggregations,
        }
        for field in [
            "table",
            "sql",
            "source_uri",
            "description",
            "extends",
            "unique_keys",
            "default_time_dimension",
            "default_grain",
            "metadata",
            "auto_dimensions",
            "meta",
        ]:
            if field in model_def:
                model_kwargs[field] = model_def.get(field)

        if "primary_key_columns" in model_def:
            model_kwargs["primary_key"] = model_def.get("primary_key_columns")
        elif "primary_key" in model_def:
            model_kwargs["primary_key"] = model_def.get("primary_key")

        return Model(**model_kwargs)

    def _parse_metric(
        self,
        metric_def: dict,
        *,
        source_path: Path | None = None,
        context: str = "metric",
    ) -> Metric | None:
        """Parse measure definition.

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Measure instance or None
        """
        reject_unknown_fields(metric_def, METRIC_FIELDS, context, source_path=source_path)

        name = metric_def.get("name")
        metric_type = metric_def.get("type")

        if not name:
            return None

        metric_kwargs = {"name": name}
        for field in [
            "extends",
            "type",
            "description",
            "label",
            "metadata",
            "agg",
            "numerator",
            "denominator",
            "base_metric",
            "comparison_type",
            "time_offset",
            "calculation",
            "entity",
            "base_event",
            "conversion_event",
            "conversion_window",
            "steps",
            "offset_window",
            "cohort_event",
            "activity_event",
            "periods",
            "inner_metrics",
            "entity_dimensions",
            "having",
            "window",
            "grain_to_date",
            "window_expression",
            "window_frame",
            "window_order",
            "filters",
            "fill_nulls_with",
            "format",
            "value_format_name",
            "drill_fields",
            "non_additive_dimension",
            "meta",
            "public",
        ]:
            if field in metric_def:
                metric_kwargs[field] = metric_def.get(field)

        if "sql" in metric_def or "expr" in metric_def or "measure" in metric_def:
            metric_kwargs["sql"] = metric_def.get("sql") or metric_def.get("expr") or metric_def.get("measure")

        if metric_type == "retention" and ("retention_granularity" in metric_def or "granularity" in metric_def):
            metric_kwargs["retention_granularity"] = metric_def.get("retention_granularity") or metric_def.get(
                "granularity"
            )

        return Metric(**metric_kwargs)

    def _parse_parameter(
        self,
        parameter_def: dict,
        *,
        source_path: Path | None = None,
        context: str = "parameter",
    ) -> Parameter | None:
        """Parse parameter definition.

        Args:
            parameter_def: Parameter definition dictionary

        Returns:
            Parameter instance or None
        """
        reject_unknown_fields(parameter_def, PARAMETER_FIELDS, context, source_path=source_path)

        name = parameter_def.get("name")
        param_type = parameter_def.get("type")

        if not name or not param_type:
            return None

        return Parameter(
            name=name,
            type=param_type,
            description=parameter_def.get("description"),
            label=parameter_def.get("label"),
            default_value=parameter_def.get("default_value"),
            allowed_values=parameter_def.get("allowed_values"),
            default_to_today=parameter_def.get("default_to_today", False),
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
        if model.metadata:
            result["metadata"] = model.metadata
        if model.meta:
            result["meta"] = model.meta

        # Export joins
        if model.relationships:
            result["relationships"] = [
                {
                    "name": relationship.name,
                    "type": relationship.type,
                    **({"foreign_key": relationship.foreign_key} if relationship.foreign_key else {}),
                    **({"primary_key": relationship.primary_key} if relationship.primary_key else {}),
                    **({"metadata": relationship.metadata} if relationship.metadata else {}),
                    **({"through": relationship.through} if relationship.through else {}),
                    **(
                        {"through_foreign_key": relationship.through_foreign_key}
                        if relationship.through_foreign_key
                        else {}
                    ),
                    **(
                        {"through_foreign_key_columns": relationship.through_foreign_key_columns}
                        if relationship.through_foreign_key_columns
                        else {}
                    ),
                    **(
                        {"related_foreign_key": relationship.related_foreign_key}
                        if relationship.related_foreign_key
                        else {}
                    ),
                    **(
                        {"related_foreign_key_columns": relationship.related_foreign_key_columns}
                        if relationship.related_foreign_key_columns
                        else {}
                    ),
                    **({"sql": relationship.sql} if relationship.sql else {}),
                    **({"metadata": relationship.metadata} if relationship.metadata else {}),
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
                if dim.supported_granularities:
                    dim_def["supported_granularities"] = dim.supported_granularities
                if dim.description:
                    dim_def["description"] = dim.description
                if dim.label:
                    dim_def["label"] = dim.label
                if dim.metadata:
                    dim_def["metadata"] = dim.metadata
                if dim.meta:
                    dim_def["meta"] = dim.meta
                if dim.format:
                    dim_def["format"] = dim.format
                if dim.value_format_name:
                    dim_def["value_format_name"] = dim.value_format_name
                if dim.parent:
                    dim_def["parent"] = dim.parent
                if dim.window:
                    dim_def["window"] = dim.window
                if not dim.public:
                    dim_def["public"] = dim.public
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
                if measure.metadata:
                    measure_def["metadata"] = measure.metadata
                if measure.meta:
                    measure_def["meta"] = measure.meta
                if not measure.public:
                    measure_def["public"] = measure.public
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
                if measure.steps:
                    measure_def["steps"] = measure.steps
                if measure.offset_window:
                    measure_def["offset_window"] = measure.offset_window
                # Retention parameters
                if measure.cohort_event:
                    measure_def["cohort_event"] = measure.cohort_event
                if measure.activity_event:
                    measure_def["activity_event"] = measure.activity_event
                if measure.periods is not None:
                    measure_def["periods"] = measure.periods
                if measure.retention_granularity:
                    measure_def["retention_granularity"] = measure.retention_granularity
                # Cohort parameters
                if measure.inner_metrics:
                    measure_def["inner_metrics"] = measure.inner_metrics
                if measure.entity_dimensions:
                    measure_def["entity_dimensions"] = measure.entity_dimensions
                if measure.having:
                    measure_def["having"] = measure.having
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

        if model.pre_aggregations:
            result["pre_aggregations"] = [
                self._export_pre_aggregation(pre_aggregation) for pre_aggregation in model.pre_aggregations
            ]

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
        if measure.metadata:
            result["metadata"] = measure.metadata
        if measure.meta:
            result["meta"] = measure.meta
        if not measure.public:
            result["public"] = measure.public

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
        if measure.steps:
            result["steps"] = measure.steps
        if measure.offset_window:
            result["offset_window"] = measure.offset_window
        if measure.cohort_event:
            result["cohort_event"] = measure.cohort_event
        if measure.activity_event:
            result["activity_event"] = measure.activity_event
        if measure.periods is not None:
            result["periods"] = measure.periods
        if measure.retention_granularity:
            result["retention_granularity"] = measure.retention_granularity
        if measure.inner_metrics:
            result["inner_metrics"] = measure.inner_metrics
        if measure.entity_dimensions:
            result["entity_dimensions"] = measure.entity_dimensions
        if measure.having:
            result["having"] = measure.having
        if measure.sql:
            result["sql"] = measure.sql
        if measure.agg:
            result["agg"] = measure.agg
        if measure.window:
            result["window"] = measure.window
        if measure.filters:
            result["filters"] = measure.filters

        return result

    def _export_pre_aggregation(self, pre_aggregation) -> dict:
        result = {
            "name": pre_aggregation.name,
            "type": pre_aggregation.type,
        }

        if pre_aggregation.sql:
            result["sql"] = pre_aggregation.sql
        if pre_aggregation.measures:
            result["measures"] = pre_aggregation.measures
        if pre_aggregation.dimensions:
            result["dimensions"] = pre_aggregation.dimensions
        if pre_aggregation.time_dimension:
            result["time_dimension"] = pre_aggregation.time_dimension
        if pre_aggregation.granularity:
            result["granularity"] = pre_aggregation.granularity
        if pre_aggregation.partition_granularity:
            result["partition_granularity"] = pre_aggregation.partition_granularity
        if pre_aggregation.build_range_start:
            result["build_range_start"] = pre_aggregation.build_range_start
        if pre_aggregation.build_range_end:
            result["build_range_end"] = pre_aggregation.build_range_end
        if pre_aggregation.scheduled_refresh is False:
            result["scheduled_refresh"] = False
        if pre_aggregation.refresh_key:
            result["refresh_key"] = self._export_refresh_key(pre_aggregation.refresh_key)
        if pre_aggregation.indexes:
            result["indexes"] = [self._export_index(index) for index in pre_aggregation.indexes]
        if pre_aggregation.meta:
            result["meta"] = pre_aggregation.meta

        return result

    def _export_refresh_key(self, refresh_key) -> dict:
        result = {}
        if refresh_key.every:
            result["every"] = refresh_key.every
        if refresh_key.sql:
            result["sql"] = refresh_key.sql
        if refresh_key.incremental:
            result["incremental"] = refresh_key.incremental
        if refresh_key.update_window:
            result["update_window"] = refresh_key.update_window
        return result

    def _export_index(self, index) -> dict:
        result = {
            "name": index.name,
            "columns": index.columns,
        }
        if index.type != "regular":
            result["type"] = index.type
        return result

    def _export_parameter(self, parameter: Parameter) -> dict:
        """Export parameter to dictionary."""
        result = {
            "name": parameter.name,
            "type": parameter.type,
        }

        if parameter.description:
            result["description"] = parameter.description
        if parameter.label:
            result["label"] = parameter.label
        if parameter.default_value is not None:
            result["default_value"] = parameter.default_value
        if parameter.allowed_values is not None:
            result["allowed_values"] = parameter.allowed_values
        if parameter.default_to_today:
            result["default_to_today"] = parameter.default_to_today

        return result
