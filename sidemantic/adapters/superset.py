"""Superset adapter for importing/exporting Apache Superset datasets."""

from pathlib import Path
from typing import Any

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


class SupersetAdapter(BaseAdapter):
    """Adapter for importing/exporting Superset dataset definitions.

    Transforms Superset definitions into Sidemantic format:
    - Datasets → Models
    - Columns → Dimensions
    - Metrics → Metrics
    - main_dttm_col → Time dimension
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Superset dataset files into semantic graph.

        Args:
            source: Path to .yaml file or directory

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)

        # Collect all .yaml files
        yaml_files = []
        if source_path.is_dir():
            yaml_files = list(source_path.rglob("*.yaml")) + list(source_path.rglob("*.yml"))
        else:
            yaml_files = [source_path]

        # Parse all datasets
        for yaml_file in yaml_files:
            model = self._parse_dataset(yaml_file)
            if model:
                graph.add_model(model)

        return graph

    def _parse_dataset(self, file_path: Path) -> Model | None:
        """Parse Superset dataset YAML into Sidemantic model.

        Args:
            file_path: Path to dataset YAML file

        Returns:
            Model instance or None
        """
        with open(file_path) as f:
            dataset = yaml.safe_load(f)

        if not dataset:
            return None

        table_name = dataset.get("table_name")
        if not table_name:
            return None

        # Get table reference
        schema = dataset.get("schema")
        table = f"{schema}.{table_name}" if schema else table_name

        # Get SQL for virtual datasets
        sql = dataset.get("sql")

        # Parse columns
        dimensions = []
        primary_key = "id"  # default
        main_dttm_col = dataset.get("main_dttm_col")

        for col_def in dataset.get("columns") or []:
            dim = self._parse_column(col_def, main_dttm_col)
            if dim:
                dimensions.append(dim)

                # Check if this is the primary key
                if col_def.get("column_name") == "id":
                    primary_key = dim.name

        # Parse metrics
        metrics = []
        for metric_def in dataset.get("metrics") or []:
            metric = self._parse_metric(metric_def)
            if metric:
                metrics.append(metric)

        return Model(
            name=table_name,
            table=table if not sql else None,
            sql=sql,
            description=dataset.get("description"),
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
        )

    def _parse_column(self, col_def: dict[str, Any], main_dttm_col: str | None) -> Dimension | None:
        """Parse Superset column definition.

        Args:
            col_def: Column definition from dataset
            main_dttm_col: Name of the main datetime column

        Returns:
            Dimension instance or None
        """
        column_name = col_def.get("column_name")
        if not column_name:
            return None

        # Determine dimension type
        is_dttm = col_def.get("is_dttm", False)
        sql_type = col_def.get("type", "")

        dim_type = "categorical"
        granularity = None

        if is_dttm or column_name == main_dttm_col:
            dim_type = "time"
            # Determine granularity based on type
            if "DATE" in sql_type and "TIME" not in sql_type:
                granularity = "day"
            else:
                granularity = "hour"
        elif "INT" in sql_type or "NUMERIC" in sql_type or "FLOAT" in sql_type or "DOUBLE" in sql_type:
            dim_type = "numeric"
        elif "BOOL" in sql_type:
            dim_type = "boolean"

        # Get expression or use column name
        sql = col_def.get("expression") or column_name

        # Get label from verbose_name
        label = col_def.get("verbose_name")

        return Dimension(
            name=column_name,
            type=dim_type,
            sql=sql,
            label=label,
            granularity=granularity,
            description=col_def.get("description"),
        )

    def _parse_metric(self, metric_def: dict[str, Any]) -> Metric | None:
        """Parse Superset metric definition.

        Args:
            metric_def: Metric definition from dataset

        Returns:
            Metric instance or None
        """
        metric_name = metric_def.get("metric_name")
        if not metric_name:
            return None

        metric_type_str = metric_def.get("metric_type", "")
        expression = metric_def.get("expression", "")

        # Map Superset metric types to Sidemantic aggregation types
        type_mapping = {
            "count": "count",
            "count_distinct": "count_distinct",
            "sum": "sum",
            "avg": "avg",
            "min": "min",
            "max": "max",
        }

        agg = type_mapping.get(metric_type_str)

        # Extract inner expression if it's wrapped in an aggregation function
        # e.g., "COUNT(*)" -> "*", "SUM(amount)" -> "amount"
        sql = expression
        if agg and expression:
            import re

            # Pattern to match AGG_FUNC(...) and extract the inner part
            pattern = rf"^\s*{agg.upper()}\s*\(\s*(.*)\s*\)\s*$"
            match = re.match(pattern, expression, re.IGNORECASE)
            if match:
                sql = match.group(1).strip()
                # For COUNT(*), the inner is "*" - we store None for this case
                if sql == "*":
                    sql = None

        # If no standard aggregation, treat as derived metric
        metric_type = None
        if not agg and expression:
            metric_type = "derived"

        # Get label from verbose_name
        label = metric_def.get("verbose_name")

        return Metric(
            name=metric_name,
            type=metric_type,
            agg=agg,
            sql=sql if sql else None,
            label=label,
            description=metric_def.get("description"),
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Superset dataset format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output directory or file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # If output is a directory, create one file per model
        if output_path.is_dir() or not output_path.suffix:
            output_path.mkdir(parents=True, exist_ok=True)
            for model in resolved_models.values():
                dataset = self._export_dataset(model)
                file_path = output_path / f"{model.name}.yaml"
                with open(file_path, "w") as f:
                    yaml.dump(dataset, f, default_flow_style=False, sort_keys=False)
        else:
            # Single file export - export first model only
            if resolved_models:
                model = next(iter(resolved_models.values()))
                dataset = self._export_dataset(model)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, "w") as f:
                    yaml.dump(dataset, f, default_flow_style=False, sort_keys=False)

    def _export_dataset(self, model: Model) -> dict[str, Any]:
        """Export model to Superset dataset definition.

        Args:
            model: Model to export

        Returns:
            Dataset definition dictionary
        """
        dataset: dict[str, Any] = {
            "table_name": model.name,
            "description": model.description,
            "schema": None,
            "sql": model.sql,
        }

        # Extract schema from table name if present
        if model.table and "." in model.table:
            parts = model.table.split(".")
            dataset["schema"] = parts[0]
            dataset["table_name"] = parts[1]
        elif model.table:
            dataset["schema"] = None

        # Find main datetime column
        main_dttm_col = None
        for dim in model.dimensions:
            if dim.type == "time":
                main_dttm_col = dim.name
                break

        if main_dttm_col:
            dataset["main_dttm_col"] = main_dttm_col

        # Export columns
        columns = []
        for dim in model.dimensions:
            col_def: dict[str, Any] = {
                "column_name": dim.name,
            }

            if dim.label:
                col_def["verbose_name"] = dim.label

            if dim.type == "time":
                col_def["is_dttm"] = True
                col_def["type"] = "TIMESTAMP WITHOUT TIME ZONE"
            elif dim.type == "numeric":
                col_def["type"] = "NUMERIC"
            elif dim.type == "boolean":
                col_def["type"] = "BOOLEAN"
            else:
                col_def["type"] = "VARCHAR"

            col_def["groupby"] = True
            col_def["filterable"] = True
            col_def["is_active"] = True

            # Add expression if SQL is not just the column name
            if dim.sql and dim.sql != dim.name:
                col_def["expression"] = dim.sql
            else:
                col_def["expression"] = None

            if dim.description:
                col_def["description"] = dim.description

            columns.append(col_def)

        if columns:
            dataset["columns"] = columns

        # Export metrics
        metrics = []
        for metric in model.metrics:
            metric_def: dict[str, Any] = {
                "metric_name": metric.name,
            }

            if metric.label:
                metric_def["verbose_name"] = metric.label

            # Map aggregation type to Superset metric_type
            type_mapping = {
                "count": "count",
                "count_distinct": "count_distinct",
                "sum": "sum",
                "avg": "avg",
                "min": "min",
                "max": "max",
            }

            if metric.agg:
                metric_def["metric_type"] = type_mapping.get(metric.agg, "count")
                # Build expression
                if metric.sql:
                    metric_def["expression"] = f"{metric.agg.upper()}({metric.sql})"
                else:
                    metric_def["expression"] = f"{metric.agg.upper()}(*)"
            else:
                # Derived metric - no standard type
                metric_def["metric_type"] = None
                metric_def["expression"] = metric.sql or ""

            if metric.description:
                metric_def["description"] = metric.description

            metrics.append(metric_def)

        if metrics:
            dataset["metrics"] = metrics

        # Add version
        dataset["version"] = "1.0.0"

        return dataset
