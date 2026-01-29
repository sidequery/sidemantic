"""OSI (Open Semantic Interchange) adapter for importing and exporting OSI YAML files.

OSI is a vendor-agnostic semantic model specification designed to enable
interoperability between data analytics, AI, and BI tools.

Spec: https://github.com/open-semantic-interchange/OSI
"""

from pathlib import Path
from typing import Any

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class OSIAdapter(BaseAdapter):
    """Adapter for importing/exporting OSI (Open Semantic Interchange) YAML files.

    Transforms OSI definitions into Sidemantic format:
    - OSI semantic_model → SemanticGraph
    - OSI datasets → Models
    - OSI fields → Dimensions
    - OSI metrics → Metrics (graph-level)
    - OSI relationships → Relationships
    """

    # OSI dialect preference order for extracting SQL expressions
    DIALECT_PREFERENCE = ["ANSI_SQL", "SNOWFLAKE", "DATABRICKS"]

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse OSI YAML files into semantic graph.

        Args:
            source: Path to OSI YAML file or directory

        Returns:
            Semantic graph with imported models and metrics

        Raises:
            FileNotFoundError: If the source path does not exist
        """
        source_path = Path(source)

        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")

        graph = SemanticGraph()

        if source_path.is_dir():
            for yaml_file in source_path.rglob("*.yml"):
                self._parse_file(yaml_file, graph)
            for yaml_file in source_path.rglob("*.yaml"):
                self._parse_file(yaml_file, graph)
        else:
            self._parse_file(source_path, graph)

        # Rebuild adjacency graph after all models are added
        graph.build_adjacency()

        return graph

    def _parse_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse a single OSI YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models/metrics to
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # OSI has top-level "semantic_model" key containing list of semantic models
        semantic_models = data.get("semantic_model") or []

        for sm_def in semantic_models:
            # Parse datasets (equivalent to models)
            datasets = sm_def.get("datasets") or []
            for dataset_def in datasets:
                model = self._parse_dataset(dataset_def)
                if model:
                    graph.add_model(model)

            # Parse relationships and attach to models
            relationships = sm_def.get("relationships") or []
            for rel_def in relationships:
                self._add_relationship_to_model(rel_def, graph)

            # Parse metrics (graph-level)
            metrics = sm_def.get("metrics") or []
            for metric_def in metrics:
                metric = self._parse_metric(metric_def)
                if metric:
                    graph.add_metric(metric)

    def _parse_dataset(self, dataset_def: dict) -> Model | None:
        """Parse OSI dataset into Sidemantic Model.

        Args:
            dataset_def: Dataset definition dictionary

        Returns:
            Model instance or None
        """
        name = dataset_def.get("name")
        if not name:
            return None

        # Source is the table reference
        source = dataset_def.get("source")

        # Primary key - preserve full list for multi-column keys
        primary_key_list = dataset_def.get("primary_key") or []
        if len(primary_key_list) == 0:
            primary_key: str | list[str] = "id"
        elif len(primary_key_list) == 1:
            primary_key = primary_key_list[0]
        else:
            primary_key = primary_key_list

        # Unique keys - list of column lists
        unique_keys = dataset_def.get("unique_keys")

        # Parse fields (dimensions)
        dimensions = []
        for field_def in dataset_def.get("fields") or []:
            dim = self._parse_field(field_def)
            if dim:
                dimensions.append(dim)

        # Determine default time dimension
        default_time_dimension = None
        for dim in dimensions:
            if dim.type == "time":
                default_time_dimension = dim.name
                break

        # Build meta from ai_context and custom_extensions
        meta = None
        ai_context = dataset_def.get("ai_context")
        custom_extensions = dataset_def.get("custom_extensions")
        if ai_context or custom_extensions:
            meta = {}
            if ai_context:
                meta["ai_context"] = ai_context
            if custom_extensions:
                meta["custom_extensions"] = custom_extensions

        return Model(
            name=name,
            table=source,
            description=dataset_def.get("description"),
            primary_key=primary_key,
            unique_keys=unique_keys,
            dimensions=dimensions,
            default_time_dimension=default_time_dimension,
            meta=meta,
        )

    def _parse_field(self, field_def: dict) -> Dimension | None:
        """Parse OSI field into Sidemantic Dimension.

        Args:
            field_def: Field definition dictionary

        Returns:
            Dimension instance or None
        """
        name = field_def.get("name")
        if not name:
            return None

        # Extract SQL expression from dialects (prefer ANSI_SQL)
        sql = self._extract_expression(field_def.get("expression"))

        # Determine dimension type from dimension.is_time
        dimension_meta = field_def.get("dimension") or {}
        is_time = dimension_meta.get("is_time", False)
        dim_type = "time" if is_time else "categorical"

        # Build meta from ai_context and custom_extensions
        meta = None
        ai_context = field_def.get("ai_context")
        custom_extensions = field_def.get("custom_extensions")
        if ai_context or custom_extensions:
            meta = {}
            if ai_context:
                meta["ai_context"] = ai_context
            if custom_extensions:
                meta["custom_extensions"] = custom_extensions

        return Dimension(
            name=name,
            type=dim_type,
            sql=sql,
            description=field_def.get("description"),
            label=field_def.get("label"),
            granularity="day" if is_time else None,
            meta=meta,
        )

    def _parse_metric(self, metric_def: dict) -> Metric | None:
        """Parse OSI metric into Sidemantic Metric.

        OSI metrics contain full aggregate expressions like "SUM(dataset.field)".
        We parse these to extract the aggregation type and inner expression.

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Metric instance or None
        """
        name = metric_def.get("name")
        if not name:
            return None

        # Extract SQL expression from dialects
        expression = self._extract_expression(metric_def.get("expression"))

        if not expression:
            return None

        # Build meta from ai_context and custom_extensions
        meta = None
        ai_context = metric_def.get("ai_context")
        custom_extensions = metric_def.get("custom_extensions")
        if ai_context or custom_extensions:
            meta = {}
            if ai_context:
                meta["ai_context"] = ai_context
            if custom_extensions:
                meta["custom_extensions"] = custom_extensions

        # Let the Metric class handle aggregation parsing via its model_validator.
        # This properly handles complex expressions like SUM(x) / SUM(y) and
        # COUNT(DISTINCT col) using sqlglot.
        return Metric(
            name=name,
            sql=expression,
            description=metric_def.get("description"),
            meta=meta,
        )

    def _extract_expression(self, expression_def: dict | None) -> str | None:
        """Extract SQL expression from OSI expression definition.

        OSI expressions have a "dialects" array with dialect-specific expressions.
        We prefer ANSI_SQL but fall back to other dialects.

        Args:
            expression_def: Expression definition with dialects

        Returns:
            SQL expression string or None
        """
        if not expression_def:
            return None

        dialects = expression_def.get("dialects") or []

        # Build a map of dialect -> expression
        dialect_map = {}
        for d in dialects:
            dialect_name = d.get("dialect")
            expr = d.get("expression")
            if dialect_name and expr:
                dialect_map[dialect_name] = expr

        # Return first available in preference order
        for preferred in self.DIALECT_PREFERENCE:
            if preferred in dialect_map:
                return dialect_map[preferred]

        # Fallback to first available
        if dialects and dialects[0].get("expression"):
            return dialects[0]["expression"]

        return None

    def _add_relationship_to_model(self, rel_def: dict, graph: SemanticGraph) -> None:
        """Parse OSI relationship and add to the appropriate model.

        OSI relationships define:
        - from: dataset on the "many" side
        - to: dataset on the "one" side
        - from_columns: foreign key columns (can be multi-column)
        - to_columns: primary/unique key columns (can be multi-column)

        Args:
            rel_def: Relationship definition dictionary
            graph: Semantic graph with models
        """
        from_model = rel_def.get("from")
        to_model = rel_def.get("to")

        if not from_model or not to_model:
            return

        # Get the "from" model to add the relationship
        model = graph.models.get(from_model)
        if not model:
            return

        # Extract foreign key columns - preserve full list for multi-column keys
        from_columns = rel_def.get("from_columns") or []
        to_columns = rel_def.get("to_columns") or []

        # Normalize to appropriate type (str for single, list for multi)
        if len(from_columns) == 0:
            foreign_key: str | list[str] = f"{to_model}_id"
        elif len(from_columns) == 1:
            foreign_key = from_columns[0]
        else:
            foreign_key = from_columns

        if len(to_columns) == 0:
            primary_key: str | list[str] = "id"
        elif len(to_columns) == 1:
            primary_key = to_columns[0]
        else:
            primary_key = to_columns

        # Create many_to_one relationship (from many -> to one)
        relationship = Relationship(
            name=to_model,
            type="many_to_one",
            foreign_key=foreign_key,
            primary_key=primary_key,
        )

        model.relationships.append(relationship)

    # Supported OSI dialects for export
    SUPPORTED_EXPORT_DIALECTS = ["ANSI_SQL", "SNOWFLAKE", "DATABRICKS", "BIGQUERY"]

    def export(
        self,
        graph: SemanticGraph,
        output_path: str | Path,
        dialects: list[str] | None = None,
    ) -> None:
        """Export semantic graph to OSI YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
            dialects: List of OSI dialects to generate expressions for.
                      Default is ["ANSI_SQL"]. Options: ANSI_SQL, SNOWFLAKE, DATABRICKS, BIGQUERY.
                      When multiple dialects specified, sqlglot is used for transpilation.
        """
        output_path = Path(output_path)

        if dialects is None:
            dialects = ["ANSI_SQL"]

        # Store dialects for use in export methods
        self._export_dialects = dialects

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Build OSI semantic model
        semantic_model = self._export_semantic_model(resolved_models, graph)

        data = {"semantic_model": [semantic_model]}

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _generate_dialect_expressions(self, sql_expr: str) -> list[dict[str, str]]:
        """Generate expressions for multiple SQL dialects using sqlglot.

        Args:
            sql_expr: SQL expression in DuckDB/ANSI SQL format

        Returns:
            List of dialect expression dictionaries for OSI format
        """
        dialects = getattr(self, "_export_dialects", ["ANSI_SQL"])
        result = []

        for dialect in dialects:
            if dialect == "ANSI_SQL":
                result.append({"dialect": dialect, "expression": sql_expr})
            else:
                # Use sqlglot for transpilation
                import sqlglot

                # Map OSI dialect names to sqlglot dialect names
                dialect_map = {
                    "SNOWFLAKE": "snowflake",
                    "DATABRICKS": "databricks",
                    "BIGQUERY": "bigquery",
                }
                target = dialect_map.get(dialect)
                if target:
                    try:
                        transpiled = sqlglot.transpile(sql_expr, read="duckdb", write=target)[0]
                        result.append({"dialect": dialect, "expression": transpiled})
                    except Exception:
                        # Fallback to original expression if transpilation fails
                        result.append({"dialect": dialect, "expression": sql_expr})
                else:
                    result.append({"dialect": dialect, "expression": sql_expr})

        return result

    def _export_semantic_model(self, models: dict[str, Model], graph: SemanticGraph) -> dict[str, Any]:
        """Export models to OSI semantic model definition.

        Args:
            models: Resolved models dictionary
            graph: Original semantic graph (for graph-level metrics)

        Returns:
            OSI semantic model definition dictionary
        """
        result: dict[str, Any] = {
            "name": "semantic_model",
            "description": "Semantic model exported from Sidemantic",
        }

        # Export datasets
        datasets = []
        for model in models.values():
            dataset = self._export_dataset(model)
            datasets.append(dataset)
        result["datasets"] = datasets

        # Export relationships
        relationships = []
        for model in models.values():
            for rel in model.relationships:
                rel_def = self._export_relationship(model.name, rel)
                if rel_def:
                    relationships.append(rel_def)
        if relationships:
            result["relationships"] = relationships

        # Export graph-level metrics
        metrics = []
        for metric in graph.metrics.values():
            metric_def = self._export_metric(metric, models)
            if metric_def:
                metrics.append(metric_def)
        # Also export model-level metrics as graph-level (OSI style)
        for model in models.values():
            for metric in model.metrics:
                metric_def = self._export_metric(metric, models, model.name)
                if metric_def:
                    metrics.append(metric_def)
        if metrics:
            result["metrics"] = metrics

        return result

    def _export_dataset(self, model: Model) -> dict[str, Any]:
        """Export model to OSI dataset definition.

        Args:
            model: Model to export

        Returns:
            OSI dataset definition dictionary
        """
        dataset: dict[str, Any] = {"name": model.name}

        if model.table:
            dataset["source"] = model.table
        elif model.sql:
            dataset["source"] = f"({model.sql})"

        # Export primary_key as list (multi-column support)
        if model.primary_key:
            dataset["primary_key"] = model.primary_key_columns

        # Export unique_keys if present
        if model.unique_keys:
            dataset["unique_keys"] = model.unique_keys

        if model.description:
            dataset["description"] = model.description

        # Export fields (dimensions)
        if model.dimensions:
            fields = []
            for dim in model.dimensions:
                field = self._export_field(dim)
                fields.append(field)
            dataset["fields"] = fields

        # Export meta as ai_context and custom_extensions
        if model.meta:
            if "ai_context" in model.meta:
                dataset["ai_context"] = model.meta["ai_context"]
            if "custom_extensions" in model.meta:
                dataset["custom_extensions"] = model.meta["custom_extensions"]

        return dataset

    def _export_field(self, dim: Dimension) -> dict[str, Any]:
        """Export dimension to OSI field definition.

        Args:
            dim: Dimension to export

        Returns:
            OSI field definition dictionary
        """
        field: dict[str, Any] = {"name": dim.name}

        # Build expression with dialect support
        sql_expr = dim.sql or dim.name
        field["expression"] = {"dialects": self._generate_dialect_expressions(sql_expr)}

        # Set dimension.is_time for time dimensions
        if dim.type == "time":
            field["dimension"] = {"is_time": True}

        if dim.description:
            field["description"] = dim.description

        if dim.label:
            field["label"] = dim.label

        # Export meta as ai_context and custom_extensions
        if dim.meta:
            if "ai_context" in dim.meta:
                field["ai_context"] = dim.meta["ai_context"]
            if "custom_extensions" in dim.meta:
                field["custom_extensions"] = dim.meta["custom_extensions"]

        return field

    def _export_relationship(self, from_model: str, rel: Relationship) -> dict[str, Any] | None:
        """Export relationship to OSI relationship definition.

        Args:
            from_model: Name of the model containing the relationship
            rel: Relationship to export

        Returns:
            OSI relationship definition dictionary or None
        """
        if rel.type != "many_to_one":
            return None  # OSI only supports many-to-one style relationships

        return {
            "name": f"{from_model}_to_{rel.name}",
            "from": from_model,
            "to": rel.name,
            "from_columns": rel.foreign_key_columns,
            "to_columns": rel.primary_key_columns,
        }

    def _export_metric(
        self, metric: Metric, models: dict[str, Model], model_name: str | None = None
    ) -> dict[str, Any] | None:
        """Export metric to OSI metric definition.

        OSI metrics use full aggregate expressions like "SUM(dataset.field)".

        Args:
            metric: Metric to export
            models: Resolved models for context
            model_name: Model name for model-level metrics (for qualifying field refs)

        Returns:
            OSI metric definition dictionary or None
        """
        result: dict[str, Any] = {"name": metric.name}

        # Build the full expression
        expression = self._build_metric_expression(metric, model_name)
        if not expression:
            return None

        result["expression"] = {"dialects": self._generate_dialect_expressions(expression)}

        if metric.description:
            result["description"] = metric.description

        # Export meta as ai_context and custom_extensions
        if metric.meta:
            if "ai_context" in metric.meta:
                result["ai_context"] = metric.meta["ai_context"]
            if "custom_extensions" in metric.meta:
                result["custom_extensions"] = metric.meta["custom_extensions"]

        return result

    def _build_metric_expression(self, metric: Metric, model_name: str | None) -> str | None:
        """Build full OSI metric expression from Sidemantic metric.

        Args:
            metric: Metric to convert
            model_name: Model name for qualifying field references

        Returns:
            Full SQL expression or None
        """
        if metric.type == "ratio":
            # Ratio: numerator / denominator
            num = metric.numerator or ""
            denom = metric.denominator or ""
            return f"{num} / NULLIF({denom}, 0)"

        if metric.type == "derived":
            # Derived: use sql expression as-is
            return metric.sql

        # Simple aggregation
        if metric.agg:
            inner = metric.sql or "*"
            agg_upper = metric.agg.upper()

            # Qualify with model name if provided
            if model_name and inner != "*" and "." not in inner:
                inner = f"{model_name}.{inner}"

            if metric.agg == "count_distinct":
                return f"COUNT(DISTINCT {inner})"
            return f"{agg_upper}({inner})"

        # No aggregation, just SQL
        return metric.sql
