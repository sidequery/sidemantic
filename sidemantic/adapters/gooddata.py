"""GoodData adapter for importing/exporting GoodData semantic models."""

import re
from pathlib import Path
from typing import Any

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class GoodDataAdapter(BaseAdapter):
    """Adapter for importing/exporting GoodData dataset definitions.

    Transforms GoodData definitions into Sidemantic format:
    - Datasets → Models
    - Attributes → Dimensions
    - Facts → Dimensions (numeric type)
    - Metrics (MAQL) → Metrics
    - References → Relationships
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse GoodData YAML files into semantic graph.

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

        # Parse all datasets and metrics
        for yaml_file in yaml_files:
            self._parse_file(yaml_file, graph)

        return graph

    def _parse_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse a GoodData YAML file (may contain multiple documents).

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models to
        """
        with open(file_path) as f:
            # Load all YAML documents (delimited by ---)
            docs = list(yaml.safe_load_all(f))

        for doc in docs:
            if not doc:
                continue

            doc_type = doc.get("type")

            if doc_type == "dataset":
                model = self._parse_dataset(doc)
                if model:
                    graph.add_model(model)
            elif doc_type == "metric":
                # Store metrics to add later after all models are loaded
                metric = self._parse_metric(doc)
                if metric:
                    # Try to determine which model this metric belongs to
                    # For now, we'll skip standalone metrics and handle them in datasets
                    pass
            # Skip date datasets, visualizations, dashboards for now

    def _parse_dataset(self, dataset_def: dict[str, Any]) -> Model | None:
        """Parse GoodData dataset into Sidemantic model.

        Args:
            dataset_def: Dataset definition dict

        Returns:
            Model instance or None
        """
        dataset_id = dataset_def.get("id")
        if not dataset_id:
            return None

        # Use id as name, title as description
        name = dataset_id
        description = dataset_def.get("description") or dataset_def.get("title")

        # Get table reference
        table = dataset_def.get("table_path")
        sql = dataset_def.get("sql")

        # Parse primary key
        primary_key_def = dataset_def.get("primary_key", "id")
        if isinstance(primary_key_def, list):
            primary_key = primary_key_def[0]  # Use first key for simplicity
        else:
            primary_key = primary_key_def

        # Parse fields (facts and attributes)
        dimensions = []
        fields = dataset_def.get("fields", {})

        for field_name, field_def in fields.items():
            if field_def is None:
                field_def = {}

            field_type = field_def.get("type", "attribute")

            if field_type == "fact":
                # Facts are numeric dimensions
                dim = Dimension(
                    name=field_name,
                    type="numeric",
                    sql=field_name,
                    description=field_def.get("title") or field_def.get("description"),
                )
                dimensions.append(dim)
            elif field_type == "attribute":
                # Attributes are categorical dimensions
                data_type = field_def.get("data_type", "STRING")

                # Determine dimension type from data type
                if data_type in ["DATE", "TIMESTAMP", "DATETIME"]:
                    dim_type = "time"
                    granularity = "day"
                else:
                    dim_type = "categorical"
                    granularity = None

                dim = Dimension(
                    name=field_name,
                    type=dim_type,
                    sql=field_name,
                    granularity=granularity,
                    description=field_def.get("title") or field_def.get("description"),
                )
                dimensions.append(dim)

                # Handle labels (alternate representations of attributes)
                labels = field_def.get("labels", {})
                for label_name, label_def in labels.items():
                    if label_def is None:
                        label_def = {}

                    # Create dimension for each label
                    label_dim = Dimension(
                        name=label_name,
                        type="categorical",
                        sql=label_def.get("source_column") or label_name,
                        description=label_def.get("title"),
                    )
                    dimensions.append(label_dim)

        # Parse references (relationships)
        relationships = []
        references = dataset_def.get("references", [])

        for ref in references:
            if isinstance(ref, dict):
                ref_dataset = ref.get("dataset")
                if ref_dataset:
                    # Assume many_to_one relationship
                    relationship = Relationship(
                        name=ref_dataset,
                        type="many_to_one",
                        foreign_key=ref.get("foreign_key"),
                    )
                    relationships.append(relationship)

        return Model(
            name=name,
            table=table,
            sql=sql,
            description=description,
            primary_key=primary_key,
            dimensions=dimensions,
            relationships=relationships,
        )

    def _parse_metric(self, metric_def: dict[str, Any]) -> Metric | None:
        """Parse GoodData MAQL metric definition.

        Args:
            metric_def: Metric definition dict

        Returns:
            Metric instance or None
        """
        metric_id = metric_def.get("id")
        if not metric_id:
            return None

        maql = metric_def.get("maql", "")

        # Try to parse MAQL to extract aggregation and SQL
        agg_type = None
        sql_expr = None

        # Parse MAQL: SELECT SUM({fact/name}) or SELECT {metric/name} WHERE ...
        if "SELECT" in maql:
            # Extract aggregation function
            agg_match = re.search(r"SELECT\s+(SUM|COUNT|AVG|MIN|MAX|MEDIAN)\s*\(", maql, re.IGNORECASE)
            if agg_match:
                agg_func = agg_match.group(1).upper()
                agg_mapping = {
                    "SUM": "sum",
                    "COUNT": "count",
                    "AVG": "avg",
                    "MIN": "min",
                    "MAX": "max",
                }
                agg_type = agg_mapping.get(agg_func)

                # Extract fact/field reference
                fact_match = re.search(r"\{fact/([^}]+)\}", maql)
                if fact_match:
                    sql_expr = fact_match.group(1)
            else:
                # Could be a derived metric or reference to another metric
                sql_expr = maql.replace("SELECT ", "")

        # Parse filters from WHERE clause
        filters = []
        where_match = re.search(r"WHERE\s+(.+)$", maql)
        if where_match:
            where_clause = where_match.group(1)
            # Parse {label/name} = "value"
            filter_matches = re.findall(r'\{label/([^}]+)\}\s*=\s*"([^"]+)"', where_clause)
            for field, value in filter_matches:
                filters.append(f"{field} = '{value}'")

        # Determine metric type
        metric_type = None
        if not agg_type and sql_expr:
            metric_type = "derived"

        return Metric(
            name=metric_id,
            type=metric_type,
            agg=agg_type,
            sql=sql_expr,
            filters=filters if filters else None,
            description=metric_def.get("description") or metric_def.get("title"),
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to GoodData YAML format.

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
            # Single file export - use multi-document YAML
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                for i, model in enumerate(resolved_models.values()):
                    if i > 0:
                        f.write("\n---\n")
                    dataset = self._export_dataset(model)
                    yaml.dump(dataset, f, default_flow_style=False, sort_keys=False)

    def _export_dataset(self, model: Model) -> dict[str, Any]:
        """Export model to GoodData dataset definition.

        Args:
            model: Model to export

        Returns:
            Dataset definition dictionary
        """
        dataset: dict[str, Any] = {
            "type": "dataset",
            "id": model.name,
        }

        if model.description:
            dataset["title"] = model.description

        if model.table:
            dataset["table_path"] = model.table
        elif model.sql:
            dataset["sql"] = model.sql

        # Export primary key
        dataset["primary_key"] = model.primary_key

        # Export fields (dimensions as facts or attributes)
        fields: dict[str, Any] = {}

        for dim in model.dimensions:
            field_def: dict[str, Any] = {}

            # Determine if fact or attribute based on type
            if dim.type == "numeric":
                field_def["type"] = "fact"
            else:
                field_def["type"] = "attribute"

                # Add data type for attributes
                if dim.type == "time":
                    field_def["data_type"] = "DATE"
                else:
                    field_def["data_type"] = "STRING"

            if dim.description:
                field_def["title"] = dim.description

            fields[dim.name] = field_def

        if fields:
            dataset["fields"] = fields

        # Export references (relationships)
        if model.relationships:
            references = []
            for rel in model.relationships:
                ref_def: dict[str, Any] = {"dataset": rel.name}
                if rel.foreign_key:
                    ref_def["foreign_key"] = rel.foreign_key
                references.append(ref_def)
            dataset["references"] = references

        return dataset

    def _export_metrics(self, models: dict[str, Model], output_path: Path) -> None:
        """Export metrics as separate MAQL metric definitions.

        Args:
            models: Dictionary of models
            output_path: Output directory path
        """
        for model in models.values():
            for metric in model.metrics:
                # Build MAQL expression
                maql = self._build_maql(metric)

                metric_def = {
                    "type": "metric",
                    "id": f"{model.name}_{metric.name}",
                    "maql": maql,
                }

                if metric.description:
                    metric_def["title"] = metric.description

                # Write to file
                file_path = output_path / f"{model.name}_{metric.name}.yaml"
                with open(file_path, "w") as f:
                    yaml.dump(metric_def, f, default_flow_style=False, sort_keys=False)

    def _build_maql(self, metric: Metric) -> str:
        """Build MAQL expression from metric definition.

        Args:
            metric: Metric to convert to MAQL

        Returns:
            MAQL expression string
        """
        if metric.agg and metric.sql:
            # Standard aggregation
            agg_mapping = {
                "sum": "SUM",
                "count": "COUNT",
                "count_distinct": "COUNT",
                "avg": "AVG",
                "min": "MIN",
                "max": "MAX",
            }
            agg_func = agg_mapping.get(metric.agg, "SUM")
            maql = f"SELECT {agg_func}({{fact/{metric.sql}}})"
        elif metric.sql:
            # Derived metric
            maql = f"SELECT {metric.sql}"
        else:
            maql = "SELECT COUNT(*)"

        # Add filters
        if metric.filters:
            where_parts = []
            for filter_str in metric.filters:
                # Parse "field = 'value'" format
                if "=" in filter_str:
                    parts = filter_str.split("=", 1)
                    field = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    where_parts.append(f'{{label/{field}}}="{value}"')

            if where_parts:
                maql += " WHERE " + " AND ".join(where_parts)

        return maql
