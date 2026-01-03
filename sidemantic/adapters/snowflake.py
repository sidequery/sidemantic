"""Snowflake Cortex Semantic Model adapter for importing/exporting semantic models."""

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

# Common SQL keywords to skip when qualifying column names
_SQL_KEYWORDS = {
    "AND",
    "OR",
    "NOT",
    "IN",
    "IS",
    "NULL",
    "TRUE",
    "FALSE",
    "LIKE",
    "BETWEEN",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AS",
    "FROM",
    "WHERE",
    "SELECT",
    "SUM",
    "COUNT",
    "AVG",
    "MIN",
    "MAX",
    "MEDIAN",
    "DISTINCT",
    "NULLIF",
}


def _qualify_columns(sql_expr: str) -> str:
    """Add {model} qualifier to bare column names in SQL expression.

    Snowflake semantic model expressions use bare column names (e.g., "status = 'delivered'").
    Sidemantic requires {model} placeholder for column references.

    Args:
        sql_expr: SQL expression with bare column names

    Returns:
        SQL expression with {model}.column qualified references
    """
    if not sql_expr:
        return sql_expr

    # Skip if already has {model} references
    if "{model}" in sql_expr:
        return sql_expr

    # First, protect string literals by replacing them with numeric placeholders
    # Numbers can't be identifiers so they won't be matched by the pattern
    string_literals = []

    def save_string(match):
        idx = len(string_literals)
        string_literals.append(match.group(0))
        # Use 0x prefix to make it look like a number, won't be matched as identifier
        return f"0x{idx:08x}"

    # Match single-quoted strings (handling escaped quotes)
    protected_expr = re.sub(r"'(?:[^'\\]|\\.)*'", save_string, sql_expr)

    # Pattern to match potential column names
    # Uses word boundaries \b to match complete identifiers only
    # Negative lookbehind for dot prevents matching already-qualified names
    # We check for function calls (followed by parenthesis) in the replace function
    pattern = r"(?<![.\w])\b([a-zA-Z_][a-zA-Z0-9_]*)\b"

    def replace_column(match):
        col = match.group(1)
        # Skip if it's a SQL keyword or function name
        if col.upper() in _SQL_KEYWORDS:
            return col
        # Check if this is followed by a parenthesis (function call)
        end_pos = match.end()
        remaining = protected_expr[end_pos:].lstrip()
        if remaining.startswith("("):
            return col
        return "{model}." + col

    result = re.sub(pattern, replace_column, protected_expr)

    # Restore string literals
    for i, literal in enumerate(string_literals):
        result = result.replace(f"0x{i:08x}", literal)

    return result


class SnowflakeAdapter(BaseAdapter):
    """Adapter for importing/exporting Snowflake Cortex Semantic Models.

    Transforms Snowflake Cortex Analyst semantic model definitions into Sidemantic format:
    - tables -> Models
    - dimensions -> Dimensions (categorical)
    - time_dimensions -> Dimensions (time)
    - facts -> Metrics (with default_aggregation)
    - metrics -> Metrics (derived, table-scoped aggregations)
    - relationships -> Relationships
    - filters -> Segments

    Reference: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/semantic-model-spec
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Snowflake semantic model YAML files into semantic graph.

        Args:
            source: Path to YAML file or directory containing semantic model definitions

        Returns:
            Semantic graph with imported models and metrics
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
        """Parse a single Snowflake semantic model YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models/metrics to
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # Snowflake semantic model has top-level "name" and "tables" keys
        # Each table becomes a Model in Sidemantic
        tables = data.get("tables") or []

        for table_def in tables:
            model = self._parse_table(table_def)
            if model:
                graph.add_model(model)

        # Parse relationships (defined at semantic model level, not table level)
        relationships_def = data.get("relationships") or []
        self._apply_relationships(relationships_def, graph)

    def _parse_table(self, table_def: dict) -> Model | None:
        """Parse Snowflake table definition into Model.

        Args:
            table_def: Table definition dictionary

        Returns:
            Model instance or None
        """
        name = table_def.get("name")
        if not name:
            return None

        # Get physical table reference
        base_table = table_def.get("base_table", {})
        database = base_table.get("database", "")
        schema = base_table.get("schema", "")
        table = base_table.get("table", "")

        # Build fully qualified table name
        table_name = None
        if table:
            parts = [p for p in [database, schema, table] if p]
            table_name = ".".join(parts)

        # Parse primary key
        primary_key_def = table_def.get("primary_key", {})
        primary_key_columns = primary_key_def.get("columns") or []
        primary_key = primary_key_columns[0] if primary_key_columns else "id"

        # Parse dimensions (categorical attributes)
        dimensions = []
        for dim_def in table_def.get("dimensions") or []:
            dim = self._parse_dimension(dim_def)
            if dim:
                dimensions.append(dim)

        # Parse time dimensions
        for time_dim_def in table_def.get("time_dimensions") or []:
            dim = self._parse_time_dimension(time_dim_def)
            if dim:
                dimensions.append(dim)

        # Parse facts (row-level measures with default aggregation)
        metrics = []
        for fact_def in table_def.get("facts") or []:
            metric = self._parse_fact(fact_def)
            if metric:
                metrics.append(metric)

        # Parse metrics (table-scoped aggregations)
        for metric_def in table_def.get("metrics") or []:
            metric = self._parse_metric(metric_def)
            if metric:
                metrics.append(metric)

        # Parse filters as segments
        segments = []
        for filter_def in table_def.get("filters") or []:
            segment = self._parse_filter(filter_def)
            if segment:
                segments.append(segment)

        return Model(
            name=name,
            table=table_name,
            description=table_def.get("description"),
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            segments=segments,
        )

    def _parse_dimension(self, dim_def: dict) -> Dimension | None:
        """Parse Snowflake dimension into Sidemantic dimension.

        Args:
            dim_def: Dimension definition dictionary

        Returns:
            Dimension instance or None
        """
        name = dim_def.get("name")
        if not name:
            return None

        data_type = dim_def.get("data_type", "TEXT").upper()

        # Map Snowflake data types to Sidemantic dimension types
        if data_type in ("NUMBER", "INT", "INTEGER", "FLOAT", "DECIMAL", "NUMERIC", "DOUBLE"):
            dim_type = "numeric"
        elif data_type in ("BOOLEAN", "BOOL"):
            dim_type = "boolean"
        else:
            dim_type = "categorical"

        return Dimension(
            name=name,
            type=dim_type,
            sql=dim_def.get("expr"),
            description=dim_def.get("description"),
        )

    def _parse_time_dimension(self, dim_def: dict) -> Dimension | None:
        """Parse Snowflake time dimension into Sidemantic dimension.

        Args:
            dim_def: Time dimension definition dictionary

        Returns:
            Dimension instance or None
        """
        name = dim_def.get("name")
        if not name:
            return None

        return Dimension(
            name=name,
            type="time",
            sql=dim_def.get("expr"),
            description=dim_def.get("description"),
            granularity="day",  # Default granularity
        )

    def _parse_fact(self, fact_def: dict) -> Metric | None:
        """Parse Snowflake fact into Sidemantic metric.

        Facts are row-level numerical values with a default aggregation.

        Args:
            fact_def: Fact definition dictionary

        Returns:
            Metric instance or None
        """
        name = fact_def.get("name")
        if not name:
            return None

        # Map Snowflake default_aggregation to Sidemantic agg
        default_agg = fact_def.get("default_aggregation", "sum").lower()
        agg_mapping = {
            "sum": "sum",
            "avg": "avg",
            "average": "avg",
            "count": "count",
            "count_distinct": "count_distinct",
            "min": "min",
            "max": "max",
            "median": "median",
        }
        agg = agg_mapping.get(default_agg, "sum")

        return Metric(
            name=name,
            agg=agg,
            sql=fact_def.get("expr"),
            description=fact_def.get("description"),
        )

    def _parse_metric(self, metric_def: dict) -> Metric | None:
        """Parse Snowflake metric into Sidemantic metric.

        Metrics in Snowflake are table-scoped aggregations (already contain aggregate functions).

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Metric instance or None
        """
        name = metric_def.get("name")
        if not name:
            return None

        expr = metric_def.get("expr", "")

        # Snowflake metrics contain full aggregate expressions like "SUM(amount)"
        # Check if this is a simple aggregation or complex expression

        # Count how many aggregate function calls are in the expression
        agg_pattern = r"\b(SUM|COUNT|AVG|MIN|MAX|MEDIAN)\s*\("
        agg_matches = re.findall(agg_pattern, expr, re.IGNORECASE)

        if len(agg_matches) == 1:
            # Single aggregation - try to parse it
            # Simple pattern: just one aggregation wrapping the whole expression
            simple_agg_pattern = r"^\s*(SUM|COUNT|AVG|MIN|MAX|MEDIAN)\s*\((.+)\)\s*$"
            match = re.match(simple_agg_pattern, expr, re.IGNORECASE | re.DOTALL)

            if match:
                agg_func = match.group(1).lower()
                inner_expr = match.group(2).strip()

                # Handle COUNT(DISTINCT col)
                if agg_func == "count":
                    distinct_match = re.match(r"^\s*DISTINCT\s+(.+)$", inner_expr, re.IGNORECASE)
                    if distinct_match:
                        agg_func = "count_distinct"
                        inner_expr = distinct_match.group(1).strip()

                return Metric(
                    name=name,
                    agg=agg_func,
                    sql=inner_expr,
                    description=metric_def.get("description"),
                )

        # Complex expression (multiple aggregations or couldn't parse simple one)
        # Mark as derived and qualify column references with {model} placeholder
        qualified_expr = _qualify_columns(expr)
        return Metric(
            name=name,
            type="derived",
            sql=qualified_expr,
            description=metric_def.get("description"),
        )

    def _parse_filter(self, filter_def: dict) -> Segment | None:
        """Parse Snowflake filter into Sidemantic segment.

        Snowflake filters use bare column names (e.g., "status = 'delivered'").
        Sidemantic segments require {model} placeholder for column references.

        Args:
            filter_def: Filter definition dictionary

        Returns:
            Segment instance or None
        """
        name = filter_def.get("name")
        expr = filter_def.get("expr")

        if not name or not expr:
            return None

        # Qualify bare column references with {model} placeholder
        qualified_expr = _qualify_columns(expr)

        return Segment(
            name=name,
            sql=qualified_expr,
            description=filter_def.get("description"),
        )

    def _apply_relationships(self, relationships_def: list, graph: SemanticGraph) -> None:
        """Apply relationships from semantic model to models in graph.

        Snowflake defines relationships at the semantic model level, referencing tables.
        We need to add these as Relationship objects on the appropriate models.

        Args:
            relationships_def: List of relationship definitions
            graph: Semantic graph with models
        """
        for rel_def in relationships_def:
            left_table = rel_def.get("left_table")
            right_table = rel_def.get("right_table")
            rel_type = rel_def.get("relationship_type", "many_to_one")

            if not left_table or not right_table:
                continue

            # Get relationship columns
            rel_columns = rel_def.get("relationship_columns") or []
            if not rel_columns:
                continue

            # Use first column pair for foreign key
            first_col = rel_columns[0]
            left_column = first_col.get("left_column")
            right_column = first_col.get("right_column")

            # In Snowflake, left_table is the "many" side, right_table is the "one" side
            # Add relationship to left_table pointing to right_table
            if left_table in graph.models:
                model = graph.models[left_table]
                relationship = Relationship(
                    name=right_table,
                    type=rel_type,
                    foreign_key=left_column,
                    primary_key=right_column,
                )
                model.relationships.append(relationship)
                # Rebuild adjacency after adding relationship
                graph.build_adjacency()

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Snowflake semantic model YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Build semantic model structure
        semantic_model = {
            "name": output_path.stem,
            "tables": [],
            "relationships": [],
        }

        # Export each model as a table
        for model in resolved_models.values():
            table = self._export_table(model)
            semantic_model["tables"].append(table)

        # Export relationships
        for model in resolved_models.values():
            for rel in model.relationships:
                if rel.type in ("many_to_one", "one_to_one"):
                    rel_def = self._export_relationship(model, rel)
                    semantic_model["relationships"].append(rel_def)

        # Remove empty relationships list
        if not semantic_model["relationships"]:
            del semantic_model["relationships"]

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(semantic_model, f, sort_keys=False, default_flow_style=False)

    def _export_table(self, model: Model) -> dict:
        """Export model to Snowflake table definition.

        Args:
            model: Model to export

        Returns:
            Table definition dictionary
        """
        table = {"name": model.name}

        if model.description:
            table["description"] = model.description

        # Export base_table
        if model.table:
            parts = model.table.split(".")
            if len(parts) >= 3:
                table["base_table"] = {
                    "database": parts[0],
                    "schema": parts[1],
                    "table": parts[2],
                }
            elif len(parts) == 2:
                table["base_table"] = {
                    "schema": parts[0],
                    "table": parts[1],
                }
            else:
                table["base_table"] = {"table": parts[0]}

        # Export primary key
        if model.primary_key:
            table["primary_key"] = {"columns": [model.primary_key]}

        # Separate dimensions by type
        dimensions = []
        time_dimensions = []

        for dim in model.dimensions:
            if dim.type == "time":
                time_dim = self._export_time_dimension(dim)
                time_dimensions.append(time_dim)
            else:
                dim_def = self._export_dimension(dim)
                dimensions.append(dim_def)

        if dimensions:
            table["dimensions"] = dimensions
        if time_dimensions:
            table["time_dimensions"] = time_dimensions

        # Separate metrics into facts and metrics
        facts = []
        metrics = []

        for metric in model.metrics:
            if metric.agg and not metric.type:
                # Simple aggregation -> fact
                fact = self._export_fact(metric)
                facts.append(fact)
            else:
                # Complex metric or derived -> metric
                metric_def = self._export_metric(metric)
                metrics.append(metric_def)

        if facts:
            table["facts"] = facts
        if metrics:
            table["metrics"] = metrics

        # Export segments as filters
        if model.segments:
            table["filters"] = []
            for segment in model.segments:
                filter_def = self._export_filter(segment)
                table["filters"].append(filter_def)

        return table

    def _export_dimension(self, dim: Dimension) -> dict:
        """Export dimension to Snowflake dimension format.

        Args:
            dim: Dimension to export

        Returns:
            Dimension definition dictionary
        """
        dim_def = {"name": dim.name}

        if dim.description:
            dim_def["description"] = dim.description

        if dim.sql:
            dim_def["expr"] = dim.sql

        # Map Sidemantic types to Snowflake data types
        type_mapping = {
            "categorical": "TEXT",
            "numeric": "NUMBER",
            "boolean": "BOOLEAN",
        }
        dim_def["data_type"] = type_mapping.get(dim.type, "TEXT")

        return dim_def

    def _export_time_dimension(self, dim: Dimension) -> dict:
        """Export time dimension to Snowflake time_dimension format.

        Args:
            dim: Time dimension to export

        Returns:
            Time dimension definition dictionary
        """
        dim_def = {"name": dim.name}

        if dim.description:
            dim_def["description"] = dim.description

        if dim.sql:
            dim_def["expr"] = dim.sql

        dim_def["data_type"] = "TIMESTAMP"

        return dim_def

    def _export_fact(self, metric: Metric) -> dict:
        """Export metric as Snowflake fact.

        Args:
            metric: Metric to export

        Returns:
            Fact definition dictionary
        """
        fact = {"name": metric.name}

        if metric.description:
            fact["description"] = metric.description

        if metric.sql:
            fact["expr"] = metric.sql

        # Map Sidemantic agg to Snowflake default_aggregation
        agg_mapping = {
            "sum": "sum",
            "avg": "avg",
            "count": "count",
            "count_distinct": "count_distinct",
            "min": "min",
            "max": "max",
            "median": "median",
        }
        fact["default_aggregation"] = agg_mapping.get(metric.agg, "sum")

        fact["data_type"] = "NUMBER"

        return fact

    def _export_metric(self, metric: Metric) -> dict:
        """Export metric to Snowflake metric format.

        Args:
            metric: Metric to export

        Returns:
            Metric definition dictionary
        """
        metric_def = {"name": metric.name}

        if metric.description:
            metric_def["description"] = metric.description

        # Build expression based on metric type
        if metric.type == "ratio" and metric.numerator and metric.denominator:
            # Extract measure names from qualified references
            num = metric.numerator.split(".")[-1] if "." in metric.numerator else metric.numerator
            denom = metric.denominator.split(".")[-1] if "." in metric.denominator else metric.denominator
            metric_def["expr"] = f"{num} / NULLIF({denom}, 0)"
        elif metric.type == "derived" and metric.sql:
            metric_def["expr"] = metric.sql
        elif metric.agg and metric.sql:
            # Simple aggregation - wrap in aggregate function
            agg_func = metric.agg.upper()
            if agg_func == "COUNT_DISTINCT":
                metric_def["expr"] = f"COUNT(DISTINCT {metric.sql})"
            else:
                metric_def["expr"] = f"{agg_func}({metric.sql})"
        elif metric.sql:
            metric_def["expr"] = metric.sql

        return metric_def

    def _export_filter(self, segment: Segment) -> dict:
        """Export segment as Snowflake filter.

        Args:
            segment: Segment to export

        Returns:
            Filter definition dictionary
        """
        filter_def = {"name": segment.name}

        if segment.description:
            filter_def["description"] = segment.description

        if segment.sql:
            filter_def["expr"] = segment.sql

        return filter_def

    def _export_relationship(self, model: Model, rel: Relationship) -> dict:
        """Export relationship to Snowflake relationship format.

        Args:
            model: Model containing the relationship
            rel: Relationship to export

        Returns:
            Relationship definition dictionary
        """
        rel_def = {
            "left_table": model.name,
            "right_table": rel.name,
            "relationship_columns": [
                {
                    "left_column": rel.sql_expr,
                    "right_column": rel.related_key,
                }
            ],
            "relationship_type": rel.type,
            "join_type": "left_outer",
        }

        return rel_def
