"""Snowflake Cortex Semantic Model adapter for importing/exporting semantic models."""

import re
from pathlib import Path

import sqlglot
import yaml
from sqlglot import exp

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import record_export_note
from sidemantic.sql.fragment import parse_sql_fragment, rewrite_sql_column_spans
from sidemantic.yaml_compat import safe_load as _yaml_safe_load


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

    def qualify(column: exp.Column, source: str) -> str | None:
        return f"{{model}}.{source}" if len(column.parts) == 1 else None

    rewritten = rewrite_sql_column_spans(sql_expr, qualify, dialect="snowflake", mask_kinds={"placeholder"})
    return rewritten if rewritten is not None else sql_expr


_SIMPLE_AGGREGATES = {
    exp.Sum: "sum",
    exp.Count: "count",
    exp.Avg: "avg",
    exp.Min: "min",
    exp.Max: "max",
    exp.Median: "median",
}


def _simple_aggregate(sql_expr: str) -> tuple[str, str] | None:
    """Return an outer aggregate and its source argument, if the whole expression is one."""
    tree = parse_sql_fragment(sql_expr, "snowflake")
    if tree is None:
        return None
    agg = next((name for node_type, name in _SIMPLE_AGGREGATES.items() if isinstance(tree, node_type)), None)
    if agg is None:
        return None
    try:
        tokens = sqlglot.tokenize(sql_expr, dialect="snowflake")
    except Exception:
        return None
    if len(tokens) < 3 or tokens[1].token_type != sqlglot.tokens.TokenType.L_PAREN:
        return None
    if tokens[-1].token_type != sqlglot.tokens.TokenType.R_PAREN:
        return None

    argument_start = tokens[2].start
    if isinstance(tree, exp.Count) and isinstance(tree.this, exp.Distinct):
        if len(tokens) < 4 or tokens[2].token_type != sqlglot.tokens.TokenType.DISTINCT:
            return None
        agg = "count_distinct"
        argument_start = tokens[3].start
    return agg, sql_expr[argument_start : tokens[-1].start].strip()


class SnowflakeAdapter(BaseAdapter):
    """Adapter for importing/exporting Snowflake Cortex Semantic Models.

    Transforms Snowflake Cortex Analyst semantic model definitions into Sidemantic format:
    - tables -> Models
    - dimensions -> Dimensions (categorical)
    - time_dimensions -> Dimensions (time)
    - facts (a.k.a. legacy `measures`) -> Metrics (with default_aggregation)
    - metrics -> Metrics (derived, table-scoped aggregations)
    - relationships -> Relationships
    - filters -> Segments

    Also imports newer Cortex Analyst spec features:
    - `synonyms` on dimensions/facts/measures/metrics
    - `sample_values` and `cortex_search_service` / `cortex_search_service_name` on dimensions
    - top-level `verified_queries`, `custom_instructions`, `module_custom_instructions`
    - per-field keys preserved in metadata: access_modifier, is_enum, unique, labels,
      tags, non_additive_dimensions, using_relationships

    Reference: https://docs.snowflake.com/en/user-guide/views-semantic/semantic-view-yaml-spec
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

        # Top-level metrics and relationships are resolved after every file's tables
        # are loaded, so a metric or relationship referencing a table defined in a
        # later file still resolves regardless of directory traversal order.
        deferred_metrics: list[dict] = []
        deferred_relationships: list[dict] = []

        if source_path.is_dir():
            # Parse all YAML files in directory
            for yaml_file in source_path.rglob("*.yml"):
                self._parse_file(yaml_file, graph, deferred_metrics, deferred_relationships)
            for yaml_file in source_path.rglob("*.yaml"):
                self._parse_file(yaml_file, graph, deferred_metrics, deferred_relationships)
        else:
            # Parse single file
            self._parse_file(source_path, graph, deferred_metrics, deferred_relationships)

        self._apply_relationships(deferred_relationships, graph)
        self._apply_top_level_metrics(deferred_metrics, graph)

        # For a directory parse every file is seen here, so resolve pending metrics
        # against the loaded tables; anything still unresolved (table truly absent)
        # falls back to a graph-level metric so it is not dropped. For a single-file
        # parse the pending list is left intact so the directory loader can resolve
        # it across files.
        pending = getattr(graph, "_pending_table_metrics", None)
        if pending and source_path.is_dir():
            self.resolve_pending_table_metrics(graph.models, pending)
            for _table_name, metric in pending:
                graph.metrics.setdefault(metric.name, metric)
            pending.clear()

        return graph

    @staticmethod
    def resolve_pending_table_metrics(models: dict, pending_metrics: list) -> None:
        """Attach pending metrics that reference a now-loaded table.

        Multi-file CLI loads parse each Snowflake file separately, so a top-level
        metric with ``table: orders`` defined before the file that declares
        ``orders`` is collected as a ``(table_name, Metric)`` pending entry. Once
        every file's models are loaded, attach each to its table and re-qualify its
        expression with the ``{model}`` placeholder. Pending entries are a list (not
        a name-keyed map) so same-named scoped metrics on different tables do not
        overwrite one another. Unresolved entries are left in place.
        """
        remaining = []
        for table_name, metric in pending_metrics:
            model = models.get(table_name)
            if model is None:
                remaining.append((table_name, metric))
                continue
            if metric.type == "derived" and metric.sql:
                metric.sql = _qualify_columns(metric.sql)
            model.metrics.append(metric)
        pending_metrics[:] = remaining

    def _apply_top_level_metrics(self, metric_defs: list[dict], graph: SemanticGraph) -> None:
        """Attach collected top-level metrics once all tables are loaded."""
        for metric_def in metric_defs:
            table_name = metric_def.get("table")
            if table_name and table_name in graph.models:
                # Table-scoped: bare column refs are local to the table, so qualify
                # complex expressions with the {model} placeholder.
                metric = self._parse_metric(metric_def)
                if metric is None:
                    continue
                graph.models[table_name].metrics.append(metric)
            elif table_name:
                # The referenced table is not in this graph (multi-file CLI load
                # parses each file separately). Hold the metric in a table-qualified
                # pending list so the directory loader can attach it once that table
                # is loaded, without colliding on metric name.
                metric = self._parse_metric(metric_def, qualify=False)
                if metric is None:
                    continue
                if not hasattr(graph, "_pending_table_metrics"):
                    graph._pending_table_metrics = []
                graph._pending_table_metrics.append((table_name, metric))
            else:
                # Graph-level metric: expressions reference other fields as
                # `model.field` (already qualified), so leave them untouched
                # instead of corrupting them with the {model} placeholder.
                metric = self._parse_metric(metric_def, qualify=False)
                if metric is None:
                    continue
                graph.metrics[metric.name] = metric

    def _parse_file(
        self,
        file_path: Path,
        graph: SemanticGraph,
        deferred_metrics: list[dict],
        deferred_relationships: list[dict],
    ) -> None:
        """Parse a single Snowflake semantic model YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models/metrics to
            deferred_metrics: Accumulator for top-level metric definitions, resolved
                after every file's tables are loaded.
            deferred_relationships: Accumulator for top-level relationship
                definitions, applied after every file's tables are loaded.
        """
        with open(file_path) as f:
            data = _yaml_safe_load(f)

        if not data:
            return

        # Snowflake semantic model has top-level "name" and "tables" keys
        # Each table becomes a Model in Sidemantic
        tables = data.get("tables") or []

        for table_def in tables:
            model = self._parse_table(table_def)
            if model:
                graph.add_model(model)

        # Defer relationships (defined at the semantic-model level) until all files'
        # tables are loaded so they resolve regardless of traversal order.
        deferred_relationships.extend(data.get("relationships") or [])

        # Defer top-level metrics (semantic-model-scoped metrics referencing tables)
        # until all files are parsed, so a metric whose table lives in a later file
        # still attaches correctly regardless of traversal order.
        deferred_metrics.extend(data.get("metrics") or [])

        # Parse top-level Cortex Analyst sections onto the graph.
        self._apply_top_level_sections(data, graph)

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
        primary_key = self._key_value(primary_key_columns)

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

        # Parse facts (row-level measures with default aggregation).
        # Cortex Analyst's table-level `measures:` key is a legacy alias of `facts:`;
        # accept both so current Cortex Analyst files import without silent data loss.
        metrics = []
        for fact_def in (table_def.get("facts") or []) + (table_def.get("measures") or []):
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

        metadata = self._collect_metadata(table_def, ("synonyms",)) or {"snowflake": {}}
        if base_table and not base_table.get("definition"):
            metadata["snowflake"]["base_table"] = dict(base_table)
            metadata["snowflake"]["source_table"] = table_name

        return Model(
            name=name,
            table=table_name,
            sql=base_table.get("definition"),
            metadata=metadata,
            description=table_def.get("description"),
            primary_key=primary_key,
            unique_keys=[key["columns"] for key in table_def.get("unique_keys") or []] or None,
            dimensions=dimensions,
            metrics=metrics,
            segments=segments,
        )

    @staticmethod
    def _key_value(columns: list[str]) -> str | list[str] | None:
        return columns[0] if len(columns) == 1 else columns or None

    @staticmethod
    def _dimension_type(data_type: str) -> str:
        base_type = re.split(r"[\s(]", data_type.strip().upper(), maxsplit=1)[0]
        if base_type in {
            "NUMBER",
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "BYTEINT",
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
            "REAL",
            "DECIMAL",
            "DEC",
            "NUMERIC",
            "DOUBLE",
        }:
            return "numeric"
        if base_type in {"BOOLEAN", "BOOL"}:
            return "boolean"
        if base_type in {"DATE", "TIME", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}:
            return "time"
        return "categorical"

    def _export_data_type(self, dim: Dimension, default: str) -> str:
        original = (dim.metadata or {}).get("snowflake", {}).get("data_type")
        logical = {
            "String": "TEXT",
            "Integer": "NUMBER(38,0)",
            "Decimal": "NUMBER",
            "Float": "FLOAT",
            "Boolean": "BOOLEAN",
            "Date": "DATE",
            "Time": "TIME",
            "DateTime": "TIMESTAMP_NTZ",
            "DateTimeTz": "TIMESTAMP_TZ",
        }.get(dim.logical_data_type)
        if logical and self._dimension_type(logical) == dim.type:
            return logical
        return original if original and self._dimension_type(original) == dim.type else default

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

        dim_type = self._dimension_type(dim_def.get("data_type", "TEXT"))
        metadata = self._dimension_metadata(dim_def)
        if dim_type == "time":
            metadata = metadata or {"snowflake": {}}
            metadata["snowflake"]["dimension_section"] = "dimensions"

        return Dimension(
            name=name,
            type=dim_type,
            sql=dim_def.get("expr"),
            description=dim_def.get("description"),
            synonyms=dim_def.get("synonyms"),
            sample_values=self._sample_values(dim_def),
            cortex_search_service_name=self._cortex_search_service_name(dim_def),
            metadata=metadata,
            public=self._public_from_access_modifier(dim_def),
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
            synonyms=dim_def.get("synonyms"),
            sample_values=self._sample_values(dim_def),
            cortex_search_service_name=self._cortex_search_service_name(dim_def),
            metadata=self._dimension_metadata(dim_def),
            public=self._public_from_access_modifier(dim_def),
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
        default_agg = (fact_def.get("default_aggregation") or "sum").lower()
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
            synonyms=fact_def.get("synonyms"),
            metadata=self._measure_metadata(fact_def),
            public=self._public_from_access_modifier(fact_def),
        )

    def _parse_metric(self, metric_def: dict, qualify: bool = True) -> Metric | None:
        """Parse Snowflake metric into Sidemantic metric.

        Metrics in Snowflake are table-scoped aggregations (already contain aggregate functions).

        Args:
            metric_def: Metric definition dictionary
            qualify: When True (table-scoped metrics), bare column references in
                complex/derived expressions are qualified with the {model}
                placeholder. When False (graph-level metrics), the expression is
                left as-is because it already uses ``model.field`` references that
                must not be rewritten.

        Returns:
            Metric instance or None
        """
        name = metric_def.get("name")
        if not name:
            return None

        expr = metric_def.get("expr", "")

        simple_aggregate = _simple_aggregate(expr)
        if simple_aggregate is not None:
            agg_func, inner_expr = simple_aggregate
            return Metric(
                name=name,
                agg=agg_func,
                sql=inner_expr,
                description=metric_def.get("description"),
                synonyms=metric_def.get("synonyms"),
                metadata=self._metric_metadata(metric_def),
                public=self._public_from_access_modifier(metric_def),
            )

        # Complex expression (multiple aggregations or couldn't parse simple one)
        # Mark as derived. Table-scoped metrics qualify bare column references with
        # the {model} placeholder; graph-level metrics already use `model.field`
        # references and must be left untouched.
        derived_expr = _qualify_columns(expr) if qualify else expr
        return Metric(
            name=name,
            type="derived",
            sql=derived_expr,
            description=metric_def.get("description"),
            synonyms=metric_def.get("synonyms"),
            metadata=self._metric_metadata(metric_def),
            public=self._public_from_access_modifier(metric_def),
        )

    @staticmethod
    def _cortex_search_service_name(dim_def: dict) -> str | None:
        """Resolve the linked Cortex Search service name for a dimension.

        Supports both the legacy flat ``cortex_search_service_name`` string and
        the newer nested ``cortex_search_service`` object (``{service, ...}``).
        """
        flat = dim_def.get("cortex_search_service_name")
        if flat:
            return flat
        nested = dim_def.get("cortex_search_service")
        if isinstance(nested, dict):
            return nested.get("service")
        if isinstance(nested, str):
            return nested
        return None

    @staticmethod
    def _public_from_access_modifier(definition: dict) -> bool:
        """Map Snowflake ``access_modifier`` onto Sidemantic visibility.

        Snowflake uses ``private_access`` for hidden helper fields. The original
        modifier is still preserved in metadata, but reflect it on ``public`` so
        CLI ``info``/catalog and native export treat the field as non-public.
        """
        return definition.get("access_modifier") != "private_access"

    @staticmethod
    def _sample_values(dim_def: dict) -> list[str] | None:
        """Coerce Snowflake ``sample_values`` to strings.

        Snowflake documents ``sample_values`` as raw column values, so numeric or
        time dimensions can legally contain unquoted YAML scalars (e.g.
        ``sample_values: [1001, 1002]``). ``Dimension.sample_values`` is typed as
        ``list[str]``, so coerce any scalar to ``str`` to avoid rejecting valid
        Cortex files.
        """
        values = dim_def.get("sample_values")
        if values is None:
            return None
        return [str(value) for value in values]

    @staticmethod
    def _collect_metadata(definition: dict, keys: tuple[str, ...]) -> dict | None:
        """Preserve newer Cortex Analyst per-field keys under a snowflake namespace."""
        extra = {key: definition[key] for key in keys if definition.get(key) is not None}
        if not extra:
            return None
        return {"snowflake": extra}

    def _dimension_metadata(self, dim_def: dict) -> dict | None:
        return self._collect_metadata(
            dim_def,
            ("data_type", "unique", "is_enum", "access_modifier", "labels", "tags", "cortex_search_service"),
        )

    def _measure_metadata(self, measure_def: dict) -> dict | None:
        return self._collect_metadata(
            measure_def,
            ("data_type", "access_modifier", "is_enum", "labels", "tags", "non_additive_dimensions"),
        )

    def _metric_metadata(self, metric_def: dict) -> dict | None:
        return self._collect_metadata(
            metric_def,
            ("access_modifier", "labels", "tags", "non_additive_dimensions", "using_relationships"),
        )

    @staticmethod
    def _apply_top_level_sections(data: dict, graph: SemanticGraph) -> None:
        """Attach top-level Cortex Analyst sections to the graph.

        Cortex Analyst defines several semantic-model-level sections that have no
        direct Sidemantic equivalent. We expose them both as direct attributes on
        the graph (for ergonomic access) and inside ``graph.metadata`` so they
        survive serialization.
        """
        verified_queries = data.get("verified_queries") or []
        custom_instructions = data.get("custom_instructions")
        module_custom_instructions = data.get("module_custom_instructions")

        # Accumulate verified queries across files in a directory parse.
        existing = list(getattr(graph, "verified_queries", []) or [])
        existing.extend(verified_queries)
        graph.verified_queries = existing

        if custom_instructions is not None:
            graph.custom_instructions = custom_instructions
        elif not hasattr(graph, "custom_instructions"):
            graph.custom_instructions = None

        if module_custom_instructions is not None:
            existing_mci = getattr(graph, "module_custom_instructions", None)
            if isinstance(existing_mci, dict) and isinstance(module_custom_instructions, dict):
                # Accumulate module instruction keys across files in a directory
                # parse instead of overwriting with the last file's value.
                graph.module_custom_instructions = {**existing_mci, **module_custom_instructions}
            else:
                graph.module_custom_instructions = module_custom_instructions
        elif not hasattr(graph, "module_custom_instructions"):
            graph.module_custom_instructions = None

        snowflake_meta = graph.metadata.setdefault("snowflake", {})
        for key in ("name", "description"):
            if data.get(key) is not None:
                snowflake_meta.setdefault(key, data[key])
        if existing:
            snowflake_meta["verified_queries"] = existing
        if graph.custom_instructions is not None:
            snowflake_meta["custom_instructions"] = graph.custom_instructions
        if graph.module_custom_instructions is not None:
            snowflake_meta["module_custom_instructions"] = graph.module_custom_instructions

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

    def apply_pending_relationships(self, relationships_def: list, models: dict) -> None:
        """Apply relationship definitions collected from separately-parsed files.

        Used by the directory loader after every file's models are loaded (and
        before foreign-key inference) so a relationship-only Cortex sidecar attaches
        its joins and an explicit join takes precedence over a guessed one. Operates
        on the name-keyed ``models`` dict; adjacency is rebuilt later by the loader.
        """

        def _is_duplicate(existing: Relationship) -> bool:
            # Same Snowflake relationship: matched by the preserved Snowflake name
            # when both carry one, otherwise by target + join columns. Distinct
            # named relationships between the same two tables are NOT duplicates and
            # must all survive so metrics referencing them via ``using_relationships``
            # keep resolving and alternate joins still round-trip on export.
            if existing.name != right_table:
                return False
            existing_name = existing.metadata.get("snowflake", {}).get("name") if existing.metadata else None
            if snowflake_name and existing_name:
                return existing_name == snowflake_name
            return existing.foreign_key == (
                right_column if rel_type in {"one_to_many", "one_to_one"} else left_column
            ) and existing.primary_key == (left_column if rel_type in {"one_to_many", "one_to_one"} else right_column)

        for rel_def in relationships_def:
            left_table = rel_def.get("left_table")
            right_table = rel_def.get("right_table")
            rel_type = rel_def.get("relationship_type", "many_to_one")

            if not left_table or not right_table:
                continue

            rel_columns = rel_def.get("relationship_columns") or []
            if not rel_columns:
                continue

            left_column = self._key_value([pair["left_column"] for pair in rel_columns])
            right_column = self._key_value([pair["right_column"] for pair in rel_columns])

            metadata = {"snowflake": {}}
            snowflake_name = rel_def.get("name")
            if snowflake_name:
                metadata["snowflake"]["name"] = snowflake_name
            join_type = rel_def.get("join_type", "left_outer")
            if join_type not in {"left_outer", "inner"}:
                raise ValueError(f"Unsupported Snowflake join_type {join_type!r}")
            metadata["snowflake"]["source_join_type"] = join_type
            metadata["join_type"] = join_type
            if "join_type" in rel_def:
                metadata["bsl_how"] = join_type.removesuffix("_outer")
            else:
                metadata["snowflake"]["implicit_join_type"] = True

            model = models.get(left_table)
            if model is None:
                continue
            # Skip only an exact duplicate (same Snowflake name, or same join
            # columns to the same target); keep distinct alternate joins.
            if any(_is_duplicate(r) for r in model.relationships):
                continue
            model.relationships.append(
                Relationship(
                    name=right_table,
                    type=rel_type,
                    foreign_key=right_column if rel_type in {"one_to_many", "one_to_one"} else left_column,
                    primary_key=left_column if rel_type in {"one_to_many", "one_to_one"} else right_column,
                    metadata=metadata,
                )
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

            # Retain every column pair in a compound relationship.
            left_column = self._key_value([pair["left_column"] for pair in rel_columns])
            right_column = self._key_value([pair["right_column"] for pair in rel_columns])

            # The Snowflake relationship name is referenced by metric
            # `using_relationships`; preserve it so those references stay valid
            # after export. `Relationship.name` is the related-model identifier and
            # cannot hold it, so stash it in adapter metadata instead.
            metadata = {"snowflake": {}}
            snowflake_name = rel_def.get("name")
            if snowflake_name:
                metadata["snowflake"]["name"] = snowflake_name
            join_type = rel_def.get("join_type", "left_outer")
            if join_type not in {"left_outer", "inner"}:
                raise ValueError(f"Unsupported Snowflake join_type {join_type!r}")
            metadata["snowflake"]["source_join_type"] = join_type
            metadata["join_type"] = join_type
            if "join_type" in rel_def:
                metadata["bsl_how"] = join_type.removesuffix("_outer")
            else:
                metadata["snowflake"]["implicit_join_type"] = True

            # In Snowflake, left_table is the "many" side, right_table is the "one" side
            # Add relationship to left_table pointing to right_table
            if left_table in graph.models:
                model = graph.models[left_table]
                relationship = Relationship(
                    name=right_table,
                    type=rel_type,
                    foreign_key=right_column if rel_type in {"one_to_many", "one_to_one"} else left_column,
                    primary_key=left_column if rel_type in {"one_to_many", "one_to_one"} else right_column,
                    metadata=metadata,
                )
                model.relationships.append(relationship)
                # Rebuild adjacency after adding relationship
                graph.build_adjacency()
            else:
                # The left table is not in this graph (a multi-file CLI load parses
                # each file separately). Hold the definition so the directory loader
                # can apply it once that table is loaded.
                if not hasattr(graph, "_pending_relationships"):
                    graph._pending_relationships = []
                graph._pending_relationships.append(rel_def)

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
            "name": (graph.metadata.get("snowflake") or {}).get("name", output_path.stem),
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
                if not rel.active:
                    record_export_note(
                        "inactive_relationship",
                        f"Inactive Snowflake relationship {model.name}.{rel.name} omitted",
                        source="snowflake",
                    )
                    continue
                rel_def = self._export_relationship(model, rel, resolved_models.get(rel.related_model))
                semantic_model["relationships"].append(rel_def)

        # Remove empty relationships list
        if not semantic_model["relationships"]:
            del semantic_model["relationships"]

        description = (graph.metadata.get("snowflake") or {}).get("description")
        if description is not None:
            semantic_model["description"] = description

        # Export graph-level (top-level) metrics. These have no owning table and
        # were parsed from the semantic model's top-level `metrics:` section, so
        # they must be serialized back there to survive a parse/export round-trip.
        #
        # ``graph.metrics`` also contains model-owned metrics that ``add_model()``
        # auto-registers at graph level (``time_comparison``/``conversion``). Those
        # are already serialized inside their table and have no valid Snowflake
        # top-level representation, so skip any metric that is owned by a model.
        # Match by object identity, not name, so a distinct top-level metric that
        # merely shares a name with a model-local metric still round-trips.
        owned_metric_ids = {id(metric) for model in resolved_models.values() for metric in model.metrics}
        top_level_metrics = []
        for name, metric in graph.metrics.items():
            if id(metric) in owned_metric_ids:
                continue
            metric_def = self._export_metric(metric, top_level=True)
            # Skip metric types Snowflake cannot represent (no `expr`) rather than
            # emitting an invalid stub that would fail to re-parse.
            if "expr" not in metric_def:
                record_export_note(
                    "unsupported_metric",
                    f"Snowflake cannot represent graph metric {metric.name!r} of type {metric.type!r}",
                    severity="unsupported",
                    source="snowflake",
                )
                continue
            top_level_metrics.append(metric_def)
        if top_level_metrics:
            semantic_model["metrics"] = top_level_metrics

        # Export top-level Cortex Analyst sections if present on the graph. These
        # live as dynamic attributes when parsed directly, but only survive a
        # native (SidemanticAdapter) round-trip via ``graph.metadata["snowflake"]``,
        # so fall back to that when the attributes are absent.
        snowflake_meta = graph.metadata.get("snowflake") or {}
        verified_queries = getattr(graph, "verified_queries", None) or snowflake_meta.get("verified_queries")
        if verified_queries:
            semantic_model["verified_queries"] = verified_queries
        custom_instructions = getattr(graph, "custom_instructions", None) or snowflake_meta.get("custom_instructions")
        if custom_instructions:
            semantic_model["custom_instructions"] = custom_instructions
        module_custom_instructions = getattr(graph, "module_custom_instructions", None) or snowflake_meta.get(
            "module_custom_instructions"
        )
        if module_custom_instructions:
            semantic_model["module_custom_instructions"] = module_custom_instructions

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
        if model.sql:
            table["base_table"] = {"definition": model.sql}
        elif model.table:
            reference = sqlglot.parse_one(model.table, read="snowflake", into=exp.Table)
            if not 1 <= len(reference.parts) <= 3:
                raise ValueError(f"Snowflake table source {model.table!r} must contain at most three identifiers")
            parts = [part.sql(dialect="snowflake") for part in reference.parts]
            table["base_table"] = dict(zip(("database", "schema", "table")[-len(parts) :], parts))

        original = (model.metadata or {}).get("snowflake", {})
        if not model.sql and model.table == original.get("source_table") and original.get("base_table"):
            table["base_table"] = dict(original["base_table"])

        # Export primary key
        if model.primary_key:
            table["primary_key"] = {"columns": model.primary_key_columns}
        if model.unique_keys:
            table["unique_keys"] = [{"columns": key} for key in model.unique_keys]
        if original.get("synonyms") is not None:
            table["synonyms"] = original["synonyms"]

        # Separate dimensions by type
        dimensions = []
        time_dimensions = []

        for dim in model.dimensions:
            if (
                dim.type == "time"
                and (dim.metadata or {}).get("snowflake", {}).get("dimension_section") != "dimensions"
            ):
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

        # Snowflake table `metrics` carry metric-only keys (e.g. using_relationships,
        # non_additive_dimensions). A simple aggregation that carries one of these
        # was authored as a metric, so re-export it as a metric (not a fact) to keep
        # the original representation across a round-trip.
        metric_only_keys = ("using_relationships", "non_additive_dimensions")
        for metric in model.metrics:
            snowflake_meta = (metric.metadata or {}).get("snowflake", {})
            has_metric_only_key = any(key in snowflake_meta for key in metric_only_keys)
            if metric.agg and not metric.type and not has_metric_only_key:
                # Simple aggregation -> fact
                fact = self._export_fact(metric)
                facts.append(fact)
            else:
                # Complex metric or derived -> metric. Snowflake has no
                # representation for metric types like time_comparison or
                # conversion, so _export_metric() cannot build an `expr` for
                # them; skip those rather than emitting an invalid stub that
                # would fail to re-parse.
                metric_def = self._export_metric(metric)
                if "expr" not in metric_def:
                    record_export_note(
                        "unsupported_metric",
                        f"Snowflake cannot represent metric {model.name}.{metric.name} of type {metric.type!r}",
                        severity="unsupported",
                        source="snowflake",
                    )
                    continue
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
        dim_def["data_type"] = self._export_data_type(dim, type_mapping.get(dim.type, "TEXT"))

        self._export_dimension_extras(dim, dim_def)

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

        dim_def["data_type"] = self._export_data_type(dim, "TIMESTAMP")

        self._export_dimension_extras(dim, dim_def)

        return dim_def

    @staticmethod
    def _export_dimension_extras(dim: Dimension, dim_def: dict) -> None:
        """Attach Cortex Analyst enrichment keys to an exported dimension."""
        if dim.synonyms:
            dim_def["synonyms"] = dim.synonyms
        if dim.sample_values:
            dim_def["sample_values"] = dim.sample_values
        snowflake_meta = dict((dim.metadata or {}).get("snowflake", {}))
        snowflake_meta.pop("dimension_section", None)
        # Snowflake documents the nested ``cortex_search_service`` object as
        # replacing the deprecated flat ``cortex_search_service_name`` string, so
        # emit only one shape. Prefer the nested object (it can carry extra keys
        # like ``literal_column``/``database``/``schema``) and keep its ``service``
        # in sync with the first-class ``cortex_search_service_name`` so a user
        # edit to the native name wins instead of leaving a stale nested value.
        nested = snowflake_meta.pop("cortex_search_service", None)
        if dim.cortex_search_service_name:
            if isinstance(nested, dict):
                nested = {**nested, "service": dim.cortex_search_service_name}
            else:
                nested = {"service": dim.cortex_search_service_name}
            dim_def["cortex_search_service"] = nested
        elif nested is not None:
            dim_def["cortex_search_service"] = nested
        for key, value in snowflake_meta.items():
            dim_def.setdefault(key, value)
        if not dim.public:
            # The Sidemantic ``public`` flag is authoritative: hide private fields.
            dim_def["access_modifier"] = "private_access"
        elif dim_def.get("access_modifier") == "private_access":
            # Drop a stale ``private_access`` carried over in metadata so flipping
            # ``public`` back to true actually unhides the field on export.
            del dim_def["access_modifier"]

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

        fact["data_type"] = (metric.metadata or {}).get("snowflake", {}).get("data_type", "NUMBER")

        if metric.synonyms:
            fact["synonyms"] = metric.synonyms
        snowflake_meta = (metric.metadata or {}).get("snowflake", {})
        for key, value in snowflake_meta.items():
            fact.setdefault(key, value)
        if not metric.public:
            # The Sidemantic ``public`` flag is authoritative: hide private fields.
            fact["access_modifier"] = "private_access"
        elif fact.get("access_modifier") == "private_access":
            # Drop a stale ``private_access`` carried over in metadata so flipping
            # ``public`` back to true actually unhides the field on export.
            del fact["access_modifier"]

        return fact

    @staticmethod
    def _strip_model_placeholder(sql: str | None) -> str | None:
        """Drop the ``{model}.`` placeholder so Snowflake sees bare column refs.

        Table-scoped metric expressions are parsed with the ``{model}`` placeholder
        for table-local columns; Snowflake cannot resolve that token, so it must be
        removed when re-exporting these metrics to Snowflake.
        """
        if sql is None:
            return None
        return sql.replace("{model}.", "").replace("{model}", "")

    def _export_metric(self, metric: Metric, *, top_level: bool = False) -> dict:
        """Export metric to Snowflake metric format.

        Args:
            metric: Metric to export
            top_level: When True the metric is a graph-level (view) metric whose
                references already use ``model.field`` qualifiers that Snowflake
                needs to resolve cross-table references, so they are preserved.
                When False the metric is table-scoped and ``{model}`` placeholders
                are stripped to bare column references.

        Returns:
            Metric definition dictionary
        """
        metric_def = {"name": metric.name}

        if metric.description:
            metric_def["description"] = metric.description

        # Build expression based on metric type
        if metric.type == "ratio" and metric.numerator and metric.denominator:
            if top_level:
                # Graph-level metric: keep qualified references so Snowflake can
                # resolve cross-table members (e.g. ``orders.revenue``).
                num = metric.numerator
                denom = metric.denominator
            else:
                # Table-scoped metric: Snowflake expressions use bare column names.
                num = metric.numerator.split(".")[-1] if "." in metric.numerator else metric.numerator
                denom = metric.denominator.split(".")[-1] if "." in metric.denominator else metric.denominator
            metric_def["expr"] = f"{num} / NULLIF({denom}, 0)"
        elif metric.type == "derived" and metric.sql:
            metric_def["expr"] = metric.sql if top_level else self._strip_model_placeholder(metric.sql)
        elif metric.agg and metric.sql:
            # Simple aggregation - wrap in aggregate function
            agg_func = metric.agg.upper()
            sql = metric.sql if top_level else self._strip_model_placeholder(metric.sql)
            if agg_func == "COUNT_DISTINCT":
                metric_def["expr"] = f"COUNT(DISTINCT {sql})"
            else:
                metric_def["expr"] = f"{agg_func}({sql})"
        elif metric.sql:
            metric_def["expr"] = metric.sql if top_level else self._strip_model_placeholder(metric.sql)

        if metric.synonyms:
            metric_def["synonyms"] = metric.synonyms
        snowflake_meta = (metric.metadata or {}).get("snowflake", {})
        for key, value in snowflake_meta.items():
            metric_def.setdefault(key, value)
        if not metric.public:
            # The Sidemantic ``public`` flag is authoritative: hide private fields.
            metric_def["access_modifier"] = "private_access"
        elif metric_def.get("access_modifier") == "private_access":
            # Drop a stale ``private_access`` carried over in metadata so flipping
            # ``public`` back to true actually unhides the field on export.
            del metric_def["access_modifier"]

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

    def _export_relationship(self, model: Model, rel: Relationship, target: Model | None = None) -> dict:
        """Export relationship to Snowflake relationship format.

        Args:
            model: Model containing the relationship
            rel: Relationship to export

        Returns:
            Relationship definition dictionary
        """
        if rel.type not in {"many_to_one", "one_to_one"}:
            raise ValueError(
                f"Snowflake relationship {model.name}.{rel.name} has unsupported cardinality {rel.type!r}; export supports many_to_one and one_to_one"
            )
        if rel.sql or rel.through:
            raise ValueError(f"Snowflake relationship {model.name}.{rel.name} requires explicit column pairs")
        if rel.type in {"one_to_many", "one_to_one"}:
            left = rel.primary_key_columns or model.primary_key_columns
            right = rel.foreign_key_columns
        else:
            left = rel.foreign_key_columns
            right = rel.primary_key_columns or (target.primary_key_columns if target else [])
        if not left or not right or len(left) != len(right):
            raise ValueError(f"Snowflake relationship {model.name}.{rel.name} has missing or mismatched join keys")
        metadata = rel.metadata or {}
        join_type = metadata.get("join_type")
        if "bsl_how" in metadata:
            runtime_type = {
                "left": "left_outer",
                "inner": "inner",
                "right": "right_outer",
                "outer": "full_outer",
                "full": "full_outer",
            }.get(metadata["bsl_how"])
            if runtime_type is None:
                raise ValueError(f"Unsupported Snowflake join type {metadata['bsl_how']!r}")
            original_type = metadata.get("snowflake", {}).get("source_join_type")
            if join_type is None or join_type == runtime_type or join_type == original_type:
                join_type = runtime_type
            elif runtime_type != original_type:
                raise ValueError(f"Conflicting Snowflake join types on {model.name}.{rel.name}")
        if join_type is None:
            join_type = "left_outer"
        if join_type not in {"left_outer", "inner"}:
            raise ValueError(f"Unsupported Snowflake join type {join_type!r}")
        rel_def = {
            "left_table": model.name,
            "right_table": rel.related_model,
            "relationship_columns": [
                {"left_column": local, "right_column": remote} for local, remote in zip(left, right)
            ],
            "relationship_type": rel.type,
            "join_type": join_type,
        }

        if join_type == "left_outer" and (rel.metadata or {}).get("snowflake", {}).get("implicit_join_type"):
            del rel_def["join_type"]

        # Preserve the original Snowflake relationship name so metric
        # `using_relationships` references resolve after a round-trip.
        snowflake_name = (rel.metadata or {}).get("snowflake", {}).get("name")
        if snowflake_name:
            rel_def = {"name": snowflake_name, **rel_def}
        elif rel.name != rel.related_model:
            rel_def = {"name": rel.name, **rel_def}

        return rel_def
