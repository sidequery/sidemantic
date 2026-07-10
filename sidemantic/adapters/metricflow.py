"""MetricFlow adapter for importing dbt semantic layer models."""

from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class MetricFlowAdapter(BaseAdapter):
    """Adapter for importing/exporting dbt MetricFlow semantic models.

    Transforms MetricFlow definitions into Sidemantic format:
    - Semantic models → Models
    - Entities → Entities (direct mapping)
    - Dimensions → Dimensions
    - Measures → Measures
    - Metrics → Metrics (all 5 types)

    Supports both the legacy MetricFlow YAML spec (top-level ``semantic_models:`` /
    ``metrics:`` with ``type_params``) and the latest spec used by dbt Core 1.12 /
    the dbt Fusion engine (top-level ``models:`` with a nested ``semantic_model:``
    block, column-based ``entity:`` / ``dimension:`` under ``columns:``, measures
    folded into ``type: simple`` metrics with ``agg`` / ``expr``, and promoted
    top-level metric keys such as ``input_metrics`` / ``input_metric`` /
    ``base_metric`` / ``conversion_metric`` / ``numerator`` / ``denominator``).
    """

    def __init__(self):
        # Parsed top-level ``saved_queries`` keyed by name. Saved queries are a
        # MetricFlow concept without a direct Sidemantic equivalent, so they are
        # retained here (and on ``graph.metadata["saved_queries"]``) rather than
        # turned into models or metrics.
        self.saved_queries: dict[str, dict] = {}
        # Parsed conversion metrics keyed by name. MetricFlow conversion metrics
        # reference base/conversion *measures*, which have no faithful mapping to
        # Sidemantic's event-filter conversion funnel. Rather than register a
        # queryable ``type: conversion`` metric that would generate wrong SQL
        # (filtering an ``event_type`` dimension by a measure name), the spec is
        # retained here (and on ``graph.metadata["metricflow_conversion_metrics"]``)
        # for round-tripping without exposing a broken queryable metric.
        self.conversion_metrics: dict[str, dict] = {}

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse MetricFlow YAML files into semantic graph.

        Args:
            source: Path to YAML file or directory containing semantic models

        Returns:
            Semantic graph with imported models and metrics
        """
        graph = SemanticGraph()
        source_path = Path(source)

        # Reset per-parse state so reusing the adapter does not leak saved
        # queries or conversion metrics from a previously parsed source into this
        # graph's metadata.
        self.saved_queries = {}
        self.conversion_metrics = {}

        if source_path.is_dir():
            # Parse all YAML files in directory
            for yaml_file in source_path.rglob("*.yml"):
                self._parse_file(yaml_file, graph)
            for yaml_file in source_path.rglob("*.yaml"):
                self._parse_file(yaml_file, graph)
        else:
            # Parse single file
            self._parse_file(source_path, graph)

        # Resolve entity names to actual model names
        # MetricFlow uses singular entity names (e.g., "customer") while models may be plural (e.g., "customers")
        self._resolve_relationship_names(graph)

        # Rebuild adjacency graph after resolving relationship names
        graph.build_adjacency()

        # Expose parsed saved queries on the graph for downstream consumers.
        if self.saved_queries:
            graph.metadata["saved_queries"] = self.saved_queries

        # Expose parsed conversion metrics (retained as non-queryable metadata).
        if self.conversion_metrics:
            graph.metadata["metricflow_conversion_metrics"] = self.conversion_metrics

        return graph

    def _parse_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse a single MetricFlow YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models/metrics to
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # Latest spec (dbt Core 1.12 / Fusion): semantic models embedded under a
        # top-level ``models:`` block, each with a nested ``semantic_model:`` key.
        # Metrics may be declared inline on the model and/or at the top level.
        for model_def in data.get("models") or []:
            if not isinstance(model_def, dict) or "semantic_model" not in model_def:
                continue
            model = self._parse_model_spec(model_def)
            if not model:
                # ``semantic_model.enabled: false`` (or an otherwise unparseable
                # model) yields no model. Its inline metrics are model-local and
                # fold a measure on that model, so without the owning model they
                # have no SQL context and a CLI query for one would raise
                # ``No models found for query``. Skip them so the graph never
                # exposes a metric that cannot be queried.
                continue
            graph.add_model(model)
            # Inline metrics on a latest-spec model. A ``type: simple`` metric
            # folds a model measure (``agg`` + ``expr``); its expression refers
            # to columns on the owning model. Qualify the SQL with the model name
            # so a query selecting only that metric can infer its model. Without
            # this the metric carries unqualified SQL (e.g. ``amount``) or no SQL
            # at all (a bare ``count``), and the planner raises
            # ``No models found for query``.
            for metric_def in model_def.get("metrics") or []:
                metric = self._parse_metric(metric_def)
                if not metric:
                    continue
                if metric_def.get("type", "simple") == "simple" and metric.agg is not None:
                    primary_key_ref = f"{model.name}.{model.primary_key}"
                    if metric.sql is not None:
                        qualified = self._qualify_measure_sql(metric.sql, model.name)
                        # A constant count measure (``agg: count`` with ``expr: 1``
                        # or ``expr: '*'``) has no column to qualify, so the metric
                        # would carry no model reference and the planner would raise
                        # ``No models found for query``. Anchor such counts to the
                        # model via its primary key, the same as a bare ``count``.
                        # COUNT over a non-null primary key is equivalent to COUNT(*).
                        if metric.agg == "count" and not self._has_qualified_column(qualified, model.name):
                            metric.sql = primary_key_ref
                        else:
                            metric.sql = qualified
                    elif metric.agg == "count":
                        # Bare ``count`` with no ``expr``: anchor it to the model
                        # via its primary key so the planner can resolve the model.
                        # COUNT over a non-null primary key is equivalent to COUNT(*).
                        metric.sql = primary_key_ref
                    else:
                        # Any other expr-less aggregation (sum/avg/min/max/...): in
                        # MetricFlow an expr-less measure aggregates the column named
                        # after the measure, so default the SQL to the metric's own
                        # column, qualified with the model. Anchoring to the primary
                        # key here would silently aggregate the wrong column (e.g.
                        # ``SUM(orders.order_id)`` instead of ``SUM(orders.amount)``).
                        metric.sql = f"{model.name}.{metric.name}"
                self._add_metric(graph, metric)

        # Legacy spec: top-level ``semantic_models:``.
        for model_def in data.get("semantic_models") or []:
            model = self._parse_semantic_model(model_def)
            if model:
                graph.add_model(model)

        # Parse top-level metrics (shared by both specs)
        for metric_def in data.get("metrics") or []:
            metric = self._parse_metric(metric_def)
            if metric:
                self._add_metric(graph, metric)

        # Parse saved queries (top-level, both specs). These have no direct
        # Sidemantic equivalent, so retain them for downstream consumers.
        self._parse_saved_queries(data.get("saved_queries"))

    @staticmethod
    def _qualify_measure_sql(sql: str, model_name: str) -> str:
        """Qualify unqualified column references in a folded measure's SQL.

        A latest-spec ``type: simple`` metric folds a model measure, so its
        ``expr`` refers to columns on the owning model. The resulting graph-level
        metric needs at least one model-qualified column (e.g. ``orders.amount``)
        so the query planner can infer the model when the metric is selected on
        its own. Already-qualified references are left untouched.
        """
        import sqlglot
        from sqlglot import exp

        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return sql

        for column in parsed.find_all(exp.Column):
            if not column.table:
                column.set("table", exp.to_identifier(model_name))
        return parsed.sql()

    @staticmethod
    def _has_qualified_column(sql: str, model_name: str) -> bool:
        """Return True if ``sql`` references at least one column on ``model_name``.

        Used to detect folded measures whose ``expr`` is a bare constant (e.g.
        ``count`` with ``expr: 1`` or ``expr: '*'``). Such expressions contain no
        column for ``_qualify_measure_sql`` to anchor, so the metric would carry
        no model reference and could not be queried on its own.
        """
        import sqlglot
        from sqlglot import exp

        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return False

        for column in parsed.find_all(exp.Column):
            if column.table == model_name:
                return True
        return False

    @staticmethod
    def _add_metric(graph: SemanticGraph, metric: Metric) -> None:
        """Add a metric to the graph, ignoring duplicate names across files."""
        if metric.name in graph.metrics:
            return
        graph.add_metric(metric)

    def _parse_saved_queries(self, saved_queries) -> None:
        """Parse top-level ``saved_queries`` into ``self.saved_queries``.

        Saved queries can be a list (canonical MetricFlow form) or a mapping
        keyed by name (latest spec form). Each entry retains ``query_params``
        (metrics/group_by/where/order_by/limit) and ``exports``.

        Args:
            saved_queries: Raw ``saved_queries`` value from the YAML file.
        """
        if not saved_queries:
            return

        entries = []
        if isinstance(saved_queries, dict):
            for key, value in saved_queries.items():
                if not isinstance(value, dict):
                    continue
                entry = dict(value)
                entry.setdefault("name", key)
                entries.append(entry)
        else:
            entries = [sq for sq in saved_queries if isinstance(sq, dict)]

        for sq in entries:
            name = sq.get("name")
            if not name:
                continue
            query_params = sq.get("query_params") or {}
            self.saved_queries[name] = {
                "name": name,
                "description": sq.get("description"),
                "label": sq.get("label"),
                "metrics": list(query_params.get("metrics") or []),
                "group_by": list(query_params.get("group_by") or []),
                "where": query_params.get("where"),
                "order_by": query_params.get("order_by"),
                "limit": query_params.get("limit"),
                "exports": sq.get("exports") or [],
            }

    def _parse_semantic_model(self, model_def: dict) -> Model | None:
        """Parse MetricFlow semantic model into Model.

        Args:
            model_def: Semantic model definition dictionary

        Returns:
            Model instance or None
        """
        name = model_def.get("name")
        if not name:
            return None

        # Get table from model ref or config
        model_ref = model_def.get("model", "")
        table = None

        # Extract table from config.meta.hex if present
        config = model_def.get("config", {})
        meta = config.get("meta", {})
        hex_config = meta.get("hex", {})
        table = hex_config.get("table")

        # If no table in config, use model name as fallback
        if not table:
            # Try to extract from ref()
            if "ref(" in model_ref:
                ref_model = model_ref.replace("ref('", "").replace("')", "").replace('ref("', "").replace('")', "")
                table = ref_model

        # Read sql field (used for filtered/derived models)
        model_sql = model_def.get("sql")

        # Parse entities to extract primary key and relationships
        primary_key = "id"  # default
        relationships = []
        # Map entity name -> backing SQL column, so semi-additive window_groupings that name an
        # entity (e.g. `user`, backed by `user_id`) resolve to the real column the generator can
        # partition by, rather than a non-existent `user` column.
        entity_column_by_name: dict[str, str] = {}

        for entity_def in model_def.get("entities") or []:
            entity_type = entity_def.get("type", "primary")
            entity_name = entity_def.get("name")
            entity_expr = entity_def.get("expr", entity_name)
            if entity_name:
                entity_column_by_name[entity_name] = entity_expr

            if entity_type == "primary":
                # Use this as the primary key
                primary_key = entity_expr
            elif entity_type == "foreign":
                # Create a many_to_one relationship
                relationships.append(Relationship(name=entity_name, type="many_to_one", foreign_key=entity_expr))

        # Parse dimensions
        dimensions = []
        for dim_def in model_def.get("dimensions") or []:
            dim = self._parse_dimension(dim_def)
            if dim:
                dimensions.append(dim)

        # Parse measures
        measures = []
        dimension_names = {dim.name for dim in dimensions}
        for measure_def in model_def.get("measures") or []:
            measure = self._parse_measure(measure_def)
            if measure:
                # Resolve semi-additive window_groupings: an entity name maps to its backing
                # column; a dimension name is kept as-is. This avoids partitioning by a name
                # that has no projectable column on the model.
                if measure.non_additive_window_groupings:
                    measure.non_additive_window_groupings = [
                        grouping if grouping in dimension_names else entity_column_by_name.get(grouping, grouping)
                        for grouping in measure.non_additive_window_groupings
                    ]
                measures.append(measure)

        # Parse segments from meta
        from sidemantic.core.segment import Segment

        segments = []
        meta = model_def.get("meta", {})
        for segment_def in meta.get("segments") or []:
            segment_name = segment_def.get("name")
            segment_sql = segment_def.get("sql")
            if segment_name and segment_sql:
                segments.append(
                    Segment(
                        name=segment_name,
                        sql=segment_sql,
                        description=segment_def.get("description"),
                    )
                )

        # Parse inheritance
        extends = meta.get("extends")

        # Parse default time dimension (MetricFlow uses defaults.agg_time_dimension)
        defaults = model_def.get("defaults", {})
        default_time_dimension = defaults.get("agg_time_dimension")
        default_grain = meta.get("default_grain")

        return Model(
            name=name,
            table=table,
            sql=model_sql,
            description=model_def.get("description"),
            primary_key=primary_key,
            relationships=relationships,
            dimensions=dimensions,
            metrics=measures,
            segments=segments,
            extends=extends,
            default_time_dimension=default_time_dimension,
            default_grain=default_grain,
        )

    def _parse_model_spec(self, model_def: dict) -> Model | None:
        """Parse a latest-spec model entry (``models:`` with nested ``semantic_model:``).

        In the latest spec entities and dimensions are declared column-by-column
        under ``columns:`` (each column carries an ``entity:`` and/or
        ``dimension:`` block), measures are folded into ``type: simple`` metrics,
        and the aggregation time dimension is a top-level ``agg_time_dimension``
        key. This translates that shape into the same Model the legacy parser
        produces.

        Args:
            model_def: A single entry from the top-level ``models:`` list.

        Returns:
            Model instance or None
        """
        semantic_model = model_def.get("semantic_model")
        if not isinstance(semantic_model, dict):
            return None
        # ``enabled: false`` disables semantic model generation for the model.
        if semantic_model.get("enabled") is False:
            return None

        # The semantic model name defaults to the dbt model name.
        name = semantic_model.get("name") or model_def.get("name")
        if not name:
            return None

        # The underlying table is the dbt model itself.
        table = model_def.get("name")

        primary_key = "id"
        relationships = []
        dimensions = []

        for column_def in model_def.get("columns") or []:
            if not isinstance(column_def, dict):
                continue
            column_name = column_def.get("name")

            entity_def = column_def.get("entity")
            if entity_def is not None:
                # ``entity: primary`` shorthand or a full mapping.
                if isinstance(entity_def, str):
                    entity_def = {"type": entity_def}
                entity_type = entity_def.get("type", "primary")
                entity_name = entity_def.get("name") or column_name
                # The column name is the SQL expression backing the entity.
                entity_expr = entity_def.get("expr") or column_name
                if entity_type == "primary":
                    primary_key = entity_expr
                elif entity_type == "foreign":
                    relationships.append(Relationship(name=entity_name, type="many_to_one", foreign_key=entity_expr))

            dimension_def = column_def.get("dimension")
            if dimension_def is not None:
                if isinstance(dimension_def, str):
                    dimension_def = {"type": dimension_def}
                # Build a legacy-shaped dimension dict so we can reuse _parse_dimension.
                # Granularity lives at the column level in the latest spec.
                legacy_dim = {
                    "name": dimension_def.get("name") or column_name,
                    "type": dimension_def.get("type", "categorical"),
                    "expr": dimension_def.get("expr", column_name),
                    "description": dimension_def.get("description") or column_def.get("description"),
                    "label": dimension_def.get("label"),
                    "meta": dimension_def.get("meta", {}),
                }
                granularity = column_def.get("granularity") or dimension_def.get("granularity")
                if granularity:
                    legacy_dim["type_params"] = {"time_granularity": granularity}
                dim = self._parse_dimension(legacy_dim)
                if dim:
                    dimensions.append(dim)

        # Measures fold into inline ``type: simple`` metrics in the latest spec, so
        # the model itself has no separate measures list.
        measures = []

        agg_time_dimension = model_def.get("agg_time_dimension") or semantic_model.get("agg_time_dimension")
        defaults = model_def.get("defaults") or semantic_model.get("defaults") or {}
        default_time_dimension = agg_time_dimension or defaults.get("agg_time_dimension")

        return Model(
            name=name,
            table=table,
            description=semantic_model.get("description") or model_def.get("description"),
            primary_key=primary_key,
            relationships=relationships,
            dimensions=dimensions,
            metrics=measures,
            default_time_dimension=default_time_dimension,
        )

    def _parse_dimension(self, dim_def: dict) -> Dimension | None:
        """Parse MetricFlow dimension into Sidemantic dimension.

        Args:
            dim_def: Dimension definition dictionary

        Returns:
            Dimension instance or None
        """
        name = dim_def.get("name")
        if not name:
            return None

        dim_type = dim_def.get("type", "categorical")

        # MetricFlow has categorical and time types
        type_mapping = {
            "categorical": "categorical",
            "time": "time",
        }

        sidemantic_type = type_mapping.get(dim_type, "categorical")

        # For time dimensions, extract granularity from type_params
        granularity = None
        if dim_type == "time":
            type_params = dim_def.get("type_params", {})
            granularity = type_params.get("time_granularity", "day")

        # Parse metadata fields from meta
        meta = dim_def.get("meta", {})
        format_str = meta.get("format")
        value_format_name = meta.get("value_format_name")
        parent = meta.get("parent")

        # Convert expr to string if it's not None (can be various types)
        expr = dim_def.get("expr")
        sql_expr = str(expr) if expr is not None else None

        return Dimension(
            name=name,
            type=sidemantic_type,
            sql=sql_expr,
            granularity=granularity,
            description=dim_def.get("description"),
            label=dim_def.get("label"),
            format=format_str,
            value_format_name=value_format_name,
            parent=parent,
        )

    @staticmethod
    def _map_agg(agg_type: str | None) -> str | None:
        """Map a MetricFlow aggregation name to a Sidemantic aggregation name.

        Returns ``None`` for an aggregation Sidemantic cannot represent (e.g.
        ``percentile``). Callers must skip such measures/metrics rather than
        coerce them: defaulting an unrepresentable aggregation to ``sum`` would
        silently emit wrong SQL (``SUM(amount)`` for ``agg: percentile``). A
        missing ``agg`` still defaults to ``sum`` as before.
        """
        if agg_type is None:
            return "sum"
        type_mapping = {
            "sum": "sum",
            "count": "count",
            "count_distinct": "count_distinct",
            "average": "avg",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "median": "median",
            "sum_boolean": "sum",
        }
        # MetricFlow aggregation names are case-insensitive (e.g. ``SUM``).
        return type_mapping.get(agg_type.lower())

    def _parse_measure(self, measure_def: dict) -> Metric | None:
        """Parse MetricFlow measure into Sidemantic measure.

        Args:
            measure_def: Metric definition dictionary

        Returns:
            Measure instance or None
        """
        name = measure_def.get("name")
        if not name:
            return None

        sidemantic_agg = self._map_agg(measure_def.get("agg", "sum"))
        if sidemantic_agg is None:
            # Aggregation Sidemantic cannot represent (e.g. ``percentile``). Skip
            # rather than coerce to ``sum``, which would silently return a wrong
            # value for the measure.
            return None

        # Parse metadata and filters from meta
        meta = measure_def.get("meta", {})
        filters = meta.get("filters")
        format_str = meta.get("format")
        value_format_name = meta.get("value_format_name")
        drill_fields = meta.get("drill_fields")

        # Parse non_additive_dimension. MetricFlow allows a window_choice of "min"/"max"
        # (default "max" = keep the last snapshot) and window_groupings (the dimensions the
        # snapshot is taken per, e.g. balance-per-user).
        non_additive = measure_def.get("non_additive_dimension")
        non_additive_dimension = None
        non_additive_window = "max"
        non_additive_window_groupings = None
        if non_additive and isinstance(non_additive, dict):
            non_additive_dimension = non_additive.get("name")
            window_choice = non_additive.get("window_choice")
            if window_choice in ("min", "max"):
                non_additive_window = window_choice
            groupings = non_additive.get("window_groupings")
            if groupings:
                non_additive_window_groupings = list(groupings)

        # Convert expr to string if it's not None (can be int, like 1 for count)
        expr = measure_def.get("expr")
        sql_expr = str(expr) if expr is not None else None

        return Metric(
            name=name,
            agg=sidemantic_agg,
            sql=sql_expr,
            description=measure_def.get("description"),
            label=measure_def.get("label"),
            filters=filters,
            format=format_str,
            value_format_name=value_format_name,
            drill_fields=drill_fields,
            non_additive_dimension=non_additive_dimension,
            non_additive_window=non_additive_window,
            non_additive_window_groupings=non_additive_window_groupings,
        )

    @staticmethod
    def _ref_name(value):
        """Resolve a measure/metric reference that may be a bare name or a dict.

        MetricFlow allows measure/metric inputs to be either a string name or a
        mapping like ``{name: bookers, fill_nulls_with: 0, join_to_timespine: true}``.
        """
        if isinstance(value, dict):
            return value.get("name")
        return value

    def _parse_metric(self, metric_def: dict) -> Metric | None:
        """Parse MetricFlow metric into Sidemantic measure.

        Supports both the legacy spec (parameters nested under ``type_params``)
        and the latest spec (parameters promoted to top-level keys, e.g.
        ``input_metrics`` / ``input_metric`` / ``numerator`` / ``base_metric``).

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Measure instance or None
        """
        name = metric_def.get("name")
        if not name:
            return None

        metric_type = metric_def.get("type", "simple")

        # Map MetricFlow metric types to Sidemantic metric types.
        # Note: "simple" maps to None (untyped) since we removed the simple type.
        type_mapping = {
            "simple": None,  # Untyped metric with sql expression
            "ratio": "ratio",
            "derived": "derived",
            "cumulative": "cumulative",
            "conversion": "conversion",
        }

        if metric_type not in type_mapping:
            return None  # Skip genuinely unsupported metric types
        sidemantic_type = type_mapping[metric_type]

        # Type-specific parameters. In the latest spec these are promoted to the
        # top level; in the legacy spec they live under ``type_params``.
        type_params = metric_def.get("type_params") or {}

        # Conversion metrics have a dedicated parsing path.
        if metric_type == "conversion":
            return self._parse_conversion_metric(name, metric_def, type_params)

        expr = None
        agg = None
        numerator = None
        denominator = None
        window = None
        grain_to_date = None
        metadata: dict = {}

        if metric_type == "simple":
            # ``measure`` (legacy) or ``agg``/``expr`` (latest). Latest-spec simple
            # metrics defined inline on a model fold the measure into the metric and
            # carry ``agg`` plus an optional ``expr`` at the top level.
            measure = type_params.get("measure")
            if measure is None and "measure" in metric_def:
                measure = metric_def.get("measure")
            top_agg = metric_def.get("agg")
            if top_agg is not None:
                # Latest-spec folded measure: keep the aggregation and column expr.
                agg = self._map_agg(top_agg)
                if agg is None:
                    # Aggregation Sidemantic cannot represent (e.g. ``percentile``).
                    # Skip the metric rather than coerce to ``sum``, which would
                    # silently return a wrong value.
                    return None
                raw_expr = metric_def.get("expr")
                expr = str(raw_expr) if raw_expr is not None else None
            elif measure is not None:
                expr = self._ref_name(measure)
            elif metric_def.get("expr") is not None:
                expr = str(metric_def.get("expr"))
            else:
                # Inline latest-spec simple metric: the measure shares the metric name.
                expr = name

        elif metric_type == "ratio":
            numerator = self._ref_name(type_params.get("numerator", metric_def.get("numerator")))
            denominator = self._ref_name(type_params.get("denominator", metric_def.get("denominator")))

        elif metric_type == "derived":
            expr = type_params.get("expr", metric_def.get("expr"))
            # Per-input modifiers (offset_window / offset_to_grain / alias) live on
            # each entry of the metrics list. Sidemantic auto-detects derived
            # dependencies from the expression, so retain the raw inputs in
            # metadata for round-tripping and offset support.
            input_metrics = type_params.get("metrics")
            if input_metrics is None:
                input_metrics = metric_def.get("input_metrics")
            input_summary = self._summarize_input_metrics(input_metrics)
            if input_summary:
                metadata["input_metrics"] = input_summary
            # Rewrite non-offset aliases back to their real input metric so the
            # expression references metrics that actually exist in the graph.
            # MetricFlow lets a derived metric reference an input by ``alias``
            # (e.g. ``current_total`` for ``order_total``); without rewriting,
            # ``get_dependencies`` would scan the expression and resolve only the
            # aliases, so ``_find_required_models`` could not infer a model and a
            # CLI query like ``--metrics order_total_growth`` would raise
            # ``No models found for query``. An alias carrying ``offset_window`` /
            # ``offset_to_grain`` denotes a time-shifted value distinct from the
            # base metric, which Sidemantic cannot yet express, so those aliases
            # are left intact (the metric stays as round-trip metadata).
            if expr and input_summary:
                expr = self._rewrite_input_aliases(expr, input_summary)

        elif metric_type == "cumulative":
            # Base measure: ``measure`` (legacy) or ``input_metric`` (latest).
            measure = type_params.get("measure")
            if measure is None:
                measure = metric_def.get("input_metric")
            expr = self._ref_name(measure)

            # Window / grain_to_date / period_agg can sit directly under
            # type_params (legacy convenience), under cumulative_type_params
            # (canonical legacy), or be promoted to the top level (latest).
            cumulative_params = type_params.get("cumulative_type_params") or {}
            window = type_params.get("window") or cumulative_params.get("window") or metric_def.get("window")
            grain_to_date = (
                type_params.get("grain_to_date")
                or cumulative_params.get("grain_to_date")
                or metric_def.get("grain_to_date")
            )
            period_agg = (
                cumulative_params.get("period_agg") or type_params.get("period_agg") or metric_def.get("period_agg")
            )
            if period_agg:
                metadata["period_agg"] = period_agg

        # Parse filter (single string in MetricFlow)
        filter_expr = metric_def.get("filter")
        filters = [filter_expr] if filter_expr else None

        # Parse display metadata from meta
        meta = metric_def.get("meta", {})
        format_str = meta.get("format")
        value_format_name = meta.get("value_format_name")
        drill_fields = meta.get("drill_fields")
        extends = meta.get("extends")

        return Metric(
            name=name,
            type=sidemantic_type,
            agg=agg,
            description=metric_def.get("description"),
            label=metric_def.get("label"),
            sql=expr,
            numerator=numerator,
            denominator=denominator,
            window=window,
            grain_to_date=grain_to_date,
            filters=filters,
            format=format_str,
            value_format_name=value_format_name,
            drill_fields=drill_fields,
            extends=extends,
            metadata=metadata or None,
        )

    def _summarize_input_metrics(self, input_metrics) -> list[dict] | None:
        """Capture per-input derived modifiers (offset_window / offset_to_grain / alias / filter).

        Args:
            input_metrics: The ``metrics`` (legacy) / ``input_metrics`` (latest) list.

        Returns:
            A normalized list of input descriptors, or None when there is nothing
            worth retaining.
        """
        if not input_metrics:
            return None
        summary = []
        for entry in input_metrics:
            if isinstance(entry, dict):
                item = {"name": entry.get("name")}
                for key in ("alias", "offset_window", "offset_to_grain", "filter"):
                    if entry.get(key) is not None:
                        item[key] = entry.get(key)
                summary.append(item)
            else:
                summary.append({"name": entry})
        return summary or None

    @staticmethod
    def _rewrite_input_aliases(expr: str, input_summary: list[dict]) -> str:
        """Replace plain derived input aliases with their real metric names.

        Each entry in ``input_summary`` may carry an ``alias`` referenced by the
        derived expression. An alias is only rewritten when the input has no
        modifier that makes its value differ from the underlying metric:

        - ``offset_window`` / ``offset_to_grain`` denote a time-shifted value, and
        - ``filter`` denotes a filtered subset value,

        so aliases carrying either are left intact (the metric stays as round-trip
        metadata). Identifiers are matched on word boundaries so an alias is not
        rewritten inside a longer identifier.
        """
        import re

        rewritten = expr
        for item in input_summary:
            alias = item.get("alias")
            real_name = item.get("name")
            if not alias or not real_name or alias == real_name:
                continue
            if (
                item.get("offset_window") is not None
                or item.get("offset_to_grain") is not None
                or item.get("filter") is not None
            ):
                continue
            rewritten = re.sub(rf"\b{re.escape(alias)}\b", real_name, rewritten)
        return rewritten

    def _parse_conversion_metric(self, name: str, metric_def: dict, type_params: dict) -> Metric | None:
        """Record a MetricFlow conversion metric as non-queryable metadata.

        Handles both the legacy shape (``type_params.conversion_type_params`` with
        ``base_measure`` / ``conversion_measure``) and the latest shape (promoted
        top-level ``base_metric`` / ``conversion_metric`` / ``entity`` keys).

        MetricFlow conversion metrics reference base/conversion *measures*, but
        Sidemantic's ``type: conversion`` funnel filters an ``event_type``
        dimension by a string literal (e.g. ``WHERE event_type = 'visit'``). The
        measure name is not such a filter, so registering a queryable conversion
        metric would either fail to find an ``event_type`` dimension or silently
        compute wrong conversions (``WHERE event_type = 'order_count'``). Until a
        faithful measure-predicate mapping exists, the spec is captured in
        ``self.conversion_metrics`` (surfaced on
        ``graph.metadata["metricflow_conversion_metrics"]``) and no metric is
        registered, so the parsed graph never exposes a broken conversion metric.

        Args:
            name: Metric name.
            metric_def: Raw metric definition.
            type_params: The metric's ``type_params`` (may be empty in latest spec).

        Returns:
            ``None`` always; the conversion spec is retained as metadata instead.
        """
        conv = type_params.get("conversion_type_params") or {}

        # base/conversion event measures: legacy uses base_measure/conversion_measure,
        # latest uses base_metric/conversion_metric promoted to the top level.
        base = self._ref_name(conv.get("base_measure"))
        if base is None:
            base = self._ref_name(metric_def.get("base_metric"))
        conversion = self._ref_name(conv.get("conversion_measure"))
        if conversion is None:
            conversion = self._ref_name(metric_def.get("conversion_metric"))

        entity = conv.get("entity") or metric_def.get("entity")
        window = conv.get("window") or metric_def.get("window")
        # MetricFlow uses both "conversion" and "conversions" for the count flavor.
        calculation = conv.get("calculation") or metric_def.get("calculation") or "conversion_rate"
        constant_properties = conv.get("constant_properties") or metric_def.get("constant_properties")

        if not base or not conversion or not entity:
            # Not enough information to describe a valid conversion metric.
            return None

        self.conversion_metrics[name] = {
            "name": name,
            "description": metric_def.get("description"),
            "label": metric_def.get("label"),
            "entity": entity,
            "base_measure": base,
            "conversion_measure": conversion,
            "window": window,
            "calculation": calculation,
            "constant_properties": constant_properties,
            "filter": metric_def.get("filter"),
        }
        return None

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to MetricFlow YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import (
            resolve_metric_inheritance,
            resolve_model_inheritance,
        )

        resolved_models = resolve_model_inheritance(graph.models)
        resolved_metrics = resolve_metric_inheritance(graph.metrics) if graph.metrics else {}

        # Export semantic models
        semantic_models = []
        for model in resolved_models.values():
            semantic_model = self._export_semantic_model(model)
            semantic_models.append(semantic_model)

        data = {"semantic_models": semantic_models}

        # Export metrics if present
        if resolved_metrics:
            data["metrics"] = [self._export_metric(metric, graph) for metric in resolved_metrics.values()]

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _export_semantic_model(self, model: Model) -> dict:
        """Export model to MetricFlow semantic model.

        Args:
            model: Model to export

        Returns:
            Semantic model definition dictionary
        """
        result = {"name": model.name}

        if model.sql:
            result["sql"] = model.sql
        elif model.table:
            result["model"] = f"ref('{model.table.split('.')[-1]}')"

        if model.description:
            result["description"] = model.description

        # Export entities (convert from relationships and primary_key)
        result["entities"] = []

        # Add primary entity
        result["entities"].append(
            {
                "name": model.name,  # Use model name as entity name
                "type": "primary",
                "expr": model.primary_key,
            }
        )

        # Add foreign entities from relationships
        for rel in model.relationships:
            if rel.type == "many_to_one":
                result["entities"].append(
                    {
                        "name": rel.name,
                        "type": "foreign",
                        "expr": rel.foreign_key or f"{rel.name}_id",
                    }
                )

        # Export dimensions
        if model.dimensions:
            result["dimensions"] = []
            for dim in model.dimensions:
                dim_def = {"name": dim.name, "type": dim.type}

                if dim.sql:
                    dim_def["expr"] = dim.sql

                if dim.type == "time" and dim.granularity:
                    dim_def["type_params"] = {"time_granularity": dim.granularity}

                if dim.description:
                    dim_def["description"] = dim.description
                if dim.label:
                    dim_def["label"] = dim.label

                # Add metadata fields
                if dim.format:
                    dim_def["meta"] = dim_def.get("meta", {})
                    dim_def["meta"]["format"] = dim.format
                if dim.value_format_name:
                    dim_def["meta"] = dim_def.get("meta", {})
                    dim_def["meta"]["value_format_name"] = dim.value_format_name

                # Add hierarchy parent info (MetricFlow doesn't have native hierarchies, use meta)
                if dim.parent:
                    dim_def["meta"] = dim_def.get("meta", {})
                    dim_def["meta"]["parent"] = dim.parent

                result["dimensions"].append(dim_def)

        # Export measures
        if model.metrics:
            result["measures"] = []
            for measure in model.metrics:
                measure_def = {"name": measure.name}

                # Map agg types
                agg_mapping = {
                    "sum": "sum",
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "avg": "average",
                    "min": "min",
                    "max": "max",
                    "median": "median",
                }
                measure_def["agg"] = agg_mapping.get(measure.agg, "sum")

                if measure.sql:
                    measure_def["expr"] = measure.sql

                if measure.description:
                    measure_def["description"] = measure.description
                if measure.label:
                    measure_def["label"] = measure.label

                # Add metric-level filters
                if measure.filters:
                    # MetricFlow supports filters in create_metric, but we can put in meta for now
                    measure_def["meta"] = measure_def.get("meta", {})
                    measure_def["meta"]["filters"] = measure.filters

                # Add metadata fields
                if measure.format:
                    measure_def["meta"] = measure_def.get("meta", {})
                    measure_def["meta"]["format"] = measure.format
                if measure.value_format_name:
                    measure_def["meta"] = measure_def.get("meta", {})
                    measure_def["meta"]["value_format_name"] = measure.value_format_name
                if measure.drill_fields:
                    measure_def["meta"] = measure_def.get("meta", {})
                    measure_def["meta"]["drill_fields"] = measure.drill_fields
                if measure.non_additive_dimension:
                    non_additive_def = {"name": measure.non_additive_dimension}
                    # Only emit window_choice when it differs from the MetricFlow default
                    # ("max" = keep the last snapshot), to keep exports minimal.
                    if getattr(measure, "non_additive_window", "max") == "min":
                        non_additive_def["window_choice"] = "min"
                    if getattr(measure, "non_additive_window_groupings", None):
                        non_additive_def["window_groupings"] = list(measure.non_additive_window_groupings)
                    measure_def["non_additive_dimension"] = non_additive_def

                result["measures"].append(measure_def)

        # Export model-level default_time_dimension
        if model.default_time_dimension:
            result["defaults"] = {"agg_time_dimension": model.default_time_dimension}
            if model.default_grain:
                result["meta"] = result.get("meta", {})
                result["meta"]["default_grain"] = model.default_grain

        # Export segments (as meta since MetricFlow doesn't have native segment support)
        if model.segments:
            result["meta"] = result.get("meta", {})
            result["meta"]["segments"] = []
            for segment in model.segments:
                segment_def = {"name": segment.name, "sql": segment.sql}
                if segment.description:
                    segment_def["description"] = segment.description
                result["meta"]["segments"].append(segment_def)

        # Note: inheritance is resolved before export, so extends field is not exported

        return result

    def _export_metric(self, measure: Metric, graph) -> dict:
        """Export measure to MetricFlow format.

        Args:
            measure: Metric to export

        Returns:
            Measure definition dictionary
        """
        # Determine export type - untyped metrics with sql should be exported as "simple"
        export_type = measure.type or ("simple" if not measure.agg and measure.sql else None)

        result = {
            "name": measure.name,
            "type": export_type,
        }

        if measure.description:
            result["description"] = measure.description
        if measure.label:
            result["label"] = measure.label

        # Type-specific params
        type_params = {}

        # Untyped metrics with sql are treated as simple (measure references)
        if not measure.type and not measure.agg and measure.sql:
            type_params["measure"] = {"name": measure.sql}

        elif measure.type == "ratio":
            if measure.numerator:
                type_params["numerator"] = {"name": measure.numerator}
            if measure.denominator:
                type_params["denominator"] = {"name": measure.denominator}

        elif measure.type == "derived":
            if measure.sql:
                type_params["expr"] = measure.sql
            # Auto-detect dependencies from expression using graph for resolution
            dependencies = measure.get_dependencies(graph)
            if dependencies:
                type_params["metrics"] = [{"name": m} for m in dependencies]

        elif measure.type == "cumulative" and measure.window:
            type_params["cumulative_type_params"] = {"window": measure.window}

        if type_params:
            result["type_params"] = type_params

        if measure.filters:
            result["filter"] = measure.filters[0]  # MetricFlow uses single filter string

        # Add metadata fields for graph-level metrics
        if measure.format or measure.value_format_name or measure.drill_fields:
            result["meta"] = result.get("meta", {})
            if measure.format:
                result["meta"]["format"] = measure.format
            if measure.value_format_name:
                result["meta"]["value_format_name"] = measure.value_format_name
            if measure.drill_fields:
                result["meta"]["drill_fields"] = measure.drill_fields
            # Note: inheritance is resolved before export, so extends field is not exported

        return result

    def _resolve_relationship_names(self, graph: SemanticGraph) -> None:
        """Resolve MetricFlow entity names to actual model names.

        MetricFlow uses singular entity names (e.g., "customer") while models are often plural (e.g., "customers").
        This method attempts to match entity names to actual models in the graph.

        Args:
            graph: Semantic graph with models
        """
        # Get all model names
        model_names = set(graph.models.keys())

        # For each model, check its relationships
        for model in graph.models.values():
            for rel in model.relationships:
                # If the relationship name doesn't match any model, try to resolve it
                if rel.name not in model_names:
                    resolved_name = self._resolve_entity_to_model(rel.name, model_names)
                    if resolved_name:
                        # Update the relationship name to the actual model name
                        rel.name = resolved_name

    def _resolve_entity_to_model(self, entity_name: str, model_names: set[str]) -> str | None:
        """Attempt to resolve an entity name to an actual model name.

        Uses inflect library for proper pluralization/singularization.

        Args:
            entity_name: Entity name from MetricFlow
            model_names: Set of available model names

        Returns:
            Resolved model name or None if no match found
        """
        # Try exact match (case-sensitive)
        if entity_name in model_names:
            return entity_name

        # Try pluralization/singularization if inflect is available
        try:
            import inflect

            p = inflect.engine()

            plural = p.plural(entity_name)
            if plural in model_names:
                return plural

            singular = p.singular_noun(entity_name)
            if singular and singular in model_names:
                return singular
        except ImportError:
            # Without inflect, try basic s-suffix heuristics
            if entity_name + "s" in model_names:
                return entity_name + "s"
            if entity_name.endswith("s") and entity_name[:-1] in model_names:
                return entity_name[:-1]

        # Try case-insensitive match
        entity_lower = entity_name.lower()
        for model_name in model_names:
            if model_name.lower() == entity_lower:
                return model_name

        # No match found
        return None
