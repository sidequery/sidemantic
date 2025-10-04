"""SQL generation using SQLGlot builder API."""

import sqlglot
from sqlglot import exp, select

from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.symmetric_aggregate import build_symmetric_aggregate_sql


class SQLGenerator:
    """Generates SQL queries from semantic layer definitions using SQLGlot builder API."""

    def __init__(self, graph: SemanticGraph, dialect: str = "duckdb"):
        """Initialize SQL generator.

        Args:
            graph: Semantic graph with models and metrics
            dialect: SQL dialect for generation (default: duckdb)
        """
        self.graph = graph
        self.dialect = dialect

    def generate_view(
        self,
        view_name: str,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
    ) -> str:
        """Generate CREATE VIEW statement for a semantic query.

        Allows joining arbitrary SQL against semantic layer calculations.

        Args:
            view_name: Name for the view
            metrics: List of metric references
            dimensions: List of dimension references
            filters: List of filter expressions
            order_by: List of fields to order by
            limit: Maximum number of rows

        Returns:
            CREATE VIEW SQL statement
        """
        query_sql = self.generate(metrics, dimensions, filters, order_by, limit)
        return f"CREATE VIEW {view_name} AS\n{query_sql}"

    def generate(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        parameters: dict[str, any] | None = None,
    ) -> str:
        """Generate SQL query from semantic layer query.

        Args:
            metrics: List of metric references (e.g., ["orders.revenue"])
            dimensions: List of dimension references (e.g., ["orders.status", "orders.order_date__month"])
            filters: List of filter expressions (e.g., ["orders.status = 'completed'"])
            order_by: List of fields to order by
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            parameters: User-provided parameter values for interpolation

        Returns:
            SQL query string
        """
        metrics = metrics or []
        dimensions = dimensions or []
        filters = filters or []
        parameters = parameters or {}

        # Interpolate parameters into filters if provided
        from sidemantic.core.parameter import ParameterSet
        param_set = ParameterSet(self.graph.parameters, parameters)
        filters = [param_set.interpolate(f) for f in filters]

        # Check if any metrics need window functions (cumulative or time_comparison)
        def metric_needs_window(m):
            if "." in m:
                return False
            metric = self.graph.get_metric(m)
            if not metric:
                return False
            # Cumulative and time_comparison always need windows
            if metric.type in ("cumulative", "time_comparison"):
                return True
            # Ratio with offset_window needs window
            if metric.type == "ratio" and metric.offset_window:
                return True
            # Conversion metrics need special handling
            if metric.type == "conversion":
                return True
            return False

        needs_window_functions = any(metric_needs_window(m) for m in metrics)

        if needs_window_functions:
            return self._generate_with_window_functions(metrics, dimensions, filters, order_by, limit, offset)

        # Parse dimension references and extract granularities
        parsed_dims = self._parse_dimension_refs(dimensions)

        # Find all models needed for the query
        model_names = self._find_required_models(metrics, dimensions)

        if not model_names:
            raise ValueError("No models found for query")

        # Determine base model (first model in query - order is now preserved)
        base_model_name = model_names[0]

        # Find all intermediate models needed for joins
        all_models = set(model_names)
        for i, model_a in enumerate(list(model_names)):
            for model_b in list(model_names)[i+1:]:
                # Find join path and add intermediate models
                try:
                    join_path = self.graph.find_relationship_path(model_a, model_b)
                    for jp in join_path:
                        all_models.add(jp.from_model)
                        all_models.add(jp.to_model)
                except ValueError:
                    # No join path found
                    pass

        # Build CTEs for all models (including intermediate ones)
        cte_sqls = []
        for model_name in all_models:
            cte_sql = self._build_model_cte(model_name, parsed_dims, metrics)
            cte_sqls.append(cte_sql)

        # Build main SELECT using builder API
        query = self._build_main_select(
            base_model_name=base_model_name,
            other_models=model_names[1:] if len(model_names) > 1 else [],
            parsed_dims=parsed_dims,
            metrics=metrics,
            filters=filters,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        # Combine CTEs and main query
        if cte_sqls:
            # Build WITH clause manually as string since builder API doesn't support CTEs well
            cte_str = "WITH " + ",\n".join(cte_sqls)
            full_sql = cte_str + "\n" + query
        else:
            full_sql = query

        return full_sql

    def _parse_dimension_refs(self, dimensions: list[str]) -> list[tuple[str, str | None]]:
        """Parse dimension references to extract granularities.

        Args:
            dimensions: List of dimension references (e.g., ["orders.order_date__month"])

        Returns:
            List of (dimension_ref, granularity) tuples
        """
        parsed = []
        for dim in dimensions:
            if "__" in dim:
                dim_ref, gran = dim.rsplit("__", 1)
                parsed.append((dim_ref, gran))
            else:
                parsed.append((dim, None))
        return parsed

    def _find_required_models(self, metrics: list[str], dimensions: list[str]) -> list[str]:
        """Find all models required for the query.

        Returns:
            List of model names in the order they are encountered (preserves order)
        """
        models = []
        seen = set()

        def add_model(model_name: str):
            """Add model if not seen yet."""
            if model_name not in seen:
                models.append(model_name)
                seen.add(model_name)

        def collect_models_from_metric(metric_ref: str):
            """Recursively collect models needed from a metric."""
            if "." in metric_ref:
                # Direct measure reference (model.measure)
                add_model(metric_ref.split(".")[0])
            else:
                # It's a metric, need to resolve its dependencies
                try:
                    metric = self.graph.get_metric(metric_ref)
                    if metric:
                        if metric.type == "ratio":
                            if metric.numerator:
                                collect_models_from_metric(metric.numerator)
                            if metric.denominator:
                                collect_models_from_metric(metric.denominator)
                        elif metric.type == "derived" or (not metric.type and not metric.agg and metric.sql):
                            # Derived or untyped metrics with sql - auto-detect dependencies
                            for ref_metric in metric.get_dependencies(self.graph):
                                collect_models_from_metric(ref_metric)
                except KeyError:
                    pass

        # Collect from dimensions first (since they define the grain)
        for dim in dimensions:
            # Remove granularity suffix if present
            if "__" in dim:
                dim = dim.rsplit("__", 1)[0]
            if "." in dim:
                add_model(dim.split(".")[0])

        # Then collect from metrics
        for metric in metrics:
            collect_models_from_metric(metric)

        return models

    def _build_model_cte(
        self, model_name: str, dimensions: list[tuple[str, str | None]], metrics: list[str]
    ) -> str:
        """Build CTE SQL for a model.

        Args:
            model_name: Name of the model
            dimensions: Parsed dimension references
            metrics: Metric references

        Returns:
            CTE SQL string
        """
        model = self.graph.get_model(model_name)

        # Build SELECT columns
        select_cols = []

        # Add join keys
        join_keys_added = set()

        # Include this model's primary key
        if model.primary_key and model.primary_key not in join_keys_added:
            select_cols.append(f"{model.primary_key} AS {model.primary_key}")
            join_keys_added.add(model.primary_key)

        # Include foreign keys from belongs_to joins
        for relationship in model.relationships:
            if relationship.type == "many_to_one":
                fk = relationship.sql_expr
                if fk not in join_keys_added:
                    select_cols.append(f"{fk} AS {fk}")
                    join_keys_added.add(fk)

        # Check if other models have has_many/has_one pointing to this model
        for other_model_name, other_model in self.graph.models.items():
            for other_join in other_model.relationships:
                if other_join.name == model_name and other_join.type in ("one_to_one", "one_to_many"):
                    # Other model expects this model to have a foreign key
                    # For has_many/has_one, foreign_key is the FK column in THIS model
                    fk = other_join.foreign_key or other_join.sql_expr
                    if fk not in join_keys_added:
                        select_cols.append(f"{fk} AS {fk}")
                        join_keys_added.add(fk)

        # Add dimension columns
        # First, add all dimensions from this model (needed for filters/joins)
        for dimension in model.dimensions:
            select_cols.append(f"{dimension.sql_expr} AS {dimension.name}")

        # Then, add time dimensions with specific granularities
        for dim_ref, gran in dimensions:
            if not dim_ref.startswith(model_name + "."):
                continue

            dim_name = dim_ref.split(".")[1]
            dimension = model.get_dimension(dim_name)

            if not dimension:
                continue

            if gran and dimension.type == "time":
                # Apply time granularity (in addition to base column)
                dim_sql = dimension.with_granularity(gran)
                alias = f"{dim_name}__{gran}"
                select_cols.append(f"{dim_sql} AS {alias}")

        # Add measure columns (raw, not aggregated in CTE)
        # Collect all measures needed for metrics
        measures_needed = set()

        def collect_measures_from_metric(metric_ref: str):
            """Recursively collect measures needed from a metric."""
            if "." in metric_ref and metric_ref.startswith(model_name + "."):
                # Direct measure reference
                measure_name = metric_ref.split(".")[1]
                measures_needed.add(measure_name)
            else:
                # It's a metric, need to resolve its dependencies
                try:
                    metric = self.graph.get_metric(metric_ref)
                    if metric:
                        # Use auto dependency detection with graph for resolution
                        for dep in metric.get_dependencies(self.graph):
                            collect_measures_from_metric(dep)
                except KeyError:
                    pass

        for metric_ref in metrics:
            collect_measures_from_metric(metric_ref)

        for measure_name in measures_needed:
            measure = model.get_metric(measure_name)
            if measure:
                select_cols.append(f"{measure.sql_expr} AS {measure_name}_raw")

        # Build FROM clause
        if model.sql:
            from_clause = f"({model.sql}) AS t"
        else:
            from_clause = model.table

        # Build CTE
        select_str = ",\n    ".join(select_cols)
        cte_sql = f"{model_name}_cte AS (\n  SELECT\n    {select_str}\n  FROM {from_clause}\n)"

        return cte_sql

    def _has_fanout_joins(self, base_model_name: str, other_models: list[str]) -> dict[str, bool]:
        """Determine which models need symmetric aggregates due to fan-out.

        When multiple one-to-many joins exist from the base model, measures from
        the base model need symmetric aggregates to prevent double-counting.

        Args:
            base_model_name: Base model name
            other_models: Other models in the query

        Returns:
            Dict mapping model names to whether they need symmetric aggregates
        """
        needs_symmetric = {}

        # Check if there are multiple one-to-many relationships
        one_to_many_count = 0

        for other_model in other_models:
            try:
                join_path = self.graph.find_relationship_path(base_model_name, other_model)
                # Check if first hop is one-to-many
                if join_path and join_path[0].relationship == "one_to_many":
                    one_to_many_count += 1
            except (ValueError, KeyError):
                pass

        # If we have multiple one-to-many joins, the base model needs symmetric aggregates
        needs_symmetric[base_model_name] = one_to_many_count > 1

        # Other models generally don't need it (they're on the "many" side)
        for other_model in other_models:
            needs_symmetric[other_model] = False

        return needs_symmetric

    def _build_main_select(
        self,
        base_model_name: str,
        other_models: list[str],
        parsed_dims: list[tuple[str, str | None]],
        metrics: list[str],
        filters: list[str] | None,
        order_by: list[str] | None,
        limit: int | None,
        offset: int | None = None,
    ) -> str:
        """Build main SELECT using SQLGlot builder API.

        Args:
            base_model_name: Base model name
            other_models: Other models to join
            parsed_dims: Parsed dimensions with granularities
            metrics: Metric references
            filters: Filter expressions
            order_by: Order by fields
            limit: Row limit

        Returns:
            SQL SELECT statement
        """
        # Detect if symmetric aggregates are needed
        symmetric_agg_needed = self._has_fanout_joins(base_model_name, other_models)

        # Build SELECT columns
        select_exprs = []

        # Add dimensions
        for dim_ref, gran in parsed_dims:
            model_name, dim_name = dim_ref.split(".")
            cte_col_name = f"{dim_name}__{gran}" if gran else dim_name
            alias = f"{dim_name}__{gran}" if gran else dim_name

            select_exprs.append(f"{model_name}_cte.{cte_col_name} AS {alias}")

        # Add metrics
        for metric_ref in metrics:
            if "." in metric_ref:
                # It's a measure reference (model.measure)
                model_name, measure_name = metric_ref.split(".")
                model = self.graph.get_model(model_name)
                measure = model.get_metric(measure_name)

                if measure:
                    # Check if this model needs symmetric aggregates
                    if symmetric_agg_needed.get(model_name, False):
                        # Use symmetric aggregates to prevent double-counting
                        # Get primary key for this model
                        model_obj = self.graph.get_model(model_name)
                        pk = model_obj.primary_key or "id"

                        agg_expr = build_symmetric_aggregate_sql(
                            measure_expr=f"{measure_name}_raw",
                            primary_key=pk,
                            agg_type=measure.agg,
                            model_alias=f"{model_name}_cte",
                        )
                    else:
                        # Regular aggregation
                        agg_func = measure.agg.upper()
                        if agg_func == "COUNT_DISTINCT":
                            agg_func = "COUNT(DISTINCT"
                            agg_expr = f"{agg_func} {model_name}_cte.{measure_name}_raw)"
                        else:
                            agg_expr = f"{agg_func}({model_name}_cte.{measure_name}_raw)"

                    select_exprs.append(f"{agg_expr} AS {measure_name}")
                else:
                    # Try as metric
                    metric = self.graph.get_metric(metric_ref)
                    if metric:
                        metric_expr = self._build_metric_sql(metric)
                        metric_expr = self._wrap_with_fill_nulls(metric_expr, metric)
                        select_exprs.append(f"{metric_expr} AS {metric.name}")
            else:
                # It's a metric reference (just metric name)
                metric = self.graph.get_metric(metric_ref)
                if metric:
                    metric_expr = self._build_metric_sql(metric)
                    metric_expr = self._wrap_with_fill_nulls(metric_expr, metric)
                    select_exprs.append(f"{metric_expr} AS {metric.name}")
                else:
                    raise ValueError(f"Metric {metric_ref} not found")

        # Build query using builder API
        query = select(*select_exprs).from_(f"{base_model_name}_cte")

        # Add joins (supports multi-hop)
        if other_models:
            # Track which models we've already joined
            joined_models = {base_model_name}

            for other_model in other_models:
                join_path = self.graph.find_relationship_path(base_model_name, other_model)
                if join_path:
                    # Apply each join in the path
                    for jp in join_path:
                        # Skip if we've already joined this model
                        if jp.to_model in joined_models:
                            continue

                        left_table = jp.from_model + "_cte"
                        right_table = jp.to_model + "_cte"
                        join_cond = f"{left_table}.{jp.from_entity} = {right_table}.{jp.to_entity}"
                        query = query.join(right_table, on=join_cond, join_type="left")
                        joined_models.add(jp.to_model)

        # Add WHERE clause
        if filters:
            # Parse filters to add table aliases and handle measure vs dimension columns
            for filter_expr in filters:
                parsed_filter = filter_expr
                for model_name in [base_model_name] + other_models:
                    # Replace model.field references
                    # Check if field is a measure (needs _raw suffix) or dimension
                    model_obj = self.graph.get_model(model_name)

                    # Find all field references for this model in the filter
                    # Use negative lookahead/lookbehind to avoid matching inside quotes
                    import re

                    # Split by quotes to avoid replacing inside string literals
                    parts = []
                    in_quotes = False
                    current = ""

                    for char in parsed_filter:
                        if char == "'":
                            if current:
                                parts.append((current, in_quotes))
                                current = ""
                            in_quotes = not in_quotes
                            parts.append(("'", False))
                        else:
                            current += char
                    if current:
                        parts.append((current, in_quotes))

                    # Only replace in non-quoted parts
                    pattern = f"{model_name}\\.([a-zA-Z_][a-zA-Z0-9_]*)"

                    def replace_field(match):
                        field_name = match.group(1)
                        # Check if it's a measure
                        if model_obj.get_metric(field_name):
                            return f"{model_name}_cte.{field_name}_raw"
                        else:
                            # It's a dimension or other column
                            return f"{model_name}_cte.{field_name}"

                    result_parts = []
                    for part, is_quoted in parts:
                        if is_quoted or part == "'":
                            result_parts.append(part)
                        else:
                            result_parts.append(re.sub(pattern, replace_field, part))

                    parsed_filter = "".join(result_parts)

                query = query.where(parsed_filter)

        # Add GROUP BY (all dimensions by position)
        if parsed_dims:
            group_by_positions = list(range(1, len(parsed_dims) + 1))
            query = query.group_by(*group_by_positions)

        # Add ORDER BY
        if order_by:
            # Strip model prefixes from order_by fields to use column aliases
            order_by_aliases = []
            for field in order_by:
                if "." in field:
                    # Extract just the field name (with optional granularity)
                    field_alias = field.split(".", 1)[1]
                else:
                    field_alias = field
                order_by_aliases.append(field_alias)
            query = query.order_by(*order_by_aliases)

        # Add LIMIT and OFFSET
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

        return query.sql(dialect=self.dialect, pretty=True)

    def _wrap_with_fill_nulls(self, sql_expr: str, metric) -> str:
        """Wrap SQL expression with COALESCE if fill_nulls_with is specified.

        Args:
            sql_expr: Base SQL expression
            metric: Metric object

        Returns:
            Wrapped SQL expression
        """
        if metric.fill_nulls_with is not None:
            # Quote string values
            if isinstance(metric.fill_nulls_with, str):
                fill_value = f"'{metric.fill_nulls_with}'"
            else:
                fill_value = str(metric.fill_nulls_with)
            return f"COALESCE({sql_expr}, {fill_value})"
        return sql_expr

    def _build_metric_sql(self, metric) -> str:
        """Build SQL expression for a metric.

        Args:
            metric: Metric object

        Returns:
            SQL expression string
        """
        if metric.type == "ratio":
            # numerator / NULLIF(denominator, 0)
            num_model, num_measure = metric.numerator.split(".")
            denom_model, denom_measure = metric.denominator.split(".")

            num_model_obj = self.graph.get_model(num_model)
            denom_model_obj = self.graph.get_model(denom_model)

            num_measure_obj = num_model_obj.get_metric(num_measure)
            denom_measure_obj = denom_model_obj.get_metric(denom_measure)

            # Build numerator
            num_agg = num_measure_obj.agg.upper()
            if num_agg == "COUNT_DISTINCT":
                num_expr = f"COUNT(DISTINCT {num_model}_cte.{num_measure}_raw)"
            else:
                num_expr = f"{num_agg}({num_model}_cte.{num_measure}_raw)"

            # Build denominator
            denom_agg = denom_measure_obj.agg.upper()
            if denom_agg == "COUNT_DISTINCT":
                denom_expr = f"COUNT(DISTINCT {denom_model}_cte.{denom_measure}_raw)"
            else:
                denom_expr = f"{denom_agg}({denom_model}_cte.{denom_measure}_raw)"

            return f"({num_expr}) / NULLIF({denom_expr}, 0)"

        elif metric.type == "derived" or (not metric.type and not metric.agg and metric.sql):
            # Parse formula and replace metric references (handles both typed "derived" and untyped metrics with sql)
            if not metric.sql:
                raise ValueError(f"Derived metric {metric.name} missing sql")

            formula = metric.sql

            # Auto-detect dependencies from expression using graph for resolution
            dependencies = metric.get_dependencies(self.graph)

            # Replace each metric reference with its SQL expression
            for metric_name in dependencies:
                # Check if it's a measure reference (model.measure) first
                if "." in metric_name:
                    model_name, measure_name = metric_name.split(".")
                    model = self.graph.get_model(model_name)
                    measure = model.get_metric(measure_name)

                    if measure:
                        agg_func = measure.agg.upper()
                        if agg_func == "COUNT_DISTINCT":
                            metric_sql = f"COUNT(DISTINCT {model_name}_cte.{measure_name}_raw)"
                        else:
                            metric_sql = f"{agg_func}({model_name}_cte.{measure_name}_raw)"
                    else:
                        raise ValueError(f"Measure {metric_name} not found")
                else:
                    # Try as graph-level metric
                    try:
                        ref_metric = self.graph.get_metric(metric_name)
                        # Recursively build metric SQL
                        metric_sql = self._build_metric_sql(ref_metric)
                    except KeyError:
                        raise ValueError(f"Metric {metric_name} not found")

                # Replace metric name in formula
                # Handle both metric_name and model.measure format
                formula = formula.replace(metric_name, f"({metric_sql})")

            return formula

        # Note: Cumulative metrics are handled via _generate_with_window_functions
        # and should never reach this point

        raise NotImplementedError(f"Metric type {metric.type} not yet implemented")

    def _generate_conversion_query(
        self,
        metric_name: str,
        dimensions: list[str],
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
    ) -> str:
        """Generate SQL for conversion funnel metrics.

        Uses self-join pattern to find entities that had both base and conversion events
        within the specified time window.

        Args:
            metric_name: Name of the conversion metric
            dimensions: List of dimension references
            filters: List of filter expressions
            order_by: List of fields to order by
            limit: Maximum number of rows to return

        Returns:
            SQL query string
        """
        metric = self.graph.get_metric(metric_name)
        if not metric or not metric.entity or not metric.base_event or not metric.conversion_event:
            raise ValueError(f"Conversion metric {metric_name} missing required fields")

        # Get the model (assume single model for conversion)
        # Find the model that has this metric
        model = None
        for model_name, m in self.graph.models.items():
            # Just use the first model for now
            model = m
            break

        if not model:
            raise ValueError("No model found for conversion metric")

        # Build SQL with self-join pattern
        # base_events: filter for base_event
        # conversion_events: filter for conversion_event
        # Join on entity where conversion is within window

        window_parts = metric.conversion_window.split() if metric.conversion_window else ["7", "days"]
        window_num, window_unit = window_parts[0], window_parts[1] if len(window_parts) > 1 else "days"

        # Find dimension that represents event type
        event_type_dim = None
        timestamp_dim = None
        for dim in model.dimensions:
            if dim.type == "time":
                timestamp_dim = dim.name
            # Assume event_type is categorical
            if "event" in dim.name.lower() and "type" in dim.name.lower():
                event_type_dim = dim.name

        if not event_type_dim or not timestamp_dim:
            raise ValueError("Conversion metrics require event_type and timestamp dimensions")

        sql = f"""
WITH base_events AS (
  SELECT
    {metric.entity} AS entity,
    {timestamp_dim} AS event_time
  FROM ({model.sql}) AS t
  WHERE {event_type_dim} = '{metric.base_event}'
),
conversion_events AS (
  SELECT
    {metric.entity} AS entity,
    {timestamp_dim} AS event_time
  FROM ({model.sql}) AS t
  WHERE {event_type_dim} = '{metric.conversion_event}'
),
conversions AS (
  SELECT DISTINCT
    base.entity
  FROM base_events base
  JOIN conversion_events conv
    ON base.entity = conv.entity
    AND conv.event_time BETWEEN base.event_time AND base.event_time + INTERVAL '{window_num} {window_unit}'
)
SELECT
  COUNT(DISTINCT conversions.entity)::FLOAT / NULLIF(COUNT(DISTINCT base_events.entity), 0) AS {metric.name}
FROM base_events
LEFT JOIN conversions ON base_events.entity = conversions.entity
"""

        return sql.strip()

    def _generate_with_window_functions(
        self,
        metrics: list[str],
        dimensions: list[str],
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> str:
        """Generate SQL with window functions for cumulative metrics.

        This uses a subquery pattern:
        1. Inner query aggregates base metrics by dimensions
        2. Outer query applies window functions to aggregated results

        Args:
            metrics: List of metric references
            dimensions: List of dimension references
            filters: List of filter expressions
            order_by: List of fields to order by
            limit: Maximum number of rows to return

        Returns:
            SQL query string
        """
        # Separate window function metrics from regular metrics
        cumulative_metrics = []
        time_comparison_metrics = []
        offset_ratio_metrics = []
        conversion_metrics = []
        base_metrics = []

        for m in metrics:
            if "." not in m:
                metric = self.graph.get_metric(m)
                if metric and metric.type == "cumulative":
                    cumulative_metrics.append(m)
                    # Add the base measure/metric to base_metrics
                    if metric.sql:
                        base_metrics.append(metric.sql)
                elif metric and metric.type == "time_comparison":
                    time_comparison_metrics.append(m)
                    # Add the base metric to base_metrics
                    if metric.base_metric:
                        base_metrics.append(metric.base_metric)
                elif metric and metric.type == "ratio" and metric.offset_window:
                    offset_ratio_metrics.append(m)
                    # Add numerator and denominator to base_metrics
                    if metric.numerator:
                        base_metrics.append(metric.numerator)
                    if metric.denominator:
                        base_metrics.append(metric.denominator)
                elif metric and metric.type == "conversion":
                    conversion_metrics.append(m)
                    # Conversion metrics need special handling - don't add to base_metrics
                else:
                    base_metrics.append(m)
            else:
                base_metrics.append(m)

        # Handle conversion metrics separately - they need a completely different pattern
        if conversion_metrics:
            return self._generate_conversion_query(conversion_metrics[0], dimensions, filters, order_by, limit)

        # Build inner query with base aggregations
        # Dedupe base_metrics to avoid duplicate column names
        base_metrics = list(dict.fromkeys(base_metrics))

        inner_query = self.generate(
            metrics=base_metrics,
            dimensions=dimensions,
            filters=filters,
            order_by=None,  # Apply ordering in outer query
            limit=None,  # Apply limit in outer query
        )

        # Parse dimensions for outer SELECT
        parsed_dims = self._parse_dimension_refs(dimensions)

        # Build outer SELECT with window functions
        select_exprs = []

        # Add dimensions
        for dim_ref, gran in parsed_dims:
            # Inner query uses simple alias without model prefix
            dim_name = dim_ref.split(".")[1] if "." in dim_ref else dim_ref
            alias = dim_name
            if gran:
                alias = f"{alias}__{gran}"
            select_exprs.append(f"base.{alias}")

        # Add base metrics (pass through)
        for m in base_metrics:
            if "." in m:
                # Extract just the measure name
                alias = m.split(".")[1]
            else:
                # It's a metric name
                metric = self.graph.get_metric(m)
                if metric:
                    alias = m
            select_exprs.append(f"base.{alias}")

        # Add cumulative metrics with window functions
        for m in cumulative_metrics:
            metric = self.graph.get_metric(m)
            if not metric or not metric.sql:
                continue

            # Find the time dimension to order by
            time_dim = None
            for dim_ref, gran in parsed_dims:
                dim_name = dim_ref.split(".")[1] if "." in dim_ref else dim_ref
                model_name = dim_ref.split(".")[0] if "." in dim_ref else None
                if model_name:
                    model = self.graph.get_model(model_name)
                    if model:
                        dim = model.get_dimension(dim_name)
                        if dim and dim.type == "time":
                            # Use simple alias without model prefix
                            time_dim = f"base.{dim_name}"
                            if gran:
                                time_dim = f"base.{dim_name}__{gran}"
                            break

            if not time_dim:
                raise ValueError(f"Cumulative metric {m} requires a time dimension for ordering")

            # Get base measure/metric to apply window function to
            base_ref = metric.sql
            if "." in base_ref:
                # It's a direct measure reference - extract just the measure name
                base_alias = base_ref.split(".")[1]
            else:
                # It's a metric reference - check if it exists and get its underlying measure
                base_metric = self.graph.get_metric(base_ref)
                if base_metric and base_metric.sql:
                    # Use the underlying measure name
                    if "." in base_metric.sql:
                        base_alias = base_metric.sql.split(".")[1]
                    else:
                        base_alias = base_metric.sql
                else:
                    # Fallback to the metric name itself
                    base_alias = base_ref

            # Build window function
            if metric.grain_to_date:
                # Grain-to-date: MTD, QTD, YTD
                # Partition by the grain period and order within it
                grain = metric.grain_to_date
                if grain == "month":
                    partition = f"DATE_TRUNC('month', {time_dim})"
                elif grain == "quarter":
                    partition = f"DATE_TRUNC('quarter', {time_dim})"
                elif grain == "year":
                    partition = f"DATE_TRUNC('year', {time_dim})"
                elif grain == "week":
                    partition = f"DATE_TRUNC('week', {time_dim})"
                elif grain == "day":
                    partition = f"DATE_TRUNC('day', {time_dim})"
                else:
                    partition = f"DATE_TRUNC('month', {time_dim})"  # Default to month

                window_expr = f"SUM(base.{base_alias}) OVER (PARTITION BY {partition} ORDER BY {time_dim} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {m}"
            elif metric.window:
                # Parse window (e.g., "7 days")
                window_parts = metric.window.split()
                if len(window_parts) == 2:
                    num, unit = window_parts
                    # For date-based windows, use RANGE
                    window_expr = f"SUM(base.{base_alias}) OVER (ORDER BY {time_dim} RANGE BETWEEN INTERVAL '{num} {unit}' PRECEDING AND CURRENT ROW) AS {m}"
                else:
                    # Fallback to rows
                    window_expr = f"SUM(base.{base_alias}) OVER (ORDER BY {time_dim} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {m}"
            else:
                # Running total (unbounded window)
                window_expr = f"SUM(base.{base_alias}) OVER (ORDER BY {time_dim} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {m}"

            select_exprs.append(window_expr)

        # Handle offset ratio metrics - need two-step CTEs because you can't divide by window function directly
        if offset_ratio_metrics:
            # Need an intermediate CTE with LAG values
            lag_selects = []

            # Track what columns we're selecting
            lag_cte_columns = []

            # Add dimensions and base metrics
            for dim_ref, gran in parsed_dims:
                dim_name = dim_ref.split(".")[1] if "." in dim_ref else dim_ref
                alias = dim_name
                if gran:
                    alias = f"{alias}__{gran}"
                lag_selects.append(f"base.{alias}")
                lag_cte_columns.append(alias)

            for m in base_metrics:
                if "." in m:
                    alias = m.split(".")[1]
                else:
                    alias = m
                lag_selects.append(f"base.{alias}")
                lag_cte_columns.append(alias)

            # Add LAG expressions for each offset ratio metric
            for m in offset_ratio_metrics:
                metric = self.graph.get_metric(m)
                if not metric or not metric.numerator or not metric.denominator:
                    continue

                # Find time dimension
                time_dim = None
                for dim_ref, gran in parsed_dims:
                    dim_name = dim_ref.split(".")[1] if "." in dim_ref else dim_ref
                    model_name = dim_ref.split(".")[0] if "." in dim_ref else None
                    if model_name:
                        model = self.graph.get_model(model_name)
                        if model:
                            dim = model.get_dimension(dim_name)
                            if dim and dim.type == "time":
                                time_dim = f"base.{dim_name}"
                                if gran:
                                    time_dim = f"base.{dim_name}__{gran}"
                                break

                if not time_dim:
                    raise ValueError(f"Offset ratio metric {m} requires a time dimension")

                # Get denominator alias
                denom_alias = metric.denominator.split(".")[1] if "." in metric.denominator else metric.denominator

                # Add LAG for denominator - reference base.denom_alias since it's from inner query
                lag_selects.append(f"LAG(base.{denom_alias}) OVER (ORDER BY {time_dim}) AS {m}_prev_denom")

            # Build intermediate CTE - inner_query already has all the columns we need
            # We need to add "base." prefix since we're wrapping inner_query in a FROM (inner_query) AS base
            lag_cte_sql = f"WITH lag_cte AS (\n  SELECT\n    {',\n    '.join(lag_selects)}\n  FROM (\n{inner_query}\n  ) AS base\n)"

            # Now build final select from lag_cte - need to rebuild select_exprs without base. prefix
            final_selects = []

            # Re-add dimensions and base metrics from lag_cte
            for col in lag_cte_columns:
                final_selects.append(col)

            # Add offset ratio metrics
            for m in offset_ratio_metrics:
                metric = self.graph.get_metric(m)
                if not metric:
                    continue

                num_alias = metric.numerator.split(".")[1] if "." in metric.numerator else metric.numerator

                # Calculate ratio using the lagged value
                offset_expr = f"{num_alias} / NULLIF({m}_prev_denom, 0) AS {m}"
                final_selects.append(offset_expr)

            # Build final query
            outer_query = f"{lag_cte_sql}\nSELECT\n  {',\n  '.join(final_selects)}\nFROM lag_cte"
        else:
            # Build outer query without LAG CTE
            outer_query = f"SELECT\n  {',\n  '.join(select_exprs)}\nFROM (\n{inner_query}\n) AS base"

        # Add ORDER BY if specified
        if order_by:
            order_clauses = []
            for field in order_by:
                if "." in field:
                    # Extract just the field name without model prefix
                    field_alias = field.split(".")[1]
                else:
                    field_alias = field
                order_clauses.append(field_alias)
            outer_query += f"\nORDER BY {', '.join(order_clauses)}"

        # Add LIMIT and OFFSET if specified
        if limit:
            outer_query += f"\nLIMIT {limit}"
        if offset:
            outer_query += f"\nOFFSET {offset}"

        return outer_query
