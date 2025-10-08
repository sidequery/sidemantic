"""SQL generation using SQLGlot builder API."""

import sqlglot
from sqlglot import exp, select

from sidemantic.core.preagg_matcher import PreAggregationMatcher
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
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        parameters: dict[str, any] | None = None,
        ungrouped: bool = False,
        use_preaggregations: bool = False,
        aliases: dict[str, str] | None = None,
    ) -> str:
        """Generate SQL query from semantic layer query.

        Args:
            metrics: List of metric references (e.g., ["orders.revenue"])
            dimensions: List of dimension references (e.g., ["orders.status", "orders.order_date__month"])
            filters: List of filter expressions (e.g., ["orders.status = 'completed'"])
            segments: List of segment references (e.g., ["orders.active_users"])
            order_by: List of fields to order by
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            parameters: User-provided parameter values for interpolation
            ungrouped: If True, return raw rows without aggregation (no GROUP BY)
            use_preaggregations: Enable automatic pre-aggregation routing (default: False)
            aliases: Custom aliases for fields (dict mapping field reference to alias)

        Returns:
            SQL query string
        """
        metrics = metrics or []
        dimensions = dimensions or []
        filters = filters or []
        segments = segments or []
        parameters = parameters or {}
        aliases = aliases or {}

        # Resolve segments to SQL filters
        segment_filters = self._resolve_segments(segments)
        filters = filters + segment_filters

        # Interpolate parameters into filters if provided
        from sidemantic.core.parameter import ParameterSet

        param_set = ParameterSet(self.graph.parameters, parameters)
        filters = [param_set.interpolate(f) for f in filters]

        # Process relative date expressions in filters
        from sidemantic.core.relative_date import RelativeDateRange

        processed_filters = []
        for f in filters:
            # Check if filter contains a relative date expression
            # Pattern: column_name operator 'relative_date_expr'
            # e.g., "created_at >= 'last 7 days'"
            import re

            match = re.match(r"^(.+?)\s*(>=|<=|>|<|=)\s*['\"](.+?)['\"]$", f)
            if match:
                column, operator, value = match.groups()
                if RelativeDateRange.is_relative_date(value):
                    # Convert relative date to SQL expression
                    if operator in [">=", ">"]:
                        # For >= or >, just use the start date
                        sql_expr = RelativeDateRange.parse(value)
                        processed_filters.append(f"{column} {operator} {sql_expr}")
                    elif operator == "=":
                        # For =, use to_range to get proper range
                        range_expr = RelativeDateRange.to_range(value, column.strip())
                        if range_expr:
                            processed_filters.append(range_expr)
                        else:
                            processed_filters.append(f)
                    else:
                        processed_filters.append(f)
                else:
                    processed_filters.append(f)
            else:
                processed_filters.append(f)

        filters = processed_filters

        # Check if any metrics need window functions (cumulative or time_comparison)
        def metric_needs_window(m):
            # Try to get metric - could be model.measure or just metric name
            metric = None
            if "." in m:
                # model.measure format
                model_name, measure_name = m.split(".")
                model = self.graph.get_model(model_name)
                if model:
                    metric = model.get_metric(measure_name)
            else:
                # Just metric name - try graph-level metric
                try:
                    metric = self.graph.get_metric(m)
                except KeyError:
                    pass

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
            return self._generate_with_window_functions(metrics, dimensions, filters, order_by, limit, offset, aliases)

        # Parse dimension references and extract granularities
        parsed_dims = self._parse_dimension_refs(dimensions)

        # Find all models needed for the query
        model_names = self._find_required_models(metrics, dimensions, filters)

        # Try to use pre-aggregation if enabled (single model queries only)
        if use_preaggregations and len(model_names) == 1 and not ungrouped:
            preagg_sql = self._try_use_preaggregation(
                model_name=model_names[0],
                metrics=metrics,
                parsed_dims=parsed_dims,
                filters=filters,
                order_by=order_by,
                limit=limit,
                offset=offset,
            )
            if preagg_sql:
                # Add instrumentation comment
                instrumentation = self._generate_instrumentation_comment(
                    models=[model_names[0]], metrics=metrics, dimensions=dimensions, used_preagg=True
                )
                return preagg_sql + "\n" + instrumentation

        if not model_names:
            raise ValueError("No models found for query")

        # Determine base model (first model in query - order is now preserved)
        base_model_name = model_names[0]

        # Find all intermediate models needed for joins
        all_models = set(model_names)
        for i, model_a in enumerate(list(model_names)):
            for model_b in list(model_names)[i + 1 :]:
                # Find join path and add intermediate models
                try:
                    join_path = self.graph.find_relationship_path(model_a, model_b)
                    for jp in join_path:
                        all_models.add(jp.from_model)
                        all_models.add(jp.to_model)
                except ValueError:
                    # No join path found
                    pass

        # Classify filters for pushdown optimization
        pushdown_filters, main_query_filters = self._classify_filters_for_pushdown(filters or [], all_models)

        # Determine which models have filters (for join type decision)
        models_with_filters = set()
        for model_name, model_filters in pushdown_filters.items():
            if model_filters:
                models_with_filters.add(model_name)
        # Also check main query filters
        for filter_expr in main_query_filters:
            try:
                parsed = sqlglot.parse_one(filter_expr, dialect=self.dialect)
                for column in parsed.find_all(exp.Column):
                    if column.table:
                        # Remove _cte suffix if present
                        model_name = column.table.replace("_cte", "")
                        models_with_filters.add(model_name)
            except Exception:
                pass

        # Build CTEs for all models with pushed-down filters
        cte_sqls = []
        for model_name in all_models:
            model_filters = pushdown_filters.get(model_name, [])
            cte_sql = self._build_model_cte(
                model_name,
                parsed_dims,
                metrics,
                model_filters if model_filters else None,
                order_by=order_by,
                all_models=all_models,
            )
            cte_sqls.append(cte_sql)

        # Build main SELECT using builder API (only with filters that couldn't be pushed down)
        query = self._build_main_select(
            base_model_name=base_model_name,
            other_models=model_names[1:] if len(model_names) > 1 else [],
            parsed_dims=parsed_dims,
            metrics=metrics,
            filters=main_query_filters,
            models_with_filters=models_with_filters,
            order_by=order_by,
            limit=limit,
            offset=offset,
            ungrouped=ungrouped,
            aliases=aliases,
        )

        # Combine CTEs and main query
        if cte_sqls:
            # Build WITH clause manually as string since builder API doesn't support CTEs well
            cte_str = "WITH " + ",\n".join(cte_sqls)
            full_sql = cte_str + "\n" + query
        else:
            full_sql = query

        # Add instrumentation comment for query analysis
        instrumentation = self._generate_instrumentation_comment(
            models=list(all_models), metrics=metrics, dimensions=dimensions, used_preagg=False
        )
        full_sql = full_sql + "\n" + instrumentation

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

    def _resolve_segments(self, segments: list[str]) -> list[str]:
        """Resolve segment references to SQL filter expressions.

        Args:
            segments: List of segment references (e.g., ["orders.active_users"])

        Returns:
            List of SQL filter expressions

        Raises:
            ValueError: If segment not found
        """
        filters = []
        for seg_ref in segments:
            # Parse model.segment format
            if "." not in seg_ref:
                raise ValueError(f"Segment reference must be in format 'model.segment': {seg_ref}")

            model_name, segment_name = seg_ref.split(".", 1)
            model = self.graph.get_model(model_name)
            if not model:
                raise ValueError(f"Model '{model_name}' not found for segment '{seg_ref}'")

            segment = model.get_segment(segment_name)
            if not segment:
                raise ValueError(f"Segment '{segment_name}' not found on model '{model_name}'")

            # Get SQL expression with model alias replaced
            # Use model_cte as the alias (consistent with CTE naming)
            filter_sql = segment.get_sql(f"{model_name}_cte")
            filters.append(filter_sql)

        return filters

    def _find_required_models(
        self, metrics: list[str], dimensions: list[str], filters: list[str] | None = None
    ) -> list[str]:
        """Find all models required for the query.

        Args:
            metrics: List of metric references
            dimensions: List of dimension references
            filters: Optional list of filter expressions

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

        # Finally, collect from filters (to support filtering on joined tables without selecting dimensions)
        if filters:
            for filter_expr in filters:
                # Parse filter to find model references
                try:
                    parsed = sqlglot.parse_one(filter_expr, dialect=self.dialect)
                    # Find all column references in the filter
                    for column in parsed.find_all(exp.Column):
                        if column.table:
                            # Remove _cte suffix if present (shouldn't be, but be defensive)
                            model_name = column.table.replace("_cte", "")
                            add_model(model_name)
                except Exception:
                    # If parsing fails, skip this filter for model extraction
                    pass

        return models

    def _classify_filters_for_pushdown(
        self, filters: list[str], all_models: set[str]
    ) -> tuple[dict[str, list[str]], list[str]]:
        """Classify filters into those that can be pushed down vs those that must stay in main query.

        Args:
            filters: List of filter expressions
            all_models: Set of all model names in the query

        Returns:
            Tuple of (pushdown_filters_by_model, main_query_filters)
            - pushdown_filters_by_model: Dict mapping model name to list of filters for that model
            - main_query_filters: Filters that reference multiple models (can't push down)
        """
        pushdown_filters = {model: [] for model in all_models}
        main_query_filters = []

        for filter_expr in filters:
            # Parse filter expression with SQLGlot
            try:
                parsed = sqlglot.parse_one(filter_expr, dialect=self.dialect)
            except Exception:
                # If parsing fails, keep in main query to be safe
                main_query_filters.append(filter_expr)
                continue

            # Find all table references in the filter
            referenced_models = set()
            references_metric = False

            for column in parsed.find_all(exp.Column):
                table_name = column.table
                column_name = column.name

                if table_name:
                    # Remove _cte suffix if present
                    clean_name = table_name.replace("_cte", "")
                    if clean_name in all_models:
                        referenced_models.add(clean_name)

                        # Check if this column is a metric
                        model = self.graph.get_model(clean_name)
                        if model and model.get_metric(column_name):
                            references_metric = True

            # Filters that reference metrics must stay in main query (can't push down)
            # because metrics don't exist in CTEs (only _raw columns)
            if references_metric:
                main_query_filters.append(filter_expr)
            # If filter references exactly one model and no metrics, push it down
            elif len(referenced_models) == 1:
                model = list(referenced_models)[0]
                pushdown_filters[model].append(filter_expr)
            else:
                # Filter references multiple models or no models - keep in main query
                main_query_filters.append(filter_expr)

        return pushdown_filters, main_query_filters

    def _find_needed_dimensions(
        self,
        model_name: str,
        dimensions: list[tuple[str, str | None]],
        filters: list[str] | None,
        order_by: list[str] | None,
    ) -> set[str]:
        """Find which dimensions from this model are actually needed.

        Args:
            model_name: Model to check
            dimensions: Parsed dimension references from query
            filters: Filter expressions
            order_by: Order by fields

        Returns:
            Set of dimension names needed for this model
        """
        needed = set()

        # Dimensions explicitly in SELECT
        for dim_ref, _ in dimensions:
            if dim_ref.startswith(model_name + "."):
                dim_name = dim_ref.split(".")[1]
                needed.add(dim_name)

        # Dimensions referenced in filters
        if filters:
            for filter_expr in filters:
                try:
                    parsed = sqlglot.parse_one(filter_expr, dialect=self.dialect)
                    for col in parsed.find_all(exp.Column):
                        if col.table and col.table.replace("_cte", "") == model_name:
                            needed.add(col.name)
                except Exception:
                    pass

        # Dimensions referenced in ORDER BY
        if order_by:
            for order_field in order_by:
                # Strip DESC/ASC
                field = order_field.replace(" DESC", "").replace(" ASC", "").strip()
                if "." in field:
                    model_part, dim_part = field.split(".", 1)
                    if model_part == model_name:
                        needed.add(dim_part)

        return needed

    def _build_model_cte(
        self,
        model_name: str,
        dimensions: list[tuple[str, str | None]],
        metrics: list[str],
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        all_models: set[str] | None = None,
    ) -> str:
        """Build CTE SQL for a model with optional filter pushdown.

        Args:
            model_name: Name of the model
            dimensions: Parsed dimension references
            metrics: Metric references
            filters: Filters to push down into this CTE (optional)
            order_by: Order by fields (for determining needed dimensions)
            all_models: All models in query (for determining if joins needed)

        Returns:
            CTE SQL string
        """
        model = self.graph.get_model(model_name)
        all_models = all_models or {model_name}
        needs_joins = len(all_models) > 1

        # Find which dimensions are actually needed
        needed_dimensions = self._find_needed_dimensions(model_name, dimensions, filters, order_by)

        # Build SELECT columns
        select_cols = []

        # Track all columns added (not just join keys) to avoid duplicates
        columns_added = set()

        # Include this model's primary key (always needed for joins/grouping)
        if model.primary_key and model.primary_key not in columns_added:
            select_cols.append(f"{model.primary_key} AS {model.primary_key}")
            columns_added.add(model.primary_key)

        # Include foreign keys if we're joining OR if they're explicitly requested as dimensions
        for relationship in model.relationships:
            if relationship.type == "many_to_one":
                fk = relationship.sql_expr
                # Add FK if: (1) we're joining to this related model, OR (2) FK is requested as dimension
                should_include = (needs_joins and relationship.name in all_models) or fk in needed_dimensions
                if should_include and fk not in columns_added:
                    select_cols.append(f"{fk} AS {fk}")
                    columns_added.add(fk)
                    # Mark FK as "needed" so it's not duplicated as a dimension
                    needed_dimensions.discard(fk)

        # Check if other models have has_many/has_one pointing to this model
        if needs_joins:
            for other_model_name, other_model in self.graph.models.items():
                if other_model_name not in all_models:
                    continue
                for other_join in other_model.relationships:
                    if other_join.name == model_name and other_join.type in (
                        "one_to_one",
                        "one_to_many",
                    ):
                        # Other model expects this model to have a foreign key
                        # For has_many/has_one, foreign_key is the FK column in THIS model
                        fk = other_join.foreign_key or other_join.sql_expr
                        if fk not in columns_added:
                            select_cols.append(f"{fk} AS {fk}")
                            columns_added.add(fk)

        # Add only needed dimension columns
        for dimension in model.dimensions:
            if dimension.name in needed_dimensions and dimension.name not in columns_added:
                # For time dimensions with granularity, apply DATE_TRUNC
                if dimension.type == "time" and dimension.granularity:
                    dim_sql = dimension.with_granularity(dimension.granularity)
                else:
                    dim_sql = dimension.sql_expr
                select_cols.append(f"{dim_sql} AS {dimension.name}")
                columns_added.add(dimension.name)

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
                if alias not in columns_added:
                    select_cols.append(f"{dim_sql} AS {alias}")
                    columns_added.add(alias)

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
                # For COUNT(*), use 1 instead of * to avoid invalid "* AS alias" syntax
                if measure.agg == "count" and not measure.sql:
                    select_cols.append(f"1 AS {measure_name}_raw")
                else:
                    select_cols.append(f"{measure.sql_expr} AS {measure_name}_raw")

        # Build FROM clause
        if model.sql:
            from_clause = f"({model.sql}) AS t"
        else:
            from_clause = model.table

        # Build WHERE clause for pushed-down filters
        where_clause = ""
        if filters:
            # Process filters - replace model_cte references with direct column names using SQLGlot
            processed_filters = []
            for f in filters:
                try:
                    parsed = sqlglot.parse_one(f, dialect=self.dialect)
                    # Remove table qualifiers (model_name_cte. or model_name.)
                    for col in parsed.find_all(exp.Column):
                        if col.table:
                            clean_table = col.table.replace("_cte", "")
                            if clean_table == model_name:
                                col.set("table", None)
                    processed_filter = parsed.sql(dialect=self.dialect)
                    processed_filters.append(processed_filter)
                except Exception:
                    # If parsing fails, use original filter
                    processed_filters.append(f)

            where_clause = f"\n  WHERE {' AND '.join(processed_filters)}"

        # Build CTE
        select_str = ",\n    ".join(select_cols)
        cte_sql = f"{model_name}_cte AS (\n  SELECT\n    {select_str}\n  FROM {from_clause}{where_clause}\n)"

        return cte_sql

    def _has_fanout_joins(self, base_model_name: str, other_models: list[str]) -> dict[str, bool]:
        """Determine which models need symmetric aggregates due to fan-out.

        When one-to-many joins exist from the base model, measures from
        the base model need symmetric aggregates to prevent double-counting.

        Args:
            base_model_name: Base model name
            other_models: Other models in the query

        Returns:
            Dict mapping model names to whether they need symmetric aggregates
        """
        needs_symmetric = {}

        # Check if there are any one-to-many relationships
        one_to_many_count = 0
        many_to_one_models = []

        for other_model in other_models:
            try:
                join_path = self.graph.find_relationship_path(base_model_name, other_model)
                # Check if first hop is one-to-many
                if join_path and join_path[0].relationship == "one_to_many":
                    one_to_many_count += 1
                elif join_path and join_path[0].relationship == "many_to_one":
                    # Track models with many-to-one from base perspective
                    many_to_one_models.append(other_model)
            except (ValueError, KeyError):
                pass

        # Base model needs symmetric aggregates if there are any one-to-many joins
        needs_symmetric[base_model_name] = one_to_many_count > 0

        # Models on the "many" side of a many-to-one relationship also need symmetric
        # aggregation if they're being joined (because from their perspective,
        # they're creating fan-out for the "one" side)
        for other_model in other_models:
            if other_model in many_to_one_models:
                # Check if the "one" side (base) has metrics - if so, it needs symmetric agg
                # But we're checking from the perspective of this model, so mark False
                needs_symmetric[other_model] = False
            else:
                needs_symmetric[other_model] = False

        return needs_symmetric

    def _build_main_select(
        self,
        base_model_name: str,
        other_models: list[str],
        parsed_dims: list[tuple[str, str | None]],
        metrics: list[str],
        filters: list[str] | None,
        models_with_filters: set[str],
        order_by: list[str] | None,
        limit: int | None,
        offset: int | None = None,
        ungrouped: bool = False,
        aliases: dict[str, str] | None = None,
    ) -> str:
        """Build main SELECT using SQLGlot builder API.

        Args:
            base_model_name: Base model name
            other_models: Other models to join
            parsed_dims: Parsed dimensions with granularities
            metrics: Metric references
            filters: Filter expressions
            models_with_filters: Set of models that have filters (for INNER JOIN)
            order_by: Order by fields
            limit: Row limit
            offset: Row offset
            ungrouped: If True, return raw rows without aggregation
            aliases: Custom aliases for fields (dict mapping field reference to alias)

        Returns:
            SQL SELECT statement
        """
        aliases = aliases or {}
        # Detect if symmetric aggregates are needed
        symmetric_agg_needed = self._has_fanout_joins(base_model_name, other_models)

        # Check for dimension/metric name collisions across models
        # If there are collisions, prefix with model name
        field_names = {}  # field_name -> list of model names
        for dim_ref, gran in parsed_dims:
            model_name, dim_name = dim_ref.split(".")
            field_key = f"{dim_name}__{gran}" if gran else dim_name
            if field_key not in field_names:
                field_names[field_key] = []
            field_names[field_key].append(model_name)

        for metric_ref in metrics:
            if "." in metric_ref:
                model_name, measure_name = metric_ref.split(".")
                if measure_name not in field_names:
                    field_names[measure_name] = []
                field_names[measure_name].append(model_name)

        # Determine which fields have collisions
        has_collision = {name: len(models) > 1 for name, models in field_names.items()}

        # Build SELECT columns
        select_exprs = []

        # Add dimensions
        for dim_ref, gran in parsed_dims:
            model_name, dim_name = dim_ref.split(".")
            cte_col_name = f"{dim_name}__{gran}" if gran else dim_name

            # Check for custom alias first
            full_ref = f"{model_name}.{dim_name}__{gran}" if gran else dim_ref
            if full_ref in aliases:
                alias = aliases[full_ref]
            else:
                # Generate alias (with model prefix if collision)
                base_alias = f"{dim_name}__{gran}" if gran else dim_name
                if has_collision.get(base_alias, False):
                    alias = f"{model_name}_{base_alias}"
                else:
                    alias = base_alias

            select_exprs.append(f"{model_name}_cte.{cte_col_name} AS {alias}")

        # Add metrics
        for metric_ref in metrics:
            if "." in metric_ref:
                # It's a measure reference (model.measure)
                model_name, measure_name = metric_ref.split(".")
                model = self.graph.get_model(model_name)
                measure = model.get_metric(measure_name)

                if measure:
                    # Check for custom alias first
                    if metric_ref in aliases:
                        alias = aliases[metric_ref]
                    else:
                        # Add model prefix if there's a collision
                        if has_collision.get(measure_name, False):
                            alias = f"{model_name}_{measure_name}"
                        else:
                            alias = measure_name

                    # Complex metric types (derived, ratio) can be built inline
                    # Note: cumulative, time_comparison, conversion are handled via special query generators
                    # and won't appear in this code path
                    if measure.type in ["derived", "ratio"]:
                        # Use complex metric builder
                        metric_expr = self._build_metric_sql(measure)
                        metric_expr = self._wrap_with_fill_nulls(metric_expr, measure)
                        select_exprs.append(f"{metric_expr} AS {alias}")
                    elif not measure.agg:
                        # Complex types that need special handling (shouldn't reach here normally)
                        raise ValueError(
                            f"Metric '{measure.name}' with type '{measure.type}' cannot be queried directly. "
                            f"Use generate() instead of _build_main_select() for this metric type."
                        )
                    elif ungrouped:
                        # For ungrouped queries, select raw column without aggregation
                        select_exprs.append(f"{model_name}_cte.{measure_name}_raw AS {alias}")
                    else:
                        # Simple aggregation measures
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
                                dialect=self.dialect,
                            )
                        else:
                            # Regular aggregation
                            agg_func = measure.agg.upper()
                            if agg_func == "COUNT_DISTINCT":
                                agg_func = "COUNT(DISTINCT"
                                agg_expr = f"{agg_func} {model_name}_cte.{measure_name}_raw)"
                            else:
                                agg_expr = f"{agg_func}({model_name}_cte.{measure_name}_raw)"

                        select_exprs.append(f"{agg_expr} AS {alias}")
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

                        # Use INNER JOIN if this model has filters applied, otherwise LEFT JOIN
                        join_type = "inner" if jp.to_model in models_with_filters else "left"
                        query = query.join(right_table, on=join_cond, join_type=join_type)
                        joined_models.add(jp.to_model)

        # Collect metric-level filters (these are row-level filters, go in WHERE)
        metric_filters = []
        for metric_ref in metrics:
            if "." in metric_ref:
                # model.measure format
                model_name, measure_name = metric_ref.split(".")
                model = self.graph.get_model(model_name)
                if model:
                    measure = model.get_metric(measure_name)
                    if measure and measure.filters:
                        # Add metric-level filters with proper table alias
                        for f in measure.filters:
                            # Replace {model} placeholder with actual CTE alias
                            aliased_filter = f.replace("{model}", f"{model_name}_cte")
                            metric_filters.append(aliased_filter)
            else:
                # Just metric name
                metric = self.graph.get_metric(metric_ref)
                if metric and metric.filters:
                    # Need to determine which model this metric references
                    deps = metric.get_dependencies(self.graph)
                    for dep in deps:
                        if "." in dep:
                            dep_model_name = dep.split(".")[0]
                            # Add filters with proper alias
                            for f in metric.filters:
                                aliased_filter = f.replace("{model}", f"{dep_model_name}_cte")
                                metric_filters.append(aliased_filter)
                            break  # Only use first dependency's model for now

        # Separate filters into WHERE (dimension/row-level) and HAVING (metric/aggregation-level)
        # Query-level filters: check if they reference metrics (HAVING) or dimensions (WHERE)
        # Metric-level filters: always WHERE (they're row-level filters defined in Metric.filters)
        where_filters = []
        having_filters = []

        import re

        # Process query-level filters
        for filter_expr in filters or []:
            # Determine if this filter references a metric or dimension
            references_metric = False

            for model_name in [base_model_name] + other_models:
                model_obj = self.graph.get_model(model_name)
                # Check if filter contains model.metric_name
                pattern = f"{model_name}\\.([a-zA-Z_][a-zA-Z0-9_]*)"
                matches = re.findall(pattern, filter_expr)
                for field_name in matches:
                    if model_obj.get_metric(field_name):
                        references_metric = True
                        break
                if references_metric:
                    break

            if references_metric:
                having_filters.append(filter_expr)
            else:
                where_filters.append(filter_expr)

        # Metric-level filters always go to WHERE (they're row-level filters)
        where_filters.extend(metric_filters)

        # Add WHERE clause (dimension filters and metric-level row filters)
        if where_filters:
            # Parse filters to add table aliases and handle measure vs dimension columns
            for filter_expr in where_filters:
                parsed_filter = filter_expr
                for model_name in [base_model_name] + other_models:
                    # Replace model.field references
                    # Check if field is a measure (needs _raw suffix) or dimension
                    model_obj = self.graph.get_model(model_name)

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
        # Skip GROUP BY for ungrouped queries
        if parsed_dims and not ungrouped:
            group_by_positions = list(range(1, len(parsed_dims) + 1))
            query = query.group_by(*group_by_positions)

        # Add HAVING clause (metric filters applied after aggregation)
        if having_filters:
            for filter_expr in having_filters:
                # Replace model.metric_name with metric_name (the aggregated column alias)
                parsed_having = filter_expr
                for model_name in [base_model_name] + other_models:
                    model_obj = self.graph.get_model(model_name)
                    # Replace model.field with just field (aggregated column name)
                    pattern = f"{model_name}\\.([a-zA-Z_][a-zA-Z0-9_]*)"

                    def replace_metric_ref(match):
                        field_name = match.group(1)
                        # Just use the metric name (it's the SELECT alias)
                        return field_name

                    parsed_having = re.sub(pattern, replace_metric_ref, parsed_having)

                query = query.having(parsed_having)

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

    def _calculate_lag_offset(self, comparison_type: str | None, time_granularity: str | None) -> int:
        """Calculate LAG offset based on comparison type and time dimension granularity.

        Args:
            comparison_type: Type of comparison (yoy, mom, wow, dod, qoq)
            time_granularity: Time dimension granularity (day, week, month, quarter, year)
                             None means use the default offset for the comparison type

        Returns:
            Number of rows to offset for LAG function
        """
        if not comparison_type:
            return 1

        # When no explicit granularity is specified, use a sensible default
        # This assumes the data is at a matching granularity (e.g., monthly data for YoY = 12 rows)
        if not time_granularity:
            default_offsets = {
                "dod": 1,
                "wow": 1,
                "mom": 1,
                "qoq": 1,
                "yoy": 12,  # Assume monthly data for YoY
                "prior_period": 1,
            }
            return default_offsets.get(comparison_type, 1)

        # For simple comparison types, the offset depends on the granularity
        # e.g., YoY with monthly data = 12 months back, YoY with daily data = 365 days back
        offset_map = {
            # Day-over-day is always 1 period back
            "dod": {"day": 1, "week": 1, "month": 1, "quarter": 1, "year": 1},
            # Week-over-week
            "wow": {"day": 7, "week": 1, "month": 1, "quarter": 1, "year": 1},
            # Month-over-month
            "mom": {"day": 30, "week": 4, "month": 1, "quarter": 1, "year": 1},
            # Quarter-over-quarter
            "qoq": {"day": 90, "week": 13, "month": 3, "quarter": 1, "year": 1},
            # Year-over-year
            "yoy": {"day": 365, "week": 52, "month": 12, "quarter": 4, "year": 1},
            # Prior period (default to 1)
            "prior_period": {"day": 1, "week": 1, "month": 1, "quarter": 1, "year": 1},
        }

        if comparison_type in offset_map:
            return offset_map[comparison_type].get(time_granularity, 1)

        return 1

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

            # Sort dependencies by length descending to avoid partial matches
            # (e.g., replace "gross_revenue" before "revenue")
            sorted_deps = sorted(dependencies, key=len, reverse=True)

            # Replace each metric reference with its SQL expression
            for metric_name in sorted_deps:
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

                # Replace metric name in formula using word boundaries to avoid partial matches
                # Use regex to only replace whole word matches
                import re

                # Escape special regex characters in metric_name except dots
                pattern = re.escape(metric_name)
                # Use word boundaries, but handle dots specially for model.measure format
                if "." in metric_name:
                    # For model.measure, we want exact matches
                    pattern = r"\b" + pattern.replace(r"\.", r"\.") + r"\b"
                else:
                    # For simple names, use word boundaries
                    pattern = r"\b" + pattern + r"\b"

                formula = re.sub(pattern, f"({metric_sql})", formula)

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
            metric_name: Name of the conversion metric (can be "metric" or "model.metric" format)
            dimensions: List of dimension references
            filters: List of filter expressions
            order_by: List of fields to order by
            limit: Maximum number of rows to return

        Returns:
            SQL query string
        """
        # Handle both "metric" and "model.metric" formats
        metric = None
        model = None

        if "." in metric_name:
            # model.metric format
            model_name, measure_name = metric_name.split(".", 1)
            model = self.graph.get_model(model_name)
            if model:
                metric = model.get_metric(measure_name)
        else:
            # Just metric name - try graph-level metric
            try:
                metric = self.graph.get_metric(metric_name)
            except KeyError:
                pass

        if not metric or not metric.entity or not metric.base_event or not metric.conversion_event:
            raise ValueError(f"Conversion metric {metric_name} missing required fields")

        # Find the model that owns this metric if we haven't already
        if not model:
            # First, try to find which model has this conversion metric defined
            for m_name, m in self.graph.models.items():
                if m.get_metric(metric_name):
                    model = m
                    break

            # If not found in any model, try to find a model with matching entity dimension
            if not model:
                for m_name, m in self.graph.models.items():
                    # Check if this model has a dimension matching the entity
                    for dim in m.dimensions:
                        if dim.name == metric.entity:
                            model = m
                            break
                    if model:
                        break

        if not model:
            raise ValueError(f"No model found for conversion metric {metric_name}")

        # Build SQL with self-join pattern
        # base_events: filter for base_event
        # conversion_events: filter for conversion_event
        # Join on entity where conversion is within window

        window_parts = metric.conversion_window.split() if metric.conversion_window else ["7", "days"]
        window_num, window_unit = (
            window_parts[0],
            window_parts[1] if len(window_parts) > 1 else "days",
        )

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

        # Build FROM clause - handle both SQL and table-backed models
        if model.sql:
            from_clause = f"({model.sql}) AS t"
        else:
            from_clause = model.table

        sql = f"""
WITH base_events AS (
  SELECT
    {metric.entity} AS entity,
    {timestamp_dim} AS event_time
  FROM {from_clause}
  WHERE {event_type_dim} = '{metric.base_event}'
),
conversion_events AS (
  SELECT
    {metric.entity} AS entity,
    {timestamp_dim} AS event_time
  FROM {from_clause}
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
        aliases: dict[str, str] | None = None,
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
            aliases: Custom aliases for fields (dict mapping field reference to alias)

        Returns:
            SQL query string
        """
        aliases = aliases or {}
        # Separate window function metrics from regular metrics
        cumulative_metrics = []
        time_comparison_metrics = []
        offset_ratio_metrics = []
        conversion_metrics = []
        base_metrics = []

        for m in metrics:
            # Check both graph-level metrics (no dot) and model.measure format
            metric = None
            if "." not in m:
                # Graph-level metric
                metric = self.graph.get_metric(m)
            else:
                # model.measure format - check if it's a metric on the model
                model_name, measure_name = m.split(".", 1)
                model = self.graph.get_model(model_name)
                if model:
                    metric = model.get_metric(measure_name)

            # Classify metric by type
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
                # Regular metric or measure
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

        # Add time comparison metrics with LAG window functions
        # Note: We'll handle these with a CTE approach similar to offset_ratio_metrics
        # to avoid nested window functions

        # Handle offset ratio metrics OR time comparison metrics - need two-step CTEs
        if offset_ratio_metrics or time_comparison_metrics:
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

            # Add LAG expressions for each time comparison metric
            for m in time_comparison_metrics:
                metric = self.graph.get_metric(m)
                if not metric or not metric.base_metric:
                    continue

                # Find time dimension
                time_dim = None
                time_dim_gran = None
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
                                    time_dim_gran = gran
                                break

                if not time_dim:
                    raise ValueError(f"Time comparison metric {m} requires a time dimension")

                # Get base metric alias
                base_ref = metric.base_metric
                if "." in base_ref:
                    base_alias = base_ref.split(".")[1]
                else:
                    base_alias = base_ref

                # Calculate LAG offset
                lag_offset = self._calculate_lag_offset(metric.comparison_type, time_dim_gran)

                # Add LAG for base metric
                lag_selects.append(f"LAG(base.{base_alias}, {lag_offset}) OVER (ORDER BY {time_dim}) AS {m}_prev_value")

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
            lag_selects_str = ",\n    ".join(lag_selects)
            lag_cte_sql = f"WITH lag_cte AS (\n  SELECT\n    {lag_selects_str}\n  FROM (\n{inner_query}\n  ) AS base\n)"

            # Now build final select from lag_cte - need to rebuild select_exprs without base. prefix
            final_selects = []

            # Re-add dimensions and base metrics from lag_cte
            for col in lag_cte_columns:
                final_selects.append(col)

            # Add time comparison metrics
            for m in time_comparison_metrics:
                metric = self.graph.get_metric(m)
                if not metric:
                    continue

                # Get base metric alias
                base_ref = metric.base_metric
                if "." in base_ref:
                    base_alias = base_ref.split(".")[1]
                else:
                    base_alias = base_ref

                # Build calculation based on calculation type
                calc_type = metric.calculation or "percent_change"
                if calc_type == "difference":
                    expr = f"({base_alias} - {m}_prev_value) AS {m}"
                elif calc_type == "percent_change":
                    expr = f"(({base_alias} - {m}_prev_value) / NULLIF({m}_prev_value, 0) * 100) AS {m}"
                elif calc_type == "ratio":
                    expr = f"({base_alias} / NULLIF({m}_prev_value, 0)) AS {m}"
                else:
                    raise ValueError(f"Unknown calculation type: {calc_type}")

                final_selects.append(expr)

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
            final_select_str = ",\n  ".join(final_selects)
            outer_query = f"{lag_cte_sql}\nSELECT\n  {final_select_str}\nFROM lag_cte"
        else:
            # Build outer query without LAG CTE
            select_expr_str = ",\n  ".join(select_exprs)
            outer_query = f"SELECT\n  {select_expr_str}\nFROM (\n{inner_query}\n) AS base"

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

    def _try_use_preaggregation(
        self,
        model_name: str,
        metrics: list[str],
        parsed_dims: list[tuple[str, str | None]],
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> str | None:
        """Try to generate query using a pre-aggregation.

        Args:
            model_name: Name of the model to query
            metrics: List of metric references
            parsed_dims: Parsed dimension references with granularities
            filters: List of filter expressions
            order_by: List of fields to order by
            limit: Maximum number of rows
            offset: Number of rows to skip

        Returns:
            SQL query string if pre-aggregation found, None otherwise
        """
        model = self.graph.get_model(model_name)
        if not model or not model.pre_aggregations:
            return None

        # Extract dimension names (without model prefix) and find time granularity
        dim_names = []
        time_granularity = None

        for dim_ref, gran in parsed_dims:
            # Remove model prefix if present
            if "." in dim_ref:
                dim_name = dim_ref.split(".", 1)[1]
            else:
                dim_name = dim_ref

            dim_names.append(dim_name)

            # Track time granularity for matching
            if gran:
                time_granularity = gran

        # Extract metric names (without model prefix)
        metric_names = []
        for m in metrics:
            if "." in m:
                metric_name = m.split(".", 1)[1]
            else:
                metric_name = m
            metric_names.append(metric_name)

        # Try to find matching pre-aggregation
        # Need to extract filter column names (without model prefix) for compatibility checking
        filter_exprs = []
        if filters:
            for f in filters:
                # Strip model prefix for filter compatibility check
                # e.g., "orders.status = 'completed'" -> "status = 'completed'"
                filter_expr = f.replace(f"{model_name}.", "").replace(f"{model_name}_cte.", "")
                filter_exprs.append(filter_expr)

        matcher = PreAggregationMatcher(model)
        preagg = matcher.find_matching_preagg(
            metrics=metric_names,
            dimensions=dim_names,
            time_granularity=time_granularity,
            filters=filter_exprs,
        )

        if not preagg:
            return None

        # Generate SQL against pre-aggregation table
        return self._generate_from_preaggregation(
            model=model,
            preagg=preagg,
            metrics=metrics,
            parsed_dims=parsed_dims,
            filters=filters,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    def _generate_from_preaggregation(
        self,
        model,
        preagg,
        metrics: list[str],
        parsed_dims: list[tuple[str, str | None]],
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> str:
        """Generate SQL query from a pre-aggregation.

        Args:
            model: The model containing the pre-aggregation
            preagg: The pre-aggregation to use
            metrics: List of metric references
            parsed_dims: Parsed dimension references with granularities
            filters: List of filter expressions
            order_by: List of fields to order by
            limit: Maximum number of rows
            offset: Number of rows to skip

        Returns:
            SQL query string
        """
        preagg_table = preagg.get_table_name(model.name)

        # Build SELECT clause
        select_exprs = []

        # Add dimensions
        for dim_ref, gran in parsed_dims:
            # Get dimension name without model prefix
            if "." in dim_ref:
                dim_name = dim_ref.split(".", 1)[1]
            else:
                dim_name = dim_ref

            # Check if this is the time dimension and needs granularity conversion
            if gran and preagg.time_dimension == dim_name:
                # Need to convert from pre-agg granularity to query granularity
                preagg_col = f"{dim_name}_{preagg.granularity}"

                if gran == preagg.granularity:
                    # Exact match - use as is
                    select_exprs.append(f"{preagg_col} as {dim_name}")
                else:
                    # Roll up to coarser granularity
                    select_exprs.append(f"DATE_TRUNC('{gran}', {preagg_col}) as {dim_name}")
            else:
                # Regular dimension - use as is
                select_exprs.append(f"{dim_name}")

        # Add metrics
        for metric_ref in metrics:
            # Get metric name without model prefix
            if "." in metric_ref:
                metric_name = metric_ref.split(".", 1)[1]
            else:
                metric_name = metric_ref

            metric = model.get_metric(metric_name)
            if not metric:
                continue

            # Get the raw column name from pre-agg
            raw_col = f"{metric_name}_raw"

            # Determine aggregation based on metric type
            if metric.agg == "sum":
                select_exprs.append(f"SUM({raw_col}) as {metric_name}")
            elif metric.agg == "count":
                select_exprs.append(f"SUM({raw_col}) as {metric_name}")
            elif metric.agg == "avg":
                # AVG = SUM(sum_raw) / SUM(count_raw)
                # Need to find the correct count measure from pre-agg
                from sidemantic.core.preagg_matcher import PreAggregationMatcher

                matcher = PreAggregationMatcher(model)
                count_measure = matcher._find_count_measure_for_avg(metric, preagg.measures or [])

                if not count_measure:
                    # Fallback to hard-coded count_raw (old behavior)
                    count_measure = "count"

                sum_col = f"{metric_name}_raw"
                count_col = f"{count_measure}_raw"
                select_exprs.append(f"SUM({sum_col}) / NULLIF(SUM({count_col}), 0) as {metric_name}")
            elif metric.agg in ["min", "max"]:
                select_exprs.append(f"{metric.agg.upper()}({raw_col}) as {metric_name}")
            else:
                # Default to SUM
                select_exprs.append(f"SUM({raw_col}) as {metric_name}")

        # Build FROM clause
        from_clause = preagg_table

        # Build WHERE clause if filters exist
        where_clause = ""
        if filters:
            # Need to rewrite filters to use pre-agg column names
            rewritten_filters = []
            for f in filters:
                # Replace model_cte. with nothing (pre-agg table doesn't use CTEs)
                # Also replace model. with nothing
                rewritten_f = f.replace(f"{model.name}_cte.", "").replace(f"{model.name}.", "")

                # If this filter references the time dimension, map it to the pre-agg time column
                # e.g., created_at -> created_at_day for daily pre-agg
                if preagg.time_dimension and preagg.granularity:
                    time_col_name = f"{preagg.time_dimension}_{preagg.granularity}"
                    # Replace time dimension name with time column name
                    import re

                    # Match time dimension as a whole word (not part of another word)
                    rewritten_f = re.sub(r"\b" + re.escape(preagg.time_dimension) + r"\b", time_col_name, rewritten_f)

                rewritten_filters.append(rewritten_f)

            where_clause = f"\nWHERE {' AND '.join(rewritten_filters)}"

        # Build GROUP BY clause
        group_by_exprs = []
        for i in range(1, len(parsed_dims) + 1):
            group_by_exprs.append(str(i))

        group_by_clause = ""
        if group_by_exprs:
            group_by_clause = f"\nGROUP BY {', '.join(group_by_exprs)}"

        # Build ORDER BY clause
        order_by_clause = ""
        if order_by:
            order_clauses = []
            for field in order_by:
                if "." in field:
                    field_name = field.split(".", 1)[1]
                else:
                    field_name = field
                order_clauses.append(field_name)
            order_by_clause = f"\nORDER BY {', '.join(order_clauses)}"

        # Build LIMIT/OFFSET clause
        limit_clause = ""
        if limit:
            limit_clause = f"\nLIMIT {limit}"
        if offset:
            limit_clause += f"\nOFFSET {offset}"

        # Combine into final query
        select_exprs_str = ",\n  ".join(select_exprs)
        query = f"""SELECT
  {select_exprs_str}
FROM {from_clause}{where_clause}{group_by_clause}{order_by_clause}{limit_clause}"""

        return query

    def _generate_instrumentation_comment(
        self, models: list[str], metrics: list[str], dimensions: list[str], used_preagg: bool = False
    ) -> str:
        """Generate instrumentation comment for query analysis.

        Args:
            models: List of model names used in query
            metrics: List of metric references
            dimensions: List of dimension references
            used_preagg: Whether this query used a pre-aggregation

        Returns:
            SQL comment string with metadata
        """
        # Extract granularities from dimensions
        granularities = set()
        clean_dims = []
        for dim in dimensions:
            if "__" in dim:
                parts = dim.rsplit("__", 1)
                clean_dims.append(parts[0])
                granularities.add(parts[1])
            else:
                clean_dims.append(dim)

        # Build metadata
        parts = []
        parts.append(f"models={','.join(sorted(models))}")
        parts.append(f"metrics={','.join(sorted(metrics))}")
        parts.append(f"dimensions={','.join(sorted(clean_dims))}")

        if granularities:
            parts.append(f"granularities={','.join(sorted(granularities))}")

        if used_preagg:
            parts.append("used_preagg=true")

        return f"-- sidemantic: {' '.join(parts)}"
