"""SQL generation using SQLGlot builder API."""

import sqlglot
from sqlglot import exp, select

from sidemantic.core.preagg_matcher import PreAggregationMatcher
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.symmetric_aggregate import build_symmetric_aggregate_sql


class SQLGenerator:
    """Generates SQL queries from semantic layer definitions using SQLGlot builder API."""

    def __init__(
        self,
        graph: SemanticGraph,
        dialect: str = "duckdb",
        preagg_database: str | None = None,
        preagg_schema: str | None = None,
    ):
        """Initialize SQL generator.

        Args:
            graph: Semantic graph with models and metrics
            dialect: SQL dialect for generation (default: duckdb)
            preagg_database: Optional database name for pre-aggregation tables
            preagg_schema: Optional schema name for pre-aggregation tables
        """
        self.graph = graph
        self.dialect = dialect
        self.preagg_database = preagg_database
        self.preagg_schema = preagg_schema

    def _date_trunc(self, granularity: str, column_expr: str) -> str:
        """Generate dialect-specific DATE_TRUNC expression.

        Args:
            granularity: Time granularity (hour, day, week, month, quarter, year)
            column_expr: SQL column expression

        Returns:
            DATE_TRUNC SQL expression appropriate for the dialect
        """
        # Handle {model} placeholder or complex expressions - fall back to string
        if "{" in column_expr or "(" in column_expr:
            # BigQuery: DATE_TRUNC(col, MONTH), others: DATE_TRUNC('month', col)
            if self.dialect == "bigquery":
                return f"DATE_TRUNC({column_expr}, {granularity.upper()})"
            else:
                return f"DATE_TRUNC('{granularity}', {column_expr})"

        # Parse the column expression to handle table.column references
        col = sqlglot.parse_one(column_expr, into=exp.Column, dialect=self.dialect)
        date_trunc = exp.DateTrunc(this=col, unit=exp.Literal.string(granularity))
        return date_trunc.sql(dialect=self.dialect)

    def _quote_alias(self, name: str) -> str:
        """Quote an identifier for use as a SQL alias.

        Handles names with dots or other special characters by quoting them.

        Args:
            name: The identifier name (e.g., "auctions.bid_request_cnt_wow")

        Returns:
            Properly quoted identifier for the dialect (e.g., '"auctions.bid_request_cnt_wow"')
        """
        return sqlglot.to_identifier(name, quoted=True).sql(dialect=self.dialect)

    def _apply_default_time_dimensions(self, metrics: list[str], dimensions: list[str]) -> list[str]:
        """Auto-include default_time_dimension from models if not already present.

        If a model has default_time_dimension set and no time dimension from that
        model is already in the dimensions list, add it with the default_grain.

        Args:
            metrics: List of metric references
            dimensions: List of dimension references

        Returns:
            Updated dimensions list with default time dimensions added
        """
        # Extract which models already have time dimensions in the query
        models_with_time_dims = set()
        for dim_ref in dimensions:
            if "." in dim_ref:
                model_name, dim_part = dim_ref.split(".", 1)
                # Strip granularity suffix if present
                dim_name = dim_part.split("__")[0]
                model = self.graph.get_model(model_name)
                if model:
                    dim = model.get_dimension(dim_name)
                    if dim and dim.type == "time":
                        models_with_time_dims.add(model_name)

        # Check each model referenced by metrics for default_time_dimension
        added_dims = []
        models_checked = set()
        for metric_ref in metrics:
            if "." in metric_ref:
                model_name, _ = metric_ref.split(".")
                if model_name in models_checked:
                    continue
                models_checked.add(model_name)

                # Try to get model - may not exist if this is a graph-level metric
                # with a dotted name (not model.measure format)
                try:
                    model = self.graph.get_model(model_name)
                except KeyError:
                    model = None
                if model and model.default_time_dimension:
                    # Only add if this model doesn't already have a time dimension
                    if model_name not in models_with_time_dims:
                        time_dim_ref = f"{model_name}.{model.default_time_dimension}"
                        # Apply default_grain if specified
                        if model.default_grain:
                            time_dim_ref = f"{time_dim_ref}__{model.default_grain}"
                        if time_dim_ref not in dimensions and time_dim_ref not in added_dims:
                            added_dims.append(time_dim_ref)
                        models_with_time_dims.add(model_name)

        return dimensions + added_dims

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
        dimensions = list(dimensions) if dimensions else []
        filters = filters or []
        segments = segments or []
        parameters = parameters or {}
        aliases = aliases or {}

        # Auto-include default_time_dimension from metrics if not already present
        dimensions = self._apply_default_time_dimensions(metrics, dimensions)

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
                        sql_expr = RelativeDateRange.parse(value, self.dialect)
                        processed_filters.append(f"{column} {operator} {sql_expr}")
                    elif operator == "=":
                        # For =, use to_range to get proper range
                        range_expr = RelativeDateRange.to_range(value, column.strip(), self.dialect)
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
                try:
                    model = self.graph.get_model(model_name)
                    if model:
                        metric = model.get_metric(measure_name)
                except KeyError:
                    pass
                # Fall back to graph-level metric with dotted name
                if not metric:
                    try:
                        metric = self.graph.get_metric(m)
                    except KeyError:
                        pass
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

        # Check if we need symmetric aggregation (pre-aggregation approach)
        # This is needed when metrics come from different models at different join levels
        if self._needs_preaggregation_for_fanout(metrics, dimensions):
            return self._generate_with_preaggregation(
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                segments=segments,
                order_by=order_by,
                limit=limit,
                offset=offset,
                aliases=aliases,
            )

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

        # Extract columns needed for metric-level filters (before building CTEs)
        metric_filter_cols_by_model = self._extract_metric_filter_columns(metrics)

        # Build CTEs for all models with pushed-down filters
        cte_sqls = []
        for model_name in all_models:
            model_filters = pushdown_filters.get(model_name, [])
            metric_filter_cols = metric_filter_cols_by_model.get(model_name)
            cte_sql = self._build_model_cte(
                model_name,
                parsed_dims,
                metrics,
                model_filters if model_filters else None,
                order_by=order_by,
                all_models=all_models,
                metric_filter_columns=metric_filter_cols,
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

        def qualify_unaliased_columns(filter_sql: str, model_alias: str) -> str:
            """Qualify unaliased columns in segment filters with model alias."""
            try:
                parsed = sqlglot.parse_one(filter_sql, dialect=self.dialect)
            except Exception:
                return filter_sql

            def visit(node: exp.Expression) -> None:
                if isinstance(node, exp.Subquery):
                    return

                if isinstance(node, exp.Column) and not node.table:
                    node.set("table", model_alias)

                for arg in node.args.values():
                    if isinstance(arg, exp.Expression):
                        visit(arg)
                    elif isinstance(arg, list):
                        for item in arg:
                            if isinstance(item, exp.Expression):
                                visit(item)

            visit(parsed)

            return parsed.sql(dialect=self.dialect)

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
            filter_sql = qualify_unaliased_columns(filter_sql, f"{model_name}_cte")
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

    def _extract_metric_filter_columns(self, metrics: list[str]) -> dict[str, set[str]]:
        """Extract columns referenced in metric-level filters and SQL expressions.

        Recursively extracts filter columns from:
        - Direct measure references (model.measure)
        - Graph-level metrics with filters
        - Ratio/derived metrics that reference measures with filters

        Args:
            metrics: List of metric references (e.g., ["orders.revenue", "bookings.gross_value"])

        Returns:
            Dict mapping model_name -> set of column names needed for metric filters and SQL expressions
        """
        columns_by_model: dict[str, set[str]] = {}

        def add_filter_columns(model_name: str, filters: list[str]):
            """Extract columns from filter expressions and add to columns_by_model."""
            if model_name not in columns_by_model:
                columns_by_model[model_name] = set()
            for f in filters:
                aliased_filter = f.replace("{model}", f"{model_name}_cte")
                try:
                    parsed = sqlglot.parse_one(aliased_filter, dialect=self.dialect)
                    for col in parsed.find_all(exp.Column):
                        if col.table and col.table.replace("_cte", "") == model_name:
                            columns_by_model[model_name].add(col.name)
                except Exception:
                    pass

        def extract_from_measure_ref(metric_ref: str):
            """Extract filter columns from a model.measure reference."""
            if "." not in metric_ref:
                return
            model_name, measure_name = metric_ref.split(".")
            model = self.graph.get_model(model_name)
            if model:
                measure = model.get_metric(measure_name)
                if measure:
                    # Check the measure's own filters
                    if measure.filters:
                        add_filter_columns(model_name, measure.filters)
                    # If measure is a ratio/derived, recursively check dependencies
                    if measure.type == "ratio":
                        if measure.numerator:
                            extract_from_measure_ref(measure.numerator)
                        if measure.denominator:
                            extract_from_measure_ref(measure.denominator)
                    elif measure.type == "derived" or (not measure.type and not measure.agg and measure.sql):
                        # For derived metrics, also extract columns from the SQL expression itself
                        # This handles inline SQL like: COUNT(CASE WHEN {model}.status = 'approved' THEN 1 END)
                        if measure.sql:
                            aliased_sql = measure.sql.replace("{model}", f"{model_name}_cte")
                            try:
                                parsed = sqlglot.parse_one(aliased_sql, dialect=self.dialect)
                                for col in parsed.find_all(exp.Column):
                                    if col.table and col.table.replace("_cte", "") == model_name:
                                        if model_name not in columns_by_model:
                                            columns_by_model[model_name] = set()
                                        columns_by_model[model_name].add(col.name)
                            except Exception:
                                pass
                        # Also check dependencies
                        deps = measure.get_dependencies(self.graph, model_name)
                        for dep in deps:
                            if "." in dep:
                                extract_from_measure_ref(dep)
                            else:
                                try:
                                    dep_metric = self.graph.get_metric(dep)
                                    extract_from_metric(dep_metric)
                                except KeyError:
                                    pass

        def extract_from_metric(metric):
            """Recursively extract filter columns from a metric and its dependencies."""
            # Extract from the metric's own filters
            if metric.filters:
                deps = metric.get_dependencies(self.graph)
                for dep in deps:
                    if "." in dep:
                        dep_model_name = dep.split(".")[0]
                        add_filter_columns(dep_model_name, metric.filters)
                        break

            # For ratio metrics, check numerator and denominator
            if metric.type == "ratio":
                if metric.numerator:
                    extract_from_measure_ref(metric.numerator)
                if metric.denominator:
                    extract_from_measure_ref(metric.denominator)

            # For derived metrics, check all dependencies
            elif metric.type == "derived" or (not metric.type and not metric.agg and metric.sql):
                deps = metric.get_dependencies(self.graph)
                for dep in deps:
                    if "." in dep:
                        extract_from_measure_ref(dep)
                    else:
                        # Recursively check graph-level metric dependencies
                        try:
                            dep_metric = self.graph.get_metric(dep)
                            extract_from_metric(dep_metric)
                        except KeyError:
                            pass

        for metric_ref in metrics:
            if "." in metric_ref:
                # model.measure format - extract directly
                extract_from_measure_ref(metric_ref)
            else:
                # Graph-level metric - recursively extract
                try:
                    metric = self.graph.get_metric(metric_ref)
                    extract_from_metric(metric)
                except KeyError:
                    pass

        return columns_by_model

    def _find_needed_dimensions(
        self,
        model_name: str,
        dimensions: list[tuple[str, str | None]],
        filters: list[str] | None,
        order_by: list[str] | None,
        metric_filter_columns: set[str] | None = None,
    ) -> set[str]:
        """Find which dimensions from this model are actually needed.

        Args:
            model_name: Model to check
            dimensions: Parsed dimension references from query
            filters: Filter expressions
            order_by: Order by fields
            metric_filter_columns: Columns needed for metric-level filters

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

        # Columns needed for metric-level filters
        if metric_filter_columns:
            needed.update(metric_filter_columns)

        return needed

    def _build_model_cte(
        self,
        model_name: str,
        dimensions: list[tuple[str, str | None]],
        metrics: list[str],
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        all_models: set[str] | None = None,
        metric_filter_columns: set[str] | None = None,
    ) -> str:
        """Build CTE SQL for a model with optional filter pushdown.

        Args:
            model_name: Name of the model
            dimensions: Parsed dimension references
            metrics: Metric references
            filters: Filters to push down into this CTE (optional)
            order_by: Order by fields (for determining needed dimensions)
            all_models: All models in query (for determining if joins needed)
            metric_filter_columns: Columns needed for metric-level filters

        Returns:
            CTE SQL string
        """
        model = self.graph.get_model(model_name)
        all_models = all_models or {model_name}
        needs_joins = len(all_models) > 1

        # Find which dimensions are actually needed
        needed_dimensions = self._find_needed_dimensions(
            model_name, dimensions, filters, order_by, metric_filter_columns
        )

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

            for other_model_name, other_model in self.graph.models.items():
                if other_model_name not in all_models:
                    continue
                for other_join in other_model.relationships:
                    if other_join.type != "many_to_many" or other_join.through != model_name:
                        continue
                    junction_self_fk, junction_related_fk = other_join.junction_keys()
                    for fk in (junction_self_fk, junction_related_fk):
                        if fk and fk not in columns_added:
                            select_cols.append(f"{fk} AS {fk}")
                            columns_added.add(fk)

        # Determine table alias for {model} placeholder replacement
        # In CTEs, we're selecting from the raw table (or subquery AS t)
        model_table_alias = "t" if model.sql else ""

        def replace_model_placeholder(sql_expr: str) -> str:
            """Replace {model} placeholder with appropriate table reference."""
            if model_table_alias:
                return sql_expr.replace("{model}", model_table_alias)
            else:
                # No alias needed - just remove {model}.
                return sql_expr.replace("{model}.", "")

        # Add only needed dimension columns
        for dimension in model.dimensions:
            if dimension.name in needed_dimensions and dimension.name not in columns_added:
                # For time dimensions with granularity, apply DATE_TRUNC
                if dimension.type == "time" and dimension.granularity:
                    dim_sql = self._date_trunc(dimension.granularity, dimension.sql_expr)
                else:
                    dim_sql = dimension.sql_expr
                # Replace {model} placeholder with actual table reference
                dim_sql = replace_model_placeholder(dim_sql)
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
                dim_sql = self._date_trunc(gran, replace_model_placeholder(dimension.sql_expr))
                alias = f"{dim_name}__{gran}"
                if alias not in columns_added:
                    select_cols.append(f"{dim_sql} AS {alias}")
                    columns_added.add(alias)

        # Add measure columns (raw, not aggregated in CTE)
        # Collect all measures needed for metrics
        measures_needed = set()

        def collect_measures_from_metric(metric_ref: str, visited: set[str] | None = None):
            """Recursively collect measures needed from a metric.

            Handles:
            - Direct measure references: model.measure
            - Model-level derived measures that reference other measures
            - Unqualified references that need to be resolved within model context
            """
            if visited is None:
                visited = set()

            # Avoid infinite recursion
            if metric_ref in visited:
                return
            visited.add(metric_ref)

            if "." in metric_ref:
                # It's a qualified reference (model.measure)
                ref_model_name, measure_name = metric_ref.split(".", 1)
                if ref_model_name == model_name:
                    # It's for this model - check if it's a derived measure
                    measure = model.get_metric(measure_name)
                    if measure:
                        if measure.type in ("derived", "ratio") or (
                            not measure.type and not measure.agg and measure.sql
                        ):
                            # Derived/ratio measure - get its dependencies
                            for dep in measure.get_dependencies(self.graph, ref_model_name):
                                collect_measures_from_metric(dep, visited)
                        elif measure.agg:
                            # Simple aggregation measure - add it
                            measures_needed.add(measure_name)
            else:
                # Unqualified reference - could be:
                # 1. A graph-level metric
                # 2. A measure on the current model

                # First check if it's a measure on the current model
                measure = model.get_metric(metric_ref)
                if measure:
                    if measure.type in ("derived", "ratio") or (not measure.type and not measure.agg and measure.sql):
                        # Derived/ratio measure - get its dependencies
                        for dep in measure.get_dependencies(self.graph, model_name):
                            collect_measures_from_metric(dep, visited)
                    elif measure.agg:
                        # Simple aggregation measure - add it
                        measures_needed.add(metric_ref)
                else:
                    # Try as graph-level metric
                    try:
                        metric = self.graph.get_metric(metric_ref)
                        if metric:
                            # Use auto dependency detection with graph for resolution
                            for dep in metric.get_dependencies(self.graph, model_name):
                                collect_measures_from_metric(dep, visited)
                    except KeyError:
                        pass

        for metric_ref in metrics:
            collect_measures_from_metric(metric_ref)

        # Also include measure columns referenced in metric_filter_columns (for derived metrics
        # with inline SQL aggregations like "SUM(quantity * unit_price) / COUNT(DISTINCT order_id)")
        if metric_filter_columns:
            for col_name in metric_filter_columns:
                # Check if this column is a measure (not a dimension)
                measure = model.get_metric(col_name)
                if measure and measure.agg and col_name not in measures_needed:
                    measures_needed.add(col_name)

        for measure_name in measures_needed:
            measure = model.get_metric(measure_name)
            if measure:
                # Build the base SQL expression for the measure
                if measure.agg == "count" and not measure.sql:
                    base_sql = "1"
                elif measure.agg == "count_distinct" and not measure.sql:
                    pk = model.primary_key or "id"
                    base_sql = pk
                else:
                    base_sql = replace_model_placeholder(measure.sql_expr)

                # Apply measure filters if present (wrap in CASE WHEN)
                if measure.filters:
                    # Filters are SQL conditions like "{model}.field = 'value'"
                    # Replace {model} placeholder and combine into CASE WHEN
                    filter_conditions = []
                    for filter_str in measure.filters:
                        # Replace {model} with nothing since we're in the CTE selecting from raw table
                        filter_sql = filter_str.replace("{model}.", "").replace("{model}", "")
                        filter_conditions.append(filter_sql)

                    if filter_conditions:
                        filter_sql = " AND ".join(filter_conditions)
                        # For count measures, return 1 if condition met, else NULL
                        # COUNT counts non-NULL values, so we need NULL to exclude non-matching rows
                        if measure.agg == "count":
                            measure_sql = f"CASE WHEN {filter_sql} THEN 1 ELSE NULL END"
                        else:
                            measure_sql = f"CASE WHEN {filter_sql} THEN {base_sql} ELSE NULL END"
                    else:
                        measure_sql = base_sql
                else:
                    measure_sql = base_sql

                select_cols.append(f"{measure_sql} AS {measure_name}_raw")

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

    def _needs_preaggregation_for_fanout(self, metrics: list[str], dimensions: list[str]) -> bool:
        """Determine if pre-aggregation is needed to avoid fan-out.

        Pre-aggregation is needed when:
        1. Metrics come from multiple different models
        2. Those models are at different levels in the join chain
        3. A join between them would cause one model's metrics to be over-counted

        For example: employees.total_salary + departments.total_budget by companies.name
        The join path is: companies -> departments -> employees
        When employees join to departments, each department row is replicated per employee,
        causing department budgets to be summed multiple times.

        Args:
            metrics: List of metric references (e.g., ["employees.total_salary", "departments.total_budget"])
            dimensions: List of dimension references (e.g., ["companies.name"])

        Returns:
            True if pre-aggregation is needed
        """
        if not metrics or len(metrics) < 2:
            return False

        # Get unique metric models
        metric_models = set()
        for metric_ref in metrics:
            if "." in metric_ref:
                model_name = metric_ref.split(".")[0]
                metric_models.add(model_name)

        if len(metric_models) < 2:
            return False

        # Check if any pair of metric models would cause fan-out
        # Fan-out occurs when model A joins to model B via a path that includes
        # a many_to_one relationship from B's perspective (one_to_many from A's)
        metric_model_list = list(metric_models)
        for i, model_a in enumerate(metric_model_list):
            for model_b in metric_model_list[i + 1 :]:
                try:
                    # Check path from A to B
                    join_path = self.graph.find_relationship_path(model_a, model_b)
                    if join_path:
                        # If any hop is many_to_one (from A's perspective), model_a metrics
                        # would be replicated when joining to model_b
                        for jp in join_path:
                            if jp.relationship == "many_to_one":
                                # model_a is on the "many" side, so its rows fan out
                                # when we aggregate model_b metrics
                                return True

                    # Check reverse path
                    join_path_reverse = self.graph.find_relationship_path(model_b, model_a)
                    if join_path_reverse:
                        for jp in join_path_reverse:
                            if jp.relationship == "many_to_one":
                                return True

                except (ValueError, KeyError):
                    pass

        return False

    def _generate_with_preaggregation(
        self,
        metrics: list[str],
        dimensions: list[str],
        filters: list[str] | None = None,
        segments: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        aliases: dict[str, str] | None = None,
    ) -> str:
        """Generate SQL using pre-aggregation to avoid fan-out.

        This generates separate queries for each metric model, pre-aggregated
        to the dimension grain, then joins them together.

        Args:
            metrics: List of metric references
            dimensions: List of dimension references
            filters: List of filter expressions
            segments: List of segment references
            order_by: List of fields to order by
            limit: Maximum number of rows
            offset: Number of rows to skip
            aliases: Custom aliases for fields

        Returns:
            SQL query string
        """
        aliases = aliases or {}
        parsed_dims = self._parse_dimension_refs(dimensions)

        # Group metrics by their model
        metrics_by_model: dict[str, list[str]] = {}
        for metric_ref in metrics:
            if "." in metric_ref:
                model_name = metric_ref.split(".")[0]
                if model_name not in metrics_by_model:
                    metrics_by_model[model_name] = []
                metrics_by_model[model_name].append(metric_ref)

        if len(metrics_by_model) < 2:
            # Shouldn't happen, but fall back to regular generation
            return self.generate(
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                segments=segments,
                order_by=order_by,
                limit=limit,
                offset=offset,
                aliases=aliases,
            )

        # Resolve segments to SQL filters
        segment_filters = self._resolve_segments(segments or [])
        all_filters = (filters or []) + segment_filters

        # Generate a pre-aggregated CTE for each metric model
        preagg_ctes = []
        cte_names = []

        for model_name, model_metrics in metrics_by_model.items():
            cte_name = f"{model_name}_preagg"
            cte_names.append(cte_name)

            # Generate sub-query for this model's metrics at the dimension grain
            # We call generate() recursively but it won't trigger pre-aggregation
            # again because each sub-query has metrics from only one model
            sub_query = self.generate(
                metrics=model_metrics,
                dimensions=dimensions,
                filters=all_filters,
                segments=None,  # Already resolved
                order_by=None,
                limit=None,
                offset=None,
                aliases=aliases,
            )

            # Remove the instrumentation comment from sub-query
            sub_query_lines = sub_query.split("\n")
            sub_query_clean = "\n".join(
                line for line in sub_query_lines if not line.strip().startswith("-- sidemantic:")
            )

            preagg_ctes.append(f"{cte_name} AS (\n{sub_query_clean}\n)")

        # Build the final SELECT that joins all pre-aggregated CTEs
        select_exprs = []

        # Add dimensions - use COALESCE across all CTEs
        for dim_ref, gran in parsed_dims:
            dim_name = dim_ref.split(".")[1] if "." in dim_ref else dim_ref
            col_name = f"{dim_name}__{gran}" if gran else dim_name

            # Build COALESCE expression
            coalesce_parts = [f"{cte}.{col_name}" for cte in cte_names]
            select_exprs.append(f"COALESCE({', '.join(coalesce_parts)}) AS {col_name}")

        # Check for metric name collisions across models
        metric_name_counts: dict[str, int] = {}
        for model_metrics in metrics_by_model.values():
            for metric_ref in model_metrics:
                metric_name = metric_ref.split(".")[1] if "." in metric_ref else metric_ref
                metric_name_counts[metric_name] = metric_name_counts.get(metric_name, 0) + 1

        # Add metrics from each CTE
        for model_name, model_metrics in metrics_by_model.items():
            cte_name = f"{model_name}_preagg"
            for metric_ref in model_metrics:
                metric_name = metric_ref.split(".")[1] if "." in metric_ref else metric_ref
                # Check for custom alias first
                if metric_ref in aliases:
                    alias = aliases[metric_ref]
                elif metric_name_counts.get(metric_name, 1) > 1:
                    # Collision - prefix with model name
                    alias = f"{model_name}_{metric_name}"
                else:
                    alias = metric_name
                select_exprs.append(f"{cte_name}.{metric_name} AS {alias}")

        # Build FROM clause with FULL OUTER JOINs (or CROSS JOIN if no dimensions)
        # Start with first CTE
        from_clause = cte_names[0]

        # Join remaining CTEs
        join_clauses = []
        for cte_name in cte_names[1:]:
            if not parsed_dims:
                # No dimensions - use CROSS JOIN (each CTE returns single row)
                join_clauses.append(f"CROSS JOIN {cte_name}")
            else:
                # Build join condition on all dimension columns
                join_conditions = []
                for dim_ref, gran in parsed_dims:
                    dim_name = dim_ref.split(".")[1] if "." in dim_ref else dim_ref
                    col_name = f"{dim_name}__{gran}" if gran else dim_name
                    # Use COALESCE to handle NULLs in join condition
                    # Actually for FULL OUTER JOIN, we need to compare the actual columns
                    # and handle NULLs with IS NOT DISTINCT FROM or COALESCE-based comparison
                    join_conditions.append(
                        f"COALESCE({cte_names[0]}.{col_name}, '') = COALESCE({cte_name}.{col_name}, '')"
                    )

                join_clause = " AND ".join(join_conditions)
                join_clauses.append(f"FULL OUTER JOIN {cte_name} ON {join_clause}")

        # Combine into final query
        select_str = ",\n  ".join(select_exprs)
        from_str = from_clause + "\n" + "\n".join(join_clauses)

        final_query = f"SELECT\n  {select_str}\nFROM {from_str}"

        # Add ORDER BY
        if order_by:
            order_clauses = []
            for field in order_by:
                if "." in field:
                    field_name = field.split(".", 1)[1]
                else:
                    field_name = field
                order_clauses.append(field_name)
            final_query += f"\nORDER BY {', '.join(order_clauses)}"

        # Add LIMIT and OFFSET
        if limit:
            final_query += f"\nLIMIT {limit}"
        if offset:
            final_query += f"\nOFFSET {offset}"

        # Combine CTEs and main query
        cte_str = "WITH " + ",\n".join(preagg_ctes)
        full_sql = cte_str + "\n" + final_query

        # Add instrumentation comment
        all_models = list(metrics_by_model.keys())
        instrumentation = self._generate_instrumentation_comment(
            models=all_models, metrics=metrics, dimensions=dimensions, used_preagg=False
        )
        full_sql = full_sql + "\n" + instrumentation

        return full_sql

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
                    # Also handle "expression metrics" - metrics with inline aggregations like SUM(x)/SUM(y)
                    is_expression_metric = not measure.type and not measure.agg and measure.sql
                    if measure.type in ["derived", "ratio"] or is_expression_metric:
                        # Use complex metric builder
                        metric_expr = self._build_metric_sql(measure, model_name)
                        metric_expr = self._wrap_with_fill_nulls(metric_expr, measure)
                        select_exprs.append(f"{metric_expr} AS {alias}")
                    elif not measure.agg:
                        # Unknown metric type that needs special handling
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
                            # Use helper that applies metric-level filters via CASE WHEN
                            # This ensures each metric's filter only affects that metric
                            agg_expr = self._build_measure_aggregation_sql(model_name, measure)

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

        # Separate filters into WHERE (dimension/row-level) and HAVING (metric/aggregation-level)
        # Note: metric-level filters (Metric.filters) are applied via CASE WHEN inside each
        # metric's aggregation, NOT in the WHERE clause. This ensures each metric's filter
        # only affects that specific metric, not all metrics in the query.
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

        # Add WHERE clause (dimension filters only - metric-level filters are in CASE WHEN)
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

    def _build_measure_aggregation_sql(self, model_name: str, measure) -> str:
        """Build SQL aggregation expression for a measure.

        Note: Metric-level filters are already applied in the CTE via CASE WHEN
        on the raw column (see _build_model_cte). The raw column will have NULL
        for rows that don't match the filter. Therefore, we do NOT re-apply
        filters here - we just aggregate the pre-filtered raw column.

        This avoids the issue where re-applying filters on CTE columns could
        reference transformed values (e.g., DATE_TRUNC'd time dimensions)
        instead of the original raw values.

        Args:
            model_name: Name of the model containing the measure
            measure: Metric object representing the measure

        Returns:
            SQL aggregation expression string
        """
        agg_func = measure.agg.upper()
        raw_col = f"{model_name}_cte.{measure.name}_raw"

        # Simple aggregation - filters are already applied in CTE's raw column
        if agg_func == "COUNT_DISTINCT":
            return f"COUNT(DISTINCT {raw_col})"
        else:
            return f"{agg_func}({raw_col})"

    def _build_metric_sql(self, metric, model_context: str | None = None) -> str:
        """Build SQL expression for a metric.

        Args:
            metric: Metric object
            model_context: Optional model name for resolving ambiguous references

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

            # Build numerator and denominator with metric-level filters applied
            num_expr = self._build_measure_aggregation_sql(num_model, num_measure_obj)
            denom_expr = self._build_measure_aggregation_sql(denom_model, denom_measure_obj)

            return f"({num_expr}) / NULLIF({denom_expr}, 0)"

        elif metric.type == "derived" or (not metric.type and not metric.agg and metric.sql):
            # Parse formula and replace metric references (handles both typed "derived" and untyped metrics with sql)
            if not metric.sql:
                raise ValueError(f"Derived metric {metric.name} missing sql")

            formula = metric.sql

            # Check if this is a SQL expression metric (has inline aggregations)
            # These metrics already contain complete SQL and shouldn't have dependencies replaced
            try:
                parsed = sqlglot.parse_one(formula, read=self.dialect)
                agg_types = (exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max, exp.Median)
                has_inline_agg = any(parsed.find_all(*agg_types))
            except Exception:
                has_inline_agg = False

            if has_inline_agg:
                # This is a SQL expression metric with inline aggregations.
                # Column references should already be qualified with {model} placeholder
                # at parse time (e.g., by SnowflakeAdapter).
                # We need to:
                # 1. Replace {model} with the CTE alias
                # 2. Replace measure column references with their _raw suffixed versions

                # Find which model this metric belongs to
                metric_model_name = model_context
                if not metric_model_name:
                    for m_name, m in self.graph.models.items():
                        if m.get_metric(metric.name):
                            metric_model_name = m_name
                            break

                if metric_model_name:
                    cte_alias = f"{metric_model_name}_cte"
                    # Replace {model} placeholder with the CTE alias
                    formula = formula.replace("{model}", cte_alias)

                    # Replace measure columns with their _raw suffixed versions
                    # For each measure in the model, check if it's referenced in the formula
                    model_obj = self.graph.get_model(metric_model_name)
                    if model_obj:
                        for measure in model_obj.metrics:
                            if measure.agg:  # Only process actual aggregation measures
                                # Replace cte_alias.measure_name with cte_alias.measure_name_raw
                                pattern = f"{cte_alias}.{measure.name}"
                                replacement = f"{cte_alias}.{measure.name}_raw"
                                formula = formula.replace(pattern, replacement)

                return formula

            # Auto-detect dependencies from expression using graph for resolution
            dependencies = metric.get_dependencies(self.graph, model_context)

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
                        # Use helper that applies metric-level filters
                        metric_sql = self._build_measure_aggregation_sql(model_name, measure)
                    else:
                        raise ValueError(f"Measure {metric_name} not found")
                else:
                    # Try as graph-level metric
                    try:
                        ref_metric = self.graph.get_metric(metric_name)
                        # Recursively build metric SQL
                        metric_sql = self._build_metric_sql(ref_metric, model_context)
                    except KeyError:
                        raise ValueError(f"Metric {metric_name} not found")

                # Replace metric name in formula using word boundaries to avoid partial matches
                # Use regex to only replace whole word matches
                import re

                # For qualified names (model.measure), also match unqualified version (measure)
                if "." in metric_name:
                    # Split into model and measure parts
                    parts = metric_name.split(".")
                    measure_only = parts[1]

                    # First try to replace qualified form if present
                    pattern = r"\b" + re.escape(metric_name).replace(r"\.", r"\.") + r"\b"
                    formula = re.sub(pattern, f"({metric_sql})", formula)

                    # Then also replace unqualified form (measure name only)
                    pattern = r"\b" + re.escape(measure_only) + r"\b"
                    formula = re.sub(pattern, f"({metric_sql})", formula)
                else:
                    # For simple names, use word boundaries
                    pattern = r"\b" + re.escape(metric_name) + r"\b"
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
                try:
                    model = self.graph.get_model(model_name)
                    if model:
                        metric = model.get_metric(measure_name)
                except KeyError:
                    pass
                # Fall back to graph-level metric with dotted name
                if not metric:
                    try:
                        metric = self.graph.get_metric(m)
                    except KeyError:
                        pass

            # Classify metric by type
            if metric and metric.type == "cumulative":
                cumulative_metrics.append(m)
                # Add the base measure/metric to base_metrics
                if metric.sql:
                    base_ref = metric.sql
                    # Qualify unqualified references with the model name
                    if "." not in base_ref and "." in m:
                        model_name = m.split(".")[0]
                        base_ref = f"{model_name}.{base_ref}"
                    base_metrics.append(base_ref)
            elif metric and metric.type == "time_comparison":
                # Validate required fields
                if not metric.base_metric:
                    raise ValueError(f"time_comparison metric '{m}' requires 'base_metric' field")
                time_comparison_metrics.append(m)
                # Add the base metric to base_metrics
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
            # Handle both qualified (model.measure) and unqualified references
            metric = None
            if "." in m:
                model_name, measure_name = m.split(".", 1)
                try:
                    model = self.graph.get_model(model_name)
                    metric = model.get_metric(measure_name) if model else None
                except KeyError:
                    pass
                # Fall back to graph-level metric with dotted name
                if not metric:
                    try:
                        metric = self.graph.get_metric(m)
                    except KeyError:
                        pass
                # Use just the measure name as alias if it's model.measure, otherwise full name
                # Quote to handle any special characters
                metric_alias = self._quote_alias(measure_name if metric and "." not in metric.name else m)
            else:
                metric = self.graph.get_metric(m)
                # Quote to handle dotted metric names
                metric_alias = self._quote_alias(m)
            if not metric or (not metric.sql and not metric.window_expression):
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

            # Allow window_order to override auto-detected time dimension
            if metric.window_order:
                time_dim = f"base.{metric.window_order}"

            if not time_dim:
                raise ValueError(f"Cumulative metric {m} requires a time dimension for ordering")

            # Option C: Raw window_expression passthrough
            if metric.window_expression:
                order_col = time_dim
                frame = metric.window_frame or "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                window_expr = f"{metric.window_expression} OVER (ORDER BY {order_col} {frame}) AS {metric_alias}"
                select_exprs.append(window_expr)
                continue

            # Option A: Use agg + sql (supports AVG, COUNT, etc.)
            # Get base measure/metric to apply window function to
            base_ref = metric.sql
            if "." in base_ref:
                # It's a direct measure reference - extract just the measure name
                base_alias = base_ref.split(".")[1]
            else:
                # It's an unqualified reference - check model first, then graph-level
                base_metric = None
                # Get model name from the cumulative metric reference
                cum_model_name = m.split(".")[0] if "." in m else None
                if cum_model_name:
                    cum_model = self.graph.get_model(cum_model_name)
                    if cum_model:
                        base_metric = cum_model.get_metric(base_ref)

                # Fallback to graph-level metric
                if not base_metric:
                    try:
                        base_metric = self.graph.get_metric(base_ref)
                    except KeyError:
                        pass

                if base_metric and base_metric.sql:
                    # Use the underlying measure name
                    if "." in base_metric.sql:
                        base_alias = base_metric.sql.split(".")[1]
                    else:
                        base_alias = base_metric.sql
                else:
                    # Fallback to the metric name itself
                    base_alias = base_ref

            # Determine aggregation function (default to SUM for backwards compatibility)
            agg_func = (metric.agg or "sum").upper()
            if agg_func == "COUNT_DISTINCT":
                agg_func = "COUNT"
                base_col = f"DISTINCT base.{base_alias}"
            else:
                base_col = f"base.{base_alias}"

            # Build window function
            if metric.grain_to_date:
                # Grain-to-date: MTD, QTD, YTD
                # Partition by the grain period and order within it
                grain = metric.grain_to_date
                partition = self._date_trunc(grain, time_dim)

                window_expr = f"{agg_func}({base_col}) OVER (PARTITION BY {partition} ORDER BY {time_dim} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {metric_alias}"
            elif metric.window:
                # Parse window (e.g., "7 days")
                window_parts = metric.window.split()
                if len(window_parts) == 2:
                    num, unit = window_parts
                    # For date-based windows, use RANGE
                    window_expr = f"{agg_func}({base_col}) OVER (ORDER BY {time_dim} RANGE BETWEEN INTERVAL '{num} {unit}' PRECEDING AND CURRENT ROW) AS {metric_alias}"
                else:
                    # Fallback to rows
                    window_expr = f"{agg_func}({base_col}) OVER (ORDER BY {time_dim} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {metric_alias}"
            else:
                # Running total (unbounded window)
                window_expr = f"{agg_func}({base_col}) OVER (ORDER BY {time_dim} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {metric_alias}"

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

                # Add LAG for base metric (quote alias to handle dotted names)
                prev_value_alias = self._quote_alias(f"{m}_prev_value")
                lag_selects.append(
                    f"LAG(base.{base_alias}, {lag_offset}) OVER (ORDER BY {time_dim}) AS {prev_value_alias}"
                )

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
                # Quote alias to handle dotted names
                prev_denom_alias = self._quote_alias(f"{m}_prev_denom")
                lag_selects.append(f"LAG(base.{denom_alias}) OVER (ORDER BY {time_dim}) AS {prev_denom_alias}")

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
                if not metric or not metric.base_metric:
                    continue

                # Get base metric alias
                base_ref = metric.base_metric
                if "." in base_ref:
                    base_alias = base_ref.split(".")[1]
                else:
                    base_alias = base_ref

                # Quote aliases to handle dotted metric names
                prev_value_col = self._quote_alias(f"{m}_prev_value")
                final_alias = self._quote_alias(m)

                # Build calculation based on calculation type
                calc_type = metric.calculation or "percent_change"
                if calc_type == "difference":
                    expr = f"({base_alias} - {prev_value_col}) AS {final_alias}"
                elif calc_type == "percent_change":
                    expr = f"(({base_alias} - {prev_value_col}) / NULLIF({prev_value_col}, 0) * 100) AS {final_alias}"
                elif calc_type == "ratio":
                    expr = f"({base_alias} / NULLIF({prev_value_col}, 0)) AS {final_alias}"
                else:
                    raise ValueError(f"Unknown calculation type: {calc_type}")

                final_selects.append(expr)

            # Add offset ratio metrics
            for m in offset_ratio_metrics:
                metric = self.graph.get_metric(m)
                if not metric:
                    continue

                num_alias = metric.numerator.split(".")[1] if "." in metric.numerator else metric.numerator

                # Quote aliases to handle dotted metric names
                prev_denom_col = self._quote_alias(f"{m}_prev_denom")
                final_alias = self._quote_alias(m)

                # Calculate ratio using the lagged value
                offset_expr = f"{num_alias} / NULLIF({prev_denom_col}, 0) AS {final_alias}"
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
        preagg_table = preagg.get_table_name(model.name, database=self.preagg_database, schema=self.preagg_schema)

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
                    # Exact match - use as is with proper alias
                    select_exprs.append(f"{preagg_col} as {dim_name}__{gran}")
                else:
                    # Roll up to coarser granularity
                    date_trunc_expr = self._date_trunc(gran, preagg_col)
                    select_exprs.append(f"{date_trunc_expr} as {dim_name}__{gran}")
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
