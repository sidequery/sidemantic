"""SQL generation using SQLGlot."""

import sqlglot
from sqlglot import expressions as exp

from sidemantic.core.semantic_graph import SemanticGraph


class SQLGenerator:
    """Generates SQL queries from semantic layer definitions using SQLGlot."""

    def __init__(self, graph: SemanticGraph, dialect: str = "duckdb"):
        """Initialize SQL generator.

        Args:
            graph: Semantic graph with models and metrics
            dialect: SQL dialect for generation (default: duckdb)
        """
        self.graph = graph
        self.dialect = dialect

    def generate(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
    ) -> str:
        """Generate SQL query from semantic layer query.

        Args:
            metrics: List of metric references (e.g., ["orders.revenue"])
            dimensions: List of dimension references with optional granularity (e.g., ["orders.status", "orders.order_date__month"])
            filters: List of filter expressions (e.g., ["orders.status = 'completed'"])
            order_by: List of fields to order by
            limit: Maximum number of rows to return

        Returns:
            SQL query string
        """
        metrics = metrics or []
        dimensions = dimensions or []
        filters = filters or []

        # Parse dimension references and extract granularities
        parsed_dims = self._parse_dimension_refs(dimensions)

        # Find all models needed for the query
        model_names = self._find_required_models(metrics, dimensions)

        if not model_names:
            raise ValueError("No models found for query")

        # Determine base model (first model in query)
        base_model_name = list(model_names)[0]
        self.graph.get_model(base_model_name)

        # Build CTEs for each model
        ctes = []
        for model_name in model_names:
            cte = self._build_model_cte(model_name, parsed_dims, metrics)
            ctes.append(cte)

        # Build join clause if multiple models
        from_clause = exp.Table(this=exp.Identifier(this=base_model_name + "_cte"))

        if len(model_names) > 1:
            join_clause = self._build_joins(base_model_name, list(model_names)[1:])
        else:
            join_clause = None

        # Build SELECT clause
        select_exprs = []

        # Add dimensions to SELECT
        for dim_ref, gran in parsed_dims:
            model_name, dim_name = dim_ref.split(".")
            model = self.graph.get_model(model_name)
            dimension = model.get_dimension(dim_name)

            if not dimension:
                raise ValueError(f"Dimension {dim_ref} not found")

            # Column name in CTE
            cte_col_name = f"{dim_name}__{gran}" if gran else dim_name
            table_alias = model_name + "_cte"

            # Use alias for SELECT
            alias = f"{dim_name}__{gran}" if gran else dim_name
            select_exprs.append(exp.Alias(this=exp.Column(this=cte_col_name, table=table_alias), alias=alias))

        # Add metrics to SELECT
        for metric_ref in metrics:
            # For now, assume metrics reference measures directly
            if "." in metric_ref:
                model_name, measure_name = metric_ref.split(".")
                model = self.graph.get_model(model_name)
                measure = model.get_metric(measure_name)

                if measure:
                    # Build aggregation expression
                    agg_expr = self._build_measure_aggregation(model_name, measure)
                    select_exprs.append(exp.Alias(this=agg_expr, alias=measure_name))
                else:
                    # Try as metric
                    metric = self.graph.get_metric(metric_ref)
                    if not metric:
                        raise ValueError(f"Metric or measure {metric_ref} not found")
                    # Handle metric compilation
                    metric_expr = self._build_metric_expression(metric)
                    select_exprs.append(exp.Alias(this=metric_expr, alias=metric.name))

        # Build WHERE clause
        where_clause = None
        if filters:
            where_exprs = [sqlglot.parse_one(f, dialect=self.dialect) for f in filters]
            where_clause = where_exprs[0] if len(where_exprs) == 1 else exp.And(expressions=where_exprs)

        # Build GROUP BY clause (all dimensions)
        group_by_exprs = [exp.Literal.number(i + 1) for i in range(len(parsed_dims))]

        # Build ORDER BY clause
        order_by_exprs = None
        if order_by:
            order_by_exprs = [exp.Ordered(this=exp.Column(this=exp.Identifier(this=o))) for o in order_by]

        # Build main SELECT statement
        select_stmt = exp.Select(
            expressions=select_exprs,
            from_=from_clause,
            joins=join_clause if join_clause else None,
            where=where_clause,
            group=exp.Group(expressions=group_by_exprs) if group_by_exprs else None,
            order=exp.Order(expressions=order_by_exprs) if order_by_exprs else None,
            limit=exp.Limit(expression=exp.Literal.number(limit)) if limit else None,
        )

        # Combine CTEs and main query
        if ctes:
            final_query = exp.With(expressions=ctes, this=select_stmt)
        else:
            final_query = select_stmt

        return final_query.sql(dialect=self.dialect, pretty=True)

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

    def _find_required_models(self, metrics: list[str], dimensions: list[str]) -> set[str]:
        """Find all models required for the query."""
        models = set()

        for metric in metrics:
            if "." in metric:
                models.add(metric.split(".")[0])

        for dim in dimensions:
            # Remove granularity suffix if present
            if "__" in dim:
                dim = dim.rsplit("__", 1)[0]
            if "." in dim:
                models.add(dim.split(".")[0])

        return models

    def _build_model_cte(
        self, model_name: str, dimensions: list[tuple[str, str | None]], metrics: list[str]
    ) -> exp.CTE:
        """Build CTE for a model.

        Args:
            model_name: Name of the model
            dimensions: Parsed dimension references
            metrics: Metric references

        Returns:
            CTE expression
        """
        model = self.graph.get_model(model_name)

        # Collect columns needed from this model
        select_exprs = []

        # Add entity columns (for joins)
        for entity in model.entities:
            col_expr = sqlglot.parse_one(entity.expr, dialect=self.dialect)
            select_exprs.append(exp.Alias(this=col_expr, alias=entity.name))

        # Add dimension columns
        for dim_ref, gran in dimensions:
            if not dim_ref.startswith(model_name + "."):
                continue

            dim_name = dim_ref.split(".")[1]
            dimension = model.get_dimension(dim_name)

            if not dimension:
                continue

            if gran and dimension.type == "time":
                # Apply time granularity
                dim_sql = dimension.with_granularity(gran)
                alias = f"{dim_name}__{gran}"
            else:
                dim_sql = dimension.sql_expr
                alias = dim_name

            dim_expr = sqlglot.parse_one(dim_sql, dialect=self.dialect)
            select_exprs.append(exp.Alias(this=dim_expr, alias=alias))

        # Add measure columns (raw, not aggregated in CTE)
        for metric_ref in metrics:
            if not metric_ref.startswith(model_name + "."):
                continue

            measure_name = metric_ref.split(".")[1]
            measure = model.get_metric(measure_name)

            if not measure:
                continue

            # Add raw expression (aggregation happens in main query)
            measure_expr = sqlglot.parse_one(measure.sql_expr, dialect=self.dialect)
            select_exprs.append(exp.Alias(this=measure_expr, alias=f"{measure_name}_raw"))

        # Build FROM clause
        if model.sql:
            from_clause = exp.Subquery(
                this=sqlglot.parse_one(model.sql, dialect=self.dialect),
                alias=exp.TableAlias(this=exp.Identifier(this="t")),
            )
        else:
            table_parts = model.table.split(".")
            if len(table_parts) == 2:
                from_clause = exp.Table(
                    this=exp.Identifier(this=table_parts[1]), db=exp.Identifier(this=table_parts[0])
                )
            else:
                from_clause = exp.Table(this=exp.Identifier(this=model.table))

        # Build SELECT
        select_stmt = exp.Select(expressions=select_exprs, from_=from_clause)

        # Create CTE
        return exp.CTE(this=select_stmt, alias=exp.TableAlias(this=exp.Identifier(this=model_name + "_cte")))

    def _build_joins(self, base_model: str, other_models: list[str]) -> list[exp.Join]:
        """Build JOIN clauses between models.

        Args:
            base_model: Base model name
            other_models: List of other model names to join

        Returns:
            List of Join expressions
        """
        joins = []

        for other_model in other_models:
            # Find join path
            join_path = self.graph.find_join_path(base_model, other_model)

            # For now, handle direct joins (single hop)
            if len(join_path) == 1:
                jp = join_path[0]

                # Build join condition
                left_table = jp.from_model + "_cte"
                right_table = jp.to_model + "_cte"

                join_cond = exp.EQ(
                    this=exp.Column(
                        this=exp.Identifier(this=jp.from_entity),
                        table=exp.Identifier(this=left_table),
                    ),
                    expression=exp.Column(
                        this=exp.Identifier(this=jp.to_entity),
                        table=exp.Identifier(this=right_table),
                    ),
                )

                join = exp.Join(
                    this=exp.Table(this=exp.Identifier(this=right_table)),
                    on=join_cond,
                    kind="LEFT",
                )

                joins.append(join)

        return joins

    def _build_measure_aggregation(self, model_name: str, measure) -> exp.Expression:
        """Build aggregation expression for a measure.

        Args:
            model_name: Model name
            measure: Metric object

        Returns:
            SQLGlot expression for aggregation
        """
        table_alias = model_name + "_cte"
        raw_col = exp.Column(this=f"{measure.name}_raw", table=table_alias)

        agg_func = measure.agg.upper()

        if agg_func == "COUNT":
            return exp.Count(this=exp.Star() if not measure.expr else raw_col)
        elif agg_func == "COUNT_DISTINCT":
            return exp.Count(this=raw_col, distinct=True)
        elif agg_func == "SUM":
            return exp.Sum(this=raw_col)
        elif agg_func == "AVG":
            return exp.Avg(this=raw_col)
        elif agg_func == "MIN":
            return exp.Min(this=raw_col)
        elif agg_func == "MAX":
            return exp.Max(this=raw_col)
        else:
            # Fallback: generic function call
            return exp.Anonymous(this=agg_func, expressions=[raw_col])

    def _build_metric_expression(self, metric) -> exp.Expression:
        """Build expression for a metric.

        Args:
            metric: Metric object

        Returns:
            SQLGlot expression
        """
        # Handle untyped metrics with sql (references to measures)
        if not metric.type and not metric.agg and metric.sql:
            if "." in metric.sql:
                model_name, measure_name = metric.sql.split(".")
                model = self.graph.get_model(model_name)
                measure = model.get_metric(measure_name)
                return self._build_measure_aggregation(model_name, measure)
        elif metric.type == "ratio":
            # numerator / NULLIF(denominator, 0)
            num_model, num_measure = metric.numerator.split(".")
            denom_model, denom_measure = metric.denominator.split(".")

            num_model_obj = self.graph.get_model(num_model)
            denom_model_obj = self.graph.get_model(denom_model)

            num_measure_obj = num_model_obj.get_metric(num_measure)
            denom_measure_obj = denom_model_obj.get_metric(denom_measure)

            num_expr = self._build_measure_aggregation(num_model, num_measure_obj)
            denom_expr = self._build_measure_aggregation(denom_model, denom_measure_obj)

            # Build NULLIF(denominator, 0)
            nullif = exp.Nullif(this=denom_expr, expression=exp.Literal.number(0))

            return exp.Div(this=num_expr, expression=nullif)

        raise NotImplementedError(f"Metric type {metric.type} not yet implemented")
