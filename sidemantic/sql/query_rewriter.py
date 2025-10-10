"""SQL query rewriter for semantic layer.

Parses user SQL and rewrites it to use the semantic layer.
"""

import sqlglot
from sqlglot import exp

from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


class QueryRewriter:
    """Rewrites user SQL queries to use the semantic layer."""

    def __init__(self, graph: SemanticGraph, dialect: str = "duckdb"):
        """Initialize query rewriter.

        Args:
            graph: Semantic graph with models and metrics
            dialect: SQL dialect for parsing/generation
        """
        self.graph = graph
        self.dialect = dialect
        self.generator = SQLGenerator(graph, dialect=dialect)

    def rewrite(self, sql: str, strict: bool = True) -> str:
        """Rewrite user SQL to use semantic layer.

        Supports:
        - Direct semantic layer queries: SELECT revenue FROM orders
        - CTEs with semantic queries: WITH agg AS (SELECT revenue FROM orders) SELECT * FROM agg
        - Subqueries: SELECT * FROM (SELECT revenue FROM orders) WHERE revenue > 100

        Args:
            sql: User SQL query
            strict: If True, raise errors for invalid SQL or non-SELECT queries.
                   If False, pass through queries that can't be rewritten.

        Returns:
            Rewritten SQL using semantic layer

        Raises:
            ValueError: If SQL cannot be rewritten (unsupported features, invalid references, etc.)
                       Only raised when strict=True
        """
        sql = sql.strip()

        # Handle multiple statements (some PostgreSQL clients send these)
        if ";" in sql:
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            if len(statements) > 1:
                if strict:
                    raise ValueError("Multiple statements are not supported")
                # In non-strict mode, pass through
                return sql

        # Parse SQL
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        except Exception as e:
            if strict:
                raise ValueError(f"Failed to parse SQL: {e}")
            # In non-strict mode, pass through unparseable SQL (e.g., SHOW, SET commands)
            return sql

        if not isinstance(parsed, exp.Select):
            if strict:
                raise ValueError("Only SELECT queries are supported")
            # In non-strict mode, pass through non-SELECT queries
            return sql

        # In non-strict mode, pass through queries that don't reference semantic models
        if not strict and not self._references_semantic_model(parsed):
            return sql

        # Check if this is a CTE-based query or has subqueries
        has_ctes = parsed.args.get("with") is not None
        has_subquery_in_from = self._has_subquery_in_from(parsed)

        if has_ctes or has_subquery_in_from:
            # Handle CTEs and subqueries
            return self._rewrite_with_ctes_or_subqueries(parsed)

        # Otherwise, treat as simple semantic layer query
        return self._rewrite_simple_query(parsed)

    def _has_subquery_in_from(self, select: exp.Select) -> bool:
        """Check if FROM clause contains a subquery."""
        from_clause = select.args.get("from")
        if not from_clause:
            return False

        return isinstance(from_clause.this, exp.Subquery)

    def _rewrite_with_ctes_or_subqueries(self, parsed: exp.Select) -> str:
        """Rewrite query that contains CTEs or subqueries.

        Strategy:
        1. Rewrite each CTE that references semantic models
        2. Rewrite subqueries in FROM clause
        3. Return the modified SQL
        """
        # Handle CTEs
        if parsed.args.get("with"):
            with_clause = parsed.args["with"]
            for cte in with_clause.expressions:
                # Each CTE has a name (alias) and a query (this)
                cte_query = cte.this
                if isinstance(cte_query, exp.Select):
                    # Check if this CTE references a semantic model
                    if self._references_semantic_model(cte_query):
                        # Rewrite the CTE query
                        rewritten_cte_sql = self._rewrite_simple_query(cte_query)
                        # Parse the rewritten SQL and replace the CTE query
                        rewritten_cte = sqlglot.parse_one(rewritten_cte_sql, dialect=self.dialect)
                        cte.set("this", rewritten_cte)

        # Handle subquery in FROM
        from_clause = parsed.args.get("from")
        if from_clause and isinstance(from_clause.this, exp.Subquery):
            subquery = from_clause.this
            subquery_select = subquery.this
            if isinstance(subquery_select, exp.Select) and self._references_semantic_model(subquery_select):
                # Rewrite the subquery
                rewritten_subquery_sql = self._rewrite_simple_query(subquery_select)
                rewritten_subquery = sqlglot.parse_one(rewritten_subquery_sql, dialect=self.dialect)
                subquery.set("this", rewritten_subquery)

        # Return the modified SQL
        # Note: Individual CTEs/subqueries are already instrumented by _rewrite_simple_query -> generator
        # The outer query wrapper doesn't need separate instrumentation
        return parsed.sql(dialect=self.dialect)

    def _references_semantic_model(self, select: exp.Select) -> bool:
        """Check if a SELECT statement references any semantic models."""
        from_clause = select.args.get("from")
        if not from_clause:
            return False

        table_expr = from_clause.this
        if isinstance(table_expr, exp.Table):
            table_name = table_expr.name
            # "metrics" is a special virtual table for semantic layer
            if table_name == "metrics":
                return True
            # Check if this is a known model
            return table_name in self.graph.models

        return False

    def _rewrite_simple_query(self, parsed: exp.Select) -> str:
        """Rewrite a simple semantic layer query (no CTEs/subqueries).

        Args:
            parsed: Parsed SELECT statement

        Returns:
            Rewritten SQL using semantic layer
        """
        # Check for explicit JOINs - these are not supported
        if parsed.args.get("joins"):
            raise ValueError(
                "Explicit JOIN syntax is not supported. "
                "Joins are automatic based on model relationships.\n\n"
                "Instead of:\n"
                "  SELECT orders.revenue, customers.name FROM orders JOIN customers ON ...\n\n"
                "Use:\n"
                "  SELECT orders.revenue, customers.name FROM orders"
            )

        # Extract FROM table for inference
        self.inferred_table = self._extract_from_table(parsed)

        # Extract components
        metrics, dimensions, aliases = self._extract_metrics_and_dimensions(parsed)
        filters = self._extract_filters(parsed)
        order_by = self._extract_order_by(parsed)
        limit = self._extract_limit(parsed)
        offset = self._extract_offset(parsed)

        # Validate we have something to select
        if not metrics and not dimensions:
            raise ValueError("Query must select at least one metric or dimension")

        # Generate semantic layer SQL
        return self.generator.generate(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            order_by=order_by,
            limit=limit,
            offset=offset,
            aliases=aliases,
        )

    def _extract_metrics_and_dimensions(self, select: exp.Select) -> tuple[list[str], list[str], dict[str, str]]:
        """Extract metrics and dimensions from SELECT clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Tuple of (metrics, dimensions, aliases)
            where aliases is a dict mapping field reference to custom alias
        """
        metrics = []
        dimensions = []
        aliases = {}

        for projection in select.expressions:
            # Handle SELECT *
            if isinstance(projection, exp.Star):
                # Expand to all fields from the inferred table
                if not self.inferred_table:
                    raise ValueError("SELECT * requires a FROM clause with a single table")

                # FROM metrics: expand to all metrics/dimensions from all models
                if self.inferred_table == "metrics":
                    raise ValueError(
                        "SELECT * is not supported with FROM metrics.\n"
                        "You must explicitly select fields, e.g.:\n"
                        "  SELECT orders.revenue, customers.region FROM metrics"
                    )

                model = self.graph.get_model(self.inferred_table)

                # Add all dimensions
                for dim in model.dimensions:
                    dimensions.append(f"{self.inferred_table}.{dim.name}")

                # Add all measures as metrics
                for measure in model.metrics:
                    metrics.append(f"{self.inferred_table}.{measure.name}")

                continue

            # Get column name and alias
            custom_alias = None
            if isinstance(projection, exp.Alias):
                column = projection.this
                custom_alias = projection.alias
            else:
                column = projection

            # Skip literal values
            if isinstance(column, exp.Literal):
                raise ValueError(
                    "Literal values in SELECT are not supported in semantic layer queries.\n"
                    "Only metrics and dimensions can be selected."
                )

            # Extract table.column reference
            ref = self._resolve_column(column)
            if not ref:
                raise ValueError(f"Cannot resolve column: {column.sql(dialect=self.dialect)}")

            # Store custom alias if provided
            if custom_alias:
                aliases[ref] = custom_alias

            # Handle graph-level metrics (no model prefix)
            if "." not in ref:
                # This is a graph-level metric
                if ref in self.graph.metrics:
                    metrics.append(ref)
                    continue
                else:
                    raise ValueError(f"Field '{ref}' not found as a graph-level metric")

            model_name, field_name = ref.split(".", 1)

            # Check if field_name includes time granularity suffix (e.g., order_date__day)
            base_field_name = field_name
            if "__" in field_name:
                parts = field_name.rsplit("__", 1)
                potential_gran = parts[1]
                # Validate granularity
                valid_grans = ["year", "quarter", "month", "week", "day", "hour", "minute", "second"]
                if potential_gran in valid_grans:
                    base_field_name = parts[0]

            # Check if it's a metric (using base name without granularity)
            metric_ref = f"{model_name}.{base_field_name}"
            if metric_ref in self.graph.metrics:
                metrics.append(f"{model_name}.{field_name}")  # Keep original field_name with granularity
                continue

            # Check if it's a measure (should be accessed as metric)
            model = self.graph.get_model(model_name)
            if any(m.name == base_field_name for m in model.metrics):
                # Measure referenced directly - treat as implicit metric
                metrics.append(f"{model_name}.{field_name}")  # Keep original field_name
                continue

            # Check if it's a dimension
            if any(d.name == base_field_name for d in model.dimensions):
                # Keep the full ref including __granularity if present
                dimensions.append(ref)
                continue

            raise ValueError(
                f"Field '{model_name}.{base_field_name}' not found. Must be a metric, measure, or dimension in model '{model_name}'"
            )

        return metrics, dimensions, aliases

    def _extract_filters(self, select: exp.Select) -> list[str]:
        """Extract filters from WHERE clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            List of filter expressions
        """
        if not select.args.get("where"):
            return []

        where = select.args["where"].this

        # Handle compound conditions (AND/OR)
        if isinstance(where, (exp.And, exp.Or)):
            return self._extract_compound_filters(where)

        # Single condition
        return [where.sql(dialect=self.dialect)]

    def _extract_compound_filters(self, condition: exp.Expression) -> list[str]:
        """Extract filters from compound AND/OR conditions.

        Args:
            condition: Compound condition (AND/OR)

        Returns:
            List of filter expressions
        """
        filters = []

        if isinstance(condition, exp.And):
            # Split AND into separate filters
            for expr in [condition.left, condition.right]:
                if isinstance(expr, (exp.And, exp.Or)):
                    filters.extend(self._extract_compound_filters(expr))
                else:
                    filters.append(expr.sql(dialect=self.dialect))
        elif isinstance(condition, exp.Or):
            # OR must stay together as single filter
            filters.append(condition.sql(dialect=self.dialect))
        else:
            filters.append(condition.sql(dialect=self.dialect))

        return filters

    def _extract_order_by(self, select: exp.Select) -> list[str] | None:
        """Extract ORDER BY clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            List of order by expressions or None
        """
        if not select.args.get("order"):
            return None

        order_expressions = []
        for order_expr in select.args["order"].expressions:
            # Get column (might have ASC/DESC)
            if isinstance(order_expr, exp.Ordered):
                column = order_expr.this
                desc = order_expr.args.get("desc", False)
                col_name = self._get_column_name(column)
                order_expressions.append(f"{col_name} {'DESC' if desc else 'ASC'}")
            else:
                col_name = self._get_column_name(order_expr)
                order_expressions.append(col_name)

        return order_expressions if order_expressions else None

    def _extract_limit(self, select: exp.Select) -> int | None:
        """Extract LIMIT clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Limit value or None
        """
        if not select.args.get("limit"):
            return None

        limit = select.args["limit"]
        if hasattr(limit, "expression"):
            limit_expr = limit.expression
            if isinstance(limit_expr, exp.Literal):
                return int(limit_expr.this)

        return None

    def _extract_offset(self, select: exp.Select) -> int | None:
        """Extract OFFSET clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Offset value or None
        """
        if not select.args.get("offset"):
            return None

        offset = select.args["offset"]
        if hasattr(offset, "expression"):
            offset_expr = offset.expression
            if isinstance(offset_expr, exp.Literal):
                return int(offset_expr.this)

        return None

    def _extract_from_table(self, select: exp.Select) -> str | None:
        """Extract table name from FROM clause if there's only one table.

        Args:
            select: Parsed SELECT statement

        Returns:
            Table name or None if multiple tables or no FROM.
            Returns "metrics" if FROM metrics (special generic semantic layer table)
        """
        from_clause = select.args.get("from")
        if not from_clause:
            return None

        # Get the table expression
        table_expr = from_clause.this
        if isinstance(table_expr, exp.Table):
            table_name = table_expr.name
            # "metrics" is a special virtual table for generic semantic queries
            if table_name == "metrics":
                return "metrics"
            return table_name

        return None

    def _resolve_column(self, column: exp.Expression) -> str | None:
        """Resolve column reference to model.field format.

        Args:
            column: Column expression

        Returns:
            Reference like "orders.revenue" or "orders.order_date__day" or None
        """
        if isinstance(column, exp.Column):
            table = column.table
            name = column.name

            if table:
                # Explicit table.column (may include __granularity suffix)
                return f"{table}.{name}"
            else:
                # Try to infer from single FROM table
                if self.inferred_table:
                    # FROM metrics allows unqualified top-level metrics
                    if self.inferred_table == "metrics":
                        # Check if this is a top-level/graph metric
                        if name in self.graph.metrics:
                            # Top-level metric, return as-is (no model prefix)
                            return name
                        else:
                            raise ValueError(
                                f"Column '{name}' must be fully qualified when using FROM metrics.\n"
                                f"Use model.{name} for model-level metrics, or define '{name}' as a graph-level metric.\n\n"
                                f"Example: SELECT orders.revenue, total_orders FROM metrics"
                            )
                    # Column name may include __granularity suffix (e.g., order_date__day)
                    return f"{self.inferred_table}.{name}"
                else:
                    raise ValueError(f"Column '{name}' must have table prefix (e.g., orders.{name})")

        # Handle aggregate functions - must be pre-defined as measures
        if isinstance(column, exp.Func):
            func_sql = column.sql(dialect=self.dialect)
            func_name = column.key.upper()

            # Extract the expression being aggregated
            if column.args.get("this"):
                arg = column.args["this"]
                # Handle both expression objects and strings
                if isinstance(arg, str):
                    arg_sql = arg
                elif isinstance(arg, exp.Star):
                    arg_sql = "*"
                else:
                    arg_sql = arg.sql(dialect=self.dialect)
            else:
                arg_sql = "*"

            # Provide helpful error with YAML example (use wording expected by docs/tests)
            raise ValueError(
                f"Aggregate functions must be defined as a metric.\n\n"
                f"To use {func_sql}, add to your model:\n\n"
                f"measures:\n"
                f"  - name: my_metric\n"
                f"    agg: {func_name.lower()}\n"
                f"    expr: {arg_sql}\n\n"
                f"Then query with: SELECT my_metric FROM {self.inferred_table or 'your_model'}"
            )

        return None

    def _get_column_name(self, column: exp.Expression) -> str:
        """Get simple column name from expression.

        Args:
            column: Column expression

        Returns:
            Column name
        """
        if isinstance(column, exp.Column):
            return column.name
        return column.sql(dialect=self.dialect)
