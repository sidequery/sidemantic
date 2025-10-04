"""SQL query rewriter for semantic layer.

Parses user SQL and rewrites it to use the semantic layer.
"""

import sqlglot
from sqlglot import exp

from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator_v2 import SQLGenerator


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

    def rewrite(self, sql: str) -> str:
        """Rewrite user SQL to use semantic layer.

        Args:
            sql: User SQL query like "SELECT revenue, status FROM orders WHERE status = 'completed'"

        Returns:
            Rewritten SQL using semantic layer

        Raises:
            ValueError: If SQL cannot be rewritten (unsupported features, invalid references, etc.)
        """
        # Parse SQL
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        except Exception as e:
            raise ValueError(f"Failed to parse SQL: {e}")

        if not isinstance(parsed, exp.Select):
            raise ValueError("Only SELECT queries are supported")

        # Extract FROM table for inference
        self.inferred_table = self._extract_from_table(parsed)

        # Extract components
        metrics, dimensions = self._extract_metrics_and_dimensions(parsed)
        filters = self._extract_filters(parsed)
        order_by = self._extract_order_by(parsed)
        limit = self._extract_limit(parsed)
        offset = self._extract_offset(parsed)

        # Validate we have something to select
        if not metrics and not dimensions:
            raise ValueError("Query must select at least one metric or dimension")

        # Generate semantic layer SQL
        return self.generator.generate(
            metrics=metrics, dimensions=dimensions, filters=filters, order_by=order_by, limit=limit, offset=offset
        )

    def _extract_metrics_and_dimensions(self, select: exp.Select) -> tuple[list[str], list[str]]:
        """Extract metrics and dimensions from SELECT clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Tuple of (metrics, dimensions)
        """
        metrics = []
        dimensions = []

        for projection in select.expressions:
            # Handle SELECT *
            if isinstance(projection, exp.Star):
                # Expand to all fields from the inferred table
                if not self.inferred_table:
                    raise ValueError("SELECT * requires a FROM clause with a single table")

                model = self.graph.get_model(self.inferred_table)

                # Add all dimensions
                for dim in model.dimensions:
                    dimensions.append(f"{self.inferred_table}.{dim.name}")

                # Add all measures as metrics
                for measure in model.measures:
                    metrics.append(f"{self.inferred_table}.{measure.name}")

                continue

            # Get column name (handle aliases)
            if isinstance(projection, exp.Alias):
                column = projection.this
                alias = projection.alias
            else:
                column = projection
                alias = None

            # Extract table.column reference
            ref = self._resolve_column(column)
            if not ref:
                raise ValueError(f"Cannot resolve column: {column.sql(dialect=self.dialect)}")

            model_name, field_name = ref.split(".", 1)

            # Check if it's a metric
            metric_ref = f"{model_name}.{field_name}"
            if metric_ref in self.graph.metrics:
                metrics.append(metric_ref)
                continue

            # Check if it's a measure (should be accessed as metric)
            model = self.graph.get_model(model_name)
            if any(m.name == field_name for m in model.measures):
                # Measure referenced directly - treat as implicit metric
                metrics.append(metric_ref)
                continue

            # Check if it's a dimension
            if any(d.name == field_name for d in model.dimensions):
                dimensions.append(metric_ref)
                continue

            raise ValueError(
                f"Field '{metric_ref}' not found. Must be a metric, measure, or dimension in model '{model_name}'"
            )

        return metrics, dimensions

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
            Table name or None if multiple tables or no FROM
        """
        from_clause = select.args.get("from")
        if not from_clause:
            return None

        # Get the table expression
        table_expr = from_clause.this
        if isinstance(table_expr, exp.Table):
            return table_expr.name

        return None

    def _resolve_column(self, column: exp.Expression) -> str | None:
        """Resolve column reference to model.field format.

        Args:
            column: Column expression

        Returns:
            Reference like "orders.revenue" or None
        """
        if isinstance(column, exp.Column):
            table = column.table
            name = column.name

            if table:
                # Explicit table.column
                return f"{table}.{name}"
            else:
                # Try to infer from single FROM table
                if self.inferred_table:
                    return f"{self.inferred_table}.{name}"
                else:
                    raise ValueError(f"Column '{name}' must have table prefix (e.g., orders.{name})")

        # Handle aggregate functions - must be pre-defined as measures
        if isinstance(column, exp.Func):
            func_sql = column.sql(dialect=self.dialect)
            func_name = column.key.upper()

            # Extract the expression being aggregated
            if column.args.get('this'):
                arg = column.args['this']
                arg_sql = arg.sql(dialect=self.dialect) if not isinstance(arg, exp.Star) else '*'
            else:
                arg_sql = '*'

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
