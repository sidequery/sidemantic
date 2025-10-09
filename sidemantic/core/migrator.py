"""Semantic layer coverage analyzer for raw SQL queries."""

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp

from sidemantic.core.semantic_layer import SemanticLayer


@dataclass
class QueryAnalysis:
    """Analysis of a single SQL query."""

    query: str
    tables: set[str] = field(default_factory=set)
    table_aliases: dict[str, str] = field(default_factory=dict)  # alias -> table_name
    columns: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))  # table -> columns
    aggregations: list[tuple[str, str, str]] = field(default_factory=list)  # (agg_type, column, table)
    aggregation_aliases: dict[tuple[str, str, str], str] = field(
        default_factory=dict
    )  # (agg_type, col, table) -> alias
    derived_metrics: list[tuple[str, str, str]] = field(
        default_factory=list
    )  # (name/alias, sql_expression, table) for calculated metrics
    cumulative_metrics: list[dict] = field(default_factory=list)  # Cumulative/window function metrics with params
    aggregations_in_derived: set[tuple[str, str, str]] = field(
        default_factory=set
    )  # Aggregations that are part of derived metrics (exclude from SELECT)
    aggregations_in_cumulative: set[tuple[str, str, str]] = field(
        default_factory=set
    )  # Aggregations that are part of cumulative metrics (exclude from base metrics)
    group_by_columns: set[tuple[str, str]] = field(default_factory=set)  # (table, column)
    time_dimensions: list[tuple[str, str, str]] = field(
        default_factory=list
    )  # (table, column, granularity) for DATE_TRUNC etc
    joins: list[tuple[str, str, str, str]] = field(
        default_factory=list
    )  # (from_table, from_alias, to_table, to_alias, join_type, condition)
    relationships: list[tuple[str, str, str, str, str]] = field(
        default_factory=list
    )  # (from_model, to_model, relationship_type, foreign_key, primary_key)
    from_table: str | None = None  # Main FROM table
    from_alias: str | None = None  # Alias for main FROM table
    filters: list[str] = field(default_factory=list)
    having_clauses: list[str] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    limit: int | None = None
    can_rewrite: bool = False
    missing_models: set[str] = field(default_factory=set)
    missing_dimensions: set[tuple[str, str]] = field(default_factory=set)  # (model, dimension)
    missing_metrics: set[tuple[str, str, str]] = field(default_factory=set)  # (model, agg, column)
    suggested_rewrite: str | None = None
    parse_error: str | None = None

    @property
    def success(self) -> bool:
        """Return True if query was successfully parsed."""
        return self.parse_error is None


@dataclass
class MigrationReport:
    """Overall coverage analysis report."""

    total_queries: int
    parseable_queries: int
    rewritable_queries: int
    query_analyses: list[QueryAnalysis]
    missing_models: set[str] = field(default_factory=set)
    missing_dimensions: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))  # model -> dims
    missing_metrics: dict[str, set[tuple[str, str]]] = field(
        default_factory=lambda: defaultdict(set)
    )  # model -> (agg, col)
    coverage_percentage: float = 0.0


class Migrator:
    """Analyzes raw SQL queries for semantic layer coverage.

    Takes raw SQL queries and determines:
    - Which queries can be rewritten using the semantic layer
    - What models/dimensions/metrics are missing
    - How to rewrite queries using the semantic layer
    """

    def __init__(self, layer: SemanticLayer, connection=None):
        """Initialize analyzer.

        Args:
            layer: Semantic layer to analyze coverage for
            connection: Optional database connection for querying information_schema.
                       Should have an execute() method that returns results.
        """
        self.layer = layer
        self.connection = connection
        self.analyses: list[QueryAnalysis] = []

        # Build mapping from table names to model names
        self.table_to_model: dict[str, str] = {}
        for model_name, model in layer.graph.models.items():
            if model.table:
                self.table_to_model[model.table] = model_name

        # Cache information_schema data if connection available
        self.primary_keys: dict[str, set[str]] = {}  # table -> pk columns
        self.foreign_keys: dict[tuple[str, str], tuple[str, str]] = {}  # (fk_table, fk_col) -> (pk_table, pk_col)
        self.table_columns: dict[str, set[str]] = defaultdict(set)  # table -> columns

        if self.connection:
            self._load_schema_metadata()

    def _load_schema_metadata(self) -> None:
        """Load schema metadata from information_schema."""
        try:
            # Load primary keys
            pk_query = """
                SELECT table_name, column_name
                FROM information_schema.key_column_usage
                WHERE constraint_name IN (
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE constraint_type = 'PRIMARY KEY'
                )
            """
            pk_results = self.connection.execute(pk_query).fetchall()
            for table_name, column_name in pk_results:
                self.primary_keys.setdefault(table_name, set()).add(column_name)

            # Load foreign keys
            # DuckDB information_schema uses referential_constraints to map FK to PK constraints
            fk_query = """
                SELECT
                    fk_kcu.table_name AS fk_table,
                    fk_kcu.column_name AS fk_column,
                    pk_kcu.table_name AS pk_table,
                    pk_kcu.column_name AS pk_column
                FROM information_schema.referential_constraints rc
                JOIN information_schema.key_column_usage fk_kcu
                    ON rc.constraint_name = fk_kcu.constraint_name
                JOIN information_schema.key_column_usage pk_kcu
                    ON rc.unique_constraint_name = pk_kcu.constraint_name
            """
            fk_results = self.connection.execute(fk_query).fetchall()
            for fk_table, fk_column, pk_table, pk_column in fk_results:
                self.foreign_keys[(fk_table, fk_column)] = (pk_table, pk_column)

            # Load all columns for each table
            col_query = """
                SELECT table_name, column_name
                FROM information_schema.columns
            """
            col_results = self.connection.execute(col_query).fetchall()
            for table_name, column_name in col_results:
                self.table_columns[table_name].add(column_name)

        except Exception as e:
            # If information_schema queries fail, just continue without metadata
            # (will fall back to pattern-based detection)
            print(f"Warning: Could not load schema metadata: {e}")

    def analyze_queries(self, queries: list[str]) -> MigrationReport:
        """Analyze a list of SQL queries.

        Args:
            queries: List of SQL query strings

        Returns:
            Coverage report with analysis results
        """
        self.analyses = []

        for query in queries:
            analysis = self._analyze_query(query)
            self.analyses.append(analysis)

        return self._generate_report()

    def analyze_folder(self, folder_path: str, pattern: str = "*.sql") -> MigrationReport:
        """Analyze all SQL files in a folder.

        Args:
            folder_path: Path to folder containing SQL files
            pattern: Glob pattern for SQL files (default: *.sql)

        Returns:
            Coverage report with analysis results
        """
        folder = Path(folder_path)
        queries = []

        for sql_file in folder.glob(pattern):
            content = sql_file.read_text()
            # Split by semicolon for multi-query files
            file_queries = [q.strip() for q in content.split(";") if q.strip()]
            queries.extend(file_queries)

        return self.analyze_queries(queries)

    def _analyze_query(self, query: str) -> QueryAnalysis:
        """Analyze a single SQL query.

        Args:
            query: SQL query string

        Returns:
            Query analysis
        """
        analysis = QueryAnalysis(query=query)

        try:
            # Parse SQL
            parsed = sqlglot.parse_one(query, read="duckdb")

            # Extract components
            self._extract_tables(parsed, analysis)
            self._extract_columns(parsed, analysis)
            self._extract_aggregations(parsed, analysis)
            self._extract_derived_metrics(parsed, analysis)
            self._extract_group_by(parsed, analysis)
            self._extract_select_dimensions(parsed, analysis)
            self._extract_time_dimensions(parsed, analysis)
            self._extract_window_functions(parsed, analysis)
            self._extract_joins(parsed, analysis)
            self._extract_relationships(parsed, analysis)
            self._extract_filters(parsed, analysis)
            self._extract_having(parsed, analysis)
            self._extract_order_by(parsed, analysis)
            self._extract_limit(parsed, analysis)

            # Check coverage and generate suggestions
            self._check_coverage(analysis)

        except Exception as e:
            analysis.parse_error = str(e)

        return analysis

    def _extract_tables(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract table references and aliases, including subquery aliases."""
        # Find the main FROM clause
        from_clause = parsed.find(exp.From)
        if from_clause:
            if isinstance(from_clause.this, exp.Table):
                analysis.from_table = from_clause.this.name
                analysis.from_alias = from_clause.this.alias or None
                if analysis.from_alias:
                    analysis.table_aliases[analysis.from_alias] = analysis.from_table
            elif isinstance(from_clause.this, exp.Subquery):
                # FROM clause is a subquery - extract underlying tables
                subquery = from_clause.this
                subquery_alias = subquery.alias
                if subquery_alias:
                    # Find tables inside the subquery
                    inner_tables = list(subquery.find_all(exp.Table))
                    if len(inner_tables) == 1:
                        # Single table in subquery - map alias to that table
                        analysis.from_table = inner_tables[0].name
                        analysis.from_alias = subquery_alias
                        analysis.table_aliases[subquery_alias] = inner_tables[0].name
                        analysis.tables.add(inner_tables[0].name)
                    elif len(inner_tables) > 1:
                        # Multiple tables in subquery - can't map to single table
                        # Just track the tables but leave alias unmapped
                        for table in inner_tables:
                            if table.name:
                                analysis.tables.add(table.name)

        # Collect all tables and their aliases
        for table in parsed.find_all(exp.Table):
            table_name = table.name
            if table_name:
                analysis.tables.add(table_name)
                if table.alias:
                    analysis.table_aliases[table.alias] = table_name

        # Handle subqueries in JOIN clauses
        for join in parsed.find_all(exp.Join):
            if isinstance(join.this, exp.Subquery):
                subquery = join.this
                subquery_alias = subquery.alias
                if subquery_alias:
                    # Find tables inside the subquery
                    inner_tables = list(subquery.find_all(exp.Table))
                    if len(inner_tables) == 1:
                        # Single table in subquery - map alias to that table
                        inner_table = inner_tables[0].name
                        analysis.table_aliases[subquery_alias] = inner_table
                        analysis.tables.add(inner_table)
                    elif len(inner_tables) > 1:
                        # Multiple tables in subquery
                        for table in inner_tables:
                            if table.name:
                                analysis.tables.add(table.name)

    def _extract_columns(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract column references grouped by table.

        Handles both qualified (table.column) and unqualified (column) references.
        For unqualified columns, attempts to infer table using information_schema.
        """
        for col in parsed.find_all(exp.Column):
            col_name = col.name
            table_name = col.table if col.table else None

            if not col_name:
                continue

            # If column has explicit table qualifier, use it
            if table_name:
                # Resolve alias to real table name
                real_table = analysis.table_aliases.get(table_name, table_name)
                analysis.columns[real_table].add(col_name)
            else:
                # Unqualified column - try to infer table
                inferred_table = self._infer_table_for_column(col_name, analysis)
                if inferred_table:
                    analysis.columns[inferred_table].add(col_name)

    def _infer_table_for_column(self, col_name: str, analysis: QueryAnalysis) -> str | None:
        """Infer which table an unqualified column belongs to.

        Uses information_schema data if available, otherwise falls back to heuristics.

        Args:
            col_name: Column name to infer table for
            analysis: Current query analysis

        Returns:
            Table name or None if can't be inferred
        """
        # Get tables involved in this query
        query_tables = list(analysis.tables)

        if not query_tables:
            return None

        # If only one table in query, must be that table
        if len(query_tables) == 1:
            return query_tables[0]

        # Use information_schema to find which tables have this column
        if self.table_columns:
            matching_tables = [t for t in query_tables if col_name in self.table_columns.get(t, set())]

            # If exactly one table in the query has this column, use it
            if len(matching_tables) == 1:
                return matching_tables[0]

            # If multiple tables have it, prefer the FROM table (if available)
            if len(matching_tables) > 1 and analysis.from_table:
                if analysis.from_table in matching_tables:
                    return analysis.from_table

        # Fall back to FROM table if we can't determine
        return analysis.from_table

    def _extract_aggregations(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract aggregation functions from SELECT clause, capturing aliases."""
        # Find SELECT clause and extract aggregations with their aliases
        for select in parsed.find_all(exp.Select):
            for select_expr in select.expressions:
                # Check if this is an alias wrapping an aggregation
                if isinstance(select_expr, exp.Alias):
                    alias_name = select_expr.alias
                    inner_expr = select_expr.this
                else:
                    alias_name = None
                    inner_expr = select_expr

                # Find aggregations in this SELECT expression
                aggs = list(inner_expr.find_all(exp.AggFunc))

                # Check if this is a derived metric (has operators AND aggregations)
                operators = (exp.Div, exp.Mul, exp.Add, exp.Sub)
                is_derived = any(isinstance(node, operators) for node in inner_expr.walk()) and len(aggs) > 0

                # Handle aggregations (even if part of derived metrics - we need base metrics for model generation)
                for agg in aggs:
                    agg_type_name = type(agg).__name__.lower()

                    # Map common aggregation class names to standard names
                    agg_name_map = {
                        "sum": "sum",
                        "avg": "avg",
                        "count": "count",
                        "min": "min",
                        "max": "max",
                        "stddev": "stddev",
                        "stddevpop": "stddev_pop",
                        "stddevsamp": "stddev_samp",
                        "variance": "variance",
                        "variancepop": "var_pop",
                        "median": "median",
                        "approxdistinct": "approx_distinct",
                        "approxquantile": "approx_quantile",
                    }

                    agg_name = agg_name_map.get(agg_type_name, agg_type_name)
                    col = agg.this

                    # Determine table
                    table_name = None

                    # Extract column/expression info
                    if isinstance(col, exp.Column):
                        col_name = col.name
                        table_name = col.table if col.table else None
                    elif isinstance(col, exp.Star):
                        col_name = "*"
                    elif isinstance(col, exp.Distinct):
                        if col.expressions and isinstance(col.expressions[0], exp.Column):
                            distinct_col = col.expressions[0]
                            col_name = distinct_col.name
                            table_name = distinct_col.table if distinct_col.table else None
                            agg_name = "count_distinct"
                        else:
                            col_name = str(col)
                    else:
                        # Complex expression - use alias if available, otherwise full SQL
                        col_name = str(col)
                        # Try to infer table from columns in expression
                        columns = list(col.find_all(exp.Column))
                        if columns:
                            first_col = columns[0]
                            table_name = first_col.table if first_col.table else None

                    # Infer table if not found
                    if not table_name and len(analysis.tables) == 1:
                        table_name = list(analysis.tables)[0]

                    # Store aggregation
                    agg_tuple = (agg_name, col_name, table_name or "")
                    analysis.aggregations.append(agg_tuple)

                    # Track alias if this SELECT expression has only one aggregation
                    # For derived metrics (multiple aggs with operators), the alias goes on the derived metric, not individual aggs
                    if alias_name and len(aggs) == 1:
                        analysis.aggregation_aliases[agg_tuple] = alias_name

                    # Mark if part of a derived metric
                    if is_derived:
                        analysis.aggregations_in_derived.add(agg_tuple)

                    # If this is a complex expression with an alias (not part of derived), also add as derived metric
                    if alias_name and not isinstance(col, (exp.Column, exp.Star)) and not is_derived:
                        # This is a complex aggregation with an alias - treat as derived metric
                        analysis.derived_metrics.append((alias_name, str(agg), table_name or ""))
                        # Mark this aggregation as part of a derived metric
                        analysis.aggregations_in_derived.add((agg_name, col_name, table_name or ""))

    def _extract_derived_metrics(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract derived metrics from SELECT clause - expressions with operators and aggregations.

        Also tracks which aggregations are part of derived metrics so they can be excluded
        from direct SELECT (they're only needed as base metrics for derived calculations).
        """
        # Track aggregations that are part of derived metrics
        derived_agg_sql = set()

        for select_expr in parsed.find_all(exp.Select):
            for expr in select_expr.expressions:
                # Skip simple columns (dimensions)
                if isinstance(expr.this, exp.Column):
                    continue

                # Skip simple aggregations (already handled)
                if isinstance(expr.this, exp.AggFunc) and isinstance(expr, exp.Alias):
                    # Check if it's a simple aggregation with no operators
                    aggs = list(expr.find_all(exp.AggFunc))
                    if len(aggs) == 1:
                        continue

                # Check for expressions with operators (Div, Mul, Add, Sub)
                operators = (exp.Div, exp.Mul, exp.Add, exp.Sub)
                if isinstance(expr.this, operators):
                    # This is a derived metric
                    metric_name = expr.alias_or_name
                    metric_sql = str(expr.this)

                    # Find all aggregations in the expression
                    aggs = list(expr.find_all(exp.AggFunc))

                    # Track these aggregations so we don't include them separately
                    for agg in aggs:
                        derived_agg_sql.add(str(agg))

                    # Determine the table - try to infer from aggregations
                    table_name = ""
                    for agg in aggs:
                        for col in agg.find_all(exp.Column):
                            if col.table:
                                table_name = col.table
                                break
                        if table_name:
                            break

                    # If no table found and single table query, use that table
                    if not table_name and len(analysis.tables) == 1:
                        table_name = list(analysis.tables)[0]

                    analysis.derived_metrics.append((metric_name, metric_sql, table_name))

        # Mark which aggregations are part of derived metrics (for query rewriting)
        # Store this info so we can filter during rewriting but keep for model generation
        for agg_type, col_name, table_name in analysis.aggregations:
            if agg_type == "count" and col_name == "*":
                agg_sql = "COUNT(*)"
            elif agg_type == "count_distinct":
                agg_sql = f"COUNT(DISTINCT {col_name})"
            else:
                agg_sql = f"{agg_type.upper()}({col_name})"

            if agg_sql in derived_agg_sql:
                analysis.aggregations_in_derived.add((agg_type, col_name, table_name))

    def _extract_group_by(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract GROUP BY columns, handling complex expressions and ordinals."""
        for group in parsed.find_all(exp.Group):
            for expr in group.expressions:
                if isinstance(expr, exp.Column):
                    # Simple column - resolve alias to real table name
                    table_name = expr.table if expr.table else ""
                    if table_name and table_name in analysis.table_aliases:
                        table_name = analysis.table_aliases[table_name]
                    col_name = expr.name
                    if col_name:
                        analysis.group_by_columns.add((table_name, col_name))
                elif isinstance(expr, exp.Literal):
                    # GROUP BY ordinal position (e.g., GROUP BY 1, 2)
                    # Resolve ordinal to actual SELECT expression
                    try:
                        ordinal = int(str(expr))
                        # Find the SELECT clause and get the expression at this position
                        for select in parsed.find_all(exp.Select):
                            if ordinal <= len(select.expressions):
                                select_expr = select.expressions[ordinal - 1]
                                # Extract columns from this expression
                                for col in select_expr.find_all(exp.Column):
                                    table_name = col.table if col.table else ""
                                    if table_name and table_name in analysis.table_aliases:
                                        table_name = analysis.table_aliases[table_name]
                                    col_name = col.name
                                    if col_name:
                                        analysis.group_by_columns.add((table_name, col_name))
                                break
                    except (ValueError, AttributeError):
                        # Not a valid ordinal, skip
                        continue
                else:
                    # Complex expression (CASE, EXTRACT, COALESCE, etc.)
                    # Extract all columns referenced in the expression
                    for col in expr.find_all(exp.Column):
                        table_name = col.table if col.table else ""
                        if table_name and table_name in analysis.table_aliases:
                            table_name = analysis.table_aliases[table_name]
                        col_name = col.name
                        if col_name:
                            analysis.group_by_columns.add((table_name, col_name))

    def _extract_joins(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract JOIN clauses with proper table and alias tracking."""
        for join in parsed.find_all(exp.Join):
            if isinstance(join.this, exp.Table):
                to_table = join.this.name
                to_alias = join.this.alias or None

                # Get join type - sqlglot separates side (LEFT/RIGHT/FULL) from kind (JOIN/OUTER)
                join_parts = []
                if join.side:
                    join_parts.append(join.side)
                if join.kind:
                    join_parts.append(join.kind)

                # Always ensure "JOIN" is at the end if not already present
                if not join_parts or join_parts[-1] != "JOIN":
                    join_parts.append("JOIN")

                join_type = " ".join(join_parts)

                # Get ON condition
                on_clause = str(join.args.get("on", "")) if join.args.get("on") else ""

                # The from_table is the main FROM table
                from_table = analysis.from_table or ""
                from_alias = analysis.from_alias

                analysis.joins.append((from_table, from_alias, to_table, to_alias, join_type, on_clause))

    def _extract_relationships(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract relationships from JOIN ON conditions and WHERE clause equality conditions.

        Uses information_schema data if available, falls back to pattern matching.
        """

        # Helper function to extract relationship from an equality condition
        def extract_relationship_from_eq(eq_expr: exp.EQ) -> None:
            left = eq_expr.left
            right = eq_expr.right

            # Both sides should be columns
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                return

            # Resolve table aliases
            left_table = analysis.table_aliases.get(left.table, left.table) if left.table else ""
            right_table = analysis.table_aliases.get(right.table, right.table) if right.table else ""

            # Skip if same table (not a relationship)
            if left_table == right_table:
                return

            # Try information_schema first
            fk_table = None
            fk_column = None
            pk_table = None
            pk_column = None

            # Check if left side is a known FK
            if (left_table, left.name) in self.foreign_keys:
                pk_table, pk_column = self.foreign_keys[(left_table, left.name)]
                fk_table = left_table
                fk_column = left.name
            # Check if right side is a known FK
            elif (right_table, right.name) in self.foreign_keys:
                pk_table, pk_column = self.foreign_keys[(right_table, right.name)]
                fk_table = right_table
                fk_column = right.name

            # Fall back to pattern matching if information_schema didn't help
            if not fk_table:
                # Determine which side has the foreign key by checking column names
                # Foreign keys typically end with _id (e.g., customer_id, product_id)
                left_is_fk = left.name.endswith("_id")
                right_is_fk = right.name.endswith("_id")

                if left_is_fk and not right_is_fk:
                    # Left has the FK, right has the PK
                    fk_table = left_table
                    fk_column = left.name
                    pk_table = right_table
                    pk_column = right.name
                elif right_is_fk and not left_is_fk:
                    # Right has the FK, left has the PK
                    fk_table = right_table
                    fk_column = right.name
                    pk_table = left_table
                    pk_column = left.name
                else:
                    # Can't determine - use left as FK
                    fk_table = left_table
                    fk_column = left.name
                    pk_table = right_table
                    pk_column = right.name

            # Infer primary key column if not from information_schema
            if pk_column and not self.foreign_keys:
                # If it ends with _id, the actual PK is probably "id"
                pk_column = "id" if pk_column.endswith("_id") else pk_column

            # Generate relationships for both directions
            # FK table has many_to_one relationship to PK table
            analysis.relationships.append((fk_table, pk_table, "many_to_one", fk_column, pk_column))

            # PK table has one_to_many relationship to FK table
            # For one_to_many, we don't store FK (it's on the other side)
            analysis.relationships.append((pk_table, fk_table, "one_to_many", None, None))

        # Extract from JOIN ON conditions
        for join in parsed.find_all(exp.Join):
            if not isinstance(join.this, exp.Table):
                continue

            to_table = join.this.name

            # Parse ON condition
            on_clause = join.args.get("on")
            if on_clause and isinstance(on_clause, exp.EQ):
                # For ambiguous cases, pass context about which table is being joined TO
                left = on_clause.left
                right = on_clause.right

                if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                    left_table = analysis.table_aliases.get(left.table, left.table) if left.table else ""
                    right_table = analysis.table_aliases.get(right.table, right.table) if right.table else ""

                    # If both columns end with _id, use join direction to determine FK
                    if left.name.endswith("_id") and right.name.endswith("_id"):
                        # The table being joined TO (to_table) usually has the FK
                        if right_table == to_table:
                            # Right table is being joined, so it has the FK
                            fk_table = right_table
                            fk_column = right.name
                            pk_table = left_table
                            pk_column = "id" if right.name.endswith("_id") else right.name

                            analysis.relationships.append((fk_table, pk_table, "many_to_one", fk_column, pk_column))
                            analysis.relationships.append((pk_table, fk_table, "one_to_many", None, None))
                            continue
                        elif left_table == to_table:
                            # Left table is being joined, so it has the FK
                            fk_table = left_table
                            fk_column = left.name
                            pk_table = right_table
                            pk_column = "id" if left.name.endswith("_id") else left.name

                            analysis.relationships.append((fk_table, pk_table, "many_to_one", fk_column, pk_column))
                            analysis.relationships.append((pk_table, fk_table, "one_to_many", None, None))
                            continue

                extract_relationship_from_eq(on_clause)

        # Extract from WHERE clause (for implicit joins like FROM t1, t2 WHERE t1.id = t2.fk)
        for where in parsed.find_all(exp.Where):
            # Find all equality conditions in WHERE
            for eq_expr in where.find_all(exp.EQ):
                extract_relationship_from_eq(eq_expr)

    def _extract_select_dimensions(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract dimensions from SELECT clause when no GROUP BY exists.

        For queries like SELECT DISTINCT status, region FROM orders,
        treat the selected columns as dimensions.
        """
        # Only extract if there's no GROUP BY and no aggregations
        if analysis.group_by_columns or analysis.aggregations:
            return

        for select in parsed.find_all(exp.Select):
            for select_expr in select.expressions:
                # Skip Star (SELECT *)
                if isinstance(select_expr, exp.Star):
                    continue

                # Extract simple columns
                if isinstance(select_expr, exp.Column):
                    table_name = select_expr.table if select_expr.table else ""
                    col_name = select_expr.name
                    if col_name:
                        analysis.group_by_columns.add((table_name, col_name))

                # Extract from aliased columns
                elif isinstance(select_expr, exp.Alias) and isinstance(select_expr.this, exp.Column):
                    col = select_expr.this
                    table_name = col.table if col.table else ""
                    col_name = col.name
                    if col_name:
                        analysis.group_by_columns.add((table_name, col_name))

    def _extract_time_dimensions(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract time dimensions with granularity from DATE_TRUNC, TIMESTAMP_TRUNC, EXTRACT, etc."""
        # Look for TIMESTAMP_TRUNC (sqlglot converts DATE_TRUNC to this)
        for func in parsed.find_all(exp.TimestampTrunc):
            # TIMESTAMP_TRUNC(column, granularity) in sqlglot format
            column_expr = func.this
            unit_expr = func.args.get("unit")

            if isinstance(column_expr, exp.Column) and unit_expr:
                col_name = column_expr.name
                table_name = column_expr.table if column_expr.table else ""
                granularity = str(unit_expr).lower().strip("'\"")

                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]

                analysis.time_dimensions.append((table_name, col_name, granularity))

        # Look for EXTRACT(part FROM column)
        for func in parsed.find_all(exp.Extract):
            # In sqlglot's Extract: this=part (YEAR), expression=column (order_date)
            part_expr = func.this  # The date part (YEAR, MONTH, etc.)
            column_expr = func.expression  # The column being extracted from

            if isinstance(column_expr, exp.Column):
                col_name = column_expr.name
                table_name = column_expr.table if column_expr.table else ""
                granularity = str(part_expr).lower()

                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]

                analysis.time_dimensions.append((table_name, col_name, granularity))

    def _extract_window_functions(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract window functions and generate cumulative metric definitions.

        Detects patterns:
        - Running total: SUM(x) OVER (ORDER BY date)
        - Rolling window: SUM(x) OVER (ORDER BY date ROWS BETWEEN N PRECEDING AND CURRENT ROW)
        - Period-to-date: SUM(x) OVER (PARTITION BY DATE_TRUNC('month', date) ORDER BY date)
        """
        try:
            for select in parsed.find_all(exp.Select):
                for select_expr in select.expressions:
                    # Get alias if present
                    metric_name = None
                if isinstance(select_expr, exp.Alias):
                    metric_name = select_expr.alias
                    inner_expr = select_expr.this
                else:
                    inner_expr = select_expr

                # Check if this is a window function
                if not isinstance(inner_expr, exp.Window):
                    continue

                # Extract the base aggregation inside the window
                agg_func = inner_expr.this
                if not isinstance(agg_func, exp.AggFunc):
                    # Skip non-aggregation window functions (ROW_NUMBER, RANK, etc.)
                    continue

                # Get aggregation type and column
                agg_type_name = type(agg_func).__name__.lower()
                agg_name_map = {
                    "sum": "sum",
                    "avg": "avg",
                    "count": "count",
                    "min": "min",
                    "max": "max",
                }
                agg_type = agg_name_map.get(agg_type_name, agg_type_name)

                # Extract column from aggregation
                col_expr = agg_func.this
                if col_expr is None:
                    # No column expression - skip this window function
                    continue
                elif isinstance(col_expr, exp.Column):
                    col_name = col_expr.name
                    table_name = col_expr.table if col_expr.table else ""
                elif isinstance(col_expr, exp.Star):
                    col_name = "*"
                    table_name = ""
                else:
                    # Complex expression
                    col_name = str(col_expr)
                    table_name = ""
                    # Try to infer table
                    columns = list(col_expr.find_all(exp.Column))
                    if columns:
                        table_name = columns[0].table if columns[0].table else ""

                # Infer table if not found
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]

                # Generate base metric reference
                if agg_type == "count" and col_name == "*":
                    base_metric_name = "count"
                elif agg_type == "count_distinct":
                    base_metric_name = f"{col_name}_count"
                else:
                    base_metric_name = f"{agg_type}_{col_name}"

                # Build base metric reference in model.metric format
                base_metric_ref = f"{table_name}.{base_metric_name}" if table_name else base_metric_name

                # Analyze OVER clause
                cumulative_params = self._analyze_window_spec(inner_expr, analysis)

                # Generate metric name if not aliased
                if not metric_name:
                    if cumulative_params.get("window"):
                        metric_name = f"rolling_{base_metric_name}"
                    elif cumulative_params.get("grain_to_date"):
                        grain = cumulative_params["grain_to_date"]
                        metric_name = f"{grain}_to_date_{base_metric_name}"
                    else:
                        metric_name = f"running_{base_metric_name}"

                # Store cumulative metric info
                cumulative_metric = {
                    "name": metric_name,
                    "base_metric": base_metric_ref,
                    "table": table_name,
                    **cumulative_params,
                }

                analysis.cumulative_metrics.append(cumulative_metric)

                # Mark the base aggregation as being part of a cumulative metric
                # so it doesn't get added as a standalone metric
                base_agg_tuple = (agg_type, col_name, table_name)
                analysis.aggregations_in_cumulative.add(base_agg_tuple)
        except (AttributeError, TypeError, KeyError):
            # If there are any issues extracting window functions, just skip them
            # This ensures the analyzer doesn't fail on complex window function queries
            pass

    def _analyze_window_spec(self, window_expr: exp.Window, analysis: QueryAnalysis) -> dict:
        """Analyze the OVER clause to determine cumulative metric parameters.

        Returns:
            Dictionary with cumulative metric parameters (window, grain_to_date, etc.)
        """
        params = {}

        try:
            # Check for PARTITION BY
            partition_by = window_expr.args.get("partition_by")
            if partition_by:
                # Handle list of partition expressions
                partition_exprs = partition_by if isinstance(partition_by, list) else [partition_by]

                # Look for DATE_TRUNC or EXTRACT in PARTITION BY
                for partition_expr in partition_exprs:
                    if partition_expr is None:
                        continue

                    # Check for TIMESTAMP_TRUNC (DATE_TRUNC)
                    if isinstance(partition_expr, exp.TimestampTrunc):
                        unit_expr = partition_expr.args.get("unit")
                        if unit_expr:
                            grain = str(unit_expr).lower().strip("'\"")
                            params["grain_to_date"] = grain
                            break
                    # Check for EXTRACT
                    elif isinstance(partition_expr, exp.Extract):
                        part_expr = partition_expr.this
                        if part_expr:
                            grain = str(part_expr).lower()
                            # Map EXTRACT parts to grain_to_date values
                            grain_map = {
                                "year": "year",
                                "quarter": "quarter",
                                "month": "month",
                                "week": "week",
                                "day": "day",
                            }
                            if grain in grain_map:
                                params["grain_to_date"] = grain_map[grain]
                                break

            # Check for window frame (ROWS/RANGE BETWEEN)
            frame = window_expr.args.get("spec")
            if frame:
                # Look for ROWS BETWEEN N PRECEDING
                frame_str = str(frame).upper()
                if "ROWS BETWEEN" in frame_str or "RANGE BETWEEN" in frame_str:
                    # Try to extract window size
                    # Pattern: "N PRECEDING" where N is a number
                    import re

                    match = re.search(r"(\d+)\s+PRECEDING", frame_str)
                    if match:
                        window_size = int(match.group(1))
                        # Store as "N days" (we assume time-based windows)
                        # This is a simplification - ideally we'd detect the time unit from context
                        params["window"] = f"{window_size} days"
        except (AttributeError, TypeError):
            # If there are any issues parsing the window spec, just return empty params
            pass

        return params

    def _extract_filters(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract WHERE clause filters."""
        for where in parsed.find_all(exp.Where):
            filter_expr = str(where.this)
            if filter_expr:
                analysis.filters.append(filter_expr)

    def _extract_having(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract HAVING clause."""
        for having in parsed.find_all(exp.Having):
            having_expr = str(having.this)
            if having_expr:
                analysis.having_clauses.append(having_expr)

    def _extract_order_by(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract ORDER BY clause with direction (ASC/DESC)."""
        for order in parsed.find_all(exp.Order):
            for ordered_expr in order.expressions:
                # Extract the column/expression and direction
                if isinstance(ordered_expr, exp.Ordered):
                    column_expr = str(ordered_expr.this)
                    # desc attribute is True if DESC, False/None if ASC
                    direction = "DESC" if ordered_expr.args.get("desc") else "ASC"
                    order_expr = f"{column_expr} {direction}"
                else:
                    # Fallback for simple expressions without explicit ordering
                    order_expr = str(ordered_expr)

                if order_expr:
                    analysis.order_by.append(order_expr)

    def _extract_limit(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract LIMIT clause."""
        for limit in parsed.find_all(exp.Limit):
            if limit.expression:
                try:
                    analysis.limit = int(str(limit.expression))
                except ValueError:
                    pass

    def _check_coverage(self, analysis: QueryAnalysis) -> None:
        """Check if query can be rewritten and identify gaps."""
        # Check if all tables are in the semantic layer as models
        for table in analysis.tables:
            if table not in self.table_to_model:
                analysis.missing_models.add(table)

        # For each aggregation, check if we have a corresponding metric
        for agg_type, col_name, table_name in analysis.aggregations:
            if not table_name and len(analysis.tables) == 1:
                table_name = list(analysis.tables)[0]

            # Get model name from table name
            model_name = self.table_to_model.get(table_name)
            if model_name:
                model = self.layer.graph.models[model_name]

                # Check if metric exists
                metric_found = False
                for metric in model.metrics:
                    if metric.agg == agg_type and col_name in (metric.sql, "*"):
                        metric_found = True
                        break

                if not metric_found:
                    analysis.missing_metrics.add((model_name, agg_type, col_name))

        # For each GROUP BY column, check if we have a corresponding dimension
        for table_name, col_name in analysis.group_by_columns:
            if not table_name and len(analysis.tables) == 1:
                table_name = list(analysis.tables)[0]

            # Get model name from table name
            model_name = self.table_to_model.get(table_name)
            if model_name:
                model = self.layer.graph.models[model_name]

                # Check if dimension exists
                dim_found = False
                for dim in model.dimensions:
                    if dim.sql == col_name or dim.name == col_name:
                        dim_found = True
                        break

                if not dim_found:
                    analysis.missing_dimensions.add((model_name, col_name))

        # Determine if query can be rewritten
        analysis.can_rewrite = (
            len(analysis.missing_models) == 0
            and len(analysis.missing_metrics) == 0
            and len(analysis.missing_dimensions) == 0
            and analysis.parse_error is None
        )

        # Generate suggested rewrite if possible
        if analysis.can_rewrite:
            analysis.suggested_rewrite = self._generate_rewrite(analysis)

    def _generate_rewrite(self, analysis: QueryAnalysis) -> str:
        """Generate suggested semantic layer query rewrite.

        Args:
            analysis: Query analysis

        Returns:
            Python code showing how to rewrite the query
        """
        # Build dimension references
        dimensions = []
        for table_name, col_name in analysis.group_by_columns:
            if not table_name and len(analysis.tables) == 1:
                table_name = list(analysis.tables)[0]

            # Get model name from table name
            model_name = self.table_to_model.get(table_name, table_name)
            dimensions.append(f"{model_name}.{col_name}")

        # Build metric references
        metrics = []
        for agg_type, col_name, table_name in analysis.aggregations:
            if not table_name and len(analysis.tables) == 1:
                table_name = list(analysis.tables)[0]

            # Get model name from table name
            model_name = self.table_to_model.get(table_name)
            if model_name:
                model = self.layer.graph.models[model_name]
                for metric in model.metrics:
                    if metric.agg == agg_type and col_name in (metric.sql, "*"):
                        metrics.append(f"{model_name}.{metric.name}")
                        break

        # Build filter clause
        where_clause = None
        if analysis.filters:
            # Simplification: just show first filter
            where_clause = analysis.filters[0]

        # Generate code
        parts = []
        if dimensions:
            parts.append(f"dimensions={dimensions}")
        if metrics:
            parts.append(f"metrics={metrics}")
        if where_clause:
            parts.append(f'where="{where_clause}"')

        return f"layer.query({', '.join(parts)})"

    def _generate_report(self) -> MigrationReport:
        """Generate coverage report from analyses.

        Returns:
            Coverage report
        """
        total = len(self.analyses)
        parseable = sum(1 for a in self.analyses if a.parse_error is None)
        rewritable = sum(1 for a in self.analyses if a.can_rewrite)

        # Aggregate missing components
        all_missing_models = set()
        all_missing_dimensions: dict[str, set[str]] = defaultdict(set)
        all_missing_metrics: dict[str, set[tuple[str, str]]] = defaultdict(set)

        for analysis in self.analyses:
            all_missing_models.update(analysis.missing_models)

            for model, dim in analysis.missing_dimensions:
                all_missing_dimensions[model].add(dim)

            for model, agg, col in analysis.missing_metrics:
                all_missing_metrics[model].add((agg, col))

        coverage_pct = (rewritable / total * 100) if total > 0 else 0.0

        return MigrationReport(
            total_queries=total,
            parseable_queries=parseable,
            rewritable_queries=rewritable,
            query_analyses=self.analyses,
            missing_models=all_missing_models,
            missing_dimensions=all_missing_dimensions,
            missing_metrics=all_missing_metrics,
            coverage_percentage=coverage_pct,
        )

    def print_report(self, report: MigrationReport, verbose: bool = False) -> None:
        """Print coverage report to console.

        Args:
            report: Coverage report to print
            verbose: Show detailed query analysis
        """
        print(f"\n{'=' * 80}")
        print("SEMANTIC LAYER COVERAGE REPORT")
        print(f"{'=' * 80}\n")

        # Summary
        print(f"Total Queries:      {report.total_queries}")
        print(f"Parseable:          {report.parseable_queries}")
        print(f"Rewritable:         {report.rewritable_queries}")
        print(f"Coverage:           {report.coverage_percentage:.1f}%\n")

        # Missing components
        if report.missing_models:
            print(f"\n{'─' * 80}")
            print("MISSING MODELS")
            print(f"{'─' * 80}")
            for model in sorted(report.missing_models):
                print(f"  • {model}")

        if report.missing_dimensions:
            print(f"\n{'─' * 80}")
            print("MISSING DIMENSIONS")
            print(f"{'─' * 80}")
            for model in sorted(report.missing_dimensions.keys()):
                dims = report.missing_dimensions[model]
                print(f"\n  Model: {model}")
                for dim in sorted(dims):
                    print(f"    • {dim}")

        if report.missing_metrics:
            print(f"\n{'─' * 80}")
            print("MISSING METRICS")
            print(f"{'─' * 80}")
            for model in sorted(report.missing_metrics.keys()):
                metrics = report.missing_metrics[model]
                print(f"\n  Model: {model}")
                for agg, col in sorted(metrics):
                    print(f"    • {agg}({col})")

        # Detailed query analysis
        if verbose:
            print(f"\n{'─' * 80}")
            print("QUERY DETAILS")
            print(f"{'─' * 80}")

            for i, analysis in enumerate(report.query_analyses, 1):
                print(f"\nQuery #{i}:")
                print(f"  Can Rewrite: {'✓' if analysis.can_rewrite else '✗'}")

                if analysis.parse_error:
                    print(f"  Parse Error: {analysis.parse_error}")
                    continue

                if analysis.tables:
                    print(f"  Tables: {', '.join(sorted(analysis.tables))}")

                if analysis.aggregations:
                    print("  Aggregations:")
                    for agg, col, table in analysis.aggregations:
                        table_str = f"{table}." if table else ""
                        print(f"    • {agg}({table_str}{col})")

                if analysis.group_by_columns:
                    print("  Group By:")
                    for table, col in sorted(analysis.group_by_columns):
                        table_str = f"{table}." if table else ""
                        print(f"    • {table_str}{col}")

                if analysis.suggested_rewrite:
                    print("  Suggested Rewrite:")
                    print(f"    {analysis.suggested_rewrite}")

                if analysis.missing_models:
                    print(f"  Missing Models: {', '.join(sorted(analysis.missing_models))}")

                if analysis.missing_dimensions:
                    print("  Missing Dimensions:")
                    for model, dim in sorted(analysis.missing_dimensions):
                        print(f"    • {model}.{dim}")

                if analysis.missing_metrics:
                    print("  Missing Metrics:")
                    for model, agg, col in sorted(analysis.missing_metrics):
                        print(f"    • {model}.{agg}({col})")

        print(f"\n{'=' * 80}\n")

    def generate_models(self, report: MigrationReport) -> dict[str, dict]:
        """Generate model definitions from query analysis.

        Args:
            report: Coverage report

        Returns:
            Dictionary mapping model names to model definitions (YAML-ready)
        """
        models = {}

        # Aggregate all discovered patterns
        all_tables = set()
        table_dimensions = defaultdict(set)
        table_time_dimensions = defaultdict(set)  # (col_name, granularity)
        table_metrics = defaultdict(set)
        table_derived_metrics = defaultdict(list)  # (name, sql_expression)
        table_cumulative_metrics = defaultdict(list)  # cumulative metric dicts
        table_relationships = defaultdict(list)  # (to_model, type, foreign_key, primary_key)
        metric_aliases = {}  # (table, agg_type, col_name) -> alias

        for analysis in report.query_analyses:
            if analysis.parse_error:
                continue

            # Track tables
            all_tables.update(analysis.tables)

            # Track dimensions from GROUP BY
            for table_name, col_name in analysis.group_by_columns:
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]
                if table_name:
                    table_dimensions[table_name].add(col_name)

            # Track time dimensions
            for table_name, col_name, granularity in analysis.time_dimensions:
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]
                if table_name:
                    table_time_dimensions[table_name].add((col_name, granularity))

            # Track metrics from aggregations (but skip those in cumulative metrics)
            for agg_type, col_name, table_name in analysis.aggregations:
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]
                if table_name:
                    # Skip if this aggregation is part of a cumulative metric
                    agg_tuple = (agg_type, col_name, table_name)
                    if agg_tuple not in analysis.aggregations_in_cumulative:
                        table_metrics[table_name].add((agg_type, col_name))
                        # Track alias if one exists
                        if agg_tuple in analysis.aggregation_aliases:
                            metric_aliases[(table_name, agg_type, col_name)] = analysis.aggregation_aliases[agg_tuple]

            # Track derived metrics
            for metric_name, metric_sql, table_name in analysis.derived_metrics:
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]
                if table_name:
                    table_derived_metrics[table_name].append((metric_name, metric_sql))

            # Track cumulative metrics
            for cumulative_metric in analysis.cumulative_metrics:
                table_name = cumulative_metric.get("table")
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]
                if table_name:
                    table_cumulative_metrics[table_name].append(cumulative_metric)

            # Track relationships
            for from_model, to_model, rel_type, fk_col, pk_col in analysis.relationships:
                if from_model and to_model:
                    # Store relationship on the from_model
                    table_relationships[from_model].append((to_model, rel_type, fk_col, pk_col))

        # Generate model definitions
        for table in sorted(all_tables):
            model_name = table
            model_def = {
                "model": {
                    "name": model_name,
                    "table": table,
                    "description": "Auto-generated from query analysis",
                }
            }

            # Add dimensions
            dims = []
            # Build set of time dimension column names for this table
            time_dim_cols = {col_name for col_name, _ in table_time_dimensions.get(table, set())}

            if table in table_dimensions:
                for dim_name in sorted(table_dimensions[table]):
                    # Check if this is a time dimension
                    dim_type = "time" if dim_name in time_dim_cols else "categorical"
                    dims.append(
                        {
                            "name": dim_name,
                            "sql": dim_name,
                            "type": dim_type,
                        }
                    )

            # Add time dimensions that weren't in GROUP BY
            if table in table_time_dimensions:
                for col_name, _granularity in sorted(table_time_dimensions[table]):
                    # Only add if not already added from GROUP BY
                    if col_name not in table_dimensions.get(table, set()):
                        dims.append(
                            {
                                "name": col_name,
                                "sql": col_name,
                                "type": "time",
                            }
                        )

            if dims:
                model_def["dimensions"] = dims

            # Add metrics (base, derived, and cumulative)
            # Check if there are any metrics to add
            has_metrics = table in table_metrics or table in table_derived_metrics or table in table_cumulative_metrics

            if has_metrics:
                metrics = []
                seen_metrics = set()

                # Add base metrics
                if table in table_metrics:
                    for agg_type, col_name in sorted(table_metrics[table]):
                        # Check if we have an alias for this metric
                        alias_key = (table, agg_type, col_name)
                        if alias_key in metric_aliases:
                            metric_name = metric_aliases[alias_key]
                        else:
                            # Generate metric name
                            if agg_type == "count" and col_name == "*":
                                metric_name = "count"
                            elif agg_type == "count" and col_name != "*":
                                # COUNT(column) - use column_count pattern
                                metric_name = f"{col_name}_count"
                            elif agg_type == "count_distinct":
                                metric_name = f"{col_name}_count"
                            else:
                                metric_name = f"{agg_type}_{col_name}"

                        # Avoid duplicates
                        if metric_name in seen_metrics:
                            continue
                        seen_metrics.add(metric_name)

                        metric_def = {
                            "name": metric_name,
                            "agg": agg_type,
                        }

                        if col_name != "*":
                            metric_def["sql"] = col_name
                        else:
                            metric_def["sql"] = "*"

                        metrics.append(metric_def)

                # Add derived metrics
                if table in table_derived_metrics:
                    for metric_name, metric_sql in table_derived_metrics[table]:
                        # Avoid duplicates
                        if metric_name in seen_metrics:
                            continue
                        seen_metrics.add(metric_name)

                        derived_metric_def = {
                            "name": metric_name,
                            "sql": metric_sql,
                            "type": "derived",
                        }
                        metrics.append(derived_metric_def)

                # Add cumulative metrics
                if table in table_cumulative_metrics:
                    for cumulative_metric in table_cumulative_metrics[table]:
                        metric_name = cumulative_metric["name"]
                        # Avoid duplicates
                        if metric_name in seen_metrics:
                            continue
                        seen_metrics.add(metric_name)

                        cumulative_metric_def = {
                            "name": metric_name,
                            "type": "cumulative",
                            "sql": cumulative_metric["base_metric"],
                        }

                        # Add optional cumulative parameters
                        if "window" in cumulative_metric:
                            cumulative_metric_def["window"] = cumulative_metric["window"]
                        if "grain_to_date" in cumulative_metric:
                            cumulative_metric_def["grain_to_date"] = cumulative_metric["grain_to_date"]

                        metrics.append(cumulative_metric_def)

                model_def["metrics"] = metrics

            # Add relationships
            if table in table_relationships:
                relationships = []
                seen_relationships = set()

                for to_model, rel_type, fk_col, pk_col in table_relationships[table]:
                    # Create unique key to avoid duplicates
                    rel_key = (to_model, rel_type)
                    if rel_key in seen_relationships:
                        continue
                    seen_relationships.add(rel_key)

                    rel_def = {
                        "name": to_model,
                        "type": rel_type,
                    }

                    # Add foreign_key for many_to_one relationships
                    if rel_type == "many_to_one" and fk_col:
                        rel_def["foreign_key"] = fk_col

                    # Optionally add primary_key if it's not the default "id"
                    if pk_col and pk_col != "id":
                        rel_def["primary_key"] = pk_col

                    relationships.append(rel_def)

                if relationships:
                    model_def["relationships"] = relationships

            models[model_name] = model_def

        return models

    def write_model_files(self, models: dict[str, dict], output_dir: str) -> None:
        """Write model definitions to YAML files.

        Args:
            models: Dictionary of model definitions from generate_models()
            output_dir: Directory to write model files to
        """
        from pathlib import Path

        import yaml

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for model_name, model_def in models.items():
            file_path = output_path / f"{model_name}.yml"
            with open(file_path, "w") as f:
                yaml.dump(model_def, f, default_flow_style=False, sort_keys=False)

            print(f"Generated: {file_path}")

    def generate_rewritten_queries(self, report: MigrationReport) -> dict[str, str]:
        """Generate rewritten SQL queries using semantic layer syntax.

        Args:
            report: Coverage report

        Returns:
            Dictionary mapping query names to rewritten SQL
        """
        rewritten = {}

        for i, analysis in enumerate(report.query_analyses, 1):
            if analysis.parse_error:
                continue

            # Helper to resolve table name from alias
            def resolve_table(table_or_alias: str) -> str:
                if not table_or_alias:
                    return ""
                # Check if it's an alias
                if table_or_alias in analysis.table_aliases:
                    return analysis.table_aliases[table_or_alias]
                return table_or_alias

            # Build SELECT clause with model.dimension and model.metric format
            select_parts = []

            # Add regular dimensions
            for table_name, col_name in analysis.group_by_columns:
                # Resolve alias to real table name
                real_table = resolve_table(table_name)
                if not real_table and len(analysis.tables) == 1:
                    real_table = list(analysis.tables)[0]
                select_parts.append(f"{real_table}.{col_name}")

            # Add time dimensions with granularity
            for table_name, col_name, granularity in analysis.time_dimensions:
                real_table = resolve_table(table_name)
                if not real_table and len(analysis.tables) == 1:
                    real_table = list(analysis.tables)[0]
                # Use semantic layer syntax: model.dimension__granularity
                select_parts.append(f"{real_table}.{col_name}__{granularity}")

            # Add metrics (but skip those that are only part of derived metrics)
            for agg_type, col_name, table_name in analysis.aggregations:
                # Skip if this aggregation is only part of a derived metric
                if (agg_type, col_name, table_name) in analysis.aggregations_in_derived:
                    continue

                real_table = resolve_table(table_name)
                if not real_table and len(analysis.tables) == 1:
                    real_table = list(analysis.tables)[0]

                # Generate metric name
                if agg_type == "count" and col_name == "*":
                    metric_name = "count"
                elif agg_type == "count" and col_name != "*":
                    # COUNT(column) - use column_count pattern
                    metric_name = f"{col_name}_count"
                elif agg_type == "count_distinct":
                    metric_name = f"{col_name}_count"
                else:
                    metric_name = f"{agg_type}_{col_name}"

                select_parts.append(f"{real_table}.{metric_name}")

            # Add derived metrics
            for metric_name, _metric_sql, table_name in analysis.derived_metrics:
                real_table = resolve_table(table_name)
                if not real_table and len(analysis.tables) == 1:
                    real_table = list(analysis.tables)[0]
                select_parts.append(f"{real_table}.{metric_name}")

            if not select_parts:
                continue

            # Build SQL query
            sql = "SELECT\n"
            sql += "    " + ",\n    ".join(select_parts)

            # Add FROM clause with JOINs preserved
            if analysis.joins:
                # Multi-table query with JOINs
                from_table = analysis.from_table or ""
                from_alias = analysis.from_alias

                if from_alias:
                    sql += f"\nFROM {from_table} {from_alias}"
                else:
                    sql += f"\nFROM {from_table}"

                # Add each JOIN
                for _from_table, _from_alias, to_table, to_alias, join_type, on_clause in analysis.joins:
                    if to_alias:
                        sql += f"\n{join_type} {to_table} {to_alias}"
                    else:
                        sql += f"\n{join_type} {to_table}"

                    if on_clause:
                        sql += f" ON {on_clause}"

            elif len(analysis.tables) == 1:
                # Single table query
                main_table = list(analysis.tables)[0]
                sql += f"\nFROM {main_table}"
            else:
                # Multi-table query without explicit JOINs
                tables_str = ", ".join(sorted(analysis.tables))
                sql += f"\nFROM {tables_str}"

            # Add WHERE clause
            if analysis.filters:
                # Combine all filters
                where_clause = " AND ".join(analysis.filters)
                sql += f"\nWHERE {where_clause}"

            # Add HAVING clause
            if analysis.having_clauses:
                having_clause = " AND ".join(analysis.having_clauses)
                sql += f"\nHAVING {having_clause}"

            # Add ORDER BY clause
            if analysis.order_by:
                order_clause = ", ".join(analysis.order_by)
                sql += f"\nORDER BY {order_clause}"

            # Add LIMIT clause
            if analysis.limit:
                sql += f"\nLIMIT {analysis.limit}"

            query_name = f"query_{i}"
            rewritten[query_name] = sql

        return rewritten

    def write_rewritten_queries(self, queries: dict[str, str], output_dir: str) -> None:
        """Write rewritten queries to SQL files.

        Args:
            queries: Dictionary of rewritten queries from generate_rewritten_queries()
            output_dir: Directory to write query files to
        """
        from pathlib import Path

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for query_name, sql in queries.items():
            file_path = output_path / f"{query_name}.sql"
            with open(file_path, "w") as f:
                f.write(sql)
                f.write("\n")

            print(f"Generated: {file_path}")
