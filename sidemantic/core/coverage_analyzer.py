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
    columns: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))  # table -> columns
    aggregations: list[tuple[str, str, str]] = field(default_factory=list)  # (agg_type, column, table)
    group_by_columns: set[tuple[str, str]] = field(default_factory=set)  # (table, column)
    joins: list[tuple[str, str, str]] = field(default_factory=list)  # (from_table, to_table, condition)
    filters: list[str] = field(default_factory=list)
    can_rewrite: bool = False
    missing_models: set[str] = field(default_factory=set)
    missing_dimensions: set[tuple[str, str]] = field(default_factory=set)  # (model, dimension)
    missing_metrics: set[tuple[str, str, str]] = field(default_factory=set)  # (model, agg, column)
    suggested_rewrite: str | None = None
    parse_error: str | None = None


@dataclass
class CoverageReport:
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


class CoverageAnalyzer:
    """Analyzes raw SQL queries for semantic layer coverage.

    Takes raw SQL queries and determines:
    - Which queries can be rewritten using the semantic layer
    - What models/dimensions/metrics are missing
    - How to rewrite queries using the semantic layer
    """

    def __init__(self, layer: SemanticLayer):
        """Initialize analyzer.

        Args:
            layer: Semantic layer to analyze coverage for
        """
        self.layer = layer
        self.analyses: list[QueryAnalysis] = []

        # Build mapping from table names to model names
        self.table_to_model: dict[str, str] = {}
        for model_name, model in layer.graph.models.items():
            if model.table:
                self.table_to_model[model.table] = model_name

    def analyze_queries(self, queries: list[str]) -> CoverageReport:
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

    def analyze_folder(self, folder_path: str, pattern: str = "*.sql") -> CoverageReport:
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
            self._extract_group_by(parsed, analysis)
            self._extract_joins(parsed, analysis)
            self._extract_filters(parsed, analysis)

            # Check coverage and generate suggestions
            self._check_coverage(analysis)

        except Exception as e:
            analysis.parse_error = str(e)

        return analysis

    def _extract_tables(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract table references."""
        for table in parsed.find_all(exp.Table):
            table_name = table.name
            if table_name:
                analysis.tables.add(table_name)

    def _extract_columns(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract column references grouped by table."""
        for col in parsed.find_all(exp.Column):
            col_name = col.name
            table_name = col.table if col.table else None

            if col_name and table_name:
                analysis.columns[table_name].add(col_name)

    def _extract_aggregations(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract aggregation functions."""
        # Build map of available aggregation functions
        agg_map = {
            exp.Sum: "sum",
            exp.Avg: "avg",
            exp.Count: "count",
            exp.Min: "min",
            exp.Max: "max",
        }

        # Add optional aggregation functions if they exist in this version of sqlglot
        optional_aggs = {
            "Stddev": "stddev",
            "StddevPop": "stddev_pop",
            "StddevSamp": "stddev_samp",
            "Variance": "variance",
            "VariancePop": "var_pop",
            "Median": "median",
            "ApproxDistinct": "approx_distinct",
            "ApproxQuantile": "approx_quantile",
        }

        for exp_name, agg_name in optional_aggs.items():
            if hasattr(exp, exp_name):
                agg_map[getattr(exp, exp_name)] = agg_name

        for agg_type, agg_name in agg_map.items():
            for agg in parsed.find_all(agg_type):
                # Get the column being aggregated
                col = agg.this
                if isinstance(col, exp.Column):
                    col_name = col.name
                    table_name = col.table if col.table else None
                    analysis.aggregations.append((agg_name, col_name, table_name or ""))
                elif isinstance(col, exp.Star):
                    # COUNT(*)
                    analysis.aggregations.append((agg_name, "*", ""))
                elif isinstance(col, exp.Distinct):
                    # COUNT(DISTINCT col) - handle specially
                    if col.expressions and isinstance(col.expressions[0], exp.Column):
                        distinct_col = col.expressions[0]
                        col_name = distinct_col.name
                        table_name = distinct_col.table if distinct_col.table else None
                        analysis.aggregations.append(("count_distinct", col_name, table_name or ""))

    def _extract_group_by(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract GROUP BY columns."""
        for group in parsed.find_all(exp.Group):
            for expr in group.expressions:
                if isinstance(expr, exp.Column):
                    table_name = expr.table if expr.table else ""
                    col_name = expr.name
                    if col_name:
                        analysis.group_by_columns.add((table_name, col_name))

    def _extract_joins(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract JOIN clauses."""
        for join in parsed.find_all(exp.Join):
            # Get joined table
            if isinstance(join.this, exp.Table):
                to_table = join.this.name

                # Get ON condition
                on_clause = str(join.args.get("on", "")) if join.args.get("on") else ""

                # Try to find the "from" table from context
                # This is a simplification - in practice we'd need more context
                from_table = list(analysis.tables)[0] if analysis.tables else ""

                analysis.joins.append((from_table, to_table, on_clause))

    def _extract_filters(self, parsed: exp.Expression, analysis: QueryAnalysis) -> None:
        """Extract WHERE clause filters."""
        for where in parsed.find_all(exp.Where):
            filter_expr = str(where.this)
            if filter_expr:
                analysis.filters.append(filter_expr)

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

    def _generate_report(self) -> CoverageReport:
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

        return CoverageReport(
            total_queries=total,
            parseable_queries=parseable,
            rewritable_queries=rewritable,
            query_analyses=self.analyses,
            missing_models=all_missing_models,
            missing_dimensions=all_missing_dimensions,
            missing_metrics=all_missing_metrics,
            coverage_percentage=coverage_pct,
        )

    def print_report(self, report: CoverageReport, verbose: bool = False) -> None:
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

    def generate_models(self, report: CoverageReport) -> dict[str, dict]:
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
        table_metrics = defaultdict(set)

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

            # Track metrics from aggregations
            for agg_type, col_name, table_name in analysis.aggregations:
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]
                if table_name:
                    table_metrics[table_name].add((agg_type, col_name))

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
            if table in table_dimensions:
                dims = []
                for dim_name in sorted(table_dimensions[table]):
                    dims.append(
                        {
                            "name": dim_name,
                            "sql": dim_name,
                            "type": "categorical",
                        }
                    )
                model_def["dimensions"] = dims

            # Add metrics
            if table in table_metrics:
                metrics = []
                seen_metrics = set()
                for agg_type, col_name in sorted(table_metrics[table]):
                    # Generate metric name
                    if agg_type == "count" and col_name == "*":
                        metric_name = "count"
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

                model_def["metrics"] = metrics

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

    def generate_rewritten_queries(self, report: CoverageReport) -> dict[str, str]:
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

            # Build SELECT clause with model.dimension and model.metric format
            select_parts = []

            # Add dimensions
            for table_name, col_name in analysis.group_by_columns:
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]
                select_parts.append(f"{table_name}.{col_name}")

            # Add metrics
            for agg_type, col_name, table_name in analysis.aggregations:
                if not table_name and len(analysis.tables) == 1:
                    table_name = list(analysis.tables)[0]

                # Generate metric name
                if agg_type == "count" and col_name == "*":
                    metric_name = "count"
                elif agg_type == "count_distinct":
                    metric_name = f"{col_name}_count"
                else:
                    metric_name = f"{agg_type}_{col_name}"

                select_parts.append(f"{table_name}.{metric_name}")

            if not select_parts:
                continue

            # Build SQL query
            sql = "SELECT\n"
            sql += "    " + ",\n    ".join(select_parts)

            # Determine main table
            if len(analysis.tables) == 1:
                main_table = list(analysis.tables)[0]
                sql += f"\nFROM {main_table}"

            # Add WHERE clause
            if analysis.filters:
                where_clause = analysis.filters[0]
                sql += f"\nWHERE {where_clause}"

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
