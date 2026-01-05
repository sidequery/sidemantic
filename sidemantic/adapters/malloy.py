"""Malloy adapter for importing/exporting Malloy semantic models.

Uses ANTLR4-generated parser from official Malloy grammar files.
"""

from __future__ import annotations

import re
from pathlib import Path

from antlr4 import CommonTokenStream, InputStream

from sidemantic.adapters.base import BaseAdapter
from sidemantic.adapters.malloy_grammar import MalloyLexer, MalloyParser, MalloyParserVisitor
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph


class MalloyModelVisitor(MalloyParserVisitor):
    """Visitor that extracts semantic model information from Malloy AST."""

    def __init__(self):
        self.models: list[Model] = []
        # Imports: list of (file_path, items) where items is list of (name, alias) or None for import-all
        self.imports: list[tuple[str, list[tuple[str, str | None]] | None]] = []
        self.current_model_name: str | None = None
        self.current_table: str | None = None
        self.current_sql: str | None = None
        self.current_primary_key: str = "id"
        self.current_description: str | None = None
        self.current_dimensions: list[Dimension] = []
        self.current_metrics: list[Metric] = []
        self.current_relationships: list[Relationship] = []
        self.current_segments: list[Segment] = []

    def _reset_current(self):
        """Reset current model state."""
        self.current_model_name = None
        self.current_table = None
        self.current_sql = None
        self.current_primary_key = "id"
        self.current_description = None
        self.current_dimensions = []
        self.current_metrics = []
        self.current_relationships = []
        self.current_segments = []

    def _parse_annotations(self, tags_ctx) -> str | None:
        """Parse annotations from tags context.

        Malloy annotations:
        - ## Description text -> description (DOC_ANNOTATION)
        - # key: value -> metadata (ANNOTATION)

        We extract:
        - Any ## text as description
        - # desc: value as description

        Returns the description text if found.
        """
        if tags_ctx is None:
            return None

        descriptions = []

        # Iterate through ANNOTATION tokens
        for i in range(tags_ctx.getChildCount()):
            child = tags_ctx.getChild(i)
            if child is not None:
                text = child.getText()

                # ## is a doc annotation (description)
                if text.startswith("##"):
                    # Strip ## and whitespace
                    desc = text[2:].strip()
                    if desc:
                        descriptions.append(desc)
                # # is a tag annotation
                elif text.startswith("#"):
                    # Strip # and check for desc: or description:
                    tag_text = text[1:].strip()
                    # Common patterns: desc: value, description: value
                    if tag_text.lower().startswith("desc:"):
                        desc = tag_text[5:].strip()
                        if desc:
                            descriptions.append(desc)
                    elif tag_text.lower().startswith("description:"):
                        desc = tag_text[12:].strip()
                        if desc:
                            descriptions.append(desc)

        if descriptions:
            return " ".join(descriptions)
        return None

    def visitImportStatement(self, ctx: MalloyParser.ImportStatementContext):  # noqa: N802
        """Visit import statement and extract dependencies.

        Malloy import syntax:
            import 'path/to/file.malloy'                    # Import all sources
            import { source1, source2 } from 'file.malloy'  # Named imports
            import { source1 is alias1 } from 'file.malloy' # Aliased imports
        """
        import_url = ctx.importURL()
        if not import_url:
            return self.visitChildren(ctx)

        # Extract file path from string literal
        url_string = import_url.string()
        file_path = self._extract_string(self._get_text(url_string)) if url_string else ""

        if not file_path:
            return self.visitChildren(ctx)

        # Check for selective imports: import { x, y } from 'file'
        import_select = ctx.importSelect()
        if import_select:
            items = []
            for import_item in import_select.importItem():
                # Grammar uses id_() method (underscore to avoid Python keyword)
                ids = import_item.id_()
                if ids:
                    # First id is the source name, second (if present) is the alias
                    name = self._get_text(ids[0])
                    alias = self._get_text(ids[1]) if len(ids) > 1 else None
                    items.append((name, alias))
            self.imports.append((file_path, items))
        else:
            # Import all sources from file
            self.imports.append((file_path, None))

        return self.visitChildren(ctx)

    def _get_text(self, ctx) -> str:
        """Get text from context, preserving whitespace.

        ANTLR's getText() concatenates tokens without whitespace.
        We reconstruct the original text using the token stream.
        """
        if ctx is None:
            return ""

        # Try to get original text with whitespace from token stream
        try:
            start = ctx.start
            stop = ctx.stop
            if start and stop:
                input_stream = start.getInputStream()
                return input_stream.getText(start.start, stop.stop)
        except (AttributeError, TypeError):
            pass

        # Fallback to getText()
        return ctx.getText()

    def _extract_string(self, text: str) -> str:
        """Remove quotes from string literal."""
        if not text:
            return text
        # Remove single, double, or backtick quotes
        if (
            (text.startswith("'") and text.endswith("'"))
            or (text.startswith('"') and text.endswith('"'))
            or (text.startswith("`") and text.endswith("`"))
        ):
            return text[1:-1]
        # Handle triple quotes
        if text.startswith("'''") and text.endswith("'''"):
            return text[3:-3]
        if text.startswith('"""') and text.endswith('"""'):
            return text[3:-3]
        return text

    def _infer_dimension_type(self, sql: str, name: str) -> str:
        """Infer dimension type from SQL expression and name."""
        sql_lower = sql.lower() if sql else ""
        name_lower = name.lower()

        # Time dimension detection
        time_patterns = [
            "date_trunc",
            "::date",
            "::timestamp",
            "::timestamptz",
            "extract",
            "strftime",
            "to_date",
            "to_timestamp",
        ]
        if any(p in sql_lower for p in time_patterns):
            return "time"

        time_name_patterns = ["date", "time", "timestamp", "_at", "created", "updated"]
        if any(p in name_lower for p in time_name_patterns):
            return "time"

        # Boolean detection - comparison that yields true/false
        if re.search(r"[<>=!]+\s*\S", sql):
            # Check if it's a simple comparison (boolean result)
            # But not if it's part of a CASE/pick statement
            if "pick" not in sql_lower and "case" not in sql_lower:
                return "boolean"

        # Numeric detection
        if re.search(r"[+\-*/]", sql) and "||" not in sql:
            return "numeric"

        return "categorical"

    def _extract_granularity(self, sql: str) -> str | None:
        """Extract time granularity from SQL expression."""
        if not sql:
            return None

        sql_lower = sql.lower()
        valid_granularities = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")

        # DATE_TRUNC('month', field) -> 'month'
        match = re.search(r"date_trunc\s*\(\s*['\"](\w+)['\"]", sql_lower)
        if match:
            granularity = match.group(1)
            if granularity in valid_granularities:
                return granularity

        # .second, .minute, .day, .month, .year etc (Malloy time truncation)
        match = re.search(r"\.(\w+)$", sql_lower)
        if match:
            granularity = match.group(1)
            if granularity in valid_granularities:
                return granularity

        # ::date cast
        if "::date" in sql_lower:
            return "day"

        return None

    def _parse_aggregation(self, expr: str) -> tuple[str | None, str | None]:
        """Parse aggregation function from expression.

        Returns (agg_type, sql_expr) tuple.
        """
        if not expr:
            return None, None

        # Match: count(), sum(x), avg(x), etc.
        match = re.match(r"(\w+)\s*\(\s*(.*?)\s*\)$", expr.strip(), re.DOTALL)
        if match:
            agg_func = match.group(1).lower()
            agg_arg = match.group(2).strip()

            if agg_func in ("count", "sum", "avg", "min", "max"):
                return agg_func, agg_arg if agg_arg else None
            elif agg_func == "count_distinct":
                return "count_distinct", agg_arg

        return None, expr

    def _transform_pick_to_case(self, expr: str) -> str:
        """Transform Malloy pick/when/else to SQL CASE expression."""
        # Pattern: pick 'value' when condition
        # Becomes: CASE WHEN condition THEN 'value' ... END

        lines = expr.strip().split("\n")
        cases = []
        else_value = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Match: pick 'value' when condition
            pick_match = re.match(r"pick\s+(.+?)\s+when\s+(.+)$", line, re.IGNORECASE)
            if pick_match:
                value = pick_match.group(1).strip()
                condition = pick_match.group(2).strip()
                cases.append(f"WHEN {condition} THEN {value}")
                continue

            # Match: else 'value'
            else_match = re.match(r"else\s+(.+)$", line, re.IGNORECASE)
            if else_match:
                else_value = else_match.group(1).strip()

        if cases:
            case_str = "CASE " + " ".join(cases)
            if else_value:
                case_str += f" ELSE {else_value}"
            case_str += " END"
            return case_str

        return expr

    def visitDefineSourceStatement(self, ctx: MalloyParser.DefineSourceStatementContext):  # noqa: N802
        """Visit source: name is ... statement."""
        # Get statement-level tags (before 'source:' keyword)
        # These apply to all sources in the statement if there's only one,
        # or can be overridden by source-specific tags
        stmt_tags = ctx.tags()
        stmt_description = self._parse_annotations(stmt_tags) if stmt_tags else None

        # Get source definitions
        source_list = ctx.sourcePropertyList()
        if source_list:
            source_defs = source_list.sourceDefinition()
            for source_def in source_defs:
                self._reset_current()
                self._process_source_definition(source_def)

                # If source has no description but statement does, use statement description
                # (only for single-source statements, or as fallback)
                if self.current_description is None and stmt_description is not None:
                    self.current_description = stmt_description

                if self.current_model_name:
                    model = Model(
                        name=self.current_model_name,
                        table=self.current_table,
                        sql=self.current_sql,
                        primary_key=self.current_primary_key,
                        description=self.current_description,
                        dimensions=self.current_dimensions,
                        metrics=self.current_metrics,
                        relationships=self.current_relationships,
                        segments=self.current_segments,
                    )
                    self.models.append(model)

        return self.visitChildren(ctx)

    def _process_source_definition(self, ctx: MalloyParser.SourceDefinitionContext):
        """Process a single source definition."""
        # Get annotations from tags
        tags = ctx.tags()
        if tags:
            self.current_description = self._parse_annotations(tags)

        # Get source name
        name_def = ctx.sourceNameDef()
        if name_def:
            self.current_model_name = self._get_text(name_def)

        # Process the source expression (sqExplore -> sqExpr)
        sq_explore = ctx.sqExplore()
        if sq_explore:
            sq_expr = sq_explore.sqExpr()
            self._process_sq_expr(sq_expr)

    def _process_sq_expr(self, ctx: MalloyParser.SqExprContext):
        """Process source expression - table, sql, or extended source."""
        if ctx is None:
            return

        # Check for table reference: connection.table('path')
        if isinstance(ctx, MalloyParser.SQTableContext):
            explore_table = ctx.exploreTable()
            if explore_table:
                table_path = explore_table.tablePath()
                if table_path:
                    self.current_table = self._extract_string(self._get_text(table_path))
            return

        # Check for SQL reference: connection.sql('...')
        if isinstance(ctx, MalloyParser.SQSQLContext):
            sql_source = ctx.sqlSource()
            if sql_source:
                # Extract SQL string
                sql_string = sql_source.sqlString()
                if sql_string:
                    # sqlString includes triple quotes, extract the content
                    self.current_sql = self._extract_string(self._get_text(sql_string))
                else:
                    short_string = sql_source.shortString()
                    if short_string:
                        self.current_sql = self._extract_string(self._get_text(short_string))
            return

        # Check for extended source: base extend { ... }
        if isinstance(ctx, MalloyParser.SQExtendedSourceContext):
            # First process the base source
            base_sq_expr = ctx.sqExpr()
            if base_sq_expr:
                self._process_sq_expr(base_sq_expr)

            # Then process the extend block
            explore_props = ctx.exploreProperties()
            if explore_props:
                self._process_explore_properties(explore_props)
            return

        # Check for source with include: base include { ... }
        if isinstance(ctx, MalloyParser.SQIncludeContext):
            base_sq_expr = ctx.sqExpr()
            if base_sq_expr:
                self._process_sq_expr(base_sq_expr)
            return

        # Check for ID reference (another source name)
        if isinstance(ctx, MalloyParser.SQIDContext):
            # This is a reference to another source - we might need to track extends
            return

        # Check for parenthesized expression
        if isinstance(ctx, MalloyParser.SQParensContext):
            inner = ctx.sqExpr()
            if inner:
                self._process_sq_expr(inner)
            return

    def _process_explore_properties(self, ctx: MalloyParser.ExplorePropertiesContext):
        """Process the extend { ... } block of a source."""
        for stmt in ctx.exploreStatement():
            self._process_explore_statement(stmt)

    def _process_explore_statement(self, ctx: MalloyParser.ExploreStatementContext):
        """Process a single statement in explore properties."""
        # Primary key
        if isinstance(ctx, MalloyParser.DefExplorePrimaryKeyContext):
            field_name = ctx.fieldName()
            if field_name:
                self.current_primary_key = self._get_text(field_name)
            return

        # Dimensions
        if isinstance(ctx, MalloyParser.DefExploreDimension_stubContext):
            def_dims = ctx.defDimensions()
            if def_dims:
                self._process_def_dimensions(def_dims)
            return

        # Measures
        if isinstance(ctx, MalloyParser.DefExploreMeasure_stubContext):
            def_measures = ctx.defMeasures()
            if def_measures:
                self._process_def_measures(def_measures)
            return

        # Joins
        if isinstance(ctx, MalloyParser.DefJoin_stubContext):
            join_stmt = ctx.joinStatement()
            if join_stmt:
                self._process_join_statement(join_stmt)
            return

        # Where (source-level filter -> segment)
        if isinstance(ctx, MalloyParser.DefExploreWhere_stubContext):
            where_stmt = ctx.whereStatement()
            if where_stmt:
                self._process_where_as_segment(where_stmt)
            return

    def _process_def_dimensions(self, ctx: MalloyParser.DefDimensionsContext):
        """Process dimension: statements."""
        def_list = ctx.defList()
        if not def_list:
            return

        for field_def in def_list.fieldDef():
            self._process_dimension_def(field_def)

    def _process_dimension_def(self, ctx: MalloyParser.FieldDefContext):
        """Process a single dimension definition."""
        name_def = ctx.fieldNameDef()
        if not name_def:
            return

        name = self._get_text(name_def)

        # Get annotations from tags
        tags = ctx.tags()
        description = self._parse_annotations(tags) if tags else None

        # Get the expression
        field_expr = ctx.fieldExpr()
        sql = self._get_text(field_expr) if field_expr else name

        # Transform pick/when to CASE
        if "pick" in sql.lower():
            sql = self._transform_pick_to_case(sql)

        # Infer type
        dim_type = self._infer_dimension_type(sql, name)

        # Extract granularity for time dimensions
        granularity = None
        if dim_type == "time":
            granularity = self._extract_granularity(sql)

        self.current_dimensions.append(
            Dimension(
                name=name,
                type=dim_type,
                sql=sql,
                granularity=granularity,
                description=description,
            )
        )

    def _process_def_measures(self, ctx: MalloyParser.DefMeasuresContext):
        """Process measure: statements."""
        def_list = ctx.defList()
        if not def_list:
            return

        for field_def in def_list.fieldDef():
            self._process_measure_def(field_def)

    def _process_measure_def(self, ctx: MalloyParser.FieldDefContext):
        """Process a single measure definition."""
        name_def = ctx.fieldNameDef()
        if not name_def:
            return

        name = self._get_text(name_def)

        # Get annotations from tags
        tags = ctx.tags()
        description = self._parse_annotations(tags) if tags else None

        # Get the expression
        field_expr = ctx.fieldExpr()
        expr_text = self._get_text(field_expr) if field_expr else ""

        # Check for filtered measure: count() { where: ... }
        filters = None
        if isinstance(field_expr, MalloyParser.ExprFieldPropsContext):
            # Has field properties (like { where: ... })
            inner_expr = field_expr.fieldExpr()
            props = field_expr.fieldProperties()

            expr_text = self._get_text(inner_expr) if inner_expr else ""

            if props:
                for prop_stmt in props.fieldPropertyStatement():
                    if isinstance(prop_stmt, MalloyParser.WhereStatementContext) or hasattr(
                        prop_stmt, "whereStatement"
                    ):
                        where_stmt = (
                            prop_stmt
                            if isinstance(prop_stmt, MalloyParser.WhereStatementContext)
                            else getattr(prop_stmt, "whereStatement", lambda: None)()
                        )
                        if where_stmt:
                            filter_list = where_stmt.filterClauseList()
                            if filter_list:
                                filters = [self._get_text(f) for f in filter_list.fieldExpr()]

        # Parse aggregation
        agg, sql = self._parse_aggregation(expr_text)

        # Determine metric type
        metric_type = None
        if agg is None and sql:
            # All non-aggregation expressions are derived metrics
            # Ratio type requires numerator/denominator metric references which we can't
            # reliably extract from arbitrary Malloy expressions
            metric_type = "derived"

        self.current_metrics.append(
            Metric(
                name=name,
                type=metric_type,
                agg=agg,
                sql=sql,
                filters=filters,
                description=description,
            )
        )

    def _process_join_statement(self, ctx: MalloyParser.JoinStatementContext):
        """Process join_one/join_many statements."""
        # Determine join type
        if isinstance(ctx, MalloyParser.DefJoinOneContext):
            rel_type = "many_to_one"
            join_list = ctx.joinList()
        elif isinstance(ctx, MalloyParser.DefJoinManyContext):
            rel_type = "one_to_many"
            join_list = ctx.joinList()
        elif isinstance(ctx, MalloyParser.DefJoinCrossContext):
            rel_type = "one_to_one"  # Cross joins mapped to one_to_one
            join_list = ctx.joinList()
        else:
            return

        if not join_list:
            return

        for join_def in join_list.joinDef():
            self._process_join_def(join_def, rel_type)

    def _process_join_def(self, ctx: MalloyParser.JoinDefContext, rel_type: str):
        """Process a single join definition."""
        join_from = ctx.joinFrom()
        if not join_from:
            return

        # Get join name (could be alias is source or just source)
        join_name_def = join_from.joinNameDef()
        name = self._get_text(join_name_def) if join_name_def else None

        if not name:
            return

        # Check if there's an isExplore (alias is source)
        # Note: We could use this to track the target model for extended support
        # is_explore = join_from.isExplore()
        # if is_explore:
        #     sq_expr = is_explore.sqExpr()
        #     target_model = self._get_text(sq_expr) if sq_expr else name

        # Get foreign key from 'with' clause
        foreign_key = None
        if isinstance(ctx, MalloyParser.JoinWithContext):
            field_expr = ctx.fieldExpr()
            if field_expr:
                foreign_key = self._get_text(field_expr)
        elif isinstance(ctx, MalloyParser.JoinOnContext):
            # Parse 'on' clause to extract FK if possible
            join_expr = ctx.joinExpression()
            if join_expr:
                # Try to extract FK from simple equality: fk = other.pk
                expr_text = self._get_text(join_expr)
                # Simple heuristic: first identifier before = is often the FK
                match = re.match(r"(\w+)\s*=", expr_text)
                if match:
                    foreign_key = match.group(1)

        self.current_relationships.append(
            Relationship(
                name=name,
                type=rel_type,
                foreign_key=foreign_key,
            )
        )

    def _process_where_as_segment(self, ctx: MalloyParser.WhereStatementContext):
        """Process source-level where clause as a segment."""
        filter_list = ctx.filterClauseList()
        if not filter_list:
            return

        for i, filter_expr in enumerate(filter_list.fieldExpr()):
            sql = self._get_text(filter_expr)
            self.current_segments.append(
                Segment(
                    name=f"default_filter_{i}" if i > 0 else "default_filter",
                    sql=sql,
                )
            )


class MalloyAdapter(BaseAdapter):
    """Adapter for importing/exporting Malloy semantic models.

    Transforms Malloy definitions into Sidemantic format:
    - Sources -> Models
    - Dimensions -> Dimensions
    - Measures -> Metrics
    - join_one/join_many -> Relationships
    - Source-level where -> Segments

    Note: Views and queries are skipped as they are not part of
    the semantic model definition.
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Malloy files into semantic graph.

        Handles imports by recursively parsing imported files first (depth-first).
        Detects and prevents circular imports.

        Args:
            source: Path to .malloy file or directory

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)

        if source_path.is_dir():
            # Parse all .malloy files in directory
            # When parsing a directory, each file is treated as a "root" file,
            # meaning all of its models are added (no import filter).
            # We use a separate parsed_files set per root file to handle imports,
            # but we track which models have been added to avoid duplicates.
            for malloy_file in source_path.rglob("*.malloy"):
                parsed_files: set[Path] = set()
                self._parse_file(malloy_file, graph, parsed_files, import_filter=None)
        else:
            parsed_files: set[Path] = set()
            self._parse_file(source_path, graph, parsed_files)

        return graph

    def _parse_file(
        self,
        file_path: Path,
        graph: SemanticGraph,
        parsed_files: set[Path],
        import_filter: list[tuple[str, str | None]] | None = None,
    ) -> None:
        """Parse a single Malloy file with import resolution.

        Args:
            file_path: Path to .malloy file
            graph: Semantic graph to add models to
            parsed_files: Set of already-parsed file paths (for cycle detection)
            import_filter: If set, only import these sources (name, alias) pairs.
                          None means import all sources from the file.
        """
        resolved_path = file_path.resolve()

        # Cycle detection: skip if already parsed
        if resolved_path in parsed_files:
            return

        parsed_files.add(resolved_path)

        if not file_path.exists():
            return

        with open(file_path) as f:
            content = f.read()

        # Create ANTLR input stream
        input_stream = InputStream(content)
        lexer = MalloyLexer(input_stream)
        token_stream = CommonTokenStream(lexer)
        parser = MalloyParser(token_stream)

        # Parse the document
        tree = parser.malloyDocument()

        # Visit the tree to extract models and imports
        visitor = MalloyModelVisitor()
        visitor.visit(tree)

        # Process imports first (depth-first) so imported sources are available
        for import_path, import_items in visitor.imports:
            # Resolve import path relative to current file
            import_file = (file_path.parent / import_path).resolve()
            if import_file.exists():
                self._parse_file(import_file, graph, parsed_files, import_items)

        # Add models to graph (with optional filtering for selective imports)
        for model in visitor.models:
            if import_filter is not None:
                # Check if this model should be imported
                matching_import = None
                for name, alias in import_filter:
                    if model.name == name:
                        matching_import = (name, alias)
                        break

                if matching_import is None:
                    continue  # Skip this model, not in import list

                # Apply alias if specified
                name, alias = matching_import
                if alias:
                    model = Model(
                        name=alias,
                        table=model.table,
                        sql=model.sql,
                        description=model.description,
                        extends=model.extends,
                        relationships=model.relationships,
                        primary_key=model.primary_key,
                        dimensions=model.dimensions,
                        metrics=model.metrics,
                        segments=model.segments,
                        pre_aggregations=model.pre_aggregations,
                    )

            # Add to graph if not already present
            if model.name not in graph.models:
                graph.add_model(model)

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Malloy format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output .malloy file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Generate Malloy content
        lines = []
        for model in resolved_models.values():
            source_lines = self._export_source(model)
            lines.extend(source_lines)
            lines.append("")  # Empty line between sources

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            f.write("\n".join(lines))

    def _strip_model_prefix(self, sql: str) -> str:
        """Strip {model}. prefix from SQL expression.

        Malloy doesn't need table qualifiers for column references within a source,
        so we remove the {model}. placeholder that sidemantic uses internally.

        Args:
            sql: SQL expression that may contain {model}. prefixes

        Returns:
            SQL expression with {model}. prefixes removed
        """
        return sql.replace("{model}.", "")

    def _export_source(self, model: Model) -> list[str]:
        """Export a model to Malloy source definition.

        Args:
            model: Model to export

        Returns:
            List of lines for the source definition
        """
        lines = []

        # Model description as annotation
        if model.description:
            lines.append(f"# desc: {model.description}")

        # Source header
        if model.table:
            lines.append(f"source: {model.name} is duckdb.table('{model.table}') extend {{")
        elif model.sql:
            # For SQL-based sources
            lines.append(f'source: {model.name} is duckdb.sql("""{model.sql}""") extend {{')
        else:
            lines.append(f"source: {model.name} extend {{")

        # Primary key
        if model.primary_key and model.primary_key != "id":
            lines.append(f"  primary_key: {model.primary_key}")

        # Dimensions
        # Skip dimensions that match the primary key (Malloy auto-exposes the PK column).
        # Skip passthrough dimensions (sql == name) since Malloy auto-exposes underlying
        # table columns. Only export dimensions with actual transformations.
        dims_to_export: list[tuple[Dimension, str]] = []
        for dim in model.dimensions:
            if dim.name == model.primary_key:
                continue
            sql = self._strip_model_prefix(dim.sql or dim.name).strip()
            # Skip passthrough dimensions - Malloy auto-exposes table columns
            if sql == dim.name:
                continue
            dims_to_export.append((dim, sql))

        if dims_to_export:
            lines.append("")
            lines.append("  dimension:")
            for dim, sql in dims_to_export:
                if dim.description:
                    lines.append(f"    # desc: {dim.description}")
                # For time dimensions with granularity, use Malloy's time truncation syntax
                # But only if the SQL doesn't already contain a truncation function
                if dim.type == "time" and dim.granularity:
                    sql_lower = sql.lower()
                    already_has_truncation = (
                        "date_trunc" in sql_lower or "::date" in sql_lower or sql_lower.endswith(f".{dim.granularity}")
                    )
                    if not already_has_truncation:
                        # Append Malloy time accessor: .second, .minute, .hour, .day, .week, .month, .quarter, .year
                        lines.append(f"    {dim.name} is {sql}.{dim.granularity}")
                    else:
                        lines.append(f"    {dim.name} is {sql}")
                else:
                    lines.append(f"    {dim.name} is {sql}")

        # Measures
        if model.metrics:
            lines.append("")
            lines.append("  measure:")
            for metric in model.metrics:
                if metric.description:
                    lines.append(f"    # desc: {metric.description}")
                measure_expr = self._format_measure(metric)
                lines.append(f"    {metric.name} is {measure_expr}")

        # Joins
        for rel in model.relationships:
            lines.append("")
            join_type = "join_one" if rel.type == "many_to_one" else "join_many"
            if rel.foreign_key:
                lines.append(f"  {join_type}: {rel.name} with {rel.foreign_key}")
            else:
                lines.append(f"  {join_type}: {rel.name}")

        lines.append("}")

        return lines

    def _format_measure(self, metric: Metric) -> str:
        """Format a metric as Malloy measure expression.

        Args:
            metric: Metric to format

        Returns:
            Malloy measure expression string
        """
        # Simple aggregation
        if metric.agg:
            if metric.sql:
                sql = self._strip_model_prefix(metric.sql)
                expr = f"{metric.agg}({sql})"
            else:
                expr = f"{metric.agg}()"

            # Add filter if present
            if metric.filters:
                filters = [self._strip_model_prefix(f) for f in metric.filters]
                filter_str = ", ".join(filters)
                expr = f"{expr} {{ where: {filter_str} }}"

            return expr

        # Derived/ratio metric
        if metric.type == "ratio" and metric.numerator and metric.denominator:
            return f"{metric.numerator} / {metric.denominator}"

        # Fallback to sql
        if metric.sql:
            return self._strip_model_prefix(metric.sql)

        return "count()"
