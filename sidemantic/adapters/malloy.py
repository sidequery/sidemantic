"""Malloy adapter for importing/exporting Malloy semantic models.

Uses ANTLR4-generated parser from official Malloy grammar files.
"""

from __future__ import annotations

import re
from pathlib import Path

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph

try:
    from antlr4 import CommonTokenStream, InputStream

    from sidemantic.adapters.malloy_grammar import MalloyLexer, MalloyParser, MalloyParserVisitor

    _ANTLR4_AVAILABLE = True
except ImportError:
    _ANTLR4_AVAILABLE = False
    MalloyParserVisitor = object  # type: ignore[assignment,misc]
    MalloyParser = None  # type: ignore[assignment]


class MalloyModelVisitor(MalloyParserVisitor):  # type: ignore[misc]
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
        self.current_extends: str | None = None
        self.current_connection: str | None = None
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
        self.current_extends = None
        self.current_connection = None
        self.current_dimensions = []
        self.current_metrics = []
        self.current_relationships = []
        self.current_segments = []
        self._timezone = None
        self._model_tags = []
        self._accept_fields = []
        self._except_fields = []

    def _parse_annotations(self, tags_ctx) -> str | None:
        """Parse annotations from tags context, returning description text.

        Also stores non-description tags via _parse_annotations_full.
        """
        if tags_ctx is None:
            return None
        desc, _ = self._parse_annotations_full(tags_ctx)
        return desc

    def _parse_annotations_full(self, tags_ctx) -> tuple[str | None, list[str]]:
        """Parse annotations from tags context.

        Returns (description, tags) where tags is a list of non-description
        tag strings like "line_chart", "percent", "currency", etc.
        """
        if tags_ctx is None:
            return None, []

        descriptions = []
        tags = []

        for i in range(tags_ctx.getChildCount()):
            child = tags_ctx.getChild(i)
            if child is not None:
                text = child.getText()

                # ## is a doc annotation (description)
                if text.startswith("##"):
                    desc = text[2:].strip()
                    if desc:
                        descriptions.append(desc)
                # # is a tag annotation
                elif text.startswith("#"):
                    tag_text = text[1:].strip()
                    if tag_text.lower().startswith("desc:"):
                        desc = tag_text[5:].strip()
                        if desc:
                            descriptions.append(desc)
                    elif tag_text.lower().startswith("description:"):
                        desc = tag_text[12:].strip()
                        if desc:
                            descriptions.append(desc)
                    elif tag_text:
                        tags.append(tag_text)

        desc = " ".join(descriptions) if descriptions else None
        return desc, tags

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
        # Handle triple quotes BEFORE single/double quotes
        if text.startswith("'''") and text.endswith("'''"):
            return text[3:-3]
        if text.startswith('"""') and text.endswith('"""'):
            return text[3:-3]
        # Remove single, double, or backtick quotes
        if (
            (text.startswith("'") and text.endswith("'"))
            or (text.startswith('"') and text.endswith('"'))
            or (text.startswith("`") and text.endswith("`"))
        ):
            return text[1:-1]
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

        Handles both standard SQL syntax (func(arg)) and Malloy dot-method
        syntax (field.func()). In Malloy, count(field) means count_distinct(field).
        """
        if not expr:
            return None, None

        expr_stripped = expr.strip()

        # Pattern 1: dot-method aggregation - field.func() or field.func(args)
        # Handles: cost.sum(), averageRating.avg(), `number`.sum(), images.count()
        # Also handles dotted paths: event_params.value.double_value.sum()
        dot_match = re.match(
            r"^(.+)\.(sum|avg|count|min|max|count_distinct)\s*\(\s*(.*?)\s*\)$",
            expr_stripped,
            re.DOTALL,
        )
        if dot_match:
            field = dot_match.group(1).strip()
            agg_func = dot_match.group(2).lower()
            extra_arg = dot_match.group(3).strip()
            # For dot-method, the field IS the argument
            if agg_func == "count" and not extra_arg:
                return "count", field
            return agg_func, field

        # Pattern 2: standard func(arg) syntax
        match = re.match(r"(\w+)\s*\(\s*(.*?)\s*\)$", expr_stripped, re.DOTALL)
        if match:
            agg_func = match.group(1).lower()
            agg_arg = match.group(2).strip()

            if agg_func == "count":
                # In Malloy, count(field) means count_distinct(field)
                # count() with no args is just count
                if agg_arg:
                    return "count_distinct", agg_arg
                return "count", None
            elif agg_func in ("sum", "avg", "min", "max"):
                return agg_func, agg_arg if agg_arg else None
            elif agg_func == "count_distinct":
                return "count_distinct", agg_arg

        return None, expr

    def _transform_malloy_expr(self, expr: str) -> str:
        """Transform Malloy-specific expression syntax to standard SQL.

        Handles:
        - ?? null coalescing -> COALESCE
        - ! type assertions -> stripped (e.g., timestamp_seconds!timestamp(x) -> timestamp_seconds(x))
        - ~ regex match -> REGEXP_MATCHES
        - @date literals -> DATE 'YYYY-MM-DD'
        - now -> CURRENT_TIMESTAMP
        """
        if not expr:
            return expr

        # ?? null coalescing -> COALESCE
        if "??" in expr:
            expr = self._transform_null_coalesce(expr)

        # ! type assertion: func!type(args) -> func(args)
        # Matches: timestamp_seconds!timestamp(x), left!(s,1), md5!(x), to_base64!(x)
        # Pattern: identifier!identifier( -> identifier(
        expr = re.sub(r"(\w+)!\w+\(", r"\1(", expr)

        # ~ regex match with r'' literal: expr ~ r'pattern' -> REGEXP_MATCHES(expr, 'pattern')
        # Use (.+?) with lookahead to capture full LHS including spaces/parens
        expr = re.sub(
            r"(.+?)\s+~\s+r'([^']*)'",
            r"REGEXP_MATCHES(\1, '\2')",
            expr,
        )
        expr = re.sub(
            r'(.+?)\s+~\s+r"([^"]*)"',
            r"REGEXP_MATCHES(\1, '\2')",
            expr,
        )
        # !~ negated regex
        expr = re.sub(
            r"(.+?)\s+!~\s+r'([^']*)'",
            r"NOT REGEXP_MATCHES(\1, '\2')",
            expr,
        )

        # @date literals: @YYYY-MM-DD -> DATE 'YYYY-MM-DD'
        # @YYYY-MM -> DATE 'YYYY-MM-01'
        # @YYYY -> DATE 'YYYY-01-01'
        # @YYYY-Qn -> handled as text
        expr = re.sub(r"@(\d{4}-\d{2}-\d{2})", r"DATE '\1'", expr)
        expr = re.sub(r"@(\d{4}-\d{2})(?!\d)", r"DATE '\1-01'", expr)
        expr = re.sub(r"@(\d{4})(?![-\d])", r"DATE '\1-01-01'", expr)

        # now -> CURRENT_TIMESTAMP (only when it's the entire expression or clearly standalone)
        if expr.strip() == "now":
            expr = "CURRENT_TIMESTAMP"

        # & (and-tree): expands partial conditions with the base field
        # e.g., "field < 2031 & > -8000" -> "field < 2031 AND field > -8000"
        # e.g., "status != 'Cancelled' & 'Returned'" -> "status != 'Cancelled' AND status != 'Returned'"
        if " & " in expr and "?" not in expr:
            expr = self._transform_and_tree(expr)

        # | (or-tree / alternatives): used with ? apply operator
        # e.g., "field ? 'a' | 'b'" -> "field IN ('a', 'b')"
        # Only transform the simple value-matching pattern, not general uses of |
        if " ? " in expr and " | " in expr and "pick" not in expr.lower():
            expr = self._transform_or_tree(expr)

        return expr

    def _transform_null_coalesce(self, expr: str) -> str:
        """Transform Malloy ?? null coalescing to SQL COALESCE."""
        if "??" not in expr:
            return expr
        parts = re.split(r"\s*\?\?\s*", expr)
        if len(parts) > 1:
            return f"COALESCE({', '.join(p.strip() for p in parts)})"
        return expr

    def _transform_and_tree(self, expr: str) -> str:
        """Transform Malloy & (and-tree) to SQL AND with expanded base field.

        Examples:
        - "field < 2031 & > -8000" -> "field < 2031 AND field > -8000"
        - "status != 'Cancelled' & 'Returned'" -> "status != 'Cancelled' AND status != 'Returned'"
        """
        parts = re.split(r"\s+&\s+", expr)
        if len(parts) < 2:
            return expr

        # First part should be a complete condition with the base field and operator
        first = parts[0].strip()
        # Extract base field and operator from the first condition
        match = re.match(r"^(.+?)\s*([<>=!]+)\s*(.+)$", first)
        if not match:
            return expr

        base_field = match.group(1).strip()
        operator = match.group(2).strip()
        expanded = [first]

        for part in parts[1:]:
            part = part.strip()
            # If part starts with an operator, prepend the base field
            if re.match(r"^[<>=!]", part):
                expanded.append(f"{base_field} {part}")
            # If part is a bare value (string/number), reuse the base operator
            elif re.match(r"^['\"`\d]", part):
                expanded.append(f"{base_field} {operator} {part}")
            else:
                expanded.append(part)

        return " AND ".join(expanded)

    def _transform_or_tree(self, expr: str) -> str:
        """Transform Malloy field ? 'a' | 'b' to SQL field IN ('a', 'b').

        Only handles the value-matching pattern: field ? value1 | value2 | ...
        """
        # Match: field ? value1 | value2 | ...
        match = re.match(r"^(.+?)\s*\?\s*(.+)$", expr)
        if not match:
            return expr

        base_field = match.group(1).strip()
        values_str = match.group(2).strip()

        # Split on | and collect values
        values = [v.strip() for v in values_str.split("|")]
        if len(values) < 2:
            return expr

        return f"{base_field} IN ({', '.join(values)})"

    def _transform_pick_to_case(self, expr: str, base_field: str | None = None) -> str:
        """Transform Malloy pick/when/else to SQL CASE expression.

        Args:
            expr: The pick/when/else expression text.
            base_field: If provided, prepend to partial comparisons in when clauses.
                For apply-pick syntax: `field ? pick 'X' when < 5` the base_field
                is extracted by the caller and partial conditions get it prepended.
        """
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
                if base_field:
                    condition = self._expand_partial_condition(condition, base_field)
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

    def _expand_partial_condition(self, condition: str, base_field: str) -> str:
        """Expand a partial comparison by prepending the base field.

        Malloy apply-pick uses partial conditions:
        - `when < 5` -> `base_field < 5`
        - `when 'ASW'` -> `base_field = 'ASW'`
        - `when >= 1000` -> `base_field >= 1000`
        - `when ~ r'pattern'` -> `base_field ~ r'pattern'`
        - `when gender = 'F'` -> `gender = 'F'` (already complete, no change)
        """
        # Already a complete condition (contains an operator after a word)
        if re.match(r"\w+\s*[=<>!~]", condition):
            return condition
        # Partial: starts with comparison operator
        if re.match(r"[<>=!~]", condition):
            return f"{base_field} {condition}"
        # Partial: starts with a string/number literal (value matching)
        if re.match(r"['\"`\d]", condition):
            return f"{base_field} = {condition}"
        return condition

    def visitDefineSourceStatement(self, ctx: MalloyParser.DefineSourceStatementContext):  # noqa: N802
        """Visit source: name is ... statement."""
        # Get statement-level tags (before 'source:' keyword)
        # These apply to all sources in the statement if there's only one,
        # or can be overridden by source-specific tags
        stmt_tags = ctx.tags()
        stmt_description = None
        stmt_persist = None
        if stmt_tags:
            stmt_description, stmt_tag_list = self._parse_annotations_full(stmt_tags)
            # Check for #@ persist annotations
            for tag in stmt_tag_list:
                if tag.startswith("@ persist") or tag.startswith("@persist"):
                    persist_text = tag[len("@ persist") :] if tag.startswith("@ persist") else tag[len("@persist") :]
                    persist_text = persist_text.strip()
                    stmt_persist = {"persist": True}
                    # Parse name=value
                    name_match = re.match(r"name\s*=\s*(\S+)", persist_text)
                    if name_match:
                        stmt_persist["persist_name"] = name_match.group(1)

        # Get source definitions
        source_list = ctx.sourcePropertyList()
        if source_list:
            source_defs = source_list.sourceDefinition()
            for source_def in source_defs:
                self._reset_current()
                self._process_source_definition(source_def)

                # If source has no description but statement does, use statement description
                if self.current_description is None and stmt_description is not None:
                    self.current_description = stmt_description

                if self.current_model_name:
                    metadata = {}
                    if self.current_connection:
                        metadata["connection"] = self.current_connection
                    if stmt_persist:
                        metadata.update(stmt_persist)
                    if self._timezone:
                        metadata["timezone"] = self._timezone
                    if self._model_tags:
                        metadata["tags"] = self._model_tags
                    model = Model(
                        name=self.current_model_name,
                        table=self.current_table,
                        sql=self.current_sql,
                        extends=self.current_extends,
                        primary_key=self.current_primary_key,
                        description=self.current_description,
                        dimensions=self.current_dimensions,
                        metrics=self.current_metrics,
                        relationships=self.current_relationships,
                        segments=self.current_segments,
                        metadata=metadata if metadata else None,
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
                # Extract connection name
                conn_id = explore_table.connectionId()
                if conn_id:
                    id_ctx = conn_id.id_()
                    if id_ctx:
                        self.current_connection = self._get_text(id_ctx)
                table_path = explore_table.tablePath()
                if table_path:
                    self.current_table = self._extract_string(self._get_text(table_path))
            return

        # Check for SQL reference: connection.sql('...')
        if isinstance(ctx, MalloyParser.SQSQLContext):
            sql_source = ctx.sqlSource()
            if sql_source:
                # Extract connection name
                conn_id = sql_source.connectionId()
                if conn_id:
                    id_ctx = conn_id.id_()
                    if id_ctx:
                        self.current_connection = self._get_text(id_ctx)
                # Extract SQL string
                sql_string = sql_source.sqlString()
                if sql_string:
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

        # Check for ID reference (another source name) -> set extends
        if isinstance(ctx, MalloyParser.SQIDContext):
            id_ctx = ctx.id_()
            if id_ctx:
                self.current_extends = self._get_text(id_ctx)
            return

        # Check for arrow/pipeline source: base -> { ... }
        if isinstance(ctx, MalloyParser.SQArrowContext):
            # Process the base source expression (sets table/extends)
            base_sq_expr = ctx.sqExpr()
            if base_sq_expr:
                self._process_sq_expr(base_sq_expr)
            return

        # Check for refined query (old + syntax): base + { ... }
        if isinstance(ctx, MalloyParser.SQRefinedQueryContext):
            # Process the base source expression
            base_sq = ctx.sqExpr()
            if base_sq:
                self._process_sq_expr(base_sq)
            # Try to process the refinement block for explore-like statements
            seg_expr = ctx.segExpr()
            if seg_expr:
                self._process_seg_expr(seg_expr)
            return

        # Check for compose() sources
        if isinstance(ctx, MalloyParser.SQComposeContext):
            # Extract composed source names for metadata
            # compose(src1, src2, ...) - just note the first source as extends
            sq_exprs = ctx.sqExpr()
            if sq_exprs and len(sq_exprs) > 0:
                first = sq_exprs[0] if isinstance(sq_exprs, list) else sq_exprs
                self._process_sq_expr(first)
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

    def _process_seg_expr(self, ctx):
        """Process a segExpr from old + syntax refinements.

        The segExpr can be SegOpsContext (query properties block),
        SegRefineContext (lhs + rhs), or SegFieldContext (field path).
        We try to extract explore-like statements from query properties.
        """
        if ctx is None:
            return

        if isinstance(ctx, MalloyParser.SegOpsContext):
            # { queryStatement* } block - try to process as explore statements
            query_props = ctx.queryProperties()
            if query_props:
                self._process_query_properties_as_explore(query_props)
            return

        if isinstance(ctx, MalloyParser.SegRefineContext):
            # lhs + rhs - process both sides
            for seg in ctx.segExpr():
                self._process_seg_expr(seg)
            return

    def _process_query_properties_as_explore(self, ctx):
        """Best-effort extraction of explore-like statements from query properties.

        The old + syntax uses queryStatement, not exploreStatement.
        Some query statements overlap with explore statements (dimension:, measure:,
        join:, where:, primary_key:). We handle what we can.
        """
        for stmt in ctx.queryStatement():
            # Try to match known statement types that exist in both query and explore contexts
            # The grammar reuses the same context classes for some of these
            if isinstance(stmt, MalloyParser.DefExplorePrimaryKeyContext):
                field_name = stmt.fieldName()
                if field_name:
                    self.current_primary_key = self._get_text(field_name)
            elif isinstance(stmt, MalloyParser.DefExploreDimension_stubContext):
                def_dims = stmt.defDimensions()
                if def_dims:
                    self._process_def_dimensions(def_dims)
            elif isinstance(stmt, MalloyParser.DefExploreMeasure_stubContext):
                def_measures = stmt.defMeasures()
                if def_measures:
                    self._process_def_measures(def_measures)
            elif isinstance(stmt, MalloyParser.DefJoin_stubContext):
                join_stmt = stmt.joinStatement()
                if join_stmt:
                    self._process_join_statement(join_stmt)
            elif isinstance(stmt, MalloyParser.DefExploreWhere_stubContext):
                where_stmt = stmt.whereStatement()
                if where_stmt:
                    self._process_where_as_segment(where_stmt)
            elif isinstance(stmt, MalloyParser.DeclareStatementContext):
                # declare: creates fields accessible within the source
                def_list = stmt.defList()
                if def_list:
                    for field_def in def_list.fieldDef():
                        self._process_dimension_def(field_def)

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

        # Accept/except field visibility
        if isinstance(ctx, MalloyParser.DefExploreEditFieldContext):
            # Store accept/except in metadata; we'll filter after model creation
            # The grammar has includeExceptList with field names
            edit_field = ctx.editField() if hasattr(ctx, "editField") else None
            if edit_field is None:
                # Try to get the text and parse accept/except manually
                text = self._get_text(ctx).strip()
                if text.startswith("except:"):
                    fields_text = text[7:].strip()
                    field_names = [f.strip().strip("`") for f in fields_text.split(",")]
                    if not hasattr(self, "_except_fields"):
                        self._except_fields = []
                    self._except_fields.extend(field_names)
                elif text.startswith("accept:"):
                    fields_text = text[7:].strip()
                    field_names = [f.strip().strip("`") for f in fields_text.split(",")]
                    if not hasattr(self, "_accept_fields"):
                        self._accept_fields = []
                    self._accept_fields.extend(field_names)
            return

        # Timezone statement: timezone: 'US/Pacific'
        if isinstance(ctx, MalloyParser.DefExploreTimezoneContext):
            tz_stmt = ctx.timezoneStatement()
            if tz_stmt:
                tz_string = tz_stmt.string()
                if tz_string:
                    tz_value = self._extract_string(self._get_text(tz_string))
                    if not hasattr(self, "_timezone"):
                        self._timezone = None
                    self._timezone = tz_value
            return

        # Standalone annotations in extend blocks
        if isinstance(ctx, MalloyParser.DefExploreAnnotationContext):
            # These are # tag annotations not attached to a field
            # Store as model-level tags
            for i in range(ctx.getChildCount()):
                child = ctx.getChild(i)
                if child is not None:
                    text = child.getText()
                    if text.startswith("#"):
                        tag_text = text[1:].strip()
                        if tag_text:
                            if not hasattr(self, "_model_tags"):
                                self._model_tags = []
                            self._model_tags.append(tag_text)
            return

        # Rename statements: rename: new_name is old_name
        if isinstance(ctx, MalloyParser.DefExploreRenameContext):
            rename_list = ctx.renameList()
            if rename_list:
                for rename_entry in rename_list.renameEntry():
                    field_names = rename_entry.fieldName()
                    if field_names and len(field_names) >= 2:
                        new_name = self._get_text(field_names[0])
                        old_name = self._get_text(field_names[1])
                        dim_type = self._infer_dimension_type(old_name, new_name)
                        self.current_dimensions.append(
                            Dimension(
                                name=new_name,
                                sql=old_name,
                                type=dim_type,
                            )
                        )
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
        tags_ctx = ctx.tags()
        description = None
        dim_metadata = None
        if tags_ctx:
            description, tag_list = self._parse_annotations_full(tags_ctx)
            if tag_list:
                dim_metadata = {"tags": tag_list}

        # Get the expression
        field_expr = ctx.fieldExpr()
        sql = self._get_text(field_expr) if field_expr else name

        # Transform Malloy-specific expression syntax to SQL
        sql = self._transform_malloy_expr(sql)

        # Transform pick/when to CASE (with apply-pick support)
        if "pick" in sql.lower():
            # Check for apply-pick pattern: field ? pick ... when ...
            apply_match = re.match(r"^(.+?)\s*\?\s*\n?\s*(pick\s+.+)$", sql, re.DOTALL | re.IGNORECASE)
            if apply_match:
                base_field = apply_match.group(1).strip()
                pick_expr = apply_match.group(2).strip()
                sql = self._transform_pick_to_case(pick_expr, base_field=base_field)
            else:
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
                metadata=dim_metadata,
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
        tags_ctx = ctx.tags()
        description = None
        measure_tags = None
        if tags_ctx:
            description, tag_list = self._parse_annotations_full(tags_ctx)
            if tag_list:
                measure_tags = tag_list

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

        # Transform Malloy-specific expression syntax to SQL
        expr_text = self._transform_malloy_expr(expr_text)

        # Handle .granularity suffix on aggregated expressions (3.7)
        # e.g., min(post_time).day -> strip .day, parse agg, store granularity
        measure_granularity = None
        granularity_match = re.match(
            r"^(.+)\.(second|minute|hour|day|week|month|quarter|year)$",
            expr_text.strip(),
        )
        if granularity_match:
            # Check if the inner part looks like an aggregation
            inner = granularity_match.group(1).strip()
            if re.match(r"\w+\s*\(", inner):
                expr_text = inner
                measure_granularity = granularity_match.group(2)

        # Parse aggregation
        agg, sql = self._parse_aggregation(expr_text)

        # Determine metric type
        metric_type = None
        if agg is None and sql:
            # Check if this is a measure reference with filters (3.8)
            # e.g., interesting_post_count is post_count { where: is_interesting }
            # The sql will be the measure name, and filters will be set
            if filters and re.match(r"^\w+$", sql.strip()):
                # Simple identifier with filters = measure reference with filter
                # Look up the referenced measure to inherit its aggregation
                ref_name = sql.strip()
                ref_metric = next((m for m in self.current_metrics if m.name == ref_name), None)
                if ref_metric and ref_metric.agg:
                    agg = ref_metric.agg
                    sql = ref_metric.sql
                    # Merge filters
                    if ref_metric.filters:
                        filters = list(ref_metric.filters) + list(filters)
                else:
                    metric_type = "derived"
            else:
                metric_type = "derived"

        metric_metadata = {}
        if measure_granularity:
            metric_metadata["granularity"] = measure_granularity
        if measure_tags:
            metric_metadata["tags"] = measure_tags

        self.current_metrics.append(
            Metric(
                name=name,
                type=metric_type,
                agg=agg,
                sql=sql,
                filters=filters,
                description=description,
                metadata=metric_metadata if metric_metadata else None,
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

        # Check if there's an isExplore (alias is source with inline definition)
        # Only extract inline sources that define a table/sql, not simple ID references
        is_explore = join_from.isExplore()
        if is_explore:
            sq_expr = is_explore.sqExpr()
            if sq_expr and not isinstance(sq_expr, MalloyParser.SQIDContext):
                self._extract_inline_join_source(name, sq_expr)

        # Get foreign key from 'with' clause
        foreign_key = None
        join_metadata = None
        if isinstance(ctx, MalloyParser.JoinWithContext):
            field_expr = ctx.fieldExpr()
            if field_expr:
                foreign_key = self._get_text(field_expr)
        elif isinstance(ctx, MalloyParser.JoinOnContext):
            join_expr = ctx.joinExpression()
            if join_expr:
                expr_text = self._get_text(join_expr)
                # Store full condition in metadata
                join_metadata = {"on_condition": expr_text}
                # Extract all FKs from equalities: field = other.field
                fk_matches = re.findall(r"(\w+)\s*=\s*\w+\.\w+", expr_text)
                if fk_matches:
                    foreign_key = fk_matches[0]
                    if len(fk_matches) > 1:
                        join_metadata["composite_keys"] = fk_matches
                else:
                    # Fallback: first identifier before =
                    match = re.match(r"(\w+)\s*=", expr_text)
                    if match:
                        foreign_key = match.group(1)

        self.current_relationships.append(
            Relationship(
                name=name,
                type=rel_type,
                foreign_key=foreign_key,
                metadata=join_metadata,
            )
        )

    def _extract_inline_join_source(self, join_name: str, sq_expr):
        """Extract inline source definition from a join and add as a model.

        Handles: join_one: name is connection.table(...) extend { ... } with fk
        """
        # Save current state (including metadata accumulators)
        saved = (
            self.current_model_name,
            self.current_table,
            self.current_sql,
            self.current_primary_key,
            self.current_description,
            self.current_extends,
            self.current_connection,
            list(self.current_dimensions),
            list(self.current_metrics),
            list(self.current_relationships),
            list(self.current_segments),
            self._timezone,
            list(self._model_tags),
            list(self._accept_fields),
            list(self._except_fields),
        )

        # Reset and process the inline source
        self.current_model_name = join_name
        self.current_table = None
        self.current_sql = None
        self.current_primary_key = "id"
        self.current_description = None
        self.current_extends = None
        self.current_connection = None
        self.current_dimensions = []
        self.current_metrics = []
        self.current_relationships = []
        self.current_segments = []
        self._timezone = None
        self._model_tags = []
        self._accept_fields = []
        self._except_fields = []

        self._process_sq_expr(sq_expr)

        # Only create model if we found something useful
        if self.current_table or self.current_sql or self.current_extends:
            metadata = {}
            if self.current_connection:
                metadata["connection"] = self.current_connection
            inline_model = Model(
                name=join_name,
                table=self.current_table,
                sql=self.current_sql,
                extends=self.current_extends,
                primary_key=self.current_primary_key,
                description=self.current_description,
                dimensions=self.current_dimensions,
                metrics=self.current_metrics,
                relationships=self.current_relationships,
                segments=self.current_segments,
                metadata=metadata if metadata else None,
            )
            self.models.append(inline_model)

        # Restore state (including metadata accumulators)
        (
            self.current_model_name,
            self.current_table,
            self.current_sql,
            self.current_primary_key,
            self.current_description,
            self.current_extends,
            self.current_connection,
            self.current_dimensions,
            self.current_metrics,
            self.current_relationships,
            self.current_segments,
            self._timezone,
            self._model_tags,
            self._accept_fields,
            self._except_fields,
        ) = saved

    def _process_where_as_segment(self, ctx: MalloyParser.WhereStatementContext):
        """Process source-level where clause as a segment."""
        filter_list = ctx.filterClauseList()
        if not filter_list:
            return

        for i, filter_expr in enumerate(filter_list.fieldExpr()):
            sql = self._get_text(filter_expr)
            sql = self._transform_malloy_expr(sql)
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

        Raises:
            ImportError: If antlr4-python3-runtime is not installed
        """
        if not _ANTLR4_AVAILABLE:
            raise ImportError(
                'Malloy support requires antlr4-python3-runtime. Install with: pip install "sidemantic[malloy]"'
            )
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

        # Model description as tag annotation
        if model.description:
            lines.append(f"# desc: {model.description}")

        # Source header - use connection from metadata, default to duckdb
        connection = (model.metadata or {}).get("connection", "duckdb")
        if model.table:
            lines.append(f"source: {model.name} is {connection}.table('{model.table}') extend {{")
        elif model.sql:
            lines.append(f'source: {model.name} is {connection}.sql("""{model.sql}""") extend {{')
        else:
            lines.append(f"source: {model.name} extend {{")

        # Primary key
        if model.primary_key and model.primary_key != "id":
            lines.append(f"  primary_key: {model.primary_key}")

        # Segments (source-level where clauses) - Tier 4.1
        if model.segments:
            for segment in model.segments:
                lines.append(f"  where: {self._strip_model_prefix(segment.sql)}")

        # Separate renames from computed dimensions for proper export
        renames_to_export: list[tuple[str, str]] = []
        dims_to_export: list[tuple[Dimension, str]] = []
        for dim in model.dimensions:
            if dim.name == model.primary_key:
                continue
            sql = self._strip_model_prefix(dim.sql or dim.name).strip()
            # Skip passthrough dimensions - Malloy auto-exposes table columns
            if sql == dim.name:
                continue
            # Tier 4.5: detect renames (simple identifier, no operators/functions)
            if re.match(r"^[`\w]+$", sql) and sql != dim.name:
                renames_to_export.append((dim.name, sql))
            else:
                dims_to_export.append((dim, sql))

        # Export renames
        if renames_to_export:
            lines.append("")
            lines.append("  rename:")
            for new_name, old_name in renames_to_export:
                lines.append(f"    {new_name} is {old_name}")

        # Export computed dimensions
        if dims_to_export:
            lines.append("")
            lines.append("  dimension:")
            for dim, sql in dims_to_export:
                if dim.description:
                    lines.append(f"    # desc: {dim.description}")
                if dim.type == "time" and dim.granularity:
                    sql_lower = sql.lower()
                    already_has_truncation = (
                        "date_trunc" in sql_lower or "::date" in sql_lower or sql_lower.endswith(f".{dim.granularity}")
                    )
                    if not already_has_truncation:
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

        # Joins - Tier 4.4: use on condition from metadata when available
        for rel in model.relationships:
            lines.append("")
            if rel.type == "many_to_one":
                join_type = "join_one"
            elif rel.type == "one_to_one":
                join_type = "join_cross"
            else:
                join_type = "join_many"
            on_condition = (rel.metadata or {}).get("on_condition")
            if on_condition:
                lines.append(f"  {join_type}: {rel.name} on {on_condition}")
            elif rel.foreign_key:
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
