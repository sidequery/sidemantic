"""Malloy adapter for importing/exporting Malloy semantic models.

Uses ANTLR4-generated parser from official Malloy grammar files.
"""

from __future__ import annotations

import re
import warnings
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
    from antlr4.error.ErrorListener import ErrorListener

    from sidemantic.adapters.malloy_grammar import MalloyLexer, MalloyParser, MalloyParserVisitor

    _ANTLR4_AVAILABLE = True
except ImportError:
    _ANTLR4_AVAILABLE = False
    MalloyParserVisitor = object  # type: ignore[assignment,misc]
    MalloyParser = None  # type: ignore[assignment]
    ErrorListener = object  # type: ignore[assignment,misc]


class MalloySyntaxError(ValueError):
    """Raised when a Malloy document contains syntax errors and strict parsing is requested.

    The collected per-error details are available on the ``errors`` attribute as a list
    of ``(line, column, message)`` tuples.
    """

    def __init__(self, message: str, errors: list[tuple[int, int, str]]):
        super().__init__(message)
        self.errors = errors


class _CollectingErrorListener(ErrorListener):  # type: ignore[misc]
    """ANTLR error listener that collects lexer/parser syntax errors.

    The vendored ANTLR parser previously ran with the default ConsoleErrorListener,
    which printed errors to stderr and was otherwise invisible: ``parse()`` would
    silently return a degraded/partial graph built from whatever ANTLR's error
    recovery managed to salvage. This listener captures those errors so the adapter
    can surface them (warn or raise) instead of swallowing them.
    """

    def __init__(self):
        super().__init__()
        self.errors: list[tuple[int, int, str]] = []

    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):  # noqa: N802, N803
        self.errors.append((line, column, msg))


class MalloyModelVisitor(MalloyParserVisitor):  # type: ignore[misc]
    """Visitor that extracts semantic model information from Malloy AST."""

    def __init__(self):
        self.models: list[Model] = []
        # Imports: list of (file_path, items) where items is list of (name, alias) or None for import-all
        self.imports: list[tuple[str, list[tuple[str, str | None]] | None]] = []
        # Exports: top-level `export { a, b }` source names re-exported from this file.
        self.exports: list[str] = []
        # User-defined types: top-level `type: name is ...` definitions (name -> definition text).
        self.user_types: dict[str, str] = {}
        # Given parameters: top-level `given: name::type` model-input parameters (name -> type text).
        self.given: dict[str, str] = {}
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
        self._virtual = None
        self._source_type_constraints = []

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

    def visitExportStatement(self, ctx: MalloyParser.ExportStatementContext):  # noqa: N802
        """Visit top-level `export { a, b }` statement.

        Malloy 0.0.x added `export { ... }` to re-export named sources from a file
        so importers can pull them. Each item is a source name (an id). We record the
        names so callers can introspect what a file publicly exports.
        """
        for export_item in ctx.exportItem():
            id_ctx = export_item.id_()
            if id_ctx:
                name = self._get_text(id_ctx)
                if name:
                    self.exports.append(name)
        return self.visitChildren(ctx)

    def visitDefineUserTypeStatement(self, ctx: MalloyParser.DefineUserTypeStatementContext):  # noqa: N802
        """Visit top-level `type: name is <type>` user-defined type statement.

        Malloy added user-defined types so a type can be named once and reused (in
        source type constraints, casts, parameters, etc.). We record name -> definition
        text; types are metadata for the semantic layer, not models.
        """
        prop_list = ctx.userTypePropertyList()
        if not prop_list:
            return self.visitChildren(ctx)
        for type_def in prop_list.userTypeDefinition():
            name_def = type_def.userTypeNameDef()
            type_expr = type_def.userTypeExpr()
            if name_def:
                name = self._get_text(name_def)
                definition = self._get_text(type_expr) if type_expr else ""
                if name:
                    self.user_types[name] = definition
        return self.visitChildren(ctx)

    def visitDefineGivenStatement(self, ctx: MalloyParser.DefineGivenStatementContext):  # noqa: N802
        """Visit top-level `given: name::type` statement.

        Malloy added `given:` to declare model-level input parameters (similar to source
        parameters but file-scoped). We record name -> type text.
        """
        given_list = ctx.givenDefList()
        if not given_list:
            return self.visitChildren(ctx)
        for given_def in given_list.givenDef():
            name_def = given_def.givenNameDef()
            given_type = given_def.givenType()
            if name_def:
                name = self._get_text(name_def)
                type_text = self._get_text(given_type) if given_type else ""
                if name:
                    self.given[name] = type_text
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

        # Malloy trailing time truncation (created_at.month, ts.day, ...) -> time.
        # _extract_granularity reads the same trailing timeframe afterwards.
        granularities = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")
        trailing = re.search(r"\.(\w+)$", sql_lower)
        if trailing and trailing.group(1) in granularities:
            return "time"

        # Boolean detection - comparison that yields true/false
        if re.search(r"[<>=!]+\s*\S", sql):
            # Check if it's a simple comparison (boolean result)
            # But not if it's part of a CASE/pick statement
            if "pick" not in sql_lower and "case" not in sql_lower:
                return "boolean"

        # SQL DATE/TIMESTAMP literals (e.g. from a Malloy @2024-01-01 literal) are
        # time values. Check before numeric so the hyphens in the date are not
        # read as subtraction. After boolean so a comparison stays boolean.
        if re.search(r"\b(?:date|timestamp)\s+'", sql_lower):
            return "time"

        # Numeric detection
        if re.search(r"[+\-*/]", sql) and "||" not in sql:
            return "numeric"

        # Name-based time heuristic. Applied last (weakest signal) so an explicit
        # comparison or arithmetic expression is not overridden by the field name.
        time_name_patterns = ["date", "time", "timestamp", "_at", "created", "updated"]
        if any(p in name_lower for p in time_name_patterns):
            return "time"

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

    @staticmethod
    def _has_top_level_arith(expr: str) -> bool:
        """Return True if a binary arithmetic operator appears outside any
        parentheses, brackets, or quotes.

        Distinguishes a single aggregation call (`sum(x)`, `cost.sum()`) from a
        compound expression built from several aggregates (`sum(a) / sum(b)` or
        the unspaced `sum(a)/sum(b)`), which must be preserved verbatim as a
        derived measure. Spaces around the operator are not required; a left
        operand must exist so a leading unary minus is not treated as binary.
        """
        depth = 0
        quote = None
        n = len(expr)
        prev = ""  # last operand-boundary char seen at depth 0 (spaces ignored)
        skip_next = False
        for i, ch in enumerate(expr):
            if skip_next:
                skip_next = False
                continue
            if quote is not None:
                if ch == "\\":
                    skip_next = True  # ignore the escaped char so \' does not close
                    continue
                if ch == quote:
                    quote = None
                    prev = ch
                continue
            if ch in ("'", '"', "`"):
                quote = ch
                prev = ch
            elif ch in "([{":
                depth += 1
                prev = ch
            elif ch in ")]}":
                depth -= 1
                prev = ch
            elif depth == 0 and ch in "+-*/%" and i < n - 1 and (prev.isalnum() or prev in ")_.`'\""):
                return True
            elif ch != " ":
                prev = ch
        return False

    @staticmethod
    def _normalize_agg_calls(expr: str) -> str:
        """Rewrite Malloy aggregate syntax to SQL inside a preserved expression.

        `field.sum()` -> `SUM(field)`; a dotted path `a.b.c.sum()` -> `SUM(a.b.c)`;
        `count()` -> `count(*)`; and the distinct-count forms `count(x)`,
        `count_distinct(x)`, `field.count_distinct()` -> `COUNT(DISTINCT x)`
        (Malloy `count(expr)` is a distinct count, and `count_distinct` is not a
        SQL function). Used when a compound aggregate expression is kept as a
        derived measure so the stored SQL is executable.
        """

        def count_repl(m: re.Match) -> str:
            arg = m.group(1).strip()
            if arg == "":
                return "count(*)"
            if arg == "*" or arg.lower().startswith("distinct "):
                return m.group(0)
            return f"COUNT(DISTINCT {arg})"

        # Standard function forms count(x) / count_distinct(x) / count() — not
        # preceded by `.` so dot-method calls below are left for the next pass.
        expr = re.sub(
            r"(?<![\w.`])count(?:_distinct)?\s*\(\s*([^)]*?)\s*\)",
            count_repl,
            expr,
            flags=re.IGNORECASE,
        )

        def dot_repl(m: re.Match) -> str:
            field = m.group(1)
            agg = m.group(2).lower()
            args = m.group(3).strip()
            if agg == "count_distinct":
                return f"COUNT(DISTINCT {field})"
            sql_agg = agg.upper()
            return f"{sql_agg}({field}, {args})" if args else f"{sql_agg}({field})"

        # Dot-method aggregates; the field may be a backtick-quoted identifier
        # with spaces (`cost amount`.sum()).
        return re.sub(
            r"((?:`[^`]*`|[\w.])+)\.(sum|avg|count|min|max|count_distinct)\s*\(\s*(.*?)\s*\)",
            dot_repl,
            expr,
            flags=re.IGNORECASE,
        )

    def _parse_aggregation(self, expr: str) -> tuple[str | None, str | None]:
        """Parse aggregation function from expression.

        Returns (agg_type, sql_expr) tuple.

        Handles both standard SQL syntax (func(arg)) and Malloy dot-method
        syntax (field.func()). In Malloy, count(field) means count_distinct(field).
        """
        if not expr:
            return None, None

        expr_stripped = expr.strip()

        # A compound expression combining aggregates with a top-level arithmetic
        # operator (e.g. `sum(a) / sum(b)`, `cost.sum() / quantity.sum()`,
        # `sum(x) / count()`) is not a single aggregation. Keep it as a derived
        # measure, but normalize Malloy aggregate syntax (dot-method calls and a
        # bare `count()`) to SQL so the stored expression is valid SQL.
        if self._has_top_level_arith(expr_stripped):
            return None, self._normalize_agg_calls(expr_stripped)

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

        # ~ / !~ regex match: field ~ r'pattern' -> REGEXP_MATCHES(field, 'pattern')
        expr = self._transform_regex_match(expr)

        # @date / @timestamp literals:
        # @YYYY-MM-DD HH:MM:SS -> TIMESTAMP 'YYYY-MM-DD HH:MM:SS'
        # @YYYY-MM-DD -> DATE 'YYYY-MM-DD'
        # @YYYY-MM -> DATE 'YYYY-MM-01'
        # @YYYY -> DATE 'YYYY-01-01'
        # @YYYY-Qn -> handled as text
        # Timestamp literal: time is the hour with optional :MM, :SS, fractional
        # seconds (`.` or `,` separator), and an optional [zone] suffix (dropped).
        # Padded to HH:MM:SS and the fraction comma normalized to a dot so the
        # result is a valid SQL literal. Runs before the date-only rule so a time
        # component is never left dangling.
        def _timestamp_literal(m: re.Match) -> str:
            time = m.group(2).replace(",", ".")
            parts = time.split(":")
            while len(parts) < 3:
                parts.append("00")
            return f"TIMESTAMP '{m.group(1)} {':'.join(parts)}'"

        expr = re.sub(
            r"@(\d{4}-\d{2}-\d{2})[ T](\d{2}(?::\d{2}(?::\d{2}(?:[.,]\d+)?)?)?)(?:\[[^\]]+\])?",
            _timestamp_literal,
            expr,
        )
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

    @staticmethod
    def _left_operand_start(s: str, end: int) -> int | None:
        """Return the start index of the regex-match left operand ending at ``end``.

        The operand is a full arithmetic `fieldExpr` (Malloy binds `~` looser than
        arithmetic but tighter than comparison/logical operators), so the scan
        crosses `+ - * / %`, parenthesised groups, function calls, and string /
        backtick literals, but stops at a top-level comparison operator
        (`= < > ! ~`), a comma, an enclosing `(`, or a logical keyword
        (`and`/`or`/`not`). Returns None when there is no operand.
        """
        if end <= 0:
            return None

        start = 0  # operand start at the current parenthesis depth
        stack: list[int] = []  # saved starts for enclosing depths
        q = None
        i = 0
        while i < end:
            c = s[i]
            if q is not None:
                if c == "\\":
                    i += 2  # skip the escaped char
                    continue
                if c == q:
                    q = None
                i += 1
                continue
            if c in ("'", '"', "`"):
                q = c
                i += 1
                continue
            if c == "(":
                stack.append(start)
                start = i + 1
                i += 1
                continue
            if c == ")":
                if stack:
                    start = stack.pop()
                i += 1
                continue
            if c in "=<>!~,":
                start = i + 1
                i += 1
                continue
            matched = None
            # Logical operators and CASE keywords bound an arithmetic operand on
            # the left (the pick -> CASE rewrite runs before this, so a condition
            # like `WHEN title ~ r'...'` must stop the operand at `WHEN`).
            for kw in ("and", "or", "not", "when", "then", "else", "case", "end"):
                j = i + len(kw)
                if (
                    s[i:j].lower() == kw
                    and (i == 0 or not (s[i - 1].isalnum() or s[i - 1] == "_"))
                    and (j >= end or not (s[j].isalnum() or s[j] == "_"))
                ):
                    matched = j
                    break
            if matched is not None:
                start = matched
                i = matched
                continue
            i += 1

        while start < end and s[start] == " ":
            start += 1
        return start if start < end else None

    def _transform_regex_match(self, expr: str) -> str:
        """Transform Malloy regex matches to ``REGEXP_MATCHES`` calls.

        ``operand ~ r'pat'`` -> ``REGEXP_MATCHES(operand, 'pat')`` and
        ``operand !~ r'pat'`` -> ``NOT REGEXP_MATCHES(operand, 'pat')``.

        The immediate left operand is found by walking back over a single
        balanced expression, so a match never swallows preceding conditions
        (``a = 1 and name ~ r'x'``) and computed/parenthesised operands
        (``lower(name) ~ r'x'``) are preserved. Matches are rewritten
        right-to-left so earlier offsets stay valid.
        """
        pattern = re.compile(r"(!?~)\s+r(['\"])(.*?)\2")
        for m in reversed(list(pattern.finditer(expr))):
            end = m.start()
            while end > 0 and expr[end - 1] == " ":
                end -= 1
            operand_start = self._left_operand_start(expr, end)
            if operand_start is None:
                continue
            operand = expr[operand_start:end]
            replacement = f"REGEXP_MATCHES({operand}, '{m.group(3)}')"
            if m.group(1) == "!~":
                replacement = f"NOT {replacement}"
            expr = expr[:operand_start] + replacement + expr[m.end() :]
        return expr

    def _transform_null_coalesce(self, expr: str) -> str:
        """Transform Malloy ?? null coalescing to SQL COALESCE.

        Only splits on ?? at the top expression depth (not inside parens/brackets).
        """
        if "??" not in expr:
            return expr

        # Split on ?? only at depth 0 and outside string literals
        parts = []
        current = []
        depth = 0
        quote = None
        i = 0
        while i < len(expr):
            ch = expr[i]
            if quote is not None:
                current.append(ch)
                if ch == "\\" and i + 1 < len(expr):
                    current.append(expr[i + 1])  # keep the escaped char verbatim
                    i += 2
                    continue
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch in ("'", '"', "`"):
                quote = ch
                current.append(ch)
            elif ch in ("(", "[", "{"):
                depth += 1
                current.append(ch)
            elif ch in (")", "]", "}"):
                depth -= 1
                current.append(ch)
            elif depth == 0 and expr[i : i + 2] == "??":
                parts.append("".join(current).strip())
                current = []
                i += 2
                # Skip whitespace after ??
                while i < len(expr) and expr[i] == " ":
                    i += 1
                continue
            else:
                current.append(ch)
            i += 1

        if parts:
            parts.append("".join(current).strip())
            return f"COALESCE({', '.join(parts)})"
        return expr

    @staticmethod
    def _split_top_level(expr: str, sep: str) -> list[str]:
        """Split ``expr`` on ``sep`` only at the top level (outside any
        parentheses/brackets and outside string literals)."""
        parts: list[str] = []
        buf: list[str] = []
        depth = 0
        quote = None
        i = 0
        n = len(sep)
        while i < len(expr):
            ch = expr[i]
            if quote is not None:
                buf.append(ch)
                if ch == "\\" and i + 1 < len(expr):
                    buf.append(expr[i + 1])  # keep the escaped char verbatim
                    i += 2
                    continue
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch in ("'", '"', "`"):
                quote = ch
                buf.append(ch)
            elif ch in "([{":
                depth += 1
                buf.append(ch)
            elif ch in ")]}":
                depth -= 1
                buf.append(ch)
            elif depth == 0 and expr[i : i + n] == sep:
                parts.append("".join(buf))
                buf = []
                i += n
                continue
            else:
                buf.append(ch)
            i += 1
        parts.append("".join(buf))
        return parts

    def _transform_and_tree(self, expr: str) -> str:
        """Transform Malloy & (and-tree) to SQL AND with expanded base field.

        Examples:
        - "field < 2031 & > -8000" -> "field < 2031 AND field > -8000"
        - "status != 'Cancelled' & 'Returned'" -> "status != 'Cancelled' AND status != 'Returned'"

        Only splits on a top-level `&` (not one inside a string literal such as
        "label = 'A & B'").
        """
        parts = [p.strip() for p in self._split_top_level(expr, " & ")]
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
        # Locate the pick/when/else keywords at the top level (outside string
        # literals and parentheses), then slice the arms between them. This is
        # keyword-driven rather than line-driven so single-line and multi-line
        # forms both work, and quote-aware so a keyword inside a string literal
        # (e.g. `when note = 'a else b'`) is not treated as a delimiter.
        keywords = self._scan_keywords(expr, ("pick", "when", "else"))
        if not any(kw == "pick" for _, _, kw in keywords):
            return expr

        cases = []
        else_value = None
        i = 0
        while i < len(keywords):
            _, end, kw = keywords[i]
            if kw == "else":
                else_value = expr[end:].strip()
                break
            if kw == "pick" and i + 1 < len(keywords) and keywords[i + 1][2] == "when":
                when_start, when_end = keywords[i + 1][0], keywords[i + 1][1]
                value = expr[end:when_start].strip()
                cond_end = keywords[i + 2][0] if i + 2 < len(keywords) else len(expr)
                condition = expr[when_end:cond_end].strip()
                if base_field:
                    condition = self._expand_partial_condition(condition, base_field)
                cases.append(f"WHEN {condition} THEN {value}")
                i += 2
                continue
            i += 1

        if cases:
            case_str = "CASE " + " ".join(cases)
            if else_value:
                case_str += f" ELSE {else_value}"
            case_str += " END"
            return case_str

        return expr

    @staticmethod
    def _scan_keywords(s: str, keywords: tuple[str, ...]) -> list[tuple[int, int, str]]:
        """Return (start, end, keyword) for each whole-word keyword occurrence at
        the top level (outside string literals, parentheses, and any nested SQL
        ``case ... end`` block, so an inner when/then/else is not reported)."""
        found: list[tuple[int, int, str]] = []
        depth = 0
        case_depth = 0
        quote = None
        i = 0
        n = len(s)
        while i < n:
            ch = s[i]
            if quote is not None:
                if ch == "\\":
                    i += 2  # skip the escaped char so \' does not close the string
                    continue
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch in ("'", '"', "`"):
                quote = ch
                i += 1
                continue
            if ch in "([{":
                depth += 1
                i += 1
                continue
            if ch in ")]}":
                depth -= 1
                i += 1
                continue
            if depth == 0:
                # Match a whole word: case/end adjust nesting depth; the requested
                # keywords are only reported outside any nested case...end block.
                hit = None
                for kw in ("case", "end", *keywords):
                    j = i + len(kw)
                    if (
                        s[i:j].lower() == kw
                        and (i == 0 or not (s[i - 1].isalnum() or s[i - 1] == "_"))
                        and (j >= n or not (s[j].isalnum() or s[j] == "_"))
                    ):
                        hit = (kw, j)
                        break
                if hit is not None:
                    kw, j = hit
                    if kw == "case":
                        case_depth += 1
                    elif kw == "end":
                        case_depth = max(0, case_depth - 1)
                    elif case_depth == 0:
                        found.append((i, j, kw))
                    i = j
                    continue
            i += 1
        return found

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
                    if self._virtual:
                        metadata["virtual"] = self._virtual
                    if self._source_type_constraints:
                        metadata["source_type_constraints"] = list(self._source_type_constraints)
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

        # Check for virtual source: connection.virtual('name')
        # Malloy added virtual() sources (a named source resolved by the connection
        # rather than a physical table). Treat the virtual name like a table path so
        # the model still has an identifiable source.
        if isinstance(ctx, MalloyParser.SQVirtualContext):
            virtual_source = ctx.virtualSource()
            if virtual_source:
                self._process_virtual_source(virtual_source)
            return

        # Check for source type constraint: base::Type or base::(T1, T2)
        # Malloy added `source: a is b::T` to assert the source conforms to a user type.
        # We process the underlying source and record the type constraint as metadata.
        if isinstance(ctx, MalloyParser.SQTypedSourceContext):
            base_sq_expr = ctx.sqExpr()
            if base_sq_expr:
                self._process_sq_expr(base_sq_expr)
            constraints = ctx.sourceTypeConstraints()
            if constraints:
                names = [self._get_text(n) for n in constraints.userTypeName()]
                names = [n for n in names if n]
                if names:
                    self._source_type_constraints = names
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

    def _process_virtual_source(self, ctx: MalloyParser.VirtualSourceContext):
        """Process a virtual() source: connection.virtual('name').

        Records the connection and stores the virtual source name on metadata. The
        virtual name doubles as the table reference so the resulting model still has a
        usable source identifier.
        """
        conn_id = ctx.connectionId()
        if conn_id:
            id_ctx = conn_id.id_()
            if id_ctx:
                self.current_connection = self._get_text(id_ctx)
        short_string = ctx.shortString()
        if short_string:
            virtual_name = self._extract_string(self._get_text(short_string))
            self._virtual = virtual_name
            if self.current_table is None:
                self.current_table = virtual_name

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

        # Transform pick/when to CASE first (with apply-pick support) so the
        # expression transforms below operate on real field references inside the
        # WHEN clauses rather than on raw `pick ... when ...` text.
        if "pick" in sql.lower():
            # Check for apply-pick pattern: field ? pick ... when ...
            apply_match = re.match(r"^(.+?)\s*\?\s*\n?\s*(pick\s+.+)$", sql, re.DOTALL | re.IGNORECASE)
            if apply_match:
                base_field = apply_match.group(1).strip()
                pick_expr = apply_match.group(2).strip()
                sql = self._transform_pick_to_case(pick_expr, base_field=base_field)
            else:
                sql = self._transform_pick_to_case(sql)

        # Transform remaining Malloy-specific expression syntax to SQL
        sql = self._transform_malloy_expr(sql)

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
        # Successive refinements (count() { where: a } { where: b }) parse as
        # nested ExprFieldProps; Malloy ANDs them, so unwrap every level and
        # collect all filters rather than only the outermost one.
        filters = None
        if isinstance(field_expr, MalloyParser.ExprFieldPropsContext):
            collected_filters: list[str] = []
            node = field_expr
            while isinstance(node, MalloyParser.ExprFieldPropsContext):
                props = node.fieldProperties()
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
                                    collected_filters.extend(self._get_text(f) for f in filter_list.fieldExpr())
                node = node.fieldExpr()

            expr_text = self._get_text(node) if node is not None else ""
            if collected_filters:
                # Collected outermost-first; reverse to restore source order.
                filters = list(reversed(collected_filters))

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
            rel_type = "cross"  # Cross join -> cartesian product (CROSS JOIN)
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

        # Store join direction (LEFT, RIGHT, FULL, INNER) if specified
        foreign_key = None
        join_metadata = None
        matrix_op = ctx.matrixOperation() if hasattr(ctx, "matrixOperation") else None
        if matrix_op:
            direction = self._get_text(matrix_op).lower()
            join_metadata = {"join_direction": direction}

        # Get foreign key from 'with' clause
        if isinstance(ctx, MalloyParser.JoinWithContext):
            field_expr = ctx.fieldExpr()
            if field_expr:
                foreign_key = self._get_text(field_expr)
        elif isinstance(ctx, MalloyParser.JoinOnContext):
            join_expr = ctx.joinExpression()
            if join_expr:
                expr_text = self._get_text(join_expr)
                # Store full condition in metadata
                if join_metadata is None:
                    join_metadata = {}
                join_metadata["on_condition"] = expr_text
                # Extract FK column(s), handling either ordering of each equality
                # and keys qualified by the source or target name.
                fk_keys = self._extract_on_condition_keys(expr_text, name, rel_type)
                if fk_keys:
                    foreign_key = fk_keys[0]
                    if len(fk_keys) > 1:
                        join_metadata["composite_keys"] = fk_keys

        self.current_relationships.append(
            Relationship(
                name=name,
                type=rel_type,
                foreign_key=foreign_key,
                metadata=join_metadata,
            )
        )

    @staticmethod
    def _extract_on_condition_keys(expr_text: str, target_name: str, rel_type: str) -> list[str]:
        """Extract foreign-key column(s) from a join ``on`` condition.

        Handles either ordering of each equality (``src = tgt.col`` or
        ``tgt.col = src``) and multi-condition clauses joined by ``and``. For
        ``many_to_one`` / ``one_to_one`` the key is the source-side column; for
        ``one_to_many`` it is the related (target-qualified) column.
        """

        def split_qualifier(tok: str) -> tuple[str | None, str]:
            tok = tok.replace("`", "")
            if "." in tok:
                qualifier, column = tok.rsplit(".", 1)
                return qualifier, column
            return None, tok

        def is_literal(tok: str) -> bool:
            low = tok.lower()
            return low in ("true", "false", "null") or tok.replace(".", "", 1).isdigit()

        def strip_outer_parens(text: str) -> str:
            text = text.strip()
            while len(text) >= 2 and text[0] == "(" and text[-1] == ")":
                depth = 0
                wraps_all = True
                for idx, ch in enumerate(text):
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                        if depth == 0 and idx != len(text) - 1:
                            wraps_all = False
                            break
                if not wraps_all:
                    break
                text = text[1:-1].strip()
            return text

        keys: list[str] = []
        for cond in re.split(r"\s+and\s+", strip_outer_parens(expr_text), flags=re.IGNORECASE):
            m = re.match(r"\s*([\w.`]+)\s*=\s*([\w.`]+)\s*$", strip_outer_parens(cond))
            if not m:
                continue
            lq, lc = split_qualifier(m.group(1))
            rq, rc = split_qualifier(m.group(2))

            # A `col = literal` equality (e.g. `customers.active = true`) is a
            # filter predicate, not a join key, so it contributes no foreign key.
            if is_literal(lc) or is_literal(rc):
                continue

            # Identify the related (target) vs source side. A side qualified by
            # the target name is the related column; otherwise the unqualified
            # side is the related column (Malloy convention) and the other side
            # belongs to the source.
            if lq == target_name:
                related_col, source_col = lc, rc
            elif rq == target_name:
                related_col, source_col = rc, lc
            elif lq is None and rq is not None:
                related_col, source_col = lc, rc
            elif rq is None and lq is not None:
                related_col, source_col = rc, lc
            else:
                related_col, source_col = lc, rc

            # one_to_many keys on the related (foreign) column; many_to_one and
            # one_to_one key on the source column.
            keys.append(related_col if rel_type == "one_to_many" else source_col)
        return keys

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
            self._virtual,
            list(self._source_type_constraints),
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
        self._virtual = None
        self._source_type_constraints = []

        self._process_sq_expr(sq_expr)

        # Only create model if we found something useful
        if self.current_table or self.current_sql or self.current_extends:
            metadata = {}
            if self.current_connection:
                metadata["connection"] = self.current_connection
            if self._timezone:
                metadata["timezone"] = self._timezone
            if self._model_tags:
                metadata["tags"] = self._model_tags
            if self._virtual:
                metadata["virtual"] = self._virtual
            if self._source_type_constraints:
                metadata["source_type_constraints"] = list(self._source_type_constraints)
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
            self._virtual,
            self._source_type_constraints,
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

    Error handling:
        The adapter installs an ANTLR error listener so syntax errors are surfaced
        instead of being silently swallowed (which previously produced a degraded or
        empty graph). By default (``strict=False``) errors are collected on
        ``adapter.errors`` and emitted as a ``UserWarning``; parsing still returns the
        models ANTLR could recover. With ``strict=True`` any syntax error raises
        ``MalloySyntaxError``.
    """

    def __init__(self, strict: bool = False, warn_on_errors: bool = True):
        """Create a Malloy adapter.

        Args:
            strict: If True, raise ``MalloySyntaxError`` when a parsed file has any
                syntax error. If False (default), collect errors and continue using
                ANTLR's recovered parse (backward-compatible behavior).
            warn_on_errors: If True (default) and not strict, emit a ``UserWarning``
                summarizing collected syntax errors so they are not silent.
        """
        self.strict = strict
        self.warn_on_errors = warn_on_errors
        # Collected (file_path, line, column, message) syntax errors from the last parse.
        self.errors: list[tuple[str, int, int, str]] = []
        # Newer top-level Malloy constructs collected during the last parse. These are
        # metadata for the semantic layer rather than models:
        #   user_types: name -> type definition text (from `type:` statements)
        #   given:      name -> type text (from `given:` parameter statements)
        #   exports:    source names re-exported via `export { ... }`
        self.user_types: dict[str, str] = {}
        self.given: dict[str, str] = {}
        self.exports: list[str] = []

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
            MalloySyntaxError: If ``strict=True`` and a file has syntax errors
        """
        if not _ANTLR4_AVAILABLE:
            raise ImportError(
                'Malloy support requires antlr4-python3-runtime. Install with: pip install "sidemantic[malloy]"'
            )
        self.errors = []
        self.user_types = {}
        self.given = {}
        self.exports = []
        graph = SemanticGraph()
        source_path = Path(source)

        if source_path.is_dir():
            # Parse all .malloy files in directory
            # When parsing a directory, each file is treated as a "root" file,
            # meaning all of its models are added (no import filter).
            # We use a separate parsed_files set per root file to handle imports,
            # but we track which models have been added to avoid duplicates.
            for malloy_file in source_path.rglob("*.malloy"):
                parsed_files: set = set()
                self._parse_file(malloy_file, graph, parsed_files, import_filter=None)
        else:
            parsed_files: set = set()
            self._parse_file(source_path, graph, parsed_files)

        return graph

    def _parse_file(
        self,
        file_path: Path,
        graph: SemanticGraph,
        parsed_files: set,
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

        # Dedup/cycle detection keyed on (path, filter): a file imported under a
        # narrow named-import filter must still be parseable for a later, broader
        # (or differently-named) import of the same file, while an identical
        # request is parsed at most once so circular imports still terminate.
        filter_key = None if import_filter is None else frozenset(import_filter)
        cache_key = (resolved_path, filter_key)
        if cache_key in parsed_files:
            return

        parsed_files.add(cache_key)

        if not file_path.exists():
            return

        with open(file_path) as f:
            content = f.read()

        # Create ANTLR input stream
        input_stream = InputStream(content)
        lexer = MalloyLexer(input_stream)
        token_stream = CommonTokenStream(lexer)
        parser = MalloyParser(token_stream)

        # Install an error listener so syntax errors are surfaced instead of being
        # silently swallowed. The default ConsoleErrorListener only prints to stderr;
        # we collect errors so we can warn or raise. We keep ANTLR's error recovery so
        # a single bad statement does not discard the rest of the (recoverable) parse.
        error_listener = _CollectingErrorListener()
        lexer.removeErrorListeners()
        lexer.addErrorListener(error_listener)
        parser.removeErrorListeners()
        parser.addErrorListener(error_listener)

        # Parse the document
        tree = parser.malloyDocument()

        if error_listener.errors:
            self.errors.extend((str(file_path), line, col, msg) for line, col, msg in error_listener.errors)
            if self.strict:
                detail = "; ".join(f"line {line}:{col} {msg}" for line, col, msg in error_listener.errors)
                raise MalloySyntaxError(
                    f"Malloy syntax error(s) in {file_path}: {detail}",
                    error_listener.errors,
                )
            if self.warn_on_errors:
                count = len(error_listener.errors)
                first_line, first_col, first_msg = error_listener.errors[0]
                # Truncate ANTLR's verbose "expecting {...}" token sets to keep the
                # warning readable; full details remain on adapter.errors.
                short_msg = first_msg.split(" expecting ")[0]
                more = f" (+{count - 1} more)" if count > 1 else ""
                warnings.warn(
                    f"Malloy syntax error(s) in {file_path}: "
                    f"line {first_line}:{first_col} {short_msg}{more}. "
                    "Parsed models may be incomplete; inspect adapter.errors for details.",
                    UserWarning,
                    stacklevel=2,
                )

        # Visit the tree to extract models and imports
        visitor = MalloyModelVisitor()
        visitor.visit(tree)

        # Surface newer top-level constructs (type:/given:/export) as adapter metadata.
        self.user_types.update(visitor.user_types)
        self.given.update(visitor.given)
        for export_name in visitor.exports:
            if export_name not in self.exports:
                self.exports.append(export_name)

        # Process imports first (depth-first) so imported sources are available
        for import_path, import_items in visitor.imports:
            # Resolve import path relative to current file
            import_file = (file_path.parent / import_path).resolve()
            if import_file.exists():
                self._parse_file(import_file, graph, parsed_files, import_items)

        # Add models to graph (with optional filtering/aliasing for selective imports).
        for model in visitor.models:
            if import_filter is None:
                if model.name not in graph.models:
                    graph.add_model(model)
                continue

            # A source may be selected under several names in a single import
            # (e.g. `import { s is a, s is b }`), so emit one model per matching
            # entry rather than only the first.
            for name, alias in import_filter:
                if model.name != name:
                    continue
                target = model if not alias else model.model_copy(update={"name": alias})
                if target.name not in graph.models:
                    graph.add_model(target)

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
            lines.append(f"# desc: {self._one_line(model.description)}")

        # Source header - use connection from metadata, default to duckdb
        connection = (model.metadata or {}).get("connection", "duckdb")
        if model.sql:
            lines.append(f'source: {model.name} is {connection}.sql("""{model.sql}""") extend {{')
        elif model.table:
            lines.append(f"source: {model.name} is {connection}.table('{model.table}') extend {{")
        elif model.extends:
            # Tableless derived source: reference the base so the output is valid
            # Malloy (`source: x is base extend {`) rather than a bare `extend {`.
            lines.append(f"source: {model.name} is {model.extends} extend {{")
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
            # Tier 4.5: detect renames (simple identifier, no operators/functions).
            # A time dimension with a granularity is NOT a rename: it must keep its
            # `.granularity` suffix below so the time type survives the roundtrip.
            is_time_with_grain = dim.type == "time" and dim.granularity
            if re.match(r"^[`\w]+$", sql) and sql != dim.name and not is_time_with_grain:
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
                    lines.append(f"    # desc: {self._one_line(dim.description)}")
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
        measure_lines: list[str] = []
        has_real_measure = False
        for metric in model.metrics:
            measure_expr = self._format_measure(metric)
            if measure_expr is None:
                # No faithful Malloy representation (e.g. cumulative/derived with
                # no sql); skip it rather than silently emitting a bogus count().
                measure_lines.append(f"    // {metric.name}: unsupported metric type, not exported")
                continue
            has_real_measure = True
            if metric.description:
                measure_lines.append(f"    # desc: {self._one_line(metric.description)}")
            measure_lines.append(f"    {metric.name} is {measure_expr}")
        if has_real_measure:
            lines.append("")
            lines.append("  measure:")
            lines.extend(measure_lines)

        # Joins - Tier 4.4: use on condition from metadata when available
        for rel in model.relationships:
            lines.append("")
            if rel.type == "cross":
                # A cross join takes no key clause in Malloy.
                lines.append(f"  join_cross: {rel.name}")
                continue
            # one_to_many and many_to_many fan out -> join_many; many_to_one and
            # one_to_one collapse to at most one match -> join_one.
            join_type = "join_many" if rel.type in ("one_to_many", "many_to_many") else "join_one"
            on_condition = (rel.metadata or {}).get("on_condition")
            if on_condition:
                lines.append(f"  {join_type}: {rel.name} on {on_condition}")
            elif rel.foreign_key:
                lines.append(f"  {join_type}: {rel.name} with {rel.foreign_key}")
            else:
                lines.append(f"  {join_type}: {rel.name}")

        lines.append("}")

        return lines

    @staticmethod
    def _one_line(text: str) -> str:
        """Collapse a description to a single line so it is valid in a `# desc:`
        annotation (Malloy annotations are line-terminated)."""
        return " ".join(text.split())

    def _format_measure(self, metric: Metric) -> str | None:
        """Format a metric as a Malloy measure expression.

        Args:
            metric: Metric to format

        Returns:
            The Malloy measure expression, or None if the metric has no faithful
            Malloy representation (so the caller can skip it instead of emitting a
            misleading default).
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

        # No faithful Malloy representation for this metric.
        return None
