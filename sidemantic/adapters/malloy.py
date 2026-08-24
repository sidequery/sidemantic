"""Malloy adapter for importing/exporting Malloy semantic models.

Uses ANTLR4-generated parser from official Malloy grammar files.
"""

from __future__ import annotations

import re
import warnings
from decimal import Decimal
from pathlib import Path

from sidemantic.adapters.base import BaseAdapter
from sidemantic.adapters.malloy_expressions import MalloyExpressionBuilder, MalloyExpressionLoweringError
from sidemantic.adapters.malloy_modules import (
    MalloyExportStatement,
    MalloyImportItem,
    MalloyImportStatement,
    MalloyLocation,
    MalloyModule,
    MalloyModuleResolver,
    MalloyNamedStatement,
    MalloySourceStatement,
    MalloyStatement,
)
from sidemantic.adapters.malloy_queries import MalloyQueryDiagnostic, map_malloy_query
from sidemantic.core.dimension import Dimension
from sidemantic.core.inheritance import merge_model
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.schema_exposure import SchemaExposure
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import record_import_feature
from sidemantic.sql.fragment import (
    mask_sql_literals_comments_and_quoted_identifiers,
    parse_sql_fragment,
    protected_sql_spans,
    replace_outside_sql_protected,
)

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


def _sub_outside_quotes(s: str, pattern: str, repl, quotes: tuple[str, ...] = ("'", '"')) -> str:
    """Apply ``re.sub(pattern, repl, ...)`` (IGNORECASE) only to text outside the
    given quote characters, so a value inside a literal (e.g. ``'count()'``) is
    left intact. ``quotes`` defaults to single/double quotes (backticks are part
    of a field reference); pass backtick too when rewriting already-SQL text
    where a backtick identifier must be protected."""
    out: list[str] = []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c in quotes:
            j = i + 1
            while j < n:
                if s[j] == "\\":
                    j += 2
                    continue
                if s[j] == c:
                    j += 1
                    break
                j += 1
            out.append(s[i:j])
            i = j
        else:
            j = i
            while j < n and s[j] not in quotes:
                j += 1
            out.append(re.sub(pattern, repl, s[i:j], flags=re.IGNORECASE))
            i = j
    return "".join(out)


def _outside_quotes(s: str, position: int, quotes: tuple[str, ...] = ("'", '"', "`")) -> bool:
    quote: str | None = None
    i = 0
    while i < position:
        char = s[i]
        if quote is not None:
            if char == "\\":
                i += 2
                continue
            if char == quote:
                quote = None
        elif char in quotes:
            quote = char
        i += 1
    return quote is None


def _mask_malloy_quoted_text(expr: str) -> str:
    """Hide literal/quoted-identifier contents while preserving token boundaries."""
    chars = list(expr)
    i = 0
    while i < len(expr):
        marker = next((quote for quote in ("'''", '"""', "'", '"', "`") if expr.startswith(quote, i)), None)
        if marker is None:
            i += 1
            continue
        end = i + len(marker)
        while end < len(expr):
            if expr.startswith(marker, end):
                end += len(marker)
                break
            if expr[end] == "\\" and len(marker) == 1:
                end += 2
            else:
                end += 1
        content_start = i + len(marker)
        content_end = max(content_start, end - len(marker))
        chars[content_start:content_end] = " " * (content_end - content_start)
        i = end
    return "".join(chars)


def _protect_malloy_comments(expr: str) -> tuple[str, list[tuple[str, str]]]:
    """Replace hidden-channel Malloy comments with inert quoted sentinels."""
    out: list[str] = []
    replacements: list[tuple[str, str]] = []
    i = 0
    quote: str | None = None
    while i < len(expr):
        char = expr[i]
        if quote is not None:
            out.append(char)
            if char == "\\" and i + 1 < len(expr):
                out.append(expr[i + 1])
                i += 2
                continue
            if char == quote:
                quote = None
            i += 1
            continue
        if char in ("'", '"', "`"):
            quote = char
            out.append(char)
            i += 1
            continue

        end = None
        if expr.startswith("/*", i):
            depth = 1
            cursor = i + 2
            while cursor < len(expr) and depth:
                if expr.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif expr.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            end = cursor
        elif expr.startswith("--", i) or expr.startswith("//", i):
            newline = expr.find("\n", i + 2)
            end = len(expr) if newline < 0 else newline

        if end is None:
            out.append(char)
            i += 1
            continue
        original = expr[i:end]
        sentinel = f"'__sidemantic_malloy_comment_{len(replacements)}__'"
        while sentinel in expr:
            sentinel = f"'__sidemantic_malloy_comment_{len(replacements)}_{len(sentinel)}__'"
        replacements.append((sentinel, original))
        out.append(sentinel)
        i = end
    return "".join(out), replacements


def _normalize_count_calls(s: str) -> str:
    """Rewrite Malloy count syntax to SQL with quote- and paren-aware scanning.

    count() -> count(*); count(x) / count_distinct(x) -> COUNT(DISTINCT x). The
    argument is captured by balanced parens (so a string literal inside it, e.g.
    count(case when p = 'pro' then x end), does not break the match), while a
    literal that merely looks like a count ('count()') is left untouched.
    """
    out: list[str] = []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        # Copy string and backtick-identifier literals verbatim (a backtick field
        # may contain parens, e.g. `user(id`).
        if c in ("'", '"', "`"):
            j = i + 1
            while j < n:
                if s[j] == "\\":
                    j += 2
                    continue
                if s[j] == c:
                    j += 1
                    break
                j += 1
            out.append(s[i:j])
            i = j
            continue
        m = re.match(r"count(?:_distinct)?", s[i:], re.IGNORECASE)
        if m and (i == 0 or not (s[i - 1].isalnum() or s[i - 1] in "_.`")):
            k = i + m.end()
            while k < n and s[k].isspace():  # any whitespace, e.g. count\n(x)
                k += 1
            if k < n and s[k] == "(":
                depth, p, q = 0, k, None
                while p < n:
                    ch = s[p]
                    if q is not None:
                        if ch == "\\":
                            p += 2
                            continue
                        if ch == q:
                            q = None
                        p += 1
                        continue
                    if ch in ("'", '"', "`"):
                        q = ch
                    elif ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                        if depth == 0:
                            p += 1
                            break
                    p += 1
                if depth == 0:
                    arg = s[k + 1 : p - 1].strip()
                    if arg == "":
                        out.append("count(*)")
                    elif arg == "*" or arg.lower().startswith("distinct "):
                        out.append(s[i:p])
                    else:
                        out.append(f"COUNT(DISTINCT {arg})")
                    i = p
                    continue
        out.append(c)
        i += 1
    return "".join(out)


_DOT_AGGS = ("count_distinct", "sum", "avg", "count", "min", "max")


def _normalize_dot_aggregates(s: str) -> str:
    """Rewrite Malloy dot-method aggregates ``field.sum()`` -> ``SUM(field)`` and
    ``field.count_distinct()`` -> ``COUNT(DISTINCT field)``.

    Scans with quote/backtick awareness: a backtick-quoted field name is treated
    as an atomic part of the field reference, so aggregate-looking text inside a
    backtick identifier (like a field literally named "gross.sum()") is
    preserved, while a real backtick-field followed by .sum() is normalized.
    """
    result: list[str] = []
    last, i, n = 0, 0, len(s)
    field_start: int | None = None
    while i < n:
        c = s[i]
        if c in ("'", '"'):
            field_start = None
            i += 1
            while i < n:
                if s[i] == "\\":
                    i += 2
                    continue
                if s[i] == c:
                    i += 1
                    break
                i += 1
            continue
        if c == "`":
            if field_start is None:
                field_start = i
            i += 1
            while i < n:
                if s[i] == "\\":
                    i += 2
                    continue
                if s[i] == "`":
                    i += 1
                    break
                i += 1
            continue
        if c == ".":
            matched = None
            for agg in _DOT_AGGS:
                ke = i + 1 + len(agg)
                if s[i + 1 : ke].lower() == agg and (ke >= n or not (s[ke].isalnum() or s[ke] == "_")):
                    k = ke
                    while k < n and s[k].isspace():
                        k += 1
                    if k < n and s[k] == "(":
                        matched = (agg, k)
                        break
            if matched is not None and field_start is not None:
                agg, paren = matched
                depth, p, q = 0, paren, None
                while p < n:
                    ch = s[p]
                    if q is not None:
                        if ch == "\\":
                            p += 2
                            continue
                        if ch == q:
                            q = None
                        p += 1
                        continue
                    if ch in ("'", '"', "`"):
                        q = ch
                    elif ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                        if depth == 0:
                            p += 1
                            break
                    p += 1
                if depth == 0:
                    field = s[field_start:i]
                    args = s[paren + 1 : p - 1].strip()
                    if agg == "count_distinct":
                        repl = f"COUNT(DISTINCT {field})"
                    else:
                        sql_agg = agg.upper()
                        repl = f"{sql_agg}({field}, {args})" if args else f"{sql_agg}({field})"
                    result.append(s[last:field_start])
                    result.append(repl)
                    last = i = p
                    field_start = None
                    continue
            # plain '.' inside a dotted field reference
            if field_start is None:
                field_start = i
            i += 1
            continue
        if c.isalnum() or c == "_":
            if field_start is None:
                field_start = i
            i += 1
            continue
        field_start = None
        i += 1
    result.append(s[last:])
    return "".join(result)


class MalloySyntaxError(ValueError):
    """Raised when a Malloy document contains syntax errors and strict parsing is requested.

    The collected per-error details are available on the ``errors`` attribute as a list
    of ``(line, column, message)`` tuples.
    """

    def __init__(self, message: str, errors: list[tuple[int, int, str]]):
        super().__init__(message)
        self.errors = errors


class MalloySchemaExposureError(ValueError):
    """Raised when strict import cannot safely discover a Malloy source schema."""


class _UnsupportedTypedShapeError(ValueError):
    """Expression belongs to an established non-scalar compatibility path."""


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

    def __init__(
        self,
        strict_schema_exposure: bool = False,
        source_path: Path | None = None,
        connection_dialects: dict[str, str] | None = None,
    ):
        self.strict_schema_exposure = strict_schema_exposure
        self.source_path = source_path or Path("<malloy>")
        self.connection_dialects = dict(connection_dialects or {})
        self.models: list[Model] = []
        self.statements: list[MalloyStatement] = []
        # Compatibility introspection for callers which inspect the visitor directly.
        self.imports: list[tuple[str, list[tuple[str, str | None]] | None]] = []
        self.exports: list[str] = []
        # User-defined types: top-level `type: name is ...` definitions (name -> definition text).
        self.user_types: dict[str, str] = {}
        # Given parameters: top-level `given: name::type` model-input parameters (name -> type text).
        self.given: dict[str, str] = {}
        self.current_model_name: str | None = None
        self.current_table: str | None = None
        self.current_sql: str | None = None
        # Malloy does not infer a primary key for every source. Keep an unknown
        # key as ``None`` until an explicit ``primary_key:`` statement is seen.
        self.current_primary_key: str | None = None
        self.current_description: str | None = None
        self.current_extends: str | None = None
        self.current_connection: str | None = None
        self.current_dimensions: list[Dimension] = []
        self.current_metrics: list[Metric] = []
        self.current_relationships: list[Relationship] = []
        self.current_segments: list[Segment] = []
        self.current_invariant_filters: list[str] = []
        self.unsupported_features: list[str] = []
        self.expression_diagnostics: list[tuple[str, str, str]] = []
        self.source_diagnostics: list[tuple[str, str, str]] = []
        self._source_invalid = False

    def _reset_current(self):
        """Reset current model state."""
        self.current_model_name = None
        self.current_table = None
        self.current_sql = None
        self.current_primary_key = None
        self.current_description = None
        self.current_extends = None
        self.current_connection = None
        self.current_dimensions = []
        self.current_metrics = []
        self.current_relationships = []
        self.current_segments = []
        self.current_invariant_filters = []
        self._timezone = None
        self._model_tags = []
        self._accept_fields = []
        self._except_fields = []
        self._virtual = None
        self._source_type_constraints = []
        self._source_invalid = False

    @staticmethod
    def _deduplicate(names: list[str]) -> list[str]:
        """Deduplicate field names without changing Malloy declaration order."""
        return list(dict.fromkeys(names))

    def _schema_exposure_options(self) -> dict:
        """Build runtime schema exposure for an introspectable physical source.

        Malloy table and SQL sources expose their physical fields intrinsically.
        Source governance is applied during introspection, before those fields can
        become Sidemantic dimensions. Explicit declarations remain on the model and
        therefore continue to take precedence over auto-discovered fields.
        """
        if self.current_extends and not self.current_table and not self.current_sql:
            # Inherited sources receive their base model's exposure during model
            # inheritance resolution; retain only child-authored narrowing edits.
            private = self._deduplicate(
                [field.name for field in (*self.current_dimensions, *self.current_metrics) if not field.public]
                + [
                    field.metadata["malloy_rename_source"]
                    for field in self.current_dimensions
                    if field.metadata and field.metadata.get("malloy_rename_source")
                ]
            )
            if not (self._accept_fields or self._except_fields or private):
                return {}
            private_set = set(private)
            excluded = set(self._except_fields)
            exposure_data: dict = {
                "strict": self.strict_schema_exposure,
                "include_primary_key": True,
                "private": private,
            }
            if self._accept_fields:
                exposure_data["accept"] = [
                    name
                    for name in self._deduplicate(self._accept_fields)
                    if name not in excluded and name not in private_set
                ]
            else:
                exposure_data["except"] = [
                    name for name in self._deduplicate(self._except_fields) if name not in private_set
                ]
            return {
                "auto_dimensions": True,
                "schema_exposure": SchemaExposure.model_validate(exposure_data),
            }

        if self._virtual or not (self.current_table or self.current_sql):
            source_kind = "virtual source" if self._virtual else "source expression"
            issue = (
                f"source '{self.current_model_name}' uses a {source_kind} whose physical schema "
                "cannot be safely introspected by the Malloy adapter"
            )
            if self.strict_schema_exposure:
                raise MalloySchemaExposureError(issue)
            self.unsupported_features.append(issue)
            return {}

        private = self._deduplicate(
            [field.name for field in (*self.current_dimensions, *self.current_metrics) if not field.public]
            + [
                field.metadata["malloy_rename_source"]
                for field in self.current_dimensions
                if field.metadata and field.metadata.get("malloy_rename_source")
            ]
        )
        private_set = set(private)
        excluded = set(self._except_fields)

        exposure_data: dict = {
            "strict": self.strict_schema_exposure,
            "include_primary_key": True,
            "private": private,
        }
        if self._accept_fields:
            # Malloy permits field edits to compose. Collapse accept+except into
            # one exact allowlist because SchemaExposure deliberately rejects
            # ambiguous overlapping controls.
            exposure_data["accept"] = [
                name
                for name in self._deduplicate(self._accept_fields)
                if name not in excluded and name not in private_set
            ]
        else:
            exposure_data["except"] = [
                name for name in self._deduplicate(self._except_fields) if name not in private_set
            ]

        return {
            "auto_dimensions": True,
            "schema_exposure": SchemaExposure.model_validate(exposure_data),
        }

    def _record_unsupported(self, issue: str, *, fail_strict: bool = False) -> None:
        """Record an unsupported Malloy shape, rejecting unsafe strict imports."""
        if fail_strict and self.strict_schema_exposure:
            raise MalloySchemaExposureError(issue)
        self.unsupported_features.append(issue)

    def _reject_source_shape(self, ctx, feature: str, detail: str) -> None:
        """Reject a source expression before any lossy partial model is emitted."""
        self._source_invalid = True
        self.source_diagnostics.append((feature, detail, self._location(ctx).display))
        self._record_unsupported(detail, fail_strict=True)

    def _unsupported_source_shape(self, ctx) -> tuple[str, str] | None:
        """Return the first source shape that cannot be represented faithfully.

        This walks the complete source-expression subtree before extraction begins,
        so wrappers such as ``extend`` and parentheses cannot leave a processed base
        source behind when an unsupported descendant is encountered.
        """
        source_name = self.current_model_name
        if isinstance(ctx, MalloyParser.SQArrowContext):
            return (
                "malloy_source_pipeline_rejected",
                f"source '{source_name}' uses a query pipeline; source-definition pipelines cannot be "
                "represented faithfully",
            )
        if isinstance(ctx, MalloyParser.SQComposeContext):
            return (
                "malloy_source_compose_rejected",
                f"source '{source_name}' uses compose(...); composite-source semantics cannot be represented faithfully",
            )
        if isinstance(ctx, MalloyParser.SQIDContext) and ctx.sourceArguments() is not None:
            source_id = self._get_text(ctx.id_()).strip("`").lower()
            if source_id == "from":
                return (
                    "malloy_source_from_query_rejected",
                    f"source '{source_name}' is defined from a query; query-backed sources cannot be represented "
                    "faithfully",
                )
            return (
                "malloy_source_arguments_rejected",
                f"source '{source_name}' invokes a source with arguments; parameter substitution and "
                "source-from-query semantics cannot be represented faithfully",
            )
        if isinstance(ctx, MalloyParser.SQSQLContext):
            sql_source = ctx.sqlSource()
            sql_string = sql_source.sqlString() if sql_source else None
            if sql_string is not None and sql_string.sqlInterpolation():
                return (
                    "malloy_sql_interpolation_rejected",
                    f"source '{source_name}' contains SQL interpolation; interpolated SQL cannot be retained safely",
                )

        get_children = getattr(ctx, "getChildren", None)
        if get_children is None:
            return None
        for child in get_children():
            unsupported = self._unsupported_source_shape(child)
            if unsupported is not None:
                return unsupported
        return None

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
            import { alias1 is source1 } from 'file.malloy' # Aliased imports
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
            compatibility_items = []
            items = []
            for import_item in import_select.importItem():
                # Grammar uses id_() method (underscore to avoid Python keyword)
                ids = import_item.id_()
                if ids:
                    # Malloy names the local binding first and the exported source
                    # second: ``import { local_name is exported_name }``.
                    local_name = self._get_text(ids[0])
                    exported_name = self._get_text(ids[1]) if len(ids) > 1 else local_name
                    compatibility_items.append((local_name, exported_name if exported_name != local_name else None))
                    items.append(MalloyImportItem(local_name=local_name, exported_name=exported_name))
            self.imports.append((file_path, compatibility_items))
            resolved_items: tuple[MalloyImportItem, ...] | None = tuple(items)
        else:
            # Import all sources from file
            self.imports.append((file_path, None))
            resolved_items = None

        self.statements.append(
            MalloyImportStatement(
                specifier=file_path,
                items=resolved_items,
                location=self._location(ctx),
            )
        )

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
        self.statements.append(
            MalloyExportStatement(
                names=tuple(self._get_text(item.id_()) for item in ctx.exportItem()), location=self._location(ctx)
            )
        )
        return self.visitChildren(ctx)

    def _location(self, ctx) -> MalloyLocation:
        token = getattr(ctx, "start", None)
        return MalloyLocation(
            path=self.source_path,
            line=getattr(token, "line", 1),
            column=getattr(token, "column", 0),
        )

    def visitDefineUserTypeStatement(self, ctx: MalloyParser.DefineUserTypeStatementContext):  # noqa: N802
        """Visit top-level `type: name is <type>` user-defined type statement.

        Malloy added user-defined types so a type can be named once and reused (in
        source type constraints, casts, parameters, etc.). We record name -> definition
        text; types are metadata for the semantic layer, not models.
        """
        prop_list = ctx.userTypePropertyList()
        if not prop_list:
            return self.visitChildren(ctx)
        values = []
        for type_def in prop_list.userTypeDefinition():
            name_def = type_def.userTypeNameDef()
            type_expr = type_def.userTypeExpr()
            if name_def:
                name = self._get_text(name_def)
                definition = self._get_text(type_expr) if type_expr else ""
                if name:
                    self.user_types[name] = definition
                    values.append((name, definition))
        if values:
            self.statements.append(MalloyNamedStatement("type", tuple(values), self._location(ctx)))
        return self.visitChildren(ctx)

    def visitDefineGivenStatement(self, ctx: MalloyParser.DefineGivenStatementContext):  # noqa: N802
        """Visit top-level `given: name::type` statement.

        Malloy added `given:` to declare model-level input parameters (similar to source
        parameters but file-scoped). We record name -> type text.
        """
        given_list = ctx.givenDefList()
        if not given_list:
            return self.visitChildren(ctx)
        values = []
        for given_def in given_list.givenDef():
            name_def = given_def.givenNameDef()
            given_type = given_def.givenType()
            if name_def:
                name = self._get_text(name_def)
                type_text = self._get_text(given_type) if given_type else ""
                if name:
                    self.given[name] = type_text
                    values.append((name, type_text))
        if values:
            self.statements.append(MalloyNamedStatement("given", tuple(values), self._location(ctx)))
        return self.visitChildren(ctx)

    def visitUse_top_level_query_defs(self, ctx):  # noqa: N802
        """Retain query names for module visibility while execution stays unsupported."""
        values = []
        query_defs = ctx.topLevelQueryDefs()
        if query_defs:
            for query_def in query_defs.topLevelQueryDef():
                query_name = query_def.queryName()
                if query_name:
                    name = self._get_text(query_name)
                    if name:
                        values.append((name, self._get_text(query_def)))
        if values:
            self.statements.append(MalloyNamedStatement("query", tuple(values), self._location(ctx)))
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
        comment_protected, _ = _protect_malloy_comments(sql or "")
        syntax_sql = _mask_malloy_quoted_text(comment_protected)
        sql_lower = syntax_sql.lower()
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
        if re.search(r"[<>=!]+\s*\S", syntax_sql):
            # Check if it's a simple comparison (boolean result)
            # But not if it's part of a CASE/pick statement
            if "pick" not in sql_lower and "case" not in sql_lower:
                return "boolean"

        # SQL DATE/TIMESTAMP literals (e.g. from a Malloy @2024-01-01 literal) are
        # time values. Check before numeric so the hyphens in the date are not
        # read as subtraction. After boolean so a comparison stays boolean.
        if re.search(r"\b(?:date|timestamp)\s+'", sql_lower):
            return "time"

        # Duration arithmetic (created_at + 1 day, ts - 7 days) yields a time
        # value. Check before numeric so the operator is not read as numeric.
        if re.search(r"\b\d+\s+(?:second|minute|hour|day|week|month|quarter|year)s?\b", sql_lower):
            return "time"
        if re.search(r"\binterval\s+'?\d+'?\s+(?:second|minute|hour|day)\b", sql_lower):
            return "time"

        # Numeric detection
        if re.search(r"[+\-*/]", syntax_sql) and "||" not in syntax_sql:
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
            elif not ch.isspace():  # ignore all whitespace (newlines/tabs) before the operator
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
        # Standard function forms count(x) / count_distinct(x) / count() first,
        # then dot-method aggregates; both scan with quote/paren/backtick
        # awareness so literals and backtick identifiers are preserved.
        return _normalize_dot_aggregates(_normalize_count_calls(expr))

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

        expr, comment_replacements = _protect_malloy_comments(expr)

        # ?? null coalescing -> COALESCE
        if "??" in expr:
            expr = self._transform_null_coalesce(expr)

        # ! type assertion: func!type(args) -> func(args)
        # Matches: timestamp_seconds!timestamp(x), left!(s,1), md5!(x), to_base64!(x)
        # Pattern: identifier!identifier( -> identifier(
        expr = _sub_outside_quotes(
            expr,
            r"(\w+)\s*!\s*(?:\w+\s*)?\(",
            r"\1(",
            quotes=("'", '"', "`"),
        )

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

        expr = _sub_outside_quotes(
            expr,
            r"@(\d{4}-\d{2}-\d{2})[ T](\d{2}(?::\d{2}(?::\d{2}(?:[.,]\d+)?)?)?)(?:\[[^\]]+\])?",
            _timestamp_literal,
            quotes=("'", '"', "`"),
        )
        expr = _sub_outside_quotes(expr, r"@(\d{4}-\d{2}-\d{2})", r"DATE '\1'", quotes=("'", '"', "`"))
        expr = _sub_outside_quotes(expr, r"@(\d{4}-\d{2})(?!\d)", r"DATE '\1-01'", quotes=("'", '"', "`"))
        expr = _sub_outside_quotes(expr, r"@(\d{4})(?![-\d])", r"DATE '\1-01-01'", quotes=("'", '"', "`"))

        # now -> CURRENT_TIMESTAMP (only when it's the entire expression or clearly standalone)
        if expr.strip() == "now":
            expr = "CURRENT_TIMESTAMP"

        # & (and-tree): expands partial conditions with the base field
        # e.g., "field < 2031 & > -8000" -> "field < 2031 AND field > -8000"
        # e.g., "status != 'Cancelled' & 'Returned'" -> "status != 'Cancelled' AND status != 'Returned'"
        and_parts = self._split_top_level_operator(expr, "&")
        apply_parts = self._split_top_level_operator(expr, "?")
        if len(and_parts) > 1 and len(apply_parts) == 1:
            expr = self._transform_and_tree(expr)

        # | (or-tree / alternatives): used with ? apply operator
        # e.g., "field ? 'a' | 'b'" -> "field IN ('a', 'b')"
        # Only transform the simple value-matching pattern, not general uses of |
        value_parts = self._split_top_level_operator(apply_parts[1], "|") if len(apply_parts) == 2 else []
        has_pick_syntax = any(keyword == "pick" for _, _, keyword in self._scan_keywords(expr, ("pick",)))
        if len(apply_parts) == 2 and len(value_parts) > 1 and not has_pick_syntax:
            expr = self._transform_or_tree(expr)

        for sentinel, comment in comment_replacements:
            expr = expr.replace(sentinel, comment)
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
            # Whole-word keyword handling. `case`/`end` bracket a CASE expression
            # and behave like parentheses so the whole `case ... end` is one
            # operand; `and`/`or`/`not`/`when`/`then`/`else` bound the operand on
            # the left (the pick -> CASE rewrite runs first, so `WHEN x ~ r'...'`
            # must stop the operand at `WHEN`).
            matched_kw = None
            for kw in ("case", "end", "and", "or", "not", "when", "then", "else"):
                j = i + len(kw)
                if (
                    s[i:j].lower() == kw
                    and (i == 0 or not (s[i - 1].isalnum() or s[i - 1] == "_"))
                    and (j >= end or not (s[j].isalnum() or s[j] == "_"))
                ):
                    matched_kw = (kw, j)
                    break
            if matched_kw is not None:
                kw, j = matched_kw
                if kw == "case":
                    stack.append(start)
                    start = i
                elif kw == "end":
                    if stack:
                        start = stack.pop()
                else:
                    start = j
                i = j
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
        prefix_pattern = re.compile(r"(!?~)\s+r(?=['\"])")
        matches: list[tuple[re.Match, int, str]] = []
        for match in prefix_pattern.finditer(expr):
            if not _outside_quotes(expr, match.start()):
                continue
            quote_pos = match.end()
            quote = expr[quote_pos]
            close = quote_pos + 1
            while close < len(expr):
                if expr[close] == "\\":
                    close += 2
                    continue
                if expr[close] == quote:
                    matches.append((match, close + 1, expr[quote_pos + 1 : close]))
                    break
                close += 1

        for match, match_end, raw_pattern in reversed(matches):
            end = match.start()
            while end > 0 and expr[end - 1] == " ":
                end -= 1
            operand_start = self._left_operand_start(expr, end)
            if operand_start is None:
                continue
            operand = expr[operand_start:end]
            sql_pattern = raw_pattern.replace("'", "''")
            replacement = f"REGEXP_MATCHES({operand}, '{sql_pattern}')"
            if match.group(1) == "!~":
                replacement = f"NOT {replacement}"
            expr = expr[:operand_start] + replacement + expr[match_end:]
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

    @classmethod
    def _split_top_level_operator(cls, expr: str, operator: str) -> list[str]:
        """Split on a single Malloy operator token regardless of whitespace."""
        parts: list[str] = []
        start = 0
        quote: str | None = None
        depth = 0
        i = 0
        while i < len(expr):
            char = expr[i]
            if quote is not None:
                if char == "\\" and i + 1 < len(expr):
                    i += 2
                    continue
                if char == quote:
                    quote = None
                i += 1
                continue
            if char in ("'", '"', "`"):
                quote = char
            elif char in "([{":
                depth += 1
            elif char in ")]}":
                depth -= 1
            elif (
                depth == 0
                and char == operator
                and (i == 0 or expr[i - 1] != operator)
                and (i + 1 == len(expr) or expr[i + 1] != operator)
            ):
                parts.append(expr[start:i])
                start = i + 1
            i += 1
        parts.append(expr[start:])
        return parts

    def _transform_and_tree(self, expr: str) -> str:
        """Transform Malloy & (and-tree) to SQL AND with expanded base field.

        Examples:
        - "field < 2031 & > -8000" -> "field < 2031 AND field > -8000"
        - "status != 'Cancelled' & 'Returned'" -> "status != 'Cancelled' AND status != 'Returned'"

        Only splits on a top-level `&` (not one inside a string literal such as
        "label = 'A & B'").
        """
        parts = [p.strip() for p in self._split_top_level_operator(expr, "&")]
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
        apply_parts = self._split_top_level_operator(expr, "?")
        if len(apply_parts) != 2:
            return expr

        base_field = apply_parts[0].strip()
        values_str = apply_parts[1].strip()

        # Split on | and collect values
        values = [value.strip() for value in self._split_top_level_operator(values_str, "|")]
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
        model_start = len(self.models)
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

                if self.current_model_name and not self._source_invalid:
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
                    if self._accept_fields:
                        metadata["malloy_accept"] = list(self._accept_fields)
                    if self._except_fields:
                        metadata["malloy_except"] = list(self._except_fields)
                    self._apply_explicit_field_visibility()
                    source_scalar_data = {
                        field: value
                        for field, value in {
                            "table": self.current_table,
                            "sql": self.current_sql,
                            "extends": self.current_extends,
                            "primary_key": self.current_primary_key,
                            "description": self.current_description,
                            "metadata": metadata if metadata else None,
                        }.items()
                        if value is not None
                    }
                    model = Model(
                        name=self.current_model_name,
                        dimensions=self.current_dimensions,
                        metrics=self.current_metrics,
                        relationships=self.current_relationships,
                        segments=self.current_segments,
                        invariant_filters=self.current_invariant_filters,
                        **self._schema_exposure_options(),
                        **source_scalar_data,
                    )
                    self.models.append(model)

        statement_models = tuple(self.models[model_start:])
        if statement_models:
            self.statements.append(MalloySourceStatement(models=statement_models, location=self._location(ctx)))

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

        # ``from(...)`` is a reserved-token spelling in the bundled grammar and
        # may arrive through ANTLR error recovery rather than SQIDContext. Detect
        # that recovered source definition before its inner query can be mistaken
        # for a physical source.
        compact_definition = re.sub(r"\s+", "", self._get_text(ctx)).lower()
        source_from_query = name_def is not None and f"{self._get_text(name_def).lower()}isfrom(" in compact_definition
        if source_from_query:
            self._reject_source_shape(
                ctx,
                "malloy_source_from_query_rejected",
                f"source '{self.current_model_name}' is defined from a query; query-backed sources cannot be "
                "represented faithfully",
            )
            return

        if ctx.sourceParameters() is not None:
            self._reject_source_shape(
                ctx.sourceParameters(),
                "malloy_source_parameters_rejected",
                f"source '{self.current_model_name}' declares parameters; parameterized sources cannot be "
                "represented faithfully",
            )
            return

        # Process the source expression (sqExplore -> sqExpr)
        sq_explore = ctx.sqExplore()
        if sq_explore:
            sq_expr = sq_explore.sqExpr()
            self._process_sq_expr(sq_expr)

    def _process_sq_expr(self, ctx: MalloyParser.SqExprContext):
        """Process source expression - table, sql, or extended source."""
        if ctx is None:
            return

        unsupported = self._unsupported_source_shape(ctx)
        if unsupported is not None:
            self._reject_source_shape(ctx, *unsupported)
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
            if ctx.includeBlock():
                self._record_unsupported(
                    f"source '{self.current_model_name}' uses an include block; inherited-field selection and "
                    "include access modifiers cannot be represented without resolved source schemas",
                    fail_strict=True,
                )

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
            self._record_unsupported(
                f"source '{self.current_model_name}' uses an include block; inherited-field selection and "
                "include access modifiers cannot be represented without resolved source schemas",
                fail_strict=True,
            )
            return

        # Check for ID reference (another source name) -> set extends
        if isinstance(ctx, MalloyParser.SQIDContext):
            id_ctx = ctx.id_()
            if id_ctx:
                self.current_extends = self._get_text(id_ctx)
            return

        # Unsupported source shapes are rejected by the whole-subtree preflight
        # above. Keep these guards fail-closed if the traversal changes later.
        if isinstance(ctx, MalloyParser.SQArrowContext):
            self._reject_source_shape(
                ctx,
                "malloy_source_pipeline_rejected",
                f"source '{self.current_model_name}' uses a query pipeline; source-definition pipelines cannot be "
                "represented faithfully",
            )
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
            self._reject_source_shape(
                ctx,
                "malloy_source_compose_rejected",
                f"source '{self.current_model_name}' uses compose(...); composite-source semantics cannot be "
                "represented faithfully",
            )
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
                    self._process_source_where(where_stmt)
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
                self._process_source_where(where_stmt)
            return

        # Accept/except field visibility
        if isinstance(ctx, MalloyParser.DefExploreEditFieldContext):
            field_list = ctx.fieldNameList()
            field_names = [self._get_text(field).strip().strip("`") for field in field_list.fieldName()]
            if ctx.EXCEPT():
                self._except_fields.extend(field_names)
            else:
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
            access = self._access_label(ctx)
            rename_list = ctx.renameList()
            if rename_list:
                for rename_entry in rename_list.renameEntry():
                    field_names = rename_entry.fieldName()
                    if field_names and len(field_names) >= 2:
                        new_name = self._get_text(field_names[0])
                        old_name = self._get_text(field_names[1])
                        dim_type = self._infer_dimension_type(old_name, new_name)
                        metadata = {"malloy_rename_source": old_name}
                        if access != "public":
                            metadata["malloy_access"] = access
                        if access == "internal":
                            self.unsupported_features.append(
                                f"internal renamed dimension '{new_name}' is enforced as non-public; "
                                "Sidemantic cannot distinguish Malloy internal from private field access"
                            )
                        self.current_dimensions.append(
                            Dimension(
                                name=new_name,
                                sql=old_name,
                                type=dim_type,
                                metadata=metadata,
                                public=access == "public",
                            )
                        )
            return

    @staticmethod
    def _access_label(ctx) -> str:
        access = ctx.accessLabel() if hasattr(ctx, "accessLabel") else None
        return access.getText().strip().lower() if access else "public"

    def _process_def_dimensions(self, ctx: MalloyParser.DefDimensionsContext):
        """Process dimension: statements."""
        def_list = ctx.defList()
        if not def_list:
            return

        access = self._access_label(ctx)
        for field_def in def_list.fieldDef():
            self._process_dimension_def(field_def, access=access)

    def _process_dimension_def(self, ctx: MalloyParser.FieldDefContext, access: str = "public"):
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
        if access != "public":
            dim_metadata = dict(dim_metadata or {})
            dim_metadata["malloy_access"] = access
            if access == "internal":
                self.unsupported_features.append(
                    f"internal dimension '{name}' is enforced as non-public; Sidemantic cannot distinguish "
                    "Malloy internal from private field access"
                )

        # Get the expression
        field_expr = ctx.fieldExpr()
        if field_expr is None:
            sql = name
        else:
            sql = self._lower_scalar_field_expression(field_expr, name, "dimension")
            if sql is None:
                return

        # Legacy pick/when is deliberately outside the typed subset. Keep this
        # code reachable only for already-lowered SQL for backward-compatible
        # inference; raw Malloy text never enters it.
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

            sql = self._transform_malloy_expr(sql)

        # Infer type
        dim_type = self._infer_dimension_type(sql, name)
        if field_expr is not None and any(
            isinstance(node, MalloyParser.ExprDurationContext) for node in self._walk_expression_context(field_expr)
        ):
            dim_type = "time"

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
                public=access == "public",
            )
        )

    def _process_def_measures(self, ctx: MalloyParser.DefMeasuresContext):
        """Process measure: statements."""
        def_list = ctx.defList()
        if not def_list:
            return

        access = self._access_label(ctx)
        for field_def in def_list.fieldDef():
            self._process_measure_def(field_def, access=access)

    def _expression_dialect(self) -> str | None:
        connection = self.current_connection or "duckdb"
        return self.connection_dialects.get(connection)

    def _field_semantic_type(self, path: str) -> str:
        name = path.rsplit(".", 1)[-1].strip("`")
        dimension = next((item for item in self.current_dimensions if item.name == name), None)
        if dimension is not None:
            return {
                "number": "number",
                "boolean": "boolean",
                "string": "string",
                "date": "date",
                "time": "timestamp",
            }.get(dimension.type, "unknown")
        if any(item.name == name for item in self.current_metrics):
            return "number"
        return "unknown"

    @staticmethod
    def _walk_expression_context(ctx):
        yield ctx
        for child in ctx.getChildren():
            if hasattr(child, "getChildren"):
                yield from MalloyModelVisitor._walk_expression_context(child)

    def _typed_scalar_expression(self, ctx, builder: MalloyExpressionBuilder):
        if isinstance(ctx, MalloyParser.ExprExprContext):
            return self._typed_scalar_expression(ctx.fieldExpr(), builder)
        if isinstance(ctx, MalloyParser.ExprFieldPathContext):
            parts = tuple(self._get_text(part).strip("`") for part in ctx.fieldPath().fieldName())
            path = ".".join(parts)
            return builder.field(*parts, semantic_type=self._field_semantic_type(path))
        if isinstance(ctx, MalloyParser.ExprLiteralContext):
            literal = ctx.literal()
            text = self._get_text(literal)
            if isinstance(literal, MalloyParser.ExprNULLContext):
                return builder.literal(None)
            if isinstance(literal, MalloyParser.ExprBoolContext):
                return builder.literal(text.lower() == "true")
            if isinstance(literal, MalloyParser.ExprNumberContext):
                return builder.literal(Decimal(text))
            if isinstance(literal, MalloyParser.ExprStringContext):
                return builder.literal(self._extract_string(text))
            raise _UnsupportedTypedShapeError(f"Unsupported Malloy literal {text!r}")
        binary_contexts = (
            MalloyParser.ExprAddSubContext,
            MalloyParser.ExprMulDivContext,
            MalloyParser.ExprCompareContext,
            MalloyParser.ExprLogicalAndContext,
            MalloyParser.ExprLogicalOrContext,
        )
        if isinstance(ctx, binary_contexts):
            operands = ctx.fieldExpr()
            if len(operands) != 2:
                raise ValueError("Binary expression must have two operands")
            if isinstance(ctx, MalloyParser.ExprAddSubContext) and isinstance(
                operands[1], MalloyParser.ExprDurationContext
            ):
                duration_text = self._get_text(operands[1].fieldExpr())
                if not re.fullmatch(r"\d+", duration_text):
                    raise ValueError("duration amount must be a non-negative integer")
                return builder.duration(
                    self._typed_scalar_expression(operands[0], builder),
                    "+" if ctx.PLUS() else "-",
                    int(duration_text),
                    self._get_text(operands[1].timeframe()),
                )
            if isinstance(ctx, MalloyParser.ExprCompareContext):
                operator = self._get_text(ctx.compareOp())
                if operator in {"~", "!~"}:
                    right = operands[1]
                    literal = right.literal() if isinstance(right, MalloyParser.ExprLiteralContext) else None
                    if not isinstance(literal, MalloyParser.ExprRegexContext):
                        raise ValueError("regex comparison requires a regex literal on the right")
                    pattern = self._get_text(literal)
                    if not pattern.startswith("r"):
                        raise ValueError("regex literal is malformed")
                    return builder.regex(
                        self._typed_scalar_expression(operands[0], builder),
                        self._extract_string(pattern[1:]),
                        negated=operator == "!~",
                    )
                if operator not in {"=", "!=", "<>", "<", "<=", ">", ">="}:
                    raise _UnsupportedTypedShapeError(f"Unsupported typed comparison {operator!r}")
            elif isinstance(ctx, MalloyParser.ExprLogicalAndContext):
                operator = "and"
            elif isinstance(ctx, MalloyParser.ExprLogicalOrContext):
                operator = "or"
            elif getattr(ctx, "PLUS", lambda: None)():
                operator = "+"
            elif getattr(ctx, "MINUS", lambda: None)():
                operator = "-"
            elif getattr(ctx, "STAR", lambda: None)():
                operator = "*"
            elif getattr(ctx, "SLASH", lambda: None)():
                operator = "/"
            else:
                operator = "%"
            return builder.binary(
                self._typed_scalar_expression(operands[0], builder),
                operator,
                self._typed_scalar_expression(operands[1], builder),
            )
        if isinstance(ctx, MalloyParser.ExprMinusContext):
            return builder.unary("-", self._typed_scalar_expression(ctx.fieldExpr(), builder))
        if isinstance(ctx, MalloyParser.ExprNotContext):
            return builder.unary("not", self._typed_scalar_expression(ctx.fieldExpr(), builder))
        if isinstance(ctx, MalloyParser.ExprNullCheckContext):
            return builder.binary(
                self._typed_scalar_expression(ctx.fieldExpr(), builder),
                "!=" if ctx.NOT() else "=",
                builder.literal(None),
            )
        if isinstance(ctx, MalloyParser.ExprTimeTruncContext):
            return builder.date_trunc(
                self._get_text(ctx.timeframe()), self._typed_scalar_expression(ctx.fieldExpr(), builder)
            )
        if isinstance(ctx, MalloyParser.ExprCastContext):
            target = self._get_text(ctx.malloyOrSQLType()).lower()
            target = {
                "bool": "boolean",
                "number": "decimal",
                "string": "text",
                "timestamptz": "timestamp",
            }.get(target, target)
            return builder.cast(self._typed_scalar_expression(ctx.fieldExpr(), builder), target)
        if isinstance(ctx, MalloyParser.ExprCoalesceContext):
            if any(isinstance(item, MalloyParser.ExprCoalesceContext) for item in ctx.fieldExpr()):
                raise _UnsupportedTypedShapeError("coalesce chain uses legacy flattening")
            return builder.coalesce(*(self._typed_scalar_expression(item, builder) for item in ctx.fieldExpr()))
        if isinstance(ctx, MalloyParser.ExprFuncContext):
            args = ctx.argumentList()
            expressions = args.fieldExpr() if args is not None else []
            if self._get_text(ctx.id_()).lower() == "date_trunc" and len(expressions) == 2:
                unit = self._get_text(expressions[0]).strip("'\"")
                return builder.date_trunc(unit, self._typed_scalar_expression(expressions[1], builder))
            return builder.function(
                self._get_text(ctx.id_()), *(self._typed_scalar_expression(item, builder) for item in expressions)
            )
        if isinstance(
            ctx,
            (
                MalloyParser.ExprPickContext,
                MalloyParser.ExprApplyContext,
                MalloyParser.ExprAndTreeContext,
                MalloyParser.ExprOrTreeContext,
                MalloyParser.ExprCaseContext,
            ),
        ):
            raise _UnsupportedTypedShapeError(type(ctx).__name__)
        raise ValueError(f"Unsupported Malloy scalar expression {type(ctx).__name__}")

    def _validate_legacy_expression_tree(self, ctx, *, allow_aggregates: bool = False) -> None:
        """Reject unsafe descendants before an exact legacy root transform."""
        rejected = (
            MalloyParser.ExprGivenRefContext,
            MalloyParser.ExprInGivenContext,
            MalloyParser.ExprSafeCastContext,
            MalloyParser.ExprLiteralRecordContext,
            MalloyParser.ExprRangeContext,
            MalloyParser.ExprForRangeContext,
        )
        aggregate_nodes = (
            MalloyParser.ExprAggregateContext,
            MalloyParser.ExprAggFuncContext,
            MalloyParser.ExprPathlessAggregateContext,
            MalloyParser.ExprUngroupContext,
        )
        scalar_functions = {
            "abs",
            "ceil",
            "coalesce",
            "concat",
            "date_trunc",
            "exp",
            "floor",
            "greatest",
            "least",
            "length",
            "ln",
            "lower",
            "nullif",
            "power",
            "replace",
            "round",
            "sqrt",
            "substring",
            "trim",
            "upper",
        }
        aggregate_functions = {"avg", "average", "count", "count_distinct", "max", "min", "sum"}
        allowed_field_nodes = (
            MalloyParser.ExprExprContext,
            MalloyParser.ExprFieldPathContext,
            MalloyParser.ExprLiteralContext,
            MalloyParser.ExprMinusContext,
            MalloyParser.ExprAddSubContext,
            MalloyParser.ExprNullCheckContext,
            MalloyParser.ExprLogicalOrContext,
            MalloyParser.ExprCompareContext,
            MalloyParser.ExprFuncContext,
            MalloyParser.ExprCastContext,
            MalloyParser.ExprTimeTruncContext,
            MalloyParser.ExprLogicalAndContext,
            MalloyParser.ExprMulDivContext,
            MalloyParser.ExprNotContext,
            MalloyParser.ExprDurationContext,
            MalloyParser.ExprApplyContext,
            MalloyParser.ExprAndTreeContext,
            MalloyParser.ExprOrTreeContext,
            MalloyParser.ExprPickContext,
            MalloyParser.ExprCaseContext,
            MalloyParser.ExprCoalesceContext,
        )
        for node in self._walk_expression_context(ctx):
            if isinstance(node, rejected):
                raise ValueError(f"unsupported descendant {type(node).__name__}")
            if isinstance(node, aggregate_nodes) and not allow_aggregates:
                raise ValueError(f"aggregate descendant {type(node).__name__} is invalid in a scalar expression")
            if isinstance(node, MalloyParser.FieldExprContext) and not isinstance(
                node, allowed_field_nodes + (aggregate_nodes if allow_aggregates else ())
            ):
                raise ValueError(f"unsupported descendant {type(node).__name__}")
            if isinstance(node, MalloyParser.ExprFuncContext):
                function = self._get_text(node.id_()).lower()
                allowed = scalar_functions | (aggregate_functions if allow_aggregates else set())
                if function not in allowed:
                    raise ValueError(f"function {function!r} is not safe for legacy lowering")

    def _requires_resolved_expression_dialect(self, ctx) -> bool:
        sensitive = (
            MalloyParser.ExprArrayLiteralContext,
            MalloyParser.ExprCastContext,
            MalloyParser.ExprDurationContext,
            MalloyParser.ExprTimeTruncContext,
        )
        for node in self._walk_expression_context(ctx):
            if isinstance(node, sensitive):
                return True
            if isinstance(node, MalloyParser.ExprCompareContext) and self._get_text(node.compareOp()) in {"~", "!~"}:
                return True
            if isinstance(node, MalloyParser.ExprFuncContext) and self._get_text(node.id_()).lower() == "date_trunc":
                return True
        return False

    def _lower_scalar_field_expression(self, ctx, name: str, kind: str) -> str | None:
        try:
            # DuckDB regex dimensions are an established exact legacy path whose
            # spelling and quoting are part of the adapter's export contract.
            # Filters still use the typed builder so connection dialects affect
            # their executable predicate syntax.
            if (
                kind == "dimension"
                and self._expression_dialect() == "duckdb"
                and any(
                    isinstance(node, MalloyParser.ExprCompareContext)
                    and self._get_text(node.compareOp()) in {"~", "!~"}
                    for node in self._walk_expression_context(ctx)
                )
            ):
                self._validate_legacy_expression_tree(ctx)
                return self._transform_malloy_expr(self._get_text(ctx))
            dialect = self._expression_dialect()
            if dialect is None and self._requires_resolved_expression_dialect(ctx):
                raise ValueError(
                    f"connection '{self.current_connection}' has no resolved SQL dialect for this expression"
                )
            builder = MalloyExpressionBuilder(dialect=dialect)
            return builder.lower(self._typed_scalar_expression(ctx, builder)).sql
        except _UnsupportedTypedShapeError:
            try:
                self._validate_legacy_expression_tree(ctx)
            except ValueError as exc:
                self.expression_diagnostics.append(
                    ("malloy_expression_legacy_descendant", str(exc), self._location(ctx).display)
                )
                self._record_unsupported(f"{kind} '{name}' contains a rejected descendant: {exc}", fail_strict=True)
                return None
            return self._transform_malloy_expr(self._get_text(ctx))
        except (MalloyExpressionLoweringError, ValueError) as exc:
            feature = exc.diagnostic.feature if isinstance(exc, MalloyExpressionLoweringError) else "expression_shape"
            detail = exc.diagnostic.detail if isinstance(exc, MalloyExpressionLoweringError) else str(exc)
            self.expression_diagnostics.append((f"malloy_expression_{feature}", detail, self._location(ctx).display))
            self._record_unsupported(
                f"{kind} '{name}' uses an expression that cannot be lowered safely: {exc}", fail_strict=True
            )
            return None

    def _process_measure_def(self, ctx: MalloyParser.FieldDefContext, access: str = "public"):
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
        filter_contexts: list = []
        scalar_node = field_expr
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
                                    expressions = list(filter_list.fieldExpr())
                                    collected_filters.extend(self._get_text(f) for f in expressions)
                                    filter_contexts.extend(expressions)
                node = node.fieldExpr()

            expr_text = self._get_text(node) if node is not None else ""
            scalar_node = node
            if collected_filters:
                # Collected outermost-first; reverse to restore source order.
                filters = list(reversed(collected_filters))
                filter_contexts.reverse()

        if filter_contexts:
            lowered_filters: list[str] = []
            for filter_ctx in filter_contexts:
                lowered_filter = self._lower_scalar_field_expression(filter_ctx, name, "measure filter")
                if lowered_filter is None:
                    return
                lowered_filters.append(lowered_filter)
            filters = lowered_filters

        aggregate_contexts = (
            MalloyParser.ExprAggregateContext,
            MalloyParser.ExprAggFuncContext,
            MalloyParser.ExprPathlessAggregateContext,
            MalloyParser.ExprUngroupContext,
        )

        def has_aggregate(node) -> bool:
            if isinstance(node, aggregate_contexts):
                return True
            if isinstance(node, MalloyParser.ExprFuncContext) and self._get_text(node.id_()).lower() in {
                "avg",
                "average",
                "count",
                "count_distinct",
                "max",
                "min",
                "sum",
            }:
                return True
            return any(has_aggregate(child) for child in node.getChildren() if hasattr(child, "getChildren"))

        def lower_aggregate_tree(node) -> tuple[bool, str]:
            if isinstance(node, MalloyParser.ExprExprContext):
                handled, inner = lower_aggregate_tree(node.fieldExpr())
                return handled, f"({inner})" if handled else ""
            if isinstance(node, MalloyParser.ExprPathlessAggregateContext):
                argument = node.fieldExpr()
                lowered_argument = ""
                if argument is not None:
                    lowered = self._lower_scalar_field_expression(argument, name, "aggregate argument")
                    if lowered is None:
                        return True, ""
                    lowered_argument = lowered
                return True, f"{self._get_text(node.aggregate())}({lowered_argument})"
            if isinstance(node, MalloyParser.ExprAggregateContext):
                field_path = self._get_text(node.fieldPath())
                argument = node.fieldExpr()
                if argument is not None:
                    # Malloy permits a refinement argument on relationship-scoped
                    # aggregates. Validate and lower it even though the native
                    # metric stores the aggregate's scoped field as its SQL input.
                    if self._lower_scalar_field_expression(argument, name, "aggregate argument") is None:
                        return True, ""
                return True, f"{field_path}.{self._get_text(node.aggregate())}()"
            if isinstance(node, MalloyParser.ExprAggFuncContext):
                arguments = node.argumentList()
                lowered_arguments: list[str] = []
                for argument in arguments.fieldExpr() if arguments is not None else []:
                    lowered = self._lower_scalar_field_expression(argument, name, "aggregate argument")
                    if lowered is None:
                        return True, ""
                    lowered_arguments.append(lowered)
                return True, (
                    f"{self._get_text(node.fieldPath())}.{self._get_text(node.id_())}({', '.join(lowered_arguments)})"
                )
            if isinstance(node, MalloyParser.ExprFuncContext) and self._get_text(node.id_()).lower() in {
                "avg",
                "average",
                "count",
                "count_distinct",
                "max",
                "min",
                "sum",
            }:
                arguments = node.argumentList()
                lowered_arguments: list[str] = []
                for argument in arguments.fieldExpr() if arguments is not None else []:
                    lowered = self._lower_scalar_field_expression(argument, name, "aggregate argument")
                    if lowered is None:
                        return True, ""
                    lowered_arguments.append(lowered)
                return True, f"{self._get_text(node.id_())}({', '.join(lowered_arguments)})"
            if isinstance(node, (MalloyParser.ExprAddSubContext, MalloyParser.ExprMulDivContext)):
                operands = node.fieldExpr()
                if len(operands) != 2 or not any(has_aggregate(operand) for operand in operands):
                    return False, ""
                rendered: list[str] = []
                for operand in operands:
                    if has_aggregate(operand):
                        handled, sql = lower_aggregate_tree(operand)
                        if not handled or not sql:
                            return handled, ""
                    else:
                        sql = self._lower_scalar_field_expression(operand, name, "aggregate expression")
                        if sql is None:
                            return True, ""
                    rendered.append(sql)
                if isinstance(node, MalloyParser.ExprAddSubContext):
                    operator = "+" if node.PLUS() else "-"
                elif node.STAR():
                    operator = "*"
                elif node.SLASH():
                    operator = "/"
                else:
                    operator = "%"
                return True, f"{rendered[0]} {operator} {rendered[1]}"
            return False, ""

        if scalar_node is not None and not filters and not has_aggregate(scalar_node):
            lowered = self._lower_scalar_field_expression(scalar_node, name, "measure")
            if lowered is None:
                return
            expr_text = lowered
        else:
            # Aggregate roots retain their specialized Malloy lowering.
            if scalar_node is not None:
                try:
                    self._validate_legacy_expression_tree(scalar_node, allow_aggregates=True)
                except ValueError as exc:
                    self.expression_diagnostics.append(
                        ("malloy_expression_aggregate_descendant", str(exc), self._location(scalar_node).display)
                    )
                    self._record_unsupported(
                        f"measure '{name}' contains a rejected aggregate descendant: {exc}", fail_strict=True
                    )
                    return
            dialect_sensitive = scalar_node is not None and self._requires_resolved_expression_dialect(scalar_node)
            handled, lowered_aggregate = (
                lower_aggregate_tree(scalar_node) if dialect_sensitive and scalar_node is not None else (False, "")
            )
            if handled:
                if not lowered_aggregate:
                    return
                expr_text = lowered_aggregate
            else:
                if scalar_node is not None and self._expression_dialect() is None and dialect_sensitive:
                    self._record_unsupported(
                        f"measure '{name}' uses a dialect-sensitive aggregate expression on unresolved connection "
                        f"'{self.current_connection}'",
                        fail_strict=True,
                    )
                    return
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
        if access != "public":
            metric_metadata["malloy_access"] = access

        self.current_metrics.append(
            Metric(
                name=name,
                type=metric_type,
                agg=agg,
                sql=sql,
                filters=filters,
                description=description,
                metadata=metric_metadata if metric_metadata else None,
                visibility=access,
                public=access == "public",
            )
        )

    def _process_join_statement(self, ctx: MalloyParser.JoinStatementContext):
        """Process join_one/join_many statements."""
        access = self._access_label(ctx)
        if access != "public":
            self._record_unsupported(
                f"source '{self.current_model_name}' uses a {access} join; Relationship has no field-access "
                "boundary, so the join is omitted rather than imported as public",
                fail_strict=True,
            )
            return

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

        # Check if there's an isExplore (alias is source with inline definition)
        # Only extract inline sources that define a table/sql, not simple ID references
        is_explore = join_from.isExplore()
        target_name = name
        aliased_reference = False
        if is_explore:
            sq_expr = is_explore.sqExpr()
            if sq_expr and not isinstance(sq_expr, MalloyParser.SQIDContext):
                if not self._extract_inline_join_source(name, sq_expr):
                    return
            elif sq_expr:
                target_name = self._get_text(sq_expr)
                aliased_reference = target_name != name

        join_metadata = None
        matrix_op = ctx.matrixOperation() if hasattr(ctx, "matrixOperation") else None
        if matrix_op:
            direction = self._get_text(matrix_op).lower()
            if direction != "left":
                self._record_unsupported(
                    f"source '{self.current_model_name}' join '{name}' uses unsupported {direction} join direction",
                    fail_strict=True,
                )
                return
            join_metadata = {"join_direction": direction}

        if rel_type == "cross":
            has_condition = isinstance(ctx, MalloyParser.JoinWithContext) or (
                isinstance(ctx, MalloyParser.JoinOnContext) and ctx.joinExpression() is not None
            )
            if has_condition or matrix_op:
                self._record_unsupported(
                    f"source '{self.current_model_name}' join_cross '{name}' must be an unconditional bare cross join",
                    fail_strict=True,
                )
                return
            self.current_relationships.append(
                Relationship(
                    name=name,
                    target_model=target_name if aliased_reference else None,
                    type="cross",
                )
            )
            return

        if isinstance(ctx, MalloyParser.JoinWithContext):
            field_expr = ctx.fieldExpr()
            field_text = self._get_text(field_expr) if field_expr else ""
            with_column = self._physical_with_column(field_text)
            if with_column is None:
                self._record_unsupported(
                    f"source '{self.current_model_name}' join '{name}' with clause must name one physical column",
                    fail_strict=True,
                )
                return
            join_metadata = join_metadata or {}
            join_metadata["malloy_with"] = with_column
            if rel_type == "many_to_one":
                foreign_key = with_column
                primary_key = None  # resolved against the target source after module binding
            else:
                foreign_key = with_column
                primary_key = with_column
            self.current_relationships.append(
                Relationship(
                    name=name,
                    target_model=target_name if aliased_reference else None,
                    type=rel_type,
                    foreign_key=foreign_key,
                    primary_key=primary_key,
                    metadata=join_metadata,
                )
            )
            return

        if isinstance(ctx, MalloyParser.JoinOnContext):
            join_expr = ctx.joinExpression()
            if not join_expr:
                self._record_unsupported(
                    f"source '{self.current_model_name}' join '{name}' has no join condition",
                    fail_strict=True,
                )
                return
            expr_text = self._get_text(join_expr)
            lowered = self._lower_join_condition(expr_text, name)
            if lowered is None:
                self._record_unsupported(
                    f"source '{self.current_model_name}' join '{name}' uses a condition that cannot be lowered safely",
                    fail_strict=True,
                )
                return
            source_keys, target_keys, join_sql, pure_key_equality = lowered
            if not source_keys and pure_key_equality:
                self._record_unsupported(
                    f"source '{self.current_model_name}' join '{name}' has no source-to-target equality",
                    fail_strict=True,
                )
                return
            join_metadata = join_metadata or {}
            join_metadata["on_condition"] = expr_text
            if source_keys:
                join_metadata["join_key_pairs"] = [
                    {"source": source_key, "target": target_key}
                    for source_key, target_key in zip(source_keys, target_keys, strict=True)
                ]
            foreign_keys = source_keys if rel_type == "many_to_one" else target_keys
            primary_keys = target_keys if rel_type == "many_to_one" else source_keys
            self.current_relationships.append(
                Relationship(
                    name=name,
                    target_model=target_name if aliased_reference else None,
                    type=rel_type,
                    foreign_key=self._collapse_join_keys(foreign_keys),
                    primary_key=self._collapse_join_keys(primary_keys),
                    sql=None if pure_key_equality else join_sql,
                    metadata=join_metadata,
                )
            )
            return

        self._record_unsupported(
            f"source '{self.current_model_name}' join '{name}' has an unsupported declaration shape",
            fail_strict=True,
        )

    @staticmethod
    def _collapse_join_keys(keys: list[str]) -> str | list[str] | None:
        if not keys:
            return None
        return keys[0] if len(keys) == 1 else keys

    def _physical_with_column(self, expr_text: str) -> str | None:
        """Return a conservative physical source column used by Malloy ``with``."""
        match = re.fullmatch(
            r"\s*(?:(?P<qualifier>[A-Za-z_]\w*|`[^`]+`)\s*\.\s*)?(?P<column>[A-Za-z_]\w*|`[^`]+`)\s*",
            expr_text,
        )
        if not match:
            return None
        qualifier = match.group("qualifier")
        if qualifier is not None and qualifier.strip("`") != self.current_model_name:
            return None
        return match.group("column").strip("`")

    @staticmethod
    def _strip_join_parens(expr: str) -> str:
        text = expr.strip()
        while text.startswith("(") and text.endswith(")"):
            depth = 0
            quote = None
            wraps = True
            for index, char in enumerate(text):
                if quote is not None:
                    if char == quote and (index == 0 or text[index - 1] != "\\"):
                        quote = None
                    continue
                if char in ("'", '"', "`"):
                    quote = char
                elif char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0 and index != len(text) - 1:
                        wraps = False
                        break
            if not wraps:
                break
            text = text[1:-1].strip()
        return text

    @classmethod
    def _split_join_conjunctions(cls, expr: str) -> list[str]:
        text = cls._strip_join_parens(expr)
        parts: list[str] = []
        start = 0
        depth = 0
        quote = None
        index = 0
        while index < len(text):
            char = text[index]
            if quote is not None:
                if char == "\\":
                    index += 2
                    continue
                if char == quote:
                    quote = None
                index += 1
                continue
            if char in ("'", '"', "`"):
                quote = char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0:
                match = re.match(r"(?i)and\b", text[index:])
                before_ok = index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_")
                if match and before_ok:
                    parts.append(text[start:index].strip())
                    index += match.end()
                    start = index
                    continue
            index += 1
        parts.append(text[start:].strip())
        return [part for part in parts if part]

    def _join_field_side(self, expression: str, target_name: str) -> tuple[str, str] | None:
        match = re.fullmatch(
            r"\s*(?:(?P<qualifier>[A-Za-z_]\w*|`[^`]+`)\s*\.\s*)?(?P<column>[A-Za-z_]\w*|`[^`]+`)\s*",
            self._strip_join_parens(expression),
        )
        if not match:
            return None
        qualifier = match.group("qualifier")
        qualifier = qualifier.strip("`") if qualifier else None
        column = match.group("column").strip("`")
        if qualifier is None and column.lower() in {
            "true",
            "false",
            "null",
            "current_date",
            "current_timestamp",
        }:
            return None
        if qualifier == target_name:
            return "target", column
        if qualifier is None or qualifier == self.current_model_name:
            return "source", column
        return None

    def _lower_join_condition(self, expr_text: str, target_name: str) -> tuple[list[str], list[str], str, bool] | None:
        """Lower one unaliased Malloy join predicate to exact native semantics."""
        if re.search(r"(?i)\b(?:pick|when)\b|[?&|~]", expr_text) or "::" in expr_text:
            return None

        source_keys: list[str] = []
        target_keys: list[str] = []
        pure_key_equality = True
        for conjunct in self._split_join_conjunctions(expr_text):
            comparison = re.fullmatch(r"\s*(.+?)\s*=\s*(.+?)\s*", self._strip_join_parens(conjunct))
            if comparison and not re.search(r"(?:!=|<=|>=|<>)", conjunct):
                left = self._join_field_side(comparison.group(1), target_name)
                right = self._join_field_side(comparison.group(2), target_name)
                if left and right and left[0] != right[0]:
                    source, target = (left, right) if left[0] == "source" else (right, left)
                    source_keys.append(source[1])
                    target_keys.append(target[1])
                    continue
            pure_key_equality = False

        lowered_sql = self._lower_join_sql(expr_text, target_name)
        if lowered_sql is None or "{from}" not in lowered_sql or "{to}" not in lowered_sql:
            return None
        return source_keys, target_keys, lowered_sql, pure_key_equality

    def _lower_join_sql(self, expr_text: str, target_name: str) -> str | None:
        """Convert a conservative SQL-compatible Malloy predicate to placeholders."""
        transformed = self._transform_malloy_expr(expr_text)
        if re.search(r"[;{}]", transformed) or "--" in transformed or "/*" in transformed:
            return None

        keywords = {
            "and",
            "or",
            "not",
            "is",
            "null",
            "true",
            "false",
            "like",
            "ilike",
            "in",
            "between",
            "date",
            "timestamp",
            "interval",
            "current_date",
            "current_timestamp",
        }
        token_pattern = re.compile(r"(?:(?P<qualifier>[A-Za-z_]\w*|`[^`]+`)\s*\.\s*)?(?P<column>[A-Za-z_]\w*|`[^`]+`)")
        output: list[str] = []
        position = 0
        quote = None
        index = 0
        while index < len(transformed):
            char = transformed[index]
            if quote is not None:
                if char == "\\":
                    index += 2
                    continue
                if char == quote:
                    quote = None
                index += 1
                continue
            if char in ("'", '"'):
                quote = char
                index += 1
                continue
            match = token_pattern.match(transformed, index)
            if not match:
                index += 1
                continue
            output.append(transformed[position : match.start()])
            qualifier = match.group("qualifier")
            qualifier_name = qualifier.strip("`") if qualifier else None
            column_text = match.group("column")
            column_name = column_text.strip("`")
            next_nonspace = transformed[match.end() :].lstrip()[:1]
            if qualifier_name == target_name:
                replacement = f"{{to}}.{column_text}"
            elif qualifier_name == self.current_model_name:
                replacement = f"{{from}}.{column_text}"
            elif qualifier_name is not None:
                return None
            elif column_name.lower() in keywords or next_nonspace == "(":
                replacement = column_text
            else:
                replacement = f"{{from}}.{column_text}"
            output.append(replacement)
            position = match.end()
            index = match.end()
        output.append(transformed[position:])
        result = "".join(output).strip()
        if quote is not None or not result:
            return None
        return result

    def _apply_explicit_field_visibility(self) -> None:
        """Apply Malloy accept/except to explicitly declared semantic fields."""
        if not self._accept_fields and not self._except_fields:
            return

        accepted = set(self._accept_fields)
        excluded = set(self._except_fields)
        for field in (*self.current_dimensions, *self.current_metrics):
            if accepted and field.name not in accepted:
                field.public = False
                if isinstance(field, Metric):
                    field.visibility = "private"
            if field.name in excluded:
                field.public = False
                if isinstance(field, Metric):
                    field.visibility = "private"

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
                # Both sides unqualified: keep the first identifier (the
                # documented "first identifier before =" behavior).
                related_col = source_col = lc

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
            list(self.current_invariant_filters),
            self._timezone,
            list(self._model_tags),
            list(self._accept_fields),
            list(self._except_fields),
            self._virtual,
            list(self._source_type_constraints),
            self._source_invalid,
        )

        # Reset and process the inline source
        self.current_model_name = join_name
        self.current_table = None
        self.current_sql = None
        self.current_primary_key = None
        self.current_description = None
        self.current_extends = None
        self.current_connection = None
        self.current_dimensions = []
        self.current_metrics = []
        self.current_relationships = []
        self.current_segments = []
        self.current_invariant_filters = []
        self._timezone = None
        self._model_tags = []
        self._accept_fields = []
        self._except_fields = []
        self._virtual = None
        self._source_type_constraints = []
        self._source_invalid = False

        self._process_sq_expr(sq_expr)

        # Only create model if we found something useful
        inline_created = False
        if not self._source_invalid and (self.current_table or self.current_sql or self.current_extends):
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
            if self._accept_fields:
                metadata["malloy_accept"] = list(self._accept_fields)
            if self._except_fields:
                metadata["malloy_except"] = list(self._except_fields)
            self._apply_explicit_field_visibility()
            source_scalar_data = {
                field: value
                for field, value in {
                    "table": self.current_table,
                    "sql": self.current_sql,
                    "extends": self.current_extends,
                    "primary_key": self.current_primary_key,
                    "description": self.current_description,
                    "metadata": metadata if metadata else None,
                }.items()
                if value is not None
            }
            inline_model = Model(
                name=join_name,
                dimensions=self.current_dimensions,
                metrics=self.current_metrics,
                relationships=self.current_relationships,
                segments=self.current_segments,
                invariant_filters=self.current_invariant_filters,
                **self._schema_exposure_options(),
                **source_scalar_data,
            )
            existing_model = next((model for model in reversed(self.models) if model.name == join_name), None)
            if existing_model is None:
                self.models.append(inline_model)
                inline_created = True
            else:
                existing_connection = (existing_model.metadata or {}).get("connection")
                inline_connection = (inline_model.metadata or {}).get("connection")
                same_physical_source = (
                    existing_model.table == inline_model.table
                    and existing_model.sql == inline_model.sql
                    and existing_model.extends == inline_model.extends
                    and existing_connection == inline_connection
                )
                bare_inline_source = isinstance(
                    sq_expr,
                    (MalloyParser.SQTableContext, MalloyParser.SQSQLContext),
                )
                if same_physical_source and bare_inline_source:
                    # Malloy commonly repeats the physical source inline even when
                    # a canonical source of the same name already exists. Reuse the
                    # canonical model only for a bare source reference. An inline
                    # wrapper could carry semantic edits which reuse would erase.
                    inline_created = True
                else:
                    self._record_unsupported(
                        f"inline join source '{join_name}' conflicts with an existing source of the same name",
                        fail_strict=True,
                    )

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
            self.current_invariant_filters,
            self._timezone,
            self._model_tags,
            self._accept_fields,
            self._except_fields,
            self._virtual,
            self._source_type_constraints,
            self._source_invalid,
        ) = saved
        return inline_created

    def _process_source_where(self, ctx: MalloyParser.WhereStatementContext):
        """Process source-level where clauses as always-on model invariants."""
        filter_list = ctx.filterClauseList()
        if not filter_list:
            return

        for filter_expr in filter_list.fieldExpr():
            sql = self._lower_scalar_field_expression(
                filter_expr, self.current_model_name or "<source>", "source filter"
            )
            if sql is None:
                self._source_invalid = True
                self.current_invariant_filters = []
                return
            self.current_invariant_filters.append(sql)


class MalloyAdapter(BaseAdapter):
    """Adapter for importing/exporting Malloy semantic models.

    Transforms Malloy definitions into Sidemantic format:
    - Sources -> Models
    - Dimensions -> Dimensions
    - Measures -> Metrics
    - join_one/join_many -> Relationships
    - Source-level where -> Model invariant filters

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

    def __init__(
        self,
        strict: bool = False,
        warn_on_errors: bool = True,
        import_root: str | Path | None = None,
        connection_dialects: dict[str, str] | None = None,
    ):
        """Create a Malloy adapter.

        Args:
            strict: If True, raise ``MalloySyntaxError`` when a parsed file has any
                syntax error. If False (default), collect errors and continue using
                ANTLR's recovered parse (backward-compatible behavior).
            warn_on_errors: If True (default) and not strict, emit a ``UserWarning``
                summarizing collected syntax errors so they are not silent.
            import_root: Optional filesystem boundary for project-relative imports.
                When omitted, :meth:`parse` uses the parsed directory or the entry
                file's parent directory.
        """
        self.strict = strict
        self.warn_on_errors = warn_on_errors
        self.import_root = Path(import_root).resolve() if import_root is not None else None
        builtin_dialects = {
            "duckdb": "duckdb",
            "postgres": "postgres",
            "postgresql": "postgres",
            "bigquery": "bigquery",
            "snowflake": "snowflake",
        }
        self.connection_dialects = {**builtin_dialects, **(connection_dialects or {})}
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
        self.unsupported_features: list[tuple[str, str]] = []

    @staticmethod
    def _resolved_models_for_consumption(models: dict[str, Model]) -> dict[str, Model]:
        resolved: dict[str, Model] = {}

        def resolve(name: str, active: frozenset[str] = frozenset()) -> Model:
            if name in resolved:
                return resolved[name]
            model = models[name]
            if not model.extends or model.extends not in models or name in active:
                resolved[name] = model
                return model
            flattened = merge_model(model, resolve(model.extends, active | {name}))
            resolved[name] = flattened
            return flattened

        for model_name in models:
            resolve(model_name)
        return resolved

    @staticmethod
    def _query_field_catalog(models: dict[str, Model], source_model: str) -> tuple[set[str], set[str]]:
        """Return only fields reachable from the query's resolved source and role paths."""
        resolved = MalloyAdapter._resolved_models_for_consumption(models)
        dimensions: set[str] = set()
        metrics: set[str] = set()
        edge_budget = max(1, sum(len(model.relationships) for model in resolved.values()))

        def visit(
            model_name: str,
            prefix: str,
            branch_edges: frozenset[tuple[str, str, str]],
        ) -> None:
            if model_name not in resolved or len(branch_edges) > edge_budget:
                return
            model = resolved[model_name]
            dimensions.update(f"{prefix}{field.name}" for field in model.dimensions)
            dimensions.update(f"{prefix}{name}" for name in model.primary_key_columns)
            metrics.update(f"{prefix}{field.name}" for field in model.metrics)
            for relationship in model.relationships:
                edge = (model_name, relationship.name, relationship.related_model)
                if edge in branch_edges:
                    continue
                visit(
                    relationship.related_model,
                    f"{prefix}{relationship.name}.",
                    branch_edges | {edge},
                )

        visit(source_model, "", frozenset())
        return dimensions, metrics

    @staticmethod
    def _validate_consumption_pair(graph: SemanticGraph, explore, saved_query) -> list[str]:
        """Validate both contracts against a resolved staging graph before mutating the result."""
        from sidemantic.validation import validate_explore, validate_saved_query

        staging = SemanticGraph()
        for model in MalloyAdapter._resolved_models_for_consumption(graph.models).values():
            staging.add_model(model)
        staging.metrics = dict(graph.metrics)
        staging.parameters = dict(graph.parameters)
        staging.explores = dict(graph.explores)
        staging.saved_queries = dict(graph.saved_queries)
        explore_errors, _ = validate_explore(explore, staging)
        if explore_errors:
            return explore_errors
        staging.add_explore(explore)
        saved_errors, _ = validate_saved_query(saved_query, staging)
        return saved_errors

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Malloy files into semantic graph.

        Resolves project-relative imports using Malloy's ordered module semantics.

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
        self.unsupported_features = []
        graph = SemanticGraph()
        source_path = Path(source).resolve()
        if source_path.is_dir():
            entries = sorted(source_path.rglob("*.malloy"))
            default_root = source_path
        else:
            entries = [source_path]
            default_root = source_path.parent

        resolver = MalloyModuleResolver(
            self._parse_module,
            strict=self.strict,
            import_root=self.import_root or default_root,
        )
        resolution = resolver.resolve(entries)
        resolved_join_diagnostics = self._finalize_resolved_joins(resolution.models)
        for model in resolution.models.values():
            graph.add_model(model)
        for name, resolved_query in resolution.queries.items():
            raw_definition = resolved_query.raw_definition
            location = resolved_query.location
            source_model = resolved_query.source_model
            if source_model is not None and source_model in resolution.models:
                known_dimensions, known_metrics = self._query_field_catalog(resolution.models, source_model)
            else:
                known_dimensions, known_metrics = set(), set()
            declared_name = raw_definition.split(" is", 1)[0].strip()
            try:
                mapping = map_malloy_query(
                    name,
                    raw_definition,
                    declared_name=declared_name,
                    source_model_override=source_model,
                    dimensions=known_dimensions,
                    metrics=known_metrics,
                )
            except Exception as exc:
                mapping = None
                diagnostics = [
                    MalloyQueryDiagnostic(
                        "malloy_query_syntax_error", f"query '{name}' could not be reparsed safely: {exc}"
                    )
                ]
            else:
                diagnostics = list(mapping.diagnostics)
            if mapping is not None and mapping.supported and mapping.explore.model not in graph.models:
                diagnostics.append(
                    MalloyQueryDiagnostic(
                        "malloy_query_source_unknown",
                        f"query '{name}' references unknown source '{mapping.explore.model}'",
                    )
                )
            if diagnostics:
                for diagnostic in diagnostics:
                    record_import_feature(
                        diagnostic.code,
                        "rejected",
                        detail=diagnostic.message,
                        source=str(location.path),
                        location=location.display,
                    )
                if self.strict:
                    raise MalloySchemaExposureError(diagnostics[0].message)
                graph.import_warnings.extend(
                    {
                        "code": diagnostic.code,
                        "message": diagnostic.message,
                        "severity": diagnostic.status,
                        "source_file": str(location.path),
                        "location": {"line": location.line, "column": location.column},
                    }
                    for diagnostic in diagnostics
                )
                continue
            assert mapping is not None and mapping.explore is not None and mapping.saved_query is not None
            if mapping.explore.name in graph.explores or mapping.saved_query.name in graph.saved_queries:
                message = f"query '{name}' conflicts with an existing consumption contract"
                record_import_feature(
                    "malloy_query_contract_conflict",
                    "rejected",
                    detail=message,
                    source=str(location.path),
                    location=location.display,
                )
                if self.strict:
                    raise MalloySchemaExposureError(message)
                graph.import_warnings.append(
                    {
                        "code": "malloy_query_contract_conflict",
                        "message": message,
                        "severity": "rejected",
                        "source_file": str(location.path),
                        "location": {"line": location.line, "column": location.column},
                    }
                )
                continue
            validation_errors = self._validate_consumption_pair(graph, mapping.explore, mapping.saved_query)
            if validation_errors:
                message = f"query '{name}' is not a valid native consumption contract: {validation_errors[0]}"
                record_import_feature(
                    "malloy_query_contract_invalid",
                    "rejected",
                    detail=message,
                    source=str(location.path),
                    location=location.display,
                )
                if self.strict:
                    raise MalloySchemaExposureError(message)
                graph.import_warnings.append(
                    {
                        "code": "malloy_query_contract_invalid",
                        "message": message,
                        "severity": "rejected",
                        "source_file": str(location.path),
                        "location": {"line": location.line, "column": location.column},
                    }
                )
                continue
            graph.add_explore(mapping.explore)
            graph.add_saved_query(mapping.saved_query)
        graph.import_warnings.extend(
            {
                "code": diagnostic["feature"],
                "message": diagnostic["detail"],
                "severity": diagnostic["status"],
                "source_file": diagnostic["source"],
                "location": diagnostic["location"],
            }
            for diagnostic in resolution.diagnostics
        )
        graph.import_warnings.extend(resolved_join_diagnostics)
        self.exports = list(
            dict.fromkeys(name for entry in entries for name in resolution.exports_by_file.get(entry.resolve(), ()))
        )
        self.user_types = resolution.user_types
        self.given = resolution.given
        return graph

    def _finalize_resolved_joins(self, models: dict[str, Model]) -> list[dict[str, object]]:
        """Resolve Malloy ``with`` joins which depend on the target source key."""
        diagnostics: list[dict[str, object]] = []

        def target_primary_keys(name: str, seen: set[str] | None = None) -> list[str]:
            target = models.get(name)
            if target is None:
                return []
            if "primary_key" in target.model_fields_set:
                return target.primary_key_columns
            if target.extends:
                visited = seen or set()
                if name in visited:
                    return []
                return target_primary_keys(target.extends, {*visited, name})
            return target.primary_key_columns

        for model in models.values():
            relationships: list[Relationship] = []
            for relationship in model.relationships:
                if relationship.related_model not in models:
                    issue = (
                        f"source '{model.name}' join '{relationship.name}' is rejected because target source "
                        f"'{relationship.related_model}' is unavailable"
                    )
                    if self.strict:
                        raise MalloySchemaExposureError(issue)
                    source_file = model._source_file or "<malloy>"
                    self.unsupported_features.append((source_file, issue))
                    record_import_feature(
                        "malloy_join_target_unavailable",
                        "rejected",
                        detail=issue,
                        source=source_file,
                    )
                    diagnostics.append(
                        {
                            "code": "malloy_join_target_unavailable",
                            "message": issue,
                            "severity": "rejected",
                            "source_file": source_file,
                            "location": None,
                        }
                    )
                    continue

                metadata = relationship.metadata or {}
                with_column = metadata.get("malloy_with")
                if not with_column or relationship.type != "many_to_one":
                    relationships.append(relationship)
                    continue

                target = models.get(relationship.related_model)
                target_keys = target_primary_keys(relationship.related_model)
                if len(target_keys) == 1:
                    relationship.primary_key = target_keys[0]
                    relationships.append(relationship)
                    continue

                if target is None:
                    reason = "the target source is unavailable"
                elif not target_keys:
                    reason = "the target source has no declared primary key"
                else:
                    reason = "the target source has a composite primary key which cannot match one with column"
                issue = (
                    f"source '{model.name}' join '{relationship.name}' with {with_column} is rejected because {reason}"
                )
                if self.strict:
                    raise MalloySchemaExposureError(issue)
                source_file = model._source_file or "<malloy>"
                self.unsupported_features.append((source_file, issue))
                record_import_feature(
                    "malloy_join_with_unresolved_key",
                    "rejected",
                    detail=issue,
                    source=source_file,
                )
                diagnostics.append(
                    {
                        "code": "malloy_join_with_unresolved_key",
                        "message": issue,
                        "severity": "rejected",
                        "source_file": source_file,
                        "location": None,
                    }
                )
            model.relationships = relationships
        return diagnostics

    def _parse_module(self, file_path: Path) -> MalloyModule:
        """Parse one canonical file without resolving any of its imports."""

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
        visitor = MalloyModelVisitor(
            strict_schema_exposure=self.strict,
            source_path=file_path,
            connection_dialects=self.connection_dialects,
        )
        visitor.visit(tree)

        if visitor.unsupported_features:
            self.unsupported_features.extend((str(file_path), issue) for issue in visitor.unsupported_features)
            for issue in visitor.unsupported_features:
                record_import_feature(
                    "malloy_unsupported_feature",
                    "unsupported",
                    detail=issue,
                    source=str(file_path),
                )
            if self.warn_on_errors:
                count = len(visitor.unsupported_features)
                more = f" (+{count - 1} more; inspect adapter.unsupported_features)" if count > 1 else ""
                warnings.warn(
                    f"Malloy compatibility limitation(s) in {file_path}: {visitor.unsupported_features[0]}{more}",
                    UserWarning,
                    stacklevel=2,
                )
        for feature, detail, location in visitor.expression_diagnostics:
            record_import_feature(
                feature,
                "rejected",
                detail=detail,
                source=str(file_path),
                location=location,
            )
        for feature, detail, location in visitor.source_diagnostics:
            record_import_feature(
                feature,
                "rejected",
                detail=detail,
                source=str(file_path),
                location=location,
            )

        return MalloyModule(
            path=file_path,
            statements=tuple(visitor.statements),
            user_types=dict(visitor.user_types),
            given=dict(visitor.given),
        )

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
            raw_model = graph.models.get(model.name)
            source_lines = self._export_source(
                model,
                inherited=bool(raw_model and raw_model.extends),
            )
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

    def _export_source(self, model: Model, *, inherited: bool = False) -> list[str]:
        """Export a model to Malloy source definition.

        Args:
            model: Model to export
            inherited: Whether this model was flattened from an inherited source

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
        if model.primary_key:
            lines.append(f"  primary_key: {model.primary_key}")

        # Malloy source-level where clauses are intrinsic filters, not reusable
        # named segments. Emit every predicate separately; repeated where clauses
        # are conjunctive in Malloy and reparse to the same ordered list.
        for predicate in model.invariant_filters:
            lines.append(f"  where: {self._strip_model_prefix(predicate)}")

        # Preserve source field-edit declarations when they came from Malloy.
        # Runtime schema exposure reconstructs their effective physical-column
        # visibility when the exported source is parsed again.
        malloy_accept = (model.metadata or {}).get("malloy_accept")
        malloy_except = (model.metadata or {}).get("malloy_except")
        if model.schema_exposure is not None and (inherited or (malloy_accept is None and malloy_except is None)):
            # Resolved child sources may inherit effective schema governance while
            # retaining only child-authored Malloy syntax metadata. Export their
            # effective controls so flattening cannot broaden the reparsed source.
            malloy_accept = model.schema_exposure.accept
            malloy_except = model.schema_exposure.except_fields
        if malloy_accept:
            lines.append(f"  accept: {', '.join(malloy_accept)}")
        if malloy_except:
            lines.append(f"  except: {', '.join(malloy_except)}")

        # Separate renames from computed dimensions for proper export
        renames_to_export: list[tuple[str, str, str]] = []
        dims_to_export: list[tuple[Dimension, str, str]] = []
        for dim in model.dimensions:
            if dim.name == model.primary_key:
                continue
            sql = self._strip_model_prefix(dim.sql or dim.name).strip()
            access = (dim.metadata or {}).get("malloy_access") or ("public" if dim.public else "private")
            # Public passthrough dimensions are intrinsic Malloy source fields.
            # Keep non-public passthrough declarations because they carry the
            # access boundary needed to reconstruct schema exposure on reparse.
            if sql == dim.name and access == "public":
                continue
            # Tier 4.5: detect renames (simple identifier, no operators/functions).
            # A time dimension with a granularity is NOT a rename: it must keep its
            # `.granularity` suffix below so the time type survives the roundtrip.
            is_time_with_grain = dim.type == "time" and dim.granularity
            if re.match(r"^[`\w]+$", sql) and sql != dim.name and not is_time_with_grain:
                renames_to_export.append((dim.name, sql, access))
            else:
                dims_to_export.append((dim, sql, access))

        # Export renames
        for access in ("public", "internal", "private"):
            access_renames = [(new, old) for new, old, item_access in renames_to_export if item_access == access]
            if not access_renames:
                continue
            lines.append("")
            prefix = "" if access == "public" else f"{access} "
            lines.append(f"  {prefix}rename:")
            for new_name, old_name in access_renames:
                lines.append(f"    {new_name} is {old_name}")

        # Export computed dimensions
        for access in ("public", "internal", "private"):
            access_dims = [(dim, sql) for dim, sql, item_access in dims_to_export if item_access == access]
            if not access_dims:
                continue
            lines.append("")
            prefix = "" if access == "public" else f"{access} "
            lines.append(f"  {prefix}dimension:")
            for dim, sql in access_dims:
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
        measure_lines: dict[str, list[str]] = {"public": [], "internal": [], "private": []}
        for metric in model.metrics:
            measure_expr = self._format_measure(metric)
            if measure_expr is None:
                # No faithful Malloy representation (e.g. cumulative/derived with
                # no sql); skip it rather than silently emitting a bogus count().
                measure_lines["public"].append(f"    // {metric.name}: unsupported metric type, not exported")
                continue
            access = (metric.metadata or {}).get("malloy_access") or metric.visibility
            if metric.description:
                measure_lines[access].append(f"    # desc: {self._one_line(metric.description)}")
            measure_lines[access].append(f"    {metric.name} is {measure_expr}")
        for access in ("public", "internal", "private"):
            access_lines = measure_lines[access]
            if not any(not line.lstrip().startswith("//") for line in access_lines):
                continue
            lines.append("")
            prefix = "" if access == "public" else f"{access} "
            lines.append(f"  {prefix}measure:")
            lines.extend(access_lines)

        # Joins - Tier 4.4: use on condition from metadata when available
        for rel in model.relationships:
            lines.append("")
            join_reference = f"{rel.name} is {rel.related_model}" if rel.target_model else rel.name
            if rel.type == "cross":
                # A cross join takes no key clause in Malloy.
                lines.append(f"  join_cross: {join_reference}")
                continue
            # one_to_many and many_to_many fan out -> join_many; many_to_one and
            # one_to_one collapse to at most one match -> join_one.
            join_type = "join_many" if rel.type in ("one_to_many", "many_to_many") else "join_one"
            on_condition = (rel.metadata or {}).get("on_condition")
            if on_condition:
                lines.append(f"  {join_type}: {join_reference} on {on_condition}")
            elif rel.sql:
                rendered_condition = self._native_join_sql_to_malloy(rel, model.name)
                lines.append(f"  {join_type}: {join_reference} on {rendered_condition}")
            elif rel.foreign_key:
                if isinstance(rel.foreign_key, list):
                    raise ValueError(
                        f"Cannot export relationship '{rel.name}' with composite keys and no exact SQL predicate"
                    )
                lines.append(f"  {join_type}: {join_reference} with {rel.foreign_key}")
            else:
                lines.append(f"  {join_type}: {join_reference}")

        lines.append("}")

        return lines

    @staticmethod
    def _native_join_sql_to_malloy(relationship: Relationship, source_model_name: str) -> str:
        """Render the native placeholder join contract without weakening its predicate."""
        condition = relationship.sql or ""
        protected_placeholders = [
            condition[start:end]
            for start, end, _kind in protected_sql_spans(condition)
            if "{from}." in condition[start:end] or "{to}." in condition[start:end]
        ]
        if protected_placeholders:
            raise ValueError(
                f"Cannot export relationship '{relationship.name}': custom SQL contains protected placeholders "
                "inside a literal, quoted identifier, or comment"
            )

        executable = mask_sql_literals_comments_and_quoted_identifiers(condition)
        if "{from}." not in executable or "{to}." not in executable:
            raise ValueError(
                f"Cannot export relationship '{relationship.name}': custom SQL must qualify both sides with "
                "{from}. and {to}. placeholders"
            )
        rendered = replace_outside_sql_protected(condition, "{from}", source_model_name)
        rendered = replace_outside_sql_protected(rendered, "{to}", relationship.name)
        if "{" in rendered or "}" in rendered:
            raise ValueError(
                f"Cannot export relationship '{relationship.name}': custom SQL contains protected placeholders "
                "or unsupported placeholders"
            )
        if parse_sql_fragment(rendered) is None:
            raise ValueError(
                f"Cannot export relationship '{relationship.name}': rendered custom SQL is not a valid predicate"
            )
        return rendered

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

        # Fallback to sql. Convert the SQL aggregate forms that import
        # normalization introduces back to Malloy so the derived measure
        # round-trips (Malloy has no count(*) or count(DISTINCT ...)).
        if metric.sql:
            return self._sql_aggs_to_malloy(self._strip_model_prefix(metric.sql))

        # No faithful Malloy representation for this metric.
        return None

    @staticmethod
    def _sql_aggs_to_malloy(sql: str) -> str:
        """Rewrite SQL aggregate syntax back to Malloy for export.

        COUNT(DISTINCT x) -> count(x), count(*) -> count(), and uppercase
        SUM/AVG/MIN/MAX(...) -> lowercase, so a derived measure whose SQL came
        from import normalization parses again as Malloy. COUNT(expr) is left
        untouched: Malloy count(field) is a distinct count, so translating a
        plain SQL COUNT(field) would change non-null counts into distinct counts.
        """
        # Protect backtick identifiers too: this rewrites already-SQL text, where
        # a backtick-quoted field name may itself look like an aggregate.
        bq = ("'", '"', "`")
        sql = _sub_outside_quotes(sql, r"\bcount\s*\(\s*distinct\s+(.+?)\s*\)", r"count(\1)", bq)
        sql = _sub_outside_quotes(sql, r"\bcount\s*\(\s*\*\s*\)", "count()", bq)
        return _sub_outside_quotes(sql, r"\b(SUM|AVG|MIN|MAX)\s*\(", lambda m: m.group(1).lower() + "(", bq)
