"""SQLGlot boundary helpers for SQL embedded in LookML.

LookML SQL is not quite SQL: model/table references and Liquid/Jinja fragments are
interleaved with warehouse expressions.  This module owns the small lexical boundary
needed to protect those fragments, then lets callers use a SQL AST for semantic work.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from difflib import SequenceMatcher

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

from sidemantic.sql.aggregation_detection import _ANONYMOUS_AGGREGATE_FUNCTIONS
from sidemantic.sql.fragment import mask_sql_literals_comments_and_quoted_identifiers

_PROTECTED_PREFIX = "__sidemantic_lookml_fragment_"

_PARSE_DIALECTS: tuple[str | None, ...] = (None, "bigquery", "snowflake", "duckdb", "postgres", "tsql")

_DATE_PARTS = frozenset(
    {
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
        "nanosecond",
        "epoch",
        "date",
        "time",
        "dayofweek",
        "dayofyear",
        "dow",
        "doy",
        "isoweek",
        "isoyear",
        "isodow",
        "weekday",
        "yearofweek",
        "century",
        "decade",
        "millennium",
        "yy",
        "yyyy",
        "qq",
        "q",
        "mm",
        "m",
        "dy",
        "y",
        "dd",
        "d",
        "wk",
        "ww",
        "dw",
        "w",
        "hh",
        "mi",
        "n",
        "ss",
        "s",
        "ms",
        "mcs",
        "ns",
    }
)

_DATE_TRUNC_UNITS = frozenset(
    {
        "millennium",
        "century",
        "decade",
        "year",
        "isoyear",
        "quarter",
        "month",
        "week",
        "isoweek",
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
        "nanosecond",
    }
)

_DATE_PART_RANK = {
    "millennium": 0,
    "century": 1,
    "decade": 2,
    "year": 3,
    "isoyear": 3,
    "yearofweek": 3,
    "quarter": 4,
    "month": 5,
    "week": 6,
    "isoweek": 6,
    "day": 7,
    "date": 7,
    "dayofweek": 7,
    "dayofyear": 7,
    "dow": 7,
    "doy": 7,
    "weekday": 7,
    "isodow": 7,
    "hour": 8,
    "time": 8,
    "minute": 9,
    "second": 10,
    "millisecond": 11,
    "microsecond": 12,
    "nanosecond": 13,
}

_DATEPART_FIRST_FUNCTIONS = frozenset(
    {
        "datetrunc",
        "datediff",
        "datediff_big",
        "dateadd",
        "datepart",
        "date_part",
        "datename",
        "date_bucket",
        "timestampadd",
        "timestampdiff",
        "timestampdiff_big",
        "datetimediff",
        "timeadd",
        "timediff",
    }
)


@dataclass(frozen=True)
class ProtectedLookMLSQL:
    """SQL text with non-SQL LookML fragments replaced by identifier sentinels."""

    text: str
    replacements: tuple[tuple[str, str], ...]

    def restore(self, text: str, *, original_overrides: Mapping[str, str] | None = None) -> str:
        for sentinel, original in self.replacements:
            replacement = original_overrides.get(original, original) if original_overrides else original
            text = text.replace(sentinel, replacement)
        return text

    def restore_identifier(self, value: str) -> str:
        for sentinel, original in self.replacements:
            if value == sentinel:
                return original
        return value


@dataclass(frozen=True)
class ParsedLookMLSQL:
    """A parsed LookML SQL expression and the lexical substitutions used for it."""

    protected: ProtectedLookMLSQL
    tree: exp.Expression
    dialect: str | None


def _quoted_end(sql: str, start: int, closing: str) -> int:
    """Return the exclusive end of a SQL quoted token, honoring doubled closers."""
    i = start + 1
    while i < len(sql):
        if sql[i] == closing:
            if i + 1 < len(sql) and sql[i + 1] == closing:
                i += 2
                continue
            return i + 1
        i += 1
    return len(sql)


def _unique_identifier(forbidden: str, label: str, used: set[str] | None = None) -> str:
    """Return a deterministic generated identifier absent from all caller-owned SQL."""
    if used is None:
        used = set()
    nonce = 0
    while True:
        candidate = f"{_PROTECTED_PREFIX}{label}_{nonce}__"
        if candidate not in forbidden and candidate not in used:
            return candidate
        nonce += 1


def protect_lookml_sql(sql: str) -> ProtectedLookMLSQL:
    """Protect LookML/Liquid fragments without touching markers inside SQL literals.

    The output is valid SQL whenever the surrounding expression is valid SQL.  Quoted
    strings, quoted identifiers, and comments are copied verbatim; only fragments in
    executable SQL are replaced.
    """
    out: list[str] = []
    replacements: list[tuple[str, str]] = []
    shared_sentinels: dict[str, str] = {}
    used_sentinels: set[str] = set()
    fragment_index = 0
    i = 0

    def protect(original: str, shared_label: str | None = None) -> None:
        nonlocal fragment_index
        if shared_label is not None and original in shared_sentinels:
            sentinel = shared_sentinels[original]
        else:
            label = shared_label or f"fragment_{fragment_index}"
            sentinel = _unique_identifier(sql, label, used_sentinels)
            used_sentinels.add(sentinel)
            if shared_label is not None:
                shared_sentinels[original] = sentinel
        fragment_index += 1
        replacements.append((sentinel, original))
        out.append(sentinel)

    while i < len(sql):
        ch = sql[i]
        if ch in "'\"`":
            end = _quoted_end(sql, i, ch)
            out.append(sql[i:end])
            i = end
            continue
        if ch == "[":
            end = _quoted_end(sql, i, "]")
            out.append(sql[i:end])
            i = end
            continue
        if sql.startswith("--", i):
            end = sql.find("\n", i + 2)
            end = len(sql) if end < 0 else end
            out.append(sql[i:end])
            i = end
            continue
        if sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = len(sql) if end < 0 else end + 2
            out.append(sql[i:end])
            i = end
            continue
        if ch == "$" and not sql.startswith("${", i):
            marker_end = sql.find("$", i + 1)
            if marker_end >= 0:
                marker = sql[i : marker_end + 1]
                tag = marker[1:-1]
                if not tag or (tag[0].isalpha() or tag[0] == "_") and all(c.isalnum() or c == "_" for c in tag):
                    end_marker = sql.find(marker, marker_end + 1)
                    if end_marker >= 0:
                        end = end_marker + len(marker)
                        out.append(sql[i:end])
                        i = end
                        continue
        if sql.startswith("{{", i) or sql.startswith("{%", i):
            close = "}}" if sql.startswith("{{", i) else "%}"
            end = sql.find(close, i + 2)
            end = len(sql) if end < 0 else end + 2
            protect(sql[i:end])
            i = end
            continue
        if sql.startswith("${", i):
            end = sql.find("}", i + 2)
            end = len(sql) if end < 0 else end + 1
            original = sql[i:end]
            protect(original, "table" if original == "${TABLE}" else None)
            i = end
            continue
        if sql.startswith("{model}", i):
            protect("{model}", "model")
            i += len("{model}")
            continue
        out.append(ch)
        i += 1

    return ProtectedLookMLSQL("".join(out), tuple(replacements))


def replace_lookml_placeholders(sql: str, replacements: Mapping[str, str]) -> str:
    """Replace executable LookML placeholders while preserving literals and comments."""
    protected = protect_lookml_sql(sql)
    return protected.restore(protected.text, original_overrides=replacements)


def strip_lookml_model_qualifiers(sql: str) -> str:
    """Remove executable ``{model}.`` qualifiers without changing literal text."""
    protected = protect_lookml_sql(sql)
    stripped = protected.text
    for sentinel, original in protected.replacements:
        if original == "{model}":
            stripped = stripped.replace(f"{sentinel}.", "")
    return protected.restore(stripped)


def _date_unit(node: exp.Expression) -> str | None:
    unit = node.args.get("unit")
    if isinstance(unit, (exp.Literal, exp.Var, exp.Identifier)):
        return str(unit.this).strip("'\"").lower()
    return None


def _anonymous_date_part(node: exp.Anonymous, time_columns: set[str]) -> tuple[exp.Column, str] | None:
    name = (node.name or "").lower()
    args = list(node.expressions)
    if not args:
        return None
    index: int | None = None
    if name in _DATEPART_FIRST_FUNCTIONS:
        index = 0
    elif name in ("trunc", "truncate") and len(args) == 2 and _contains_time_value(args[0], time_columns):
        index = 1
    if index is None or index >= len(args) or not isinstance(args[index], exp.Column):
        return None
    column = args[index]
    unit = column.name.lower()
    return (column, unit) if unit in _DATE_PARTS else None


def _syntax_date_part_columns(tree: exp.Expression, time_columns: set[str]) -> dict[int, str]:
    """Columns that a generic parse treated as date-part syntax, keyed by identity."""
    syntax: dict[int, str] = {}
    for node in tree.find_all(exp.Anonymous):
        found = _anonymous_date_part(node, time_columns)
        if found is not None:
            syntax[id(found[0])] = found[1]
    for node in tree.find_all(exp.Trunc):
        decimals = node.args.get("decimals")
        value = node.args.get("this")
        if (
            isinstance(decimals, exp.Column)
            and decimals.name.lower() in _DATE_PARTS
            and isinstance(value, exp.Expression)
            and _contains_time_value(value, time_columns)
        ):
            syntax[id(decimals)] = decimals.name.lower()
    return syntax


def _unit_score(unit: str, *, truncation: bool) -> int:
    score = 8 + max(0, 10 - _DATE_PART_RANK.get(unit, 10))
    if truncation:
        score += 4 if unit in _DATE_TRUNC_UNITS else -2
    else:
        score += 2 if unit in _DATE_TRUNC_UNITS else -1
    return score


def _contains_time_value(node: exp.Expression, time_columns: set[str]) -> bool:
    if any(column.name in time_columns for column in node.find_all(exp.Column)):
        return True
    for cast in node.find_all(exp.Cast):
        target = str(cast.args.get("to") or "").upper()
        if any(kind in target for kind in ("DATE", "TIME")):
            return True
    return False


def _candidate_score(tree: exp.Expression, known_columns: set[str], time_columns: set[str]) -> tuple[int, int, int]:
    """Prefer parses that recognize date units and expose model fields as columns."""
    syntax_columns = _syntax_date_part_columns(tree, time_columns)
    score = sum(
        column.name in known_columns and id(column) not in syntax_columns for column in tree.find_all(exp.Column)
    )
    recognized_units = 0
    for node in tree.walk():
        unit = _date_unit(node)
        if unit not in _DATE_PARTS:
            continue
        recognized_units += 1
        value = node.args.get("this")
        if isinstance(node, (exp.DateTrunc, exp.TimestampTrunc)) and isinstance(value, exp.Expression):
            # A numeric TRUNC(amount, month-column) is not a date truncation.  Reward a
            # unit-bearing interpretation only when its value is demonstrably temporal.
            score += _unit_score(unit, truncation=True) if _contains_time_value(value, time_columns) else -2
        else:
            score += _unit_score(unit, truncation=False)
    for unit in syntax_columns.values():
        recognized_units += 1
        score += _unit_score(unit, truncation=False)
    quoted_identifiers = sum(bool(identifier.args.get("quoted")) for identifier in tree.find_all(exp.Identifier))
    return score, recognized_units, quoted_identifiers


def _render_fidelity(source: str, tree: exp.Expression, dialect: str | None) -> int:
    """Score how faithfully a dialect round-trip retains the source's SQL spelling."""
    try:
        rendered = tree.sql(dialect=dialect)
    except Exception:
        return -1
    normalized_source = "".join(source.upper().split())
    normalized_rendered = "".join(rendered.upper().split())
    return round(SequenceMatcher(None, normalized_source, normalized_rendered, autojunk=False).ratio() * 1_000_000)


def parse_lookml_expression(
    sql: str,
    *,
    known_columns: set[str] | None = None,
    time_columns: set[str] | None = None,
) -> ParsedLookMLSQL | None:
    """Parse LookML-embedded SQL, selecting the least ambiguous supported dialect.

    LookML does not carry a SQL dialect into this adapter.  Trying the small set of
    supported warehouse dialects lets AST structure disambiguate forms such as
    ``DATE_TRUNC(value, unit)`` versus ``DATE_TRUNC(unit, value)`` without hand-scanning
    function argument text.
    """
    protected = protect_lookml_sql(sql)
    known = known_columns or set()
    temporal = time_columns or set()
    best: tuple[tuple[int, int, int, int, int], exp.Expression, str | None] | None = None
    for index, dialect in enumerate(_PARSE_DIALECTS):
        try:
            tree = sqlglot.parse_one(protected.text, read=dialect)
        except Exception:
            continue
        key = (*_candidate_score(tree, known, temporal), _render_fidelity(protected.text, tree, dialect), -index)
        if best is None or key > best[0]:
            best = (key, tree, dialect)
    if best is None:
        return None
    return ParsedLookMLSQL(protected, best[1], best[2])


def lookml_expression_has_subquery(sql: str) -> bool:
    """Whether executable LookML SQL contains a real ``SELECT``.

    Prefer the parsed AST.  Some otherwise valid SQL fragments cannot be parsed
    after Liquid control tags are replaced by identifier sentinels, however.  In
    that case SQLGlot's tokenizer provides the conservative syntax boundary after
    strings, quoted identifiers, comments, dollar literals, and LookML fragments
    have been hidden.  This fails closed for genuine subqueries without reviving
    the old false positives from literal or template contents.
    """
    parsed = parse_lookml_expression(sql)
    if parsed is not None:
        return parsed.tree.find(exp.Select) is not None

    protected = protect_lookml_sql(sql)
    masked = mask_sql_literals_comments_and_quoted_identifiers(protected.text)
    try:
        tokens = sqlglot.Tokenizer().tokenize(masked)
    except Exception:
        return False
    return any(token.token_type is TokenType.SELECT for token in tokens)


ColumnResolver = Callable[[str, tuple[str, ...], bool], str | None]


def rewrite_lookml_columns(
    sql: str,
    resolver: ColumnResolver,
    *,
    known_columns: set[str] | None = None,
    time_columns: set[str] | None = None,
) -> str | None:
    """Rewrite AST-classified column spans while preserving original dialect spelling.

    SQLGlot identifies semantic columns, including their source offsets.  Applying
    replacements to those offsets avoids reserializing the untouched expression, so
    dialect-specific functions and syntax remain byte-for-byte intact.
    """
    parsed = parse_lookml_expression(sql, known_columns=known_columns, time_columns=time_columns)
    if parsed is None:
        return None

    replacements: dict[tuple[int, int], str] = {}
    syntax_columns = _syntax_date_part_columns(parsed.tree, time_columns or set())
    for column in parsed.tree.find_all(exp.Column):
        if id(column) in syntax_columns:
            continue
        parts = list(column.parts)
        if not parts or any("start" not in part.meta or "end" not in part.meta for part in parts):
            continue
        identifier = parts[-1]
        quoted = bool(identifier.args.get("quoted"))
        name = identifier.name
        if parsed.protected.restore_identifier(name) != name:
            continue
        qualifiers = tuple(parsed.protected.restore_identifier(part.name) for part in parts[:-1])
        replacement = resolver(name, qualifiers, quoted)
        if replacement is None:
            continue
        replacements[(parts[0].meta["start"], parts[-1].meta["end"] + 1)] = replacement

    rewritten = parsed.protected.text
    for (start, end), replacement in sorted(replacements.items(), reverse=True):
        rewritten = rewritten[:start] + replacement + rewritten[end:]
    return parsed.protected.restore(rewritten)


def _aggregate_nodes(tree: exp.Expression) -> list[exp.Expression]:
    nodes: list[exp.Expression] = list(tree.find_all(exp.AggFunc))
    nodes.extend(
        node for node in tree.find_all(exp.Anonymous) if (node.name or "").lower() in _ANONYMOUS_AGGREGATE_FUNCTIONS
    )
    return nodes


def _function_name(node: exp.Expression) -> str:
    if isinstance(node, exp.Anonymous):
        return (node.name or "").upper()
    if isinstance(node, exp.Func):
        return node.sql_name().upper()
    return ""


def _source_function_names(sql: str) -> set[str]:
    """Function-call spellings in source SQL, identified by SQLGlot tokens."""
    try:
        tokens = sqlglot.tokenize(sql)
    except Exception:
        return set()
    return {
        token.text.upper() for token, following in zip(tokens, tokens[1:]) if following.token_type == TokenType.L_PAREN
    }


def _case(condition: exp.Expression, value: exp.Expression) -> exp.Case:
    return exp.Case(ifs=[exp.If(this=condition, true=value)])


def _combined_condition(conditions: list[exp.Expression]) -> exp.Expression:
    combined = conditions[0].copy()
    for condition in conditions[1:]:
        combined = exp.And(this=combined, expression=condition.copy())
    return combined


def _aggregate_scope(node: exp.Expression) -> exp.Expression:
    within_group = node.find_ancestor(exp.WithinGroup)
    return within_group if within_group is not None else node


def _real_columns(parsed: ParsedLookMLSQL, scope: exp.Expression | None = None) -> list[exp.Column]:
    """Columns originating in SQL, excluding protected LookML/Liquid fragments."""
    root = scope if scope is not None else parsed.tree
    return [
        column
        for column in root.find_all(exp.Column)
        if parsed.protected.restore_identifier(column.name) == column.name
    ]


def _nulling_is_unsafe(tree: exp.Expression) -> bool:
    case_with_default = any(node.args.get("default") is not None for node in tree.find_all(exp.Case))
    if_with_default = any(node.args.get("false") is not None for node in tree.find_all(exp.If))
    iff_with_default = any(
        (node.name or "").lower() in ("iff", "if") and len(node.expressions) >= 3
        for node in tree.find_all(exp.Anonymous)
    )
    multi_column_distinct = any(
        len(node.expressions) > 1
        or any(isinstance(item, exp.Tuple) and len(item.expressions) > 1 for item in node.expressions)
        for node in tree.find_all(exp.Distinct)
    )
    has_hash = any(_function_name(node) == "HASH" for node in tree.find_all(exp.Func))
    return (
        tree.find(exp.Is) is not None
        or tree.find(exp.Coalesce) is not None
        or has_hash
        or case_with_default
        or if_with_default
        or iff_with_default
        or multi_column_distinct
    )


def generator_column_nulling_suffices(sql: str) -> bool:
    """Whether complete-SQL filtering can safely null every aggregate input column."""
    parsed = parse_lookml_expression(sql)
    if parsed is None:
        return False
    aggregates = _aggregate_nodes(parsed.tree)
    return (
        bool(aggregates)
        and not _nulling_is_unsafe(parsed.tree)
        and all(bool(_real_columns(parsed, _aggregate_scope(node))) for node in aggregates)
    )


def complete_sql_fold_is_safe(sql: str) -> bool:
    """Whether wrapping every aggregate input in a filter produces valid semantics."""
    parsed = parse_lookml_expression(sql)
    if parsed is None:
        return False
    tree = parsed.tree
    if any(tree.find_all(exp.List)) or any(tree.find_all(exp.ArrayAgg)):
        return False
    if any(order.find_ancestor(exp.WithinGroup) is None for order in tree.find_all(exp.Order)):
        return False
    for node in tree.find_all(exp.Anonymous):
        if (node.name or "").lower() in _ANONYMOUS_AGGREGATE_FUNCTIONS and len(node.expressions) > 1:
            return False
    for aggregate in tree.find_all(exp.AggFunc):
        primary_columns = {id(column) for column in (aggregate.this.find_all(exp.Column) if aggregate.this else [])}
        if any(id(column) not in primary_columns for column in aggregate.find_all(exp.Column)):
            return False
    return all(len(node.expressions) <= 1 for node in tree.find_all(exp.Distinct))


def lookml_expression_references_column(sql: str) -> bool:
    """Whether an expression references a real SQL column rather than only templates/constants."""
    normalized, _ = strip_outer_aggregate_all(sql)
    parsed = parse_lookml_expression(normalized)
    if parsed is None:
        return True
    return bool(_real_columns(parsed))


_TEMPLATE_BLOCK_ENDS = {
    "autoescape": "endautoescape",
    "block": "endblock",
    "call": "endcall",
    "capture": "endcapture",
    "case": "endcase",
    "comment": "endcomment",
    "condition": "endcondition",
    "filter": "endfilter",
    "for": "endfor",
    "if": "endif",
    "macro": "endmacro",
    "raw": "endraw",
    "set": "endset",
    "trans": "endtrans",
    "unless": "endunless",
    "with": "endwith",
}
_TEMPLATE_BRANCH_TAGS = frozenset({"elif", "else", "elsif", "when"})
_TEMPLATE_BRANCH_PARENTS = {
    "elif": frozenset({"if"}),
    "else": frozenset({"case", "for", "if", "unless"}),
    "elsif": frozenset({"if", "unless"}),
    "when": frozenset({"case"}),
}


def _template_control_blocks_are_balanced(protected: ProtectedLookMLSQL) -> tuple[bool, bool]:
    """Return whether executable Liquid/Jinja control blocks exist and are balanced."""
    stack: list[str] = []
    has_control_block = False
    end_tags = {end: start for start, end in _TEMPLATE_BLOCK_ENDS.items()}
    for _, original in protected.replacements:
        if not original.startswith("{%"):
            continue
        body = original[2:-2].strip().strip("-").strip()
        if not body:
            continue
        parts = body.split(maxsplit=1)
        tag = parts[0].lower()
        arguments = parts[1] if len(parts) > 1 else ""
        if stack and stack[-1] in {"comment", "raw"}:
            if tag == _TEMPLATE_BLOCK_ENDS[stack[-1]]:
                stack.pop()
            continue
        before_equals, equals, _ = arguments.partition("=")
        if tag == "set" and equals and "|" not in before_equals:
            # Jinja assignment syntax is a standalone tag; only capture-style
            # ``{% set name %}...{% endset %}`` opens a control block.
            continue
        if tag in _TEMPLATE_BLOCK_ENDS:
            has_control_block = True
            stack.append(tag)
        elif tag in end_tags:
            has_control_block = True
            if not stack or stack.pop() != end_tags[tag]:
                return True, False
        elif tag in _TEMPLATE_BRANCH_TAGS:
            has_control_block = True
            if not stack or stack[-1] not in _TEMPLATE_BRANCH_PARENTS[tag]:
                return True, False
    return has_control_block, not stack


def _contains_sql_comment(sql: str) -> bool:
    """Detect statement comments outside SQL quotes across supported dialects."""
    i = 0
    temp_identifiers: set[str] = set()
    while i < len(sql):
        ch = sql[i]
        if ch in "'\"`":
            i = _quoted_end(sql, i, ch)
            continue
        if ch == "[":
            i = _quoted_end(sql, i, "]")
            continue
        hash_identifier = ""
        previous_word = ""
        if ch == "#" and not sql.startswith(("#>", "#>>", "#-"), i):
            identifier_end = i + 1
            if identifier_end < len(sql) and sql[identifier_end] == "#":
                identifier_end += 1
            while identifier_end < len(sql) and (sql[identifier_end].isalnum() or sql[identifier_end] in {"_", "$"}):
                identifier_end += 1
            hash_identifier = sql[i:identifier_end].lower()
            prefix = sql[:i].rstrip()
            word_start = len(prefix)
            while word_start and (prefix[word_start - 1].isalnum() or prefix[word_start - 1] == "_"):
                word_start -= 1
            previous_word = prefix[word_start:].upper()
            previous_nonspace = prefix[-1:] if prefix else ""
            introduces_temp_identifier = previous_word in {"FROM", "INTO", "JOIN", "TABLE", "UPDATE"} or (
                previous_nonspace == "," and bool(temp_identifiers)
            )
            if hash_identifier and introduces_temp_identifier:
                temp_identifiers.add(hash_identifier)
        known_temp_identifier = bool(hash_identifier and hash_identifier in temp_identifiers)
        hash_starts_line = ch == "#" and not known_temp_identifier and not sql[sql.rfind("\n", 0, i) + 1 : i].strip()
        hash_starts_inline_comment = False
        if (
            ch == "#"
            and i > 0
            and sql[i - 1].isspace()
            and not known_temp_identifier
            and not sql.startswith(("#>", "#>>", "#-"), i)
        ):
            # BigQuery/MySQL accept both ``# comment`` and ``#comment``. Preserve
            # SQL Server temporary identifiers after relation-introducing keywords.
            hash_starts_inline_comment = previous_word not in {"FROM", "INTO", "JOIN", "TABLE", "UPDATE"}
        if sql.startswith("--", i) or sql.startswith("/*", i) or hash_starts_line or hash_starts_inline_comment:
            return True
        i += 1
    return False


def _predicate_fragment_is_safe(filter_sql: str, parsed: ParsedLookMLSQL | None) -> bool:
    """Whether raw predicate text can be inserted inside ``CASE WHEN`` unchanged.

    A balanced Liquid/Jinja block may render a valid predicate even though its protected
    control-tag sentinels do not form a parseable SQL AST. The folder restores the exact
    predicate bytes after transforming the aggregate, so lexical statement safety and
    balanced template control flow are sufficient for that special case.
    """
    protected = parsed.protected if parsed is not None else protect_lookml_sql(filter_sql)
    has_control_block, blocks_are_balanced = _template_control_blocks_are_balanced(protected)
    if not blocks_are_balanced or (parsed is None and not has_control_block):
        return False
    if _contains_sql_comment(protected.text):
        return False
    try:
        tokens = sqlglot.tokenize(protected.text, dialect=parsed.dialect if parsed is not None else None)
    except Exception:
        return False
    return not any(token.token_type == TokenType.SEMICOLON or token.comments for token in tokens)


def fold_lookml_aggregate_filters(sql: str, filters: list[str], *, force: bool = False) -> str | None:
    """Fold predicates into every aggregate argument using a SQLGlot AST.

    ``None`` means the expression is either safely handled by generator column
    nulling, cannot be parsed, or cannot be transformed without changing semantics.
    In ``force`` mode filters must always be baked into the expression and function
    spellings must survive SQLGlot serialization.
    """
    parsed = parse_lookml_expression(sql)
    parsed_filters = [parse_lookml_expression(filter_sql) for filter_sql in filters]
    if parsed is None or any(
        not _predicate_fragment_is_safe(filter_sql, item) for filter_sql, item in zip(filters, parsed_filters)
    ):
        return None
    forbidden_markers = "\n".join((parsed.protected.text, *filters))
    used_markers: set[str] = set()
    condition_markers = []
    for index in range(len(filters)):
        marker = _unique_identifier(forbidden_markers, f"filter_{index}", used_markers)
        used_markers.add(marker)
        condition_markers.append(marker)
    # Predicates are validated as ASTs above, but represented by placeholder columns in
    # the transformed tree.  Splicing their original text back after rendering preserves
    # warehouse-specific syntax, quoting, casing, and LookML/Liquid fragments exactly.
    conditions = []
    for marker in condition_markers:
        condition = exp.EQ(this=exp.column(marker), expression=exp.Literal.number(1))
        conditions.append(condition if len(condition_markers) == 1 else exp.Paren(this=condition))
    aggregates = _aggregate_nodes(parsed.tree)
    if not aggregates or not conditions:
        return None

    if force:
        source_names = _source_function_names(parsed.protected.text)
        if any(_function_name(node) not in source_names for node in aggregates):
            return None

    if (
        not force
        and not _nulling_is_unsafe(parsed.tree)
        and all(bool(_real_columns(parsed, _aggregate_scope(node))) for node in aggregates)
    ):
        return None

    for aggregate in aggregates:
        windowed = aggregate.parent
        while isinstance(windowed, exp.Filter):
            windowed = windowed.parent
        if isinstance(windowed, exp.Window):
            inner = [node for node in _aggregate_nodes(aggregate) if node is not aggregate]
            if inner:
                continue
            return None

        within_group = aggregate.find_ancestor(exp.WithinGroup)
        if within_group is not None:
            for ordered in within_group.find_all(exp.Ordered):
                ordered.set("this", _case(_combined_condition(conditions), ordered.this.copy()))
            continue

        if isinstance(aggregate, exp.Anonymous):
            aggregate.set(
                "expressions",
                [_case(_combined_condition(conditions), argument.copy()) for argument in aggregate.expressions],
            )
            continue

        argument = aggregate.this
        if isinstance(argument, exp.Distinct):
            argument.set(
                "expressions",
                [_case(_combined_condition(conditions), item.copy()) for item in argument.expressions],
            )
        elif argument is None or isinstance(argument, exp.Star):
            aggregate.set("this", _case(_combined_condition(conditions), exp.Literal.number(1)))
        else:
            aggregate.set("this", _case(_combined_condition(conditions), argument.copy()))

    try:
        rendered = parsed.tree.sql(dialect=parsed.dialect)
    except Exception:
        return None
    rendered = parsed.protected.restore(rendered)
    for marker, filter_sql in zip(condition_markers, filters):
        rendered = rendered.replace(f"{marker} = 1", filter_sql)
    return rendered


def strip_outer_aggregate_all(sql: str) -> tuple[str, bool]:
    """Remove an explicit outer aggregate ``ALL`` modifier using SQL tokens.

    SQLGlot currently rejects ``FUNC(ALL value)`` even though ``ALL`` is the SQL
    default.  Removing it makes the expression parseable; callers can restore the
    spelling after the AST transform.  A column literally named ``all`` is not a
    modifier because it has no following argument token.
    """
    protected = protect_lookml_sql(sql)
    try:
        tokens = sqlglot.tokenize(protected.text)
    except Exception:
        return sql, False
    if (
        len(tokens) >= 5
        and tokens[1].token_type == TokenType.L_PAREN
        and tokens[2].token_type == TokenType.ALL
        and tokens[3].token_type != TokenType.R_PAREN
    ):
        normalized = protected.text[: tokens[2].start] + protected.text[tokens[3].start :]
        return protected.restore(normalized), True
    return sql, False


def restore_outer_aggregate_all(sql: str) -> str:
    """Restore ``ALL`` immediately inside the outer function call."""
    protected = protect_lookml_sql(sql)
    try:
        tokens = sqlglot.tokenize(protected.text)
    except Exception:
        return sql
    if len(tokens) >= 3 and tokens[1].token_type == TokenType.L_PAREN:
        token = tokens[1]
        restored = protected.text[: token.end + 1] + "ALL " + protected.text[token.end + 1 :]
        return protected.restore(restored)
    return sql


def strip_aggregate_all(sql: str) -> str:
    """Remove syntactic aggregate ``ALL`` modifiers while preserving comments."""
    protected = protect_lookml_sql(sql)
    try:
        tokens = sqlglot.tokenize(protected.text)
    except Exception:
        return sql

    remove: list[tuple[int, int]] = []
    for index, token in enumerate(tokens):
        if token.token_type != TokenType.ALL:
            continue
        is_leading = index == 0 and len(tokens) >= 2
        is_function_modifier = (
            index >= 2
            and index + 1 < len(tokens)
            and tokens[index - 1].token_type == TokenType.L_PAREN
            and tokens[index + 1].token_type != TokenType.R_PAREN
        )
        if not (is_leading or is_function_modifier):
            continue
        end = token.end + 1
        while end < len(protected.text) and protected.text[end].isspace():
            end += 1
        remove.append((token.start, end))

    stripped = protected.text
    for start, end in reversed(remove):
        stripped = stripped[:start] + stripped[end:]
    return protected.restore(stripped)
