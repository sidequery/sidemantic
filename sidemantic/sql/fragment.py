"""Small SQL-fragment parsing helpers shared by adapters and query generation.

SQLGlot owns syntax classification.  Rewrites are applied to source spans so the
surrounding dialect spelling, comments, literals, and quoted identifiers remain
byte-for-byte unchanged.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import sqlglot
from sqlglot import exp
from sqlglot.dialects import Dialect
from sqlglot.optimizer.scope import build_scope


def _quoted_end(sql: str, start: int, closing: str) -> int:
    i = start + 1
    while i < len(sql):
        if sql[i] == closing:
            if i + 1 < len(sql) and sql[i + 1] == closing:
                i += 2
                continue
            return i + 1
        if sql[i] == "\\" and closing in {"'", '"', "`"}:
            i += 2
            continue
        i += 1
    return len(sql)


def protected_sql_spans(sql: str) -> Iterator[tuple[int, int, str]]:
    """Yield non-code SQL spans as ``(start, end, kind)`` triples.

    This lexical boundary deliberately recognizes only quoting/comment forms;
    semantic decisions remain SQLGlot's responsibility.
    """
    i = 0
    while i < len(sql):
        if sql.startswith("--", i):
            end = sql.find("\n", i + 2)
            end = len(sql) if end < 0 else end
            yield i, end, "comment"
            i = end
            continue
        if sql.startswith("/*", i):
            depth = 1
            end = i + 2
            while end < len(sql) and depth:
                if sql.startswith("/*", end):
                    depth += 1
                    end += 2
                elif sql.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
            yield i, end, "comment"
            i = end
            continue
        ch = sql[i]
        if ch in "'\"`":
            end = _quoted_end(sql, i, ch)
            yield i, end, "string" if ch == "'" else "quoted_identifier"
            i = end
            continue
        if ch == "[":
            end = _quoted_end(sql, i, "]")
            yield i, end, "quoted_identifier"
            i = end
            continue
        if ch == "$":
            if sql.startswith("${", i):
                end = sql.find("}", i + 2)
                if end >= 0:
                    yield i, end + 1, "placeholder"
                    i = end + 1
                    continue
            marker_end = sql.find("$", i + 1)
            if marker_end >= 0:
                marker = sql[i : marker_end + 1]
                tag = marker[1:-1]
                if not tag or (tag[0].isalpha() or tag[0] == "_") and all(c.isalnum() or c == "_" for c in tag):
                    end_marker = sql.find(marker, marker_end + 1)
                    if end_marker >= 0:
                        end = end_marker + len(marker)
                        yield i, end, "string"
                        i = end
                        continue
        if sql.startswith("{model}", i):
            yield i, i + len("{model}"), "placeholder"
            i += len("{model}")
            continue
        i += 1


def mask_sql_literals_comments_and_quoted_identifiers(
    sql: str, *, kinds: set[str] | frozenset[str] | None = None
) -> str:
    """Mask non-code spans without changing source offsets.

    Literal/identifier spans become a numeric atom and padding; comments become
    spaces.  The resulting expression remains parseable for normal SQL fragments
    while every executable token retains its original offset.
    """
    chars = list(sql)
    for start, end, kind in protected_sql_spans(sql):
        if kinds is not None and kind not in kinds:
            continue
        if kind == "comment":
            replacement = " " * (end - start)
        elif kind == "placeholder":
            replacement = "m" + " " * (end - start - 1)
        else:
            replacement = "0" + " " * (end - start - 1)
        chars[start:end] = replacement
    return "".join(chars)


def parse_sql_fragment(
    sql: str,
    dialect: str | None = None,
    *,
    mask_protected: bool = False,
    mask_kinds: set[str] | frozenset[str] | None = None,
) -> exp.Expression | None:
    source = (
        mask_sql_literals_comments_and_quoted_identifiers(sql, kinds=mask_kinds)
        if mask_protected or mask_kinds is not None
        else sql
    )
    dialects = (dialect,) if dialect is not None else (None, "snowflake", "bigquery", "postgres", "duckdb", "tsql")
    for candidate in dialects:
        try:
            return sqlglot.parse_one(source, read=candidate)
        except Exception:
            continue
    return None


ColumnResolver = Callable[[exp.Column, str], str | None]


def _column_is_bound_in_select(column: exp.Column, dialect: str | None = None) -> bool:
    """Return whether a column is owned by its nearest nested SELECT scope."""
    select = column.find_ancestor(exp.Select)
    if select is None:
        return False
    if not column.table:
        return True

    try:
        scope = build_scope(select)
    except Exception:
        return False
    if scope is None:
        return False
    qualifier = column.parts[-2]
    for source_name, source in scope.references:
        alias = source.args.get("alias")
        source_identifier = alias.this if alias is not None else source.args.get("this")
        if dialect is None:
            # An unknown fragment dialect cannot determine quoted-name folding;
            # fail safely by treating a case-insensitive alias match as local.
            if source_name.casefold() == qualifier.name.casefold():
                return True
            continue
        source_identifier = (
            source_identifier
            if isinstance(source_identifier, exp.Identifier)
            else exp.to_identifier(source_name, quoted=False)
        )
        dialect_impl = Dialect.get_or_raise(dialect)
        normalized_source = dialect_impl.normalize_identifier(source_identifier.copy()).name
        normalized_qualifier = dialect_impl.normalize_identifier(qualifier.copy()).name
        if normalized_source == normalized_qualifier:
            return True
    return False


def rewrite_sql_column_spans(
    sql: str,
    resolver: ColumnResolver,
    *,
    dialect: str | None = None,
    mask_protected: bool = False,
    mask_kinds: set[str] | frozenset[str] | None = None,
) -> str | None:
    """Rewrite SQLGlot-classified columns while preserving all untouched text."""
    tree = parse_sql_fragment(sql, dialect, mask_protected=mask_protected, mask_kinds=mask_kinds)
    if tree is None:
        return None

    replacements: dict[tuple[int, int], str] = {}
    for column in tree.find_all(exp.Column):
        if _column_is_bound_in_select(column, dialect):
            continue
        parts = list(column.parts)
        if not parts or any("start" not in part.meta or "end" not in part.meta for part in parts):
            continue
        start = parts[0].meta["start"]
        end = parts[-1].meta["end"] + 1
        replacement = resolver(column, sql[start:end])
        if replacement is not None:
            replacements[(start, end)] = replacement

    rewritten = sql
    for (start, end), replacement in sorted(replacements.items(), reverse=True):
        rewritten = rewritten[:start] + replacement + rewritten[end:]
    return rewritten


def replace_outside_sql_protected(sql: str, old: str, new: str) -> str:
    """Replace literal placeholder text only in executable SQL spans."""
    out: list[str] = []
    cursor = 0
    for start, end, kind in protected_sql_spans(sql):
        out.append(sql[cursor:start].replace(old, new))
        protected = sql[start:end]
        out.append(new if kind == "placeholder" and protected == old else protected)
        cursor = end
    out.append(sql[cursor:].replace(old, new))
    return "".join(out)
