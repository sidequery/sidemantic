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
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.scope import build_scope


def parse_query_fragment(
    sql: str,
    dialect: str | None = None,
    *,
    order_by: bool = False,
    query_ctes: frozenset[str] = frozenset(),
) -> exp.Expression:
    """Parse a caller expression without granting access to new SQL relations.

    Trusted model and security predicates are deliberately outside this boundary.
    A SELECT wrapper also catches valid SQL that escapes into another clause.
    """
    clause = "order" if order_by else "where"
    prefix = "SELECT 1 ORDER BY " if order_by else "SELECT 1 WHERE "
    try:
        statements = sqlglot.parse(prefix + sql, read=dialect)
    except SqlglotError as exc:
        raise ValueError("Invalid query expression") from exc
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise ValueError("Query expression contains disallowed SQL")
    query = statements[0]
    if any(value for key, value in query.args.items() if key not in {"expressions", clause}):
        raise ValueError("Query expression contains disallowed SQL clauses")
    expression = query.args.get(clause)
    if expression is None or (order_by and len(expression.expressions) != 1):
        raise ValueError("Expected one query expression")
    for node in expression.walk():
        if isinstance(node, exp.Table):
            identifier = node.this
            if (
                not isinstance(identifier, exp.Identifier)
                or node.db
                or node.catalog
                or Dialect.get_or_raise(dialect).normalize_identifier(identifier.copy()).name not in query_ctes
            ):
                raise ValueError("Query expressions cannot introduce physical data sources")
        if isinstance(node, (exp.From, exp.Join)) and not isinstance(node.this, (exp.Table, exp.Subquery)):
            raise ValueError("Query expressions cannot introduce physical data sources")
        if isinstance(node, (exp.Command, exp.DDL, exp.DML, exp.Into)):
            raise ValueError("Query expressions cannot introduce physical data sources")
        if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func):
            raise ValueError("Qualified functions are not allowed in query expressions")
        if isinstance(node, exp.Func) and not isinstance(node, (exp.And, exp.Or, exp.Xor)):
            name = node.name if isinstance(node, exp.Anonymous) else node.sql_name()
            # Unknown/UDF functions can read files or execute SQL despite having
            # scalar syntax. Expose those through trusted model expressions only.
            if name.upper() not in _QUERY_SCALAR_FUNCTIONS:
                raise ValueError(f"Function {name} is not allowed in query expressions")
    return expression if order_by else expression.this


_QUERY_SCALAR_FUNCTIONS = frozenset(
    "ABS ACOS ASIN ATAN ATAN2 CEIL CEILING FLOOR ROUND SIGN SQRT CBRT POWER POW EXP LN LOG LOG2 LOG10 "
    "SIN COS TAN COT DEGREES RADIANS PI MOD GREATEST LEAST COALESCE NULLIF IF IIF CASE CAST TRY_CAST "
    "LOWER UPPER LENGTH CHAR_LENGTH CHARACTER_LENGTH CONCAT CONCAT_WS SUBSTRING SUBSTR LEFT RIGHT "
    "TRIM LTRIM RTRIM REPLACE REPEAT REVERSE LPAD RPAD SPLIT SPLIT_PART STARTS_WITH ENDS_WITH "
    "CONTAINS POSITION STR_POSITION REGEXP_LIKE REGEXP_REPLACE REGEXP_EXTRACT REGEXP_SPLIT "
    "COUNT SUM AVG MIN MAX MEDIAN STDDEV STDDEV_POP STDDEV_SAMP VARIANCE VAR_POP VAR_SAMP "
    "DATE TIME TIMESTAMP DATE_TRUNC TIMESTAMP_TRUNC DATETIME_TRUNC TIME_TRUNC DATE_ADD DATE_SUB "
    "DATE_DIFF DATEDIFF TIMESTAMP_ADD TIMESTAMP_SUB TIMESTAMP_DIFF EXTRACT YEAR MONTH DAY "
    "DAY_OF_MONTH DAY_OF_WEEK DAY_OF_YEAR WEEK WEEK_OF_YEAR QUARTER HOUR MINUTE SECOND "
    "CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_DATETIME TIME_TO_STR STR_TO_TIME "
    "TS_OR_DS_TO_DATE TS_OR_DS_TO_TIMESTAMP TS_OR_DS_TO_DATE_STR TIME_TO_UNIX UNIX_TO_TIME "
    "DATE_TO_DATE_STR LAST_DAY DATE_FROM_PARTS TIMESTAMP_FROM_PARTS INTERVAL "
    "ARRAY ARRAY_SIZE ARRAY_LENGTH ARRAY_CONTAINS ARRAY_SLICE ARRAY_TO_STRING "
    "JSON_EXTRACT JSON_EXTRACT_SCALAR JSONB_EXTRACT JSONB_EXTRACT_SCALAR JSON_TYPE "
    "STRUCT MAP EXISTS ISNULL IFNULL NVL".split()
)


def _quoted_end(sql: str, start: int, closing: str) -> int:
    i = start + 1
    while i < len(sql):
        if sql[i] == closing:
            if i + 1 < len(sql) and sql[i + 1] == closing:
                i += 2
                continue
            return i + 1
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
