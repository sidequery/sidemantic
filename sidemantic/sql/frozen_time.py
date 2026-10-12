"""Replace current-time SQL functions with literals fixed at compile time.

Storage planners (for example DuckDB's Iceberg scan) prune files only when a
filter compares a column with a constant. ``CURRENT_DATE``/``now()`` are
query-start-stable functions, so substituting one compile-time instant for
every occurrence in a query preserves query semantics while exposing constants
to the planner.

Rewrites are applied to token spans: everything outside a replaced call (and
any directly folded ``+``/``-`` arithmetic) stays byte-for-byte unchanged,
including comments such as the ``-- sidemantic:`` routing marker.

Timezone handling mirrors DuckDB:

- ``now()``/``CURRENT_TIMESTAMP`` (DuckDB type TIMESTAMPTZ) become a TIMESTAMPTZ
  literal of the UTC instant. Comparisons against naive TIMESTAMP columns still
  cast the column through the session timezone exactly as before.
- ``CURRENT_DATE``/``today()`` (DuckDB type DATE, evaluated in the session
  timezone) become a DATE literal of today's date in ``timezone``, which
  defaults to the process's local timezone (DuckDB's default ``TimeZone``).
- ``CURRENT_DATE +/- <integer>`` folds to a DATE literal and
  ``CURRENT_DATE +/- INTERVAL n day|week|hour|minute|second`` folds to a naive
  TIMESTAMP literal, matching DuckDB's result types. TIMESTAMPTZ values fold
  only exact hour/minute/second intervals because day arithmetic on
  TIMESTAMPTZ follows session-timezone calendar days. Calendar units (month,
  quarter, year) are never folded; the substituted literal leaves a constant
  expression the engine folds itself.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, tzinfo

from sqlglot import exp
from sqlglot.dialects import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.tokens import Token, TokenType

# Cheap pre-check so queries without time functions skip tokenization.
_CANDIDATE_PATTERN = re.compile(
    r"\b(?:current_date|current_timestamp|now|today|get_current_timestamp|transaction_timestamp)\b",
    re.IGNORECASE,
)

# Function name -> whether the bare keyword form (no parentheses) is a call.
_DATE_FUNCTIONS = {"CURRENT_DATE": True, "TODAY": False}
_TIMESTAMP_FUNCTIONS = {
    "CURRENT_TIMESTAMP": True,
    "NOW": False,
    "GET_CURRENT_TIMESTAMP": False,
    "TRANSACTION_TIMESTAMP": False,
}

_EXACT_UNITS = {
    "SECOND": timedelta(seconds=1),
    "MINUTE": timedelta(minutes=1),
    "HOUR": timedelta(hours=1),
    "DAY": timedelta(days=1),
    "WEEK": timedelta(weeks=1),
}
_SUB_DAY_UNITS = frozenset({"SECOND", "MINUTE", "HOUR"})
_INTERVAL_STRING = re.compile(r"^\s*([+-]?\d+)\s*(?:([a-z]+?)s?)?\s*$", re.IGNORECASE)


def _token_types(*names: str) -> frozenset[TokenType]:
    return frozenset(token_type for name in names if (token_type := getattr(TokenType, name, None)) is not None)


_CALL_NAME_TYPES = _token_types("VAR", "CURRENT_DATE", "CURRENT_TIMESTAMP")
_QUERY_START_TYPES = _token_types("SELECT", "WITH", "FROM", "L_PAREN")
# Arithmetic binds tighter than these neighbours, so ``<call> +/- <operand>``
# can be folded without changing how the surrounding expression parses.
_PRECEDING_FOLD_SAFE = _token_types(
    "L_PAREN", "COMMA", "AND", "OR", "NOT", "EQ", "NEQ", "GT", "GTE", "LT", "LTE", "NULLSAFE_EQ",
    "BETWEEN", "IN", "IS", "WHEN", "THEN", "ELSE", "SELECT", "WHERE", "HAVING", "ON", "QUALIFY", "FROM",
)  # fmt: skip
_FOLLOWING_FOLD_SAFE = _token_types(
    "R_PAREN", "COMMA", "AND", "OR", "NOT", "EQ", "NEQ", "GT", "GTE", "LT", "LTE", "NULLSAFE_EQ",
    "BETWEEN", "IN", "IS", "PLUS", "DASH", "WHEN", "THEN", "ELSE", "END", "ALIAS", "SEMICOLON",
    "ORDER_BY", "GROUP_BY", "LIMIT", "HAVING", "QUALIFY", "WINDOW", "UNION", "EXCEPT", "INTERSECT",
    "FROM", "WHERE",
)  # fmt: skip


@dataclass(frozen=True)
class _Value:
    """A frozen SQL value and the DuckDB type it represents."""

    kind: str  # "date", "timestamp" (naive), or "timestamptz"
    value: date | datetime

    def shifted(self, sign: int, unit: str | None, count: int) -> _Value | None:
        """Return ``self +/- count units`` or ``None`` when folding is not exact."""
        if unit is None:
            # DATE +/- INTEGER adds days and stays a DATE; other types reject it.
            if self.kind != "date":
                return None
            return _Value("date", self.value + sign * timedelta(days=count))
        step = _EXACT_UNITS.get(unit)
        if step is None or (self.kind == "timestamptz" and unit not in _SUB_DAY_UNITS):
            return None
        if self.kind == "date":
            start = datetime.combine(self.value, datetime.min.time())
            return _Value("timestamp", start + sign * count * step)
        return _Value(self.kind, self.value + sign * count * step)

    def sql(self, dialect: str) -> str:
        if self.kind == "date":
            text, data_type = self.value.isoformat(), exp.DataType.Type.DATE
        elif self.kind == "timestamp":
            text, data_type = _timestamp_text(self.value), exp.DataType.Type.TIMESTAMP
        else:
            text = self.value.astimezone(UTC).isoformat(sep=" ", timespec="microseconds")
            data_type = exp.DataType.Type.TIMESTAMPTZ
        return exp.cast(exp.Literal.string(text), data_type).sql(dialect=dialect)


def _timestamp_text(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="microseconds" if value.microsecond else "seconds")


def freeze_current_time(
    sql: str,
    dialect: str,
    *,
    now: datetime | None = None,
    timezone: tzinfo | None = None,
) -> str:
    """Return ``sql`` with current-time calls replaced by one frozen instant.

    Args:
        sql: A single SQL query. Statements that are not queries (DDL/DML, or
            multiple statements) are returned unchanged so a frozen literal is
            never persisted into a view or table definition.
        dialect: SQLGlot dialect used to tokenize the query and render literals.
        now: The instant the query is frozen at (defaults to the current time).
            A naive value is interpreted as UTC.
        timezone: Session timezone used to derive ``CURRENT_DATE``
            (defaults to the process's local timezone).

    Returns:
        The rewritten SQL, or the original SQL when nothing can be replaced or
        the query cannot be tokenized.
    """
    if not _CANDIDATE_PATTERN.search(sql):
        return sql
    try:
        tokens = Dialect.get_or_raise(dialect).tokenize(sql)
    except SqlglotError:
        return sql
    if not _is_single_query(tokens):
        return sql

    if now is None:
        now = datetime.now(UTC)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    today = now.astimezone(timezone).date()

    replacements: list[tuple[int, int, str]] = []
    index = 0
    while index < len(tokens):
        call = _match_call(tokens, index)
        if call is None:
            index += 1
            continue
        kind, call_end = call
        value = _Value("date", today) if kind == "date" else _Value("timestamptz", now)
        value, span_end = _fold_arithmetic(tokens, index, call_end, value)
        replacements.append((tokens[index].start, tokens[span_end].end + 1, value.sql(dialect)))
        index = span_end + 1

    rewritten = sql
    for start, end, text in reversed(replacements):
        rewritten = rewritten[:start] + text + rewritten[end:]
    return rewritten


def _is_single_query(tokens: list[Token]) -> bool:
    if not tokens or tokens[0].token_type not in _QUERY_START_TYPES:
        return False
    semicolons = [i for i, token in enumerate(tokens) if token.token_type == TokenType.SEMICOLON]
    return not semicolons or semicolons == [len(tokens) - 1]


def _match_call(tokens: list[Token], index: int) -> tuple[str, int] | None:
    """Match a current-time call at ``index``; return ``(kind, last_token_index)``."""
    token = tokens[index]
    if token.token_type not in _CALL_NAME_TYPES:
        return None
    name = token.text.upper()
    if name in _DATE_FUNCTIONS:
        kind, bare_ok = "date", _DATE_FUNCTIONS[name]
    elif name in _TIMESTAMP_FUNCTIONS:
        kind, bare_ok = "timestamptz", _TIMESTAMP_FUNCTIONS[name]
    else:
        return None
    previous = tokens[index - 1].token_type if index > 0 else None
    if previous in (TokenType.DOT, TokenType.ALIAS):
        return None  # qualified column or output alias, not a call

    following = tokens[index + 1].token_type if index + 1 < len(tokens) else None
    if following == TokenType.L_PAREN:
        # Arguments (for example a CURRENT_TIMESTAMP precision) are left untouched.
        if index + 2 < len(tokens) and tokens[index + 2].token_type == TokenType.R_PAREN:
            return kind, index + 2
        return None
    return (kind, index) if bare_ok else None


def _fold_arithmetic(tokens: list[Token], call_start: int, call_end: int, value: _Value) -> tuple[_Value, int]:
    """Fold ``<call> +/- <integer | INTERVAL n unit>`` chains when precedence allows."""
    previous = tokens[call_start - 1].token_type if call_start > 0 else None
    if previous is not None and previous not in _PRECEDING_FOLD_SAFE:
        return value, call_end

    best_value, best_end = value, call_end
    position = call_end
    while position + 2 < len(tokens) and tokens[position + 1].token_type in (TokenType.PLUS, TokenType.DASH):
        sign = 1 if tokens[position + 1].token_type == TokenType.PLUS else -1
        operand = _parse_operand(tokens, position + 2)
        if operand is None:
            break
        unit, count, operand_end = operand
        shifted = value.shifted(sign, unit, count)
        if shifted is None:
            break
        value, position = shifted, operand_end
        following = tokens[position + 1].token_type if position + 1 < len(tokens) else None
        if following is None or following in _FOLLOWING_FOLD_SAFE:
            best_value, best_end = value, position
    return best_value, best_end


def _parse_operand(tokens: list[Token], index: int) -> tuple[str | None, int, int] | None:
    """Parse ``<integer>`` or ``INTERVAL n [unit]``; return ``(unit, count, last_index)``."""
    token = tokens[index]
    if token.token_type == TokenType.NUMBER:
        return (None, int(token.text), index) if token.text.isdigit() else None
    if token.token_type != TokenType.INTERVAL or index + 1 >= len(tokens):
        return None

    amount = tokens[index + 1]
    unit_token = tokens[index + 2] if index + 2 < len(tokens) else None
    separate_unit = None
    if unit_token is not None and unit_token.token_type not in (TokenType.STRING, TokenType.IDENTIFIER):
        candidate = unit_token.text.upper().removesuffix("S")
        if candidate in _EXACT_UNITS or candidate in {"MONTH", "QUARTER", "YEAR", "MILLISECOND", "MICROSECOND"}:
            separate_unit = candidate

    if amount.token_type == TokenType.NUMBER and amount.text.isdigit() and separate_unit:
        return separate_unit, int(amount.text), index + 2
    if amount.token_type == TokenType.STRING:
        match = _INTERVAL_STRING.match(amount.text)
        if match is None:
            return None
        inline_unit = match.group(2).upper() if match.group(2) else None
        if inline_unit and separate_unit:
            return None
        unit = inline_unit or separate_unit
        if unit is None:
            return None
        return unit, int(match.group(1)), index + 2 if separate_unit else index + 1
    return None
