"""The OSSIE_SQL_2026 expression dialect, independently of warehouse SQL.

The source grammar uses ANSI identifiers and the argument order in Ossie's
expression_language.md. Snowflake's parser recognizes that grammar, but its
session-dependent and vendor-specific function semantics are not the contract.
Unknown functions are preserved as extensions, as the proposal requires.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.errors import ErrorLevel, SqlglotError
from sqlglot.tokens import TokenType

_TARGETS = {"postgresql": "postgres", "ansi_sql": "duckdb", "ansi": "duckdb"}
_SUPPORTED_TARGETS = {"duckdb", "postgres", "snowflake", "bigquery", "databricks"}
_EXTRACTIONS = {
    exp.Year: "YEAR",
    exp.Quarter: "QUARTER",
    exp.Month: "MONTH",
    exp.Day: "DAY",
    exp.DayOfYear: "DAYOFYEAR",
    exp.Hour: "HOUR",
    exp.Minute: "MINUTE",
    exp.Second: "SECOND",
}
_PARTS = {
    "YEAR",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "HOUR",
    "MINUTE",
    "SECOND",
    "MILLISECOND",
}


def _ansi_value_frames(sql: str) -> str:
    """Spell out ANSI defaults before the source parser adds Snowflake frames.

    Token offsets let us insert only the missing frame, preserving every other
    source function and literal. In particular, this avoids round-tripping the
    entire expression through an unrelated SQL dialect.
    """
    tokens = Snowflake().tokenize(sql)
    additions = []
    for index, token in enumerate(tokens):
        if token.text.upper() not in {"FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"} or token.token_type in {
            TokenType.STRING,
            TokenType.IDENTIFIER,
        }:
            continue
        cursor = index + 1
        if cursor >= len(tokens) or tokens[cursor].token_type != TokenType.L_PAREN:
            continue
        depth = 0
        while cursor < len(tokens):
            depth += (tokens[cursor].token_type == TokenType.L_PAREN) - (tokens[cursor].token_type == TokenType.R_PAREN)
            cursor += 1
            if depth == 0:
                break
        if (
            cursor + 1 >= len(tokens)
            or tokens[cursor].token_type != TokenType.OVER
            or tokens[cursor + 1].token_type != TokenType.L_PAREN
        ):
            continue
        cursor += 2
        depth, has_frame, ordered = 1, False, False
        while cursor < len(tokens) and depth:
            current = tokens[cursor]
            depth += (current.token_type == TokenType.L_PAREN) - (current.token_type == TokenType.R_PAREN)
            if depth == 1:
                has_frame |= current.text.upper() in {"ROWS", "RANGE", "GROUPS"}
                ordered |= current.token_type == TokenType.ORDER_BY
            if depth == 0 and not has_frame:
                frame = (
                    " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                    if ordered
                    else " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                )
                additions.append((current.start, frame))
            cursor += 1
    for position, frame in sorted(additions, reverse=True):
        sql = sql[:position] + frame + sql[position:]
    return sql


def parse_portable_expression(expression: str) -> exp.Expression:
    """Parse one expression and reject query/statement constructs before lowering."""
    try:
        expressions = sqlglot.parse(_ansi_value_frames(expression), read="snowflake")
    except SqlglotError as exc:
        raise ValueError(f"Invalid OSSIE_SQL_2026 expression: {exc}") from exc
    if len(expressions) != 1 or expressions[0] is None:
        raise ValueError("OSSIE_SQL_2026 requires exactly one expression")
    root = expressions[0]
    if isinstance(root, (exp.Alias, exp.Star, exp.Tuple)):
        raise ValueError("OSSIE_SQL_2026 requires a scalar expression without an alias")
    for node in root.walk():
        if isinstance(
            node,
            (
                exp.Query,
                exp.DDL,
                exp.Drop,
                exp.Transaction,
                exp.Commit,
                exp.Rollback,
                exp.DML,
                exp.Command,
                exp.Where,
                exp.Group,
                exp.Join,
                exp.With,
                exp.Array,
                exp.Bracket,
            ),
        ):
            raise ValueError(f"OSSIE_SQL_2026 expressions cannot contain {type(node).__name__}")
        if isinstance(node, exp.Identifier) and len(node.name) > 128:
            raise ValueError("OSSIE_SQL_2026 identifiers cannot exceed 128 characters")
        if isinstance(node, exp.Column) and len(node.parts) > 2:
            raise ValueError("OSSIE_SQL_2026 field references have at most two identifiers")
    return root


def _call(name: str, *args: exp.Expression) -> exp.Expression:
    return exp.Anonymous(this=name, expressions=[arg.copy() for arg in args])


def _part(node: exp.Expression) -> str:
    part = node.name.upper()
    if part not in _PARTS:
        raise ValueError(f"Unsupported OSSIE_SQL_2026 date part {part!r}")
    return part


def supports_ossie_sql_target(target_dialect: str | None) -> bool:
    target = (target_dialect or "duckdb").lower()
    return _TARGETS.get(target, target) in _SUPPORTED_TARGETS


def lower_ossie_sql(expression: str, target_dialect: str | None) -> str:
    """Translate portable SQL without silently approximating required operations.

    A warehouse limitation raises ValueError instead of publishing SQL that has
    a different meaning. BigQuery exact ordered-set aggregates require a query
    rewrite (its exact percentiles are analytic-only), outside scalar lowering.
    """
    target = _TARGETS.get((target_dialect or "duckdb").lower(), (target_dialect or "duckdb").lower())
    if not supports_ossie_sql_target(target):
        raise ValueError(f"Unsupported OSSIE_SQL_2026 target {target_dialect!r}")
    root = parse_portable_expression(expression)

    def rewrite(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Anonymous) and node.name.upper() == "TO_TIMESTAMP":
            if len(node.expressions) == 1:
                return exp.Cast(this=node.expressions[0].copy(), to=exp.DataType.build("TIMESTAMPNTZ"))
        if type(node) in _EXTRACTIONS:
            part = _EXTRACTIONS[type(node)]
            if part == "DAYOFYEAR" and target == "postgres":
                part = "DOY"
            return exp.Extract(this=exp.Var(this=part), expression=node.this.copy())
        if isinstance(node, exp.Extract):
            part = _part(node.this)
            if target == "postgres":
                part = {"DAYOFYEAR": "DOY", "DAYOFWEEK": "DOW"}.get(part, part)
            return exp.Extract(this=exp.Var(this=part), expression=node.expression.copy())
        if isinstance(node, exp.Log) and node.expression is not None:
            # DuckDB has only the unary base-ten LOG; division also avoids
            # PostgreSQL's numeric-only two-argument LOG overload.
            return exp.Paren(
                this=exp.Div(
                    this=exp.Ln(this=node.expression.copy()), expression=exp.Ln(this=node.this.copy()), safe=False
                )
            )
        if isinstance(node, exp.Trunc):
            decimals = node.args.get("decimals") or exp.Literal.number(0)
            # Multiplying by POWER before FLOOR loses decimal digits (1.15 *
            # 100 becomes 114.999...). ROUND retains DECIMAL arithmetic; undo
            # its one-unit adjustment only when it rounded away from zero.
            rounded = exp.Round(this=node.this.copy(), decimals=decimals.copy())
            places = decimals.sql()
            if places.lstrip("-").isdigit() and abs(int(places)) <= 38:
                count = int(places)
                unit = "0." + "0" * (count - 1) + "1" if count > 0 else "1" + "0" * -count
                quantum = exp.Literal.number(unit)
            else:
                quantum = exp.Pow(this=exp.Literal.number(10), expression=exp.Neg(this=decimals.copy()))
            return exp.If(
                this=exp.GT(this=exp.Abs(this=rounded.copy()), expression=exp.Abs(this=node.this.copy())),
                true=exp.Sub(
                    this=rounded.copy(), expression=exp.Mul(this=exp.Sign(this=node.this.copy()), expression=quantum)
                ),
                false=rounded,
            )
        if isinstance(node, exp.Contains):
            return exp.GT(
                this=exp.StrPosition(this=node.this.copy(), substr=node.expression.copy()),
                expression=exp.Literal.number(0),
            )
        if isinstance(node, (exp.StartsWith, exp.EndsWith)):
            side = exp.Left if isinstance(node, exp.StartsWith) else exp.Right
            return exp.EQ(
                this=side(this=node.this.copy(), expression=exp.Length(this=node.expression.copy())),
                expression=node.expression.copy(),
            )
        if isinstance(node, exp.RegexpLike):
            # The portable spelling denotes a match, not Snowflake's implicit
            # whole-string anchoring. Make search behavior explicit there.
            node.set("full_match", False)
            if target == "snowflake":
                return exp.GT(this=_call("REGEXP_INSTR", node.this, node.expression), expression=exp.Literal.number(0))
        if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
            part = _part(node.args["unit"])
            if part in {"DAYOFWEEK", "DAYOFYEAR", "MILLISECOND"}:
                raise ValueError(f"Unsupported OSSIE_SQL_2026 truncation part {part!r}")
            if part == "WEEK" and target == "bigquery":
                return _call("DATE_TRUNC", node.this, _call("WEEK", exp.Var(this="MONDAY")))
            if part == "WEEK" and target == "snowflake":
                # WEEK_START is a session option; ISO weekdays always start Monday.
                return sqlglot.parse_one(
                    f"DATEADD(day, 1 - DAYOFWEEKISO({node.this.sql(dialect=target)}), "
                    f"DATE_TRUNC('day', {node.this.sql(dialect=target)}))",
                    read=target,
                )
        if isinstance(node, exp.DateDiff) and target == "postgres":
            part = _part(node.args["unit"])
            start, end = node.expression.copy(), node.this.copy()

            def extract(value: exp.Expression, unit: str) -> exp.Expression:
                return exp.Extract(this=exp.Var(this=unit), expression=value.copy())

            if part in {"YEAR", "MONTH", "QUARTER"}:
                years = exp.Sub(this=extract(end, "YEAR"), expression=extract(start, "YEAR"))
                if part == "YEAR":
                    return years
                unit = "MONTH" if part == "MONTH" else "QUARTER"
                return exp.Paren(
                    this=exp.Add(
                        this=exp.Mul(
                            this=exp.Paren(this=years), expression=exp.Literal.number(12 if part == "MONTH" else 4)
                        ),
                        expression=exp.Sub(this=extract(end, unit), expression=extract(start, unit)),
                    )
                )
            if part in {"DAY", "HOUR", "MINUTE", "SECOND"}:
                # DATEDIFF counts boundaries, not elapsed whole units. Truncate
                # both endpoints before subtracting, including for negative spans.
                def truncate(value: exp.Expression) -> exp.Expression:
                    return _call(
                        "DATE_TRUNC",
                        exp.Literal.string(part.lower()),
                        exp.Cast(this=value, to=exp.DataType.build("TIMESTAMP")),
                    )

                seconds = exp.Paren(
                    this=exp.Sub(this=extract(truncate(end), "EPOCH"), expression=extract(truncate(start), "EPOCH"))
                )
                return exp.Paren(
                    this=exp.Div(
                        this=seconds,
                        expression=exp.Literal.number({"DAY": 86400, "HOUR": 3600, "MINUTE": 60, "SECOND": 1}[part]),
                        safe=False,
                        typed=True,
                    )
                )
        if isinstance(node, exp.WithinGroup) and isinstance(node.this, (exp.PercentileCont, exp.PercentileDisc)):
            if target == "bigquery":
                raise ValueError("BigQuery exact ordered-set percentiles require query-level lowering")
            if target == "databricks":
                # SQLGlot maps this to PERCENTILE_APPROX, which is not equivalent.
                node.set(
                    "this",
                    _call(
                        "PERCENTILE_CONT" if isinstance(node.this, exp.PercentileCont) else "PERCENTILE_DISC",
                        node.this.this,
                    ),
                )
        if isinstance(node, exp.Median) and target == "bigquery":
            raise ValueError("BigQuery exact MEDIAN requires query-level lowering")
        return node

    # Bottom-up replacement keeps corrections inside compound expressions.
    for node in reversed(list(root.walk())):
        replacement = rewrite(node)
        if node is root:
            root = replacement
        elif replacement is not node:
            node.replace(replacement)
    try:
        return root.sql(dialect=target, unsupported_level=ErrorLevel.RAISE)
    except SqlglotError as exc:
        raise ValueError(f"OSSIE_SQL_2026 cannot be lowered to {target}: {exc}") from exc
