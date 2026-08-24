"""Typed, fail-closed lowering for a bounded subset of Malloy expressions.

This module deliberately does not parse Malloy source text.  The Malloy parse-tree
visitor is expected to construct the typed nodes below and call
:func:`lower_expression`.  Keeping the lowering boundary typed prevents unknown
Malloy syntax from leaking into generated SQL as an unvalidated string.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Literal as TypingLiteral
from typing import TypeAlias

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError

Capability: TypeAlias = TypingLiteral["arrays", "date_trunc", "intervals", "regex"]
DiagnosticStatus: TypeAlias = TypingLiteral["unsupported", "rejected"]
DialectName: TypeAlias = TypingLiteral["duckdb", "postgres", "bigquery", "snowflake"]
SemanticType: TypeAlias = TypingLiteral[
    "array", "boolean", "date", "null", "number", "string", "time", "timestamp", "unknown"
]

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQL_RESERVED_WORDS = {
    "and",
    "as",
    "case",
    "else",
    "end",
    "false",
    "from",
    "group",
    "having",
    "is",
    "join",
    "not",
    "null",
    "on",
    "or",
    "order",
    "select",
    "then",
    "true",
    "when",
    "where",
}
_BINARY_PRECEDENCE = {
    "or": 10,
    "and": 20,
    "=": 30,
    "!=": 30,
    "<>": 30,
    "<": 30,
    "<=": 30,
    ">": 30,
    ">=": 30,
    "+": 40,
    "-": 40,
    "*": 50,
    "/": 50,
    "%": 50,
}
_SQL_BINARY_OPERATOR = {"and": "AND", "or": "OR"}
_COMPARISON_OPERATORS = frozenset({"=", "!=", "<>", "<", "<=", ">", ">="})
_CAST_TYPES = {
    "boolean",
    "date",
    "decimal",
    "double",
    "integer",
    "text",
    "time",
    "timestamp",
}
_DATE_TRUNC_UNITS = {"year", "quarter", "month", "week", "day", "hour", "minute", "second"}
_DURATION_UNITS = {"day", "hour", "minute", "second"}
_STANDARD_FUNCTION_ARITIES: dict[str, tuple[int, int | None]] = {
    "abs": (1, 1),
    "ceil": (1, 1),
    "concat": (2, None),
    "coalesce": (2, None),
    "exp": (1, 1),
    "floor": (1, 1),
    "greatest": (2, None),
    "least": (2, None),
    "length": (1, 1),
    "ln": (1, 1),
    "lower": (1, 1),
    "nullif": (2, 2),
    "power": (2, 2),
    "replace": (3, 3),
    "round": (1, 2),
    "sqrt": (1, 1),
    "substring": (2, 3),
    "trim": (1, 1),
    "upper": (1, 1),
}
_DIALECTS = frozenset({"duckdb", "postgres", "bigquery", "snowflake"})
_CAST_SQL_TYPES: dict[str, dict[str, str]] = {
    "duckdb": {
        "boolean": "BOOLEAN",
        "date": "DATE",
        "decimal": "DECIMAL",
        "double": "DOUBLE",
        "integer": "INTEGER",
        "text": "VARCHAR",
        "time": "TIME",
        "timestamp": "TIMESTAMP",
    },
    "postgres": {
        "boolean": "BOOLEAN",
        "date": "DATE",
        "decimal": "NUMERIC",
        "double": "DOUBLE PRECISION",
        "integer": "INTEGER",
        "text": "TEXT",
        "time": "TIME",
        "timestamp": "TIMESTAMP",
    },
    "bigquery": {
        "boolean": "BOOL",
        "date": "DATE",
        "decimal": "NUMERIC",
        "double": "FLOAT64",
        "integer": "INT64",
        "text": "STRING",
        "time": "TIME",
        "timestamp": "TIMESTAMP",
    },
    "snowflake": {
        "boolean": "BOOLEAN",
        "date": "DATE",
        "decimal": "NUMBER",
        "double": "DOUBLE",
        "integer": "INTEGER",
        "text": "VARCHAR",
        "time": "TIME",
        "timestamp": "TIMESTAMP_NTZ",
    },
}
_CAST_SEMANTIC_TYPES: dict[str, SemanticType] = {
    "boolean": "boolean",
    "date": "date",
    "decimal": "number",
    "double": "number",
    "integer": "number",
    "text": "string",
    "time": "time",
    "timestamp": "timestamp",
}
_SEMANTIC_TYPES = frozenset({"array", "boolean", "date", "null", "number", "string", "time", "timestamp", "unknown"})


@dataclass(frozen=True)
class ExpressionDiagnostic:
    """Structured reason that an expression cannot be lowered faithfully."""

    feature: str
    status: DiagnosticStatus
    detail: str
    required_capability: Capability | None = None


class MalloyExpressionLoweringError(ValueError):
    """Raised instead of returning approximate or raw Malloy syntax."""

    def __init__(self, diagnostic: ExpressionDiagnostic):
        self.diagnostic = diagnostic
        super().__init__(f"{diagnostic.feature}: {diagnostic.detail}")


@dataclass(frozen=True)
class FieldPath:
    parts: tuple[str, ...]
    semantic_type: SemanticType = "unknown"

    def __post_init__(self) -> None:
        if not self.parts or any(not part for part in self.parts):
            raise ValueError("A field path must contain non-empty path components")
        if self.semantic_type not in _SEMANTIC_TYPES:
            raise ValueError(f"Unknown field semantic type {self.semantic_type!r}")


ScalarValue: TypeAlias = None | bool | int | float | Decimal | str


@dataclass(frozen=True)
class Literal:
    value: ScalarValue


@dataclass(frozen=True)
class DateLiteral:
    value: date | str


@dataclass(frozen=True)
class TimestampLiteral:
    value: datetime | str


@dataclass(frozen=True)
class UnaryExpression:
    operator: TypingLiteral["not", "+", "-"]
    operand: Expression


@dataclass(frozen=True)
class BinaryExpression:
    left: Expression
    operator: str
    right: Expression


@dataclass(frozen=True)
class CastExpression:
    expression: Expression
    target_type: str


@dataclass(frozen=True)
class DateTruncExpression:
    unit: str
    expression: Expression


@dataclass(frozen=True)
class Duration:
    value: int
    unit: str


@dataclass(frozen=True)
class DurationArithmetic:
    temporal: Expression
    operator: TypingLiteral["+", "-"]
    duration: Duration


@dataclass(frozen=True)
class CoalesceExpression:
    expressions: tuple[Expression, ...]


@dataclass(frozen=True)
class CaseBranch:
    condition: Expression
    value: Expression


@dataclass(frozen=True)
class CaseExpression:
    branches: tuple[CaseBranch, ...]
    otherwise: Expression | None = None


@dataclass(frozen=True)
class RegexExpression:
    expression: Expression
    pattern: str
    negated: bool = False


@dataclass(frozen=True)
class FunctionExpression:
    name: str
    arguments: tuple[Expression, ...]


@dataclass(frozen=True)
class ArrayExpression:
    values: tuple[Expression, ...]


@dataclass(frozen=True)
class RejectedExpression:
    """Parse-tree marker for syntax that must never be passed through as SQL."""

    feature: TypingLiteral[
        "all",
        "exclude",
        "filter_string",
        "locality",
        "parameter",
        "range",
        "raw_function",
        "record",
        "safe_cast",
    ]
    detail: str | None = None


Expression: TypeAlias = (
    FieldPath
    | Literal
    | DateLiteral
    | TimestampLiteral
    | UnaryExpression
    | BinaryExpression
    | CastExpression
    | DateTruncExpression
    | DurationArithmetic
    | CoalesceExpression
    | CaseExpression
    | RegexExpression
    | FunctionExpression
    | ArrayExpression
    | RejectedExpression
)


@dataclass(frozen=True)
class LoweredExpression:
    """Canonical SQL plus the dialect capabilities needed to execute it."""

    sql: str
    expression: exp.Expression = field(compare=False, repr=False)
    required_capabilities: frozenset[Capability] = frozenset()
    semantic_type: SemanticType = "unknown"

    @property
    def result_type(self) -> SemanticType:
        """Compatibility-friendly name for callers which treat this as a typed result."""

        return self.semantic_type


def lower_expression(
    expression: Expression,
    *,
    capabilities: frozenset[Capability] | None = None,
    dialect: DialectName | None = None,
) -> LoweredExpression:
    """Lower a typed Malloy expression to canonical, target-neutral SQL.

    When ``capabilities`` is omitted, required capabilities are returned to the
    caller for later dialect validation.  When supplied, a missing capability
    fails closed with a structured diagnostic.
    """

    if dialect is not None and dialect not in _DIALECTS:
        _reject("dialect", f"Unsupported target dialect {dialect!r}")
    semantic_type = infer_expression_type(expression, dialect=dialect)
    sql, _precedence, required = _lower(expression, dialect=dialect)
    parse_dialect = dialect or "duckdb"
    parser_sql = sql.replace("\\`", "``") if dialect == "bigquery" else sql
    try:
        executable = parse_one(parser_sql, read=parse_dialect)
    except (SqlglotError, IndexError) as exc:
        _reject(
            "generated_sql",
            f"Generated expression is not executable {parse_dialect} SQL: {sql!r}: {exc}",
        )
    # Dialect-targeted output is serialized from the executable canonical tree,
    # never returned directly from the intermediate renderer.  The dialect-free
    # API retains its stable target-neutral spelling for compatibility.
    rendered_sql = executable.sql(dialect=dialect) if dialect is not None else sql
    if dialect == "bigquery":
        rendered_sql = rendered_sql.replace("``", "\\`")
    # sqlglot canonicalizes ``x IS NOT NULL`` to ``NOT x IS NULL``. Preserve
    # the standard predicate spelling required by every supported warehouse.
    if " IS NOT NULL" in sql:
        rendered_sql = sql
    result = LoweredExpression(
        sql=rendered_sql,
        expression=executable,
        required_capabilities=frozenset(required),
        semantic_type=semantic_type,
    )
    if capabilities is not None:
        missing = sorted(result.required_capabilities - capabilities)
        if missing:
            capability = missing[0]
            raise MalloyExpressionLoweringError(
                ExpressionDiagnostic(
                    feature="dialect_capability",
                    status="unsupported",
                    detail=f"The target dialect does not declare the required {capability!r} capability",
                    required_capability=capability,
                )
            )
    return result


def _lower(expression: Expression, *, dialect: DialectName | None) -> tuple[str, int, set[Capability]]:
    if isinstance(expression, RejectedExpression):
        detail = expression.detail or f"Malloy {expression.feature.replace('_', ' ')} expressions are not supported"
        raise MalloyExpressionLoweringError(
            ExpressionDiagnostic(feature=expression.feature, status="rejected", detail=detail)
        )

    if isinstance(expression, FieldPath):
        return ".".join(_quote_identifier(part, dialect) for part in expression.parts), 100, set()

    if isinstance(expression, Literal):
        return _literal_sql(expression.value, dialect), 100, set()

    if isinstance(expression, DateLiteral):
        value = _parse_date(expression.value)
        return f"DATE '{value.isoformat()}'", 100, set()

    if isinstance(expression, TimestampLiteral):
        value = _parse_timestamp(expression.value)
        if value.tzinfo is not None:
            _reject("timestamp_timezone", "Timezone-aware Malloy timestamps require dialect-specific semantics")
        rendered = value.isoformat(sep=" ", timespec="microseconds").rstrip("0").rstrip(".")
        return f"TIMESTAMP '{rendered}'", 100, set()

    if isinstance(expression, UnaryExpression):
        if expression.operator not in {"not", "+", "-"}:
            _reject("unary_operator", f"Unsupported unary operator {expression.operator!r}")
        operand_sql, operand_precedence, required = _lower(expression.operand, dialect=dialect)
        precedence = 25 if expression.operator == "not" else 60
        operand_sql = _parenthesize(
            operand_sql,
            (expression.operator == "not" and isinstance(expression.operand, BinaryExpression))
            or operand_precedence < precedence
            or (operand_precedence == precedence and expression.operator in {"+", "-"}),
        )
        operator = "NOT " if expression.operator == "not" else expression.operator
        return f"{operator}{operand_sql}", precedence, required

    if isinstance(expression, BinaryExpression):
        operator = expression.operator.lower()
        if operator not in _BINARY_PRECEDENCE:
            _reject("binary_operator", f"Unsupported binary operator {expression.operator!r}")
        precedence = _BINARY_PRECEDENCE[operator]
        left_type = infer_expression_type(expression.left, dialect=dialect)
        right_type = infer_expression_type(expression.right, dialect=dialect)
        if "null" in {left_type, right_type}:
            if operator not in {"=", "!=", "<>"}:
                _reject("null_comparison", "NULL supports only equality and inequality comparisons")
            non_null = expression.right if left_type == "null" else expression.left
            non_null_sql, non_null_precedence, required = _lower(non_null, dialect=dialect)
            non_null_sql = _parenthesize(non_null_sql, non_null_precedence < precedence)
            negated = operator in {"!=", "<>"}
            return f"{non_null_sql} IS {'NOT ' if negated else ''}NULL", precedence, required
        left_sql, left_precedence, left_required = _lower(expression.left, dialect=dialect)
        right_sql, right_precedence, right_required = _lower(expression.right, dialect=dialect)
        left_sql = _parenthesize(
            left_sql,
            left_precedence < precedence or (operator in _COMPARISON_OPERATORS and left_precedence == precedence),
        )
        # Equal-precedence right children must remain grouped to preserve the AST
        # for subtraction, division, comparisons, and mixed boolean/arithmetic.
        right_sql = _parenthesize(right_sql, right_precedence <= precedence)
        sql_operator = _SQL_BINARY_OPERATOR.get(operator, expression.operator)
        return f"{left_sql} {sql_operator} {right_sql}", precedence, left_required | right_required

    if isinstance(expression, CastExpression):
        target_type = expression.target_type.lower()
        if target_type not in _CAST_TYPES:
            _reject("cast_type", f"Unsupported target-neutral cast type {expression.target_type!r}")
        sql, _precedence, required = _lower(expression.expression, dialect=dialect)
        sql_type = target_type.upper() if dialect is None else _CAST_SQL_TYPES[dialect][target_type]
        return f"CAST({sql} AS {sql_type})", 100, required

    if isinstance(expression, DateTruncExpression):
        unit = expression.unit.lower()
        if unit not in _DATE_TRUNC_UNITS:
            _reject("date_trunc_unit", f"Unsupported date truncation unit {expression.unit!r}")
        sql, _precedence, required = _lower(expression.expression, dialect=dialect)
        if dialect == "bigquery":
            value_type = infer_expression_type(expression.expression, dialect=dialect)
            if value_type == "unknown":
                _reject(
                    "date_trunc_type",
                    "BigQuery date truncation requires a resolved date or timestamp semantic type",
                )
            function = "DATE_TRUNC" if value_type == "date" else "TIMESTAMP_TRUNC"
            return f"{function}({sql}, {unit.upper()})", 100, required | {"date_trunc"}
        return f"DATE_TRUNC('{unit}', {sql})", 100, required | {"date_trunc"}

    if isinstance(expression, DurationArithmetic):
        if expression.operator not in {"+", "-"}:
            _reject("duration_operator", f"Unsupported duration operator {expression.operator!r}")
        if (
            isinstance(expression.duration.value, bool)
            or not isinstance(expression.duration.value, int)
            or expression.duration.value < 0
        ):
            _reject("duration_value", "Durations must use non-negative integer values")
        unit = expression.duration.unit.lower()
        if unit not in _DURATION_UNITS:
            _reject(
                "duration_unit",
                f"Duration unit {expression.duration.unit!r} is not in the exact fixed-duration subset",
            )
        temporal_sql, temporal_precedence, required = _lower(expression.temporal, dialect=dialect)
        temporal_sql = _parenthesize(temporal_sql, temporal_precedence < 40)
        value = expression.duration.value
        if dialect == "snowflake":
            signed_value = value if expression.operator == "+" else -value
            return f"DATEADD({unit}, {signed_value}, {temporal_sql})", 100, required | {"intervals"}
        if dialect == "bigquery":
            temporal_type = infer_expression_type(expression.temporal, dialect=dialect)
            if temporal_type == "unknown":
                _reject(
                    "duration_type",
                    "BigQuery duration arithmetic requires a resolved date or timestamp semantic type",
                )
            if temporal_type == "date" and unit != "day":
                _reject(
                    "duration_type",
                    f"BigQuery DATE arithmetic does not support sub-day {unit!r} durations; cast to TIMESTAMP explicitly",
                )
            function_prefix = "DATE" if temporal_type == "date" else "TIMESTAMP"
            operation = "ADD" if expression.operator == "+" else "SUB"
            return (
                f"{function_prefix}_{operation}({temporal_sql}, INTERVAL {value} {unit.upper()})",
                100,
                required | {"intervals"},
            )
        interval = f"INTERVAL '{value}' {unit.upper()}" if dialect is None else f"INTERVAL '{value} {unit.lower()}'"
        return f"{temporal_sql} {expression.operator} {interval}", 40, required | {"intervals"}

    if isinstance(expression, CoalesceExpression):
        if len(expression.expressions) < 2:
            _reject("coalesce_arity", "Malloy ?? requires at least two expressions")
        lowered = [_lower(item, dialect=dialect) for item in expression.expressions]
        return f"COALESCE({', '.join(item[0] for item in lowered)})", 100, _merge_requirements(lowered)

    if isinstance(expression, CaseExpression):
        if not expression.branches:
            _reject("case_arity", "Malloy pick/case requires at least one branch")
        required: set[Capability] = set()
        parts = ["CASE"]
        for branch in expression.branches:
            condition_sql, _condition_precedence, condition_required = _lower(branch.condition, dialect=dialect)
            value_sql, _value_precedence, value_required = _lower(branch.value, dialect=dialect)
            required |= condition_required | value_required
            parts.append(f"WHEN {condition_sql} THEN {value_sql}")
        if expression.otherwise is not None:
            otherwise_sql, _precedence, otherwise_required = _lower(expression.otherwise, dialect=dialect)
            required |= otherwise_required
            parts.append(f"ELSE {otherwise_sql}")
        parts.append("END")
        return " ".join(parts), 100, required

    if isinstance(expression, RegexExpression):
        value_sql, _precedence, required = _lower(expression.expression, dialect=dialect)
        pattern_sql = _literal_sql(expression.pattern, dialect)
        if dialect in {"duckdb", "bigquery"}:
            function = "REGEXP_MATCHES" if dialect == "duckdb" else "REGEXP_CONTAINS"
            sql = f"{function}({value_sql}, {pattern_sql})"
            if expression.negated:
                sql = f"NOT {sql}"
        elif dialect == "postgres":
            sql = f"{value_sql} {'!~' if expression.negated else '~'} {pattern_sql}"
        else:
            sql = f"REGEXP_LIKE({value_sql}, {pattern_sql})"
            if expression.negated:
                sql = f"NOT {sql}"
        if expression.negated and dialect != "postgres":
            precedence = 25
        elif dialect == "postgres":
            precedence = 30
        else:
            precedence = 100
        return sql, precedence, required | {"regex"}

    if isinstance(expression, FunctionExpression):
        name = expression.name.lower()
        if name not in _STANDARD_FUNCTION_ARITIES:
            _reject("function", f"Function {expression.name!r} is outside the supported standard-function set")
        minimum, maximum = _STANDARD_FUNCTION_ARITIES[name]
        arity = len(expression.arguments)
        if arity < minimum or (maximum is not None and arity > maximum):
            expected = (
                f"at least {minimum}"
                if maximum is None
                else str(minimum)
                if minimum == maximum
                else f"{minimum}-{maximum}"
            )
            _reject("function_arity", f"Function {expression.name!r} expects {expected} arguments, received {arity}")
        lowered = [_lower(argument, dialect=dialect) for argument in expression.arguments]
        function_name = name.upper()
        if name == "length":
            argument_type = infer_expression_type(expression.arguments[0], dialect=dialect)
            if argument_type == "array":
                function_name = {
                    None: "ARRAY_LENGTH",
                    "duckdb": "ARRAY_LENGTH",
                    "postgres": "CARDINALITY",
                    "bigquery": "ARRAY_LENGTH",
                    "snowflake": "ARRAY_SIZE",
                }[dialect]
            elif argument_type == "unknown" and dialect is not None:
                _reject(
                    "function_type",
                    "length requires a resolved string or array type for dialect-specific lowering",
                )
        return f"{function_name}({', '.join(item[0] for item in lowered)})", 100, _merge_requirements(lowered)

    if isinstance(expression, ArrayExpression):
        if not expression.values:
            _reject("array_type", "Empty arrays require an explicit element type")
        lowered = [_lower(value, dialect=dialect) for value in expression.values]
        values_sql = ", ".join(item[0] for item in lowered)
        if dialect in {"duckdb", "bigquery"}:
            sql = f"[{values_sql}]"
        elif dialect == "snowflake":
            sql = f"ARRAY_CONSTRUCT({values_sql})"
        else:
            sql = f"ARRAY[{values_sql}]"
        return sql, 100, _merge_requirements(lowered) | {"arrays"}

    _reject("expression_node", f"Unknown typed expression node {type(expression).__name__!r}")


def _literal_sql(value: ScalarValue, dialect: DialectName | None = None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (float, Decimal)):
        if isinstance(value, float) and not math.isfinite(value):
            _reject("numeric_literal", "Non-finite numeric literals are not target-neutral")
        if isinstance(value, Decimal) and not value.is_finite():
            _reject("numeric_literal", "Non-finite numeric literals are not target-neutral")
        return str(value)
    if isinstance(value, str):
        if dialect == "bigquery":
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
        return "'" + value.replace("'", "''") + "'"
    _reject("literal", f"Unsupported literal type {type(value).__name__!r}")


def _parse_date(value: date | str) -> date:
    if isinstance(value, datetime):
        _reject("date_literal", "Datetime values cannot be lowered as date literals implicitly")
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except ValueError:
        _reject("date_literal", f"Invalid ISO date literal {value!r}")


def _parse_timestamp(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        _reject("timestamp_literal", f"Invalid ISO timestamp literal {value!r}")


def infer_expression_type(expression: Expression, *, dialect: DialectName | None = None) -> SemanticType:
    """Infer and validate the semantic result type of a typed expression tree.

    ``unknown`` fields remain usable so a parser can build a tree before schema
    resolution. Known incompatible types fail closed instead of relying on a
    warehouse's implicit coercions, which vary between supported dialects.
    """

    if isinstance(expression, RejectedExpression):
        detail = expression.detail or f"Malloy {expression.feature.replace('_', ' ')} expressions are not supported"
        raise MalloyExpressionLoweringError(
            ExpressionDiagnostic(feature=expression.feature, status="rejected", detail=detail)
        )
    if isinstance(expression, FieldPath):
        return expression.semantic_type
    if isinstance(expression, Literal):
        if expression.value is None:
            return "null"
        if isinstance(expression.value, bool):
            return "boolean"
        if isinstance(expression.value, (int, float, Decimal)):
            return "number"
        return "string"
    if isinstance(expression, DateLiteral):
        return "date"
    if isinstance(expression, TimestampLiteral):
        return "timestamp"
    if isinstance(expression, UnaryExpression):
        operand_type = infer_expression_type(expression.operand, dialect=dialect)
        if expression.operator == "not":
            _require_type(operand_type, {"boolean"}, "unary_type", "NOT operand")
            return "boolean"
        if expression.operator in {"+", "-"}:
            _require_type(operand_type, {"number"}, "unary_type", f"{expression.operator} operand")
            return "number" if operand_type != "unknown" else "unknown"
        _reject("unary_operator", f"Unsupported unary operator {expression.operator!r}")
    if isinstance(expression, BinaryExpression):
        operator = expression.operator.lower()
        if operator not in _BINARY_PRECEDENCE:
            _reject("binary_operator", f"Unsupported binary operator {expression.operator!r}")
        left_type = infer_expression_type(expression.left, dialect=dialect)
        right_type = infer_expression_type(expression.right, dialect=dialect)
        if operator in {"and", "or"}:
            _require_type(left_type, {"boolean"}, "binary_type", f"left {operator.upper()} operand")
            _require_type(right_type, {"boolean"}, "binary_type", f"right {operator.upper()} operand")
            return "boolean"
        if operator in _COMPARISON_OPERATORS:
            if "null" in {left_type, right_type} and operator not in {"=", "!=", "<>"}:
                _reject("null_comparison", "NULL supports only equality and inequality comparisons")
            _require_compatible(left_type, right_type, "comparison_type")
            return "boolean"
        _require_type(left_type, {"number"}, "binary_type", f"left {operator} operand")
        _require_type(right_type, {"number"}, "binary_type", f"right {operator} operand")
        return "number" if "unknown" not in {left_type, right_type} else "unknown"
    if isinstance(expression, CastExpression):
        target_type = expression.target_type.lower()
        if target_type not in _CAST_SEMANTIC_TYPES:
            _reject("cast_type", f"Unsupported target-neutral cast type {expression.target_type!r}")
        infer_expression_type(expression.expression, dialect=dialect)
        return _CAST_SEMANTIC_TYPES[target_type]
    if isinstance(expression, DateTruncExpression):
        if expression.unit.lower() not in _DATE_TRUNC_UNITS:
            _reject("date_trunc_unit", f"Unsupported date truncation unit {expression.unit!r}")
        value_type = infer_expression_type(expression.expression, dialect=dialect)
        _require_type(value_type, {"date", "timestamp"}, "date_trunc_type", "date truncation operand")
        return value_type
    if isinstance(expression, DurationArithmetic):
        if expression.operator not in {"+", "-"}:
            _reject("duration_operator", f"Unsupported duration operator {expression.operator!r}")
        if (
            isinstance(expression.duration.value, bool)
            or not isinstance(expression.duration.value, int)
            or expression.duration.value < 0
        ):
            _reject("duration_value", "Durations must use non-negative integer values")
        if expression.duration.unit.lower() not in _DURATION_UNITS:
            _reject(
                "duration_unit",
                f"Duration unit {expression.duration.unit!r} is not in the exact fixed-duration subset",
            )
        value_type = infer_expression_type(expression.temporal, dialect=dialect)
        _require_type(value_type, {"date", "timestamp"}, "duration_type", "duration arithmetic operand")
        if dialect == "bigquery" and value_type == "date" and expression.duration.unit.lower() != "day":
            _reject(
                "duration_type",
                "BigQuery DATE arithmetic with sub-day durations requires an explicit TIMESTAMP cast",
            )
        if value_type == "date":
            if dialect in {"duckdb", "postgres"}:
                return "timestamp"
            if dialect == "snowflake" and expression.duration.unit.lower() != "day":
                return "timestamp"
        return value_type
    if isinstance(expression, CoalesceExpression):
        if len(expression.expressions) < 2:
            _reject("coalesce_arity", "Malloy ?? requires at least two expressions")
        return _common_type(
            (infer_expression_type(item, dialect=dialect) for item in expression.expressions), "coalesce_type"
        )
    if isinstance(expression, CaseExpression):
        if not expression.branches:
            _reject("case_arity", "Malloy pick/case requires at least one branch")
        value_types: list[SemanticType] = []
        for branch in expression.branches:
            condition_type = infer_expression_type(branch.condition, dialect=dialect)
            _require_type(condition_type, {"boolean"}, "case_condition_type", "CASE condition")
            value_types.append(infer_expression_type(branch.value, dialect=dialect))
        if expression.otherwise is not None:
            value_types.append(infer_expression_type(expression.otherwise, dialect=dialect))
        return _common_type(value_types, "case_result_type")
    if isinstance(expression, RegexExpression):
        value_type = infer_expression_type(expression.expression, dialect=dialect)
        _require_type(value_type, {"string"}, "regex_type", "regex operand")
        return "boolean"
    if isinstance(expression, FunctionExpression):
        name = expression.name.lower()
        _validate_function(name, len(expression.arguments), original_name=expression.name)
        argument_types = tuple(infer_expression_type(argument, dialect=dialect) for argument in expression.arguments)
        if name in {"lower", "upper", "trim"}:
            _require_type(argument_types[0], {"string"}, "function_type", f"{name} argument")
            return "string"
        if name == "replace":
            for index, argument_type in enumerate(argument_types, start=1):
                _require_type(argument_type, {"string"}, "function_type", f"replace argument {index}")
            return "string"
        if name == "substring":
            _require_type(argument_types[0], {"string"}, "function_type", "substring first argument")
            for index, argument_type in enumerate(argument_types[1:], start=2):
                _require_type(argument_type, {"number"}, "function_type", f"substring argument {index}")
            return "string"
        if name == "length":
            if argument_types:
                _require_type(argument_types[0], {"string", "array"}, "function_type", "length argument")
            return "number"
        if name == "concat":
            for index, argument_type in enumerate(argument_types, start=1):
                _require_type(argument_type, {"string"}, "function_type", f"concat argument {index}")
            return "string"
        if name in {"coalesce", "greatest", "least", "nullif"}:
            return _common_type(argument_types, "function_type")
        for argument_type in argument_types:
            _require_type(argument_type, {"number"}, "function_type", f"{name} argument")
        return "number"
    if isinstance(expression, ArrayExpression):
        if not expression.values:
            _reject("array_type", "Empty arrays require an explicit element type")
        _common_type(
            (infer_expression_type(value, dialect=dialect) for value in expression.values), "array_element_type"
        )
        return "array"
    _reject("expression_node", f"Unknown typed expression node {type(expression).__name__!r}")


class MalloyExpressionBuilder:
    """Small executable construction API for parser visitors and standalone use."""

    def __init__(
        self,
        *,
        dialect: DialectName | None = None,
        capabilities: frozenset[Capability] | None = None,
    ) -> None:
        self.dialect = dialect
        self.capabilities = capabilities

    def field(self, *parts: str, semantic_type: SemanticType = "unknown") -> FieldPath:
        return FieldPath(tuple(parts), semantic_type=semantic_type)

    def literal(self, value: ScalarValue) -> Literal:
        return Literal(value)

    def date(self, value: date | str) -> DateLiteral:
        node = DateLiteral(value)
        # Validate string values at construction time.
        _parse_date(value)
        return node

    def timestamp(self, value: datetime | str) -> TimestampLiteral:
        node = TimestampLiteral(value)
        parsed = _parse_timestamp(value)
        if parsed.tzinfo is not None:
            _reject("timestamp_timezone", "Timezone-aware Malloy timestamps require dialect-specific semantics")
        return node

    def branch(self, condition: Expression, value: Expression) -> CaseBranch:
        condition_type = infer_expression_type(condition, dialect=self.dialect)
        _require_type(condition_type, {"boolean"}, "case_condition_type", "CASE condition")
        infer_expression_type(value, dialect=self.dialect)
        return CaseBranch(condition, value)

    def unary(self, operator: TypingLiteral["not", "+", "-"], operand: Expression) -> UnaryExpression:
        node = UnaryExpression(operator, operand)
        infer_expression_type(node, dialect=self.dialect)
        return node

    def binary(self, left: Expression, operator: str, right: Expression) -> BinaryExpression:
        node = BinaryExpression(left, operator, right)
        infer_expression_type(node, dialect=self.dialect)
        return node

    def cast(self, expression: Expression, target_type: str) -> CastExpression:
        node = CastExpression(expression, target_type)
        infer_expression_type(node, dialect=self.dialect)
        return node

    def date_trunc(self, unit: str, expression: Expression) -> DateTruncExpression:
        node = DateTruncExpression(unit, expression)
        infer_expression_type(node, dialect=self.dialect)
        return node

    def duration(
        self,
        temporal: Expression,
        operator: TypingLiteral["+", "-"],
        value: int,
        unit: str,
    ) -> DurationArithmetic:
        node = DurationArithmetic(temporal, operator, Duration(value, unit))
        infer_expression_type(node, dialect=self.dialect)
        return node

    def coalesce(self, *expressions: Expression) -> CoalesceExpression:
        node = CoalesceExpression(tuple(expressions))
        infer_expression_type(node, dialect=self.dialect)
        return node

    def case(self, *branches: CaseBranch, otherwise: Expression | None = None) -> CaseExpression:
        node = CaseExpression(tuple(branches), otherwise)
        infer_expression_type(node, dialect=self.dialect)
        return node

    def regex(self, expression: Expression, pattern: str, *, negated: bool = False) -> RegexExpression:
        node = RegexExpression(expression, pattern, negated)
        infer_expression_type(node, dialect=self.dialect)
        return node

    def function(self, name: str, *arguments: Expression) -> FunctionExpression:
        node = FunctionExpression(name, tuple(arguments))
        # Lowering owns the supported-function and arity checks; exercise them
        # now so builder-created nodes are guaranteed executable.
        lower_expression(node, dialect=self.dialect, capabilities=self.capabilities)
        return node

    def array(self, *values: Expression) -> ArrayExpression:
        node = ArrayExpression(tuple(values))
        infer_expression_type(node, dialect=self.dialect)
        return node

    def lower(self, expression: Expression) -> LoweredExpression:
        return lower_expression(expression, dialect=self.dialect, capabilities=self.capabilities)

    def build(self, expression: Expression) -> exp.Expression:
        """Build and validate a canonical executable SQL expression tree."""
        return self.lower(expression).expression.copy()


# The shorter name is convenient for callers which already import this module
# under a Malloy-specific namespace.
ExpressionBuilder = MalloyExpressionBuilder


def _require_type(
    actual: SemanticType,
    expected: set[SemanticType],
    feature: str,
    label: str,
) -> None:
    if actual not in expected and actual not in {"unknown", "null"}:
        _reject(feature, f"{label} must be {' or '.join(sorted(expected))}, received {actual}")


def _require_compatible(left: SemanticType, right: SemanticType, feature: str) -> None:
    known = {item for item in (left, right) if item not in {"unknown", "null"}}
    if len(known) > 1:
        _reject(feature, f"Comparison operands have incompatible semantic types {left!r} and {right!r}")


def _common_type(types: Iterable[SemanticType], feature: str) -> SemanticType:
    values = tuple(types)
    known = {item for item in values if item not in {"unknown", "null"}}
    if len(known) > 1:
        _reject(feature, f"Expression values have incompatible semantic types: {', '.join(sorted(known))}")
    if known:
        return next(iter(known))
    return "unknown" if "unknown" in values else "null"


def _validate_function(name: str, arity: int, *, original_name: str) -> None:
    if name not in _STANDARD_FUNCTION_ARITIES:
        _reject("function", f"Function {original_name!r} is outside the supported standard-function set")
    minimum, maximum = _STANDARD_FUNCTION_ARITIES[name]
    if arity < minimum or (maximum is not None and arity > maximum):
        expected = (
            f"at least {minimum}" if maximum is None else str(minimum) if minimum == maximum else f"{minimum}-{maximum}"
        )
        _reject("function_arity", f"Function {original_name!r} expects {expected} arguments, received {arity}")


def _quote_identifier(value: str, dialect: DialectName | None) -> str:
    if _IDENTIFIER.fullmatch(value) and value.lower() not in _SQL_RESERVED_WORDS:
        return value
    if dialect == "bigquery":
        return "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`"
    return '"' + value.replace('"', '""') + '"'


def _parenthesize(sql: str, needed: bool) -> str:
    return f"({sql})" if needed else sql


def _merge_requirements(lowered: list[tuple[str, int, set[Capability]]]) -> set[Capability]:
    required: set[Capability] = set()
    for _sql, _precedence, item_required in lowered:
        required |= item_required
    return required


def _reject(feature: str, detail: str) -> None:
    raise MalloyExpressionLoweringError(ExpressionDiagnostic(feature=feature, status="rejected", detail=detail))
