"""Unit contract for typed Malloy expression lowering.

Integration contract for ``MalloyModelVisitor``:

1. Visit ``fieldExpr`` children and construct only nodes exported by
   ``sidemantic.adapters.malloy_expressions``; never construct a raw-SQL node.
2. Represent safe casts, filter strings, ranges, raw functions, parameters,
   locality/all/exclude, and records as ``RejectedExpression`` nodes.
3. Call ``lower_expression`` with the destination dialect's declared capability
   set. Store ``LoweredExpression.sql`` only after successful validation and
   surface ``MalloyExpressionLoweringError.diagnostic`` through ImportReport.
"""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pytest
from sqlglot import exp, parse_one

from sidemantic.adapters.malloy_expressions import (
    ArrayExpression,
    BinaryExpression,
    CaseBranch,
    CaseExpression,
    CastExpression,
    CoalesceExpression,
    DateLiteral,
    DateTruncExpression,
    Duration,
    DurationArithmetic,
    FieldPath,
    FunctionExpression,
    Literal,
    MalloyExpressionBuilder,
    MalloyExpressionLoweringError,
    RegexExpression,
    RejectedExpression,
    TimestampLiteral,
    UnaryExpression,
    infer_expression_type,
    lower_expression,
)


def test_binary_precedence_preserves_typed_tree() -> None:
    expression = BinaryExpression(
        BinaryExpression(FieldPath(("orders", "subtotal")), "+", Literal(2)),
        "*",
        BinaryExpression(Literal(3), "-", Literal(1)),
    )

    assert lower_expression(expression).sql == "(orders.subtotal + 2) * (3 - 1)"


def test_right_associative_shape_is_not_flattened() -> None:
    expression = BinaryExpression(Literal(10), "-", BinaryExpression(Literal(4), "-", Literal(1)))

    assert lower_expression(expression).sql == "10 - (4 - 1)"


def test_non_associative_left_children_and_nested_unary_are_grouped() -> None:
    comparison = BinaryExpression(BinaryExpression(Literal(1), "<", Literal(2)), "=", Literal(True))
    unary = UnaryExpression("-", UnaryExpression("-", FieldPath(("amount",), semantic_type="number")))

    assert lower_expression(comparison).sql == "(1 < 2) = TRUE"
    assert lower_expression(unary).sql == "-(-amount)"


def test_boolean_comparison_precedence_and_unary_not() -> None:
    expression = BinaryExpression(
        UnaryExpression("not", BinaryExpression(FieldPath(("cancelled",)), "=", Literal(True))),
        "or",
        BinaryExpression(FieldPath(("amount",)), ">", Literal(100)),
    )

    assert lower_expression(expression).sql == "NOT (cancelled = TRUE) OR amount > 100"


def test_literals_casts_dates_and_timestamp_truncation() -> None:
    expression = CoalesceExpression(
        (
            CastExpression(FieldPath(("raw amount",)), "decimal"),
            Literal(0),
        )
    )

    result = lower_expression(expression)

    assert result.sql == 'COALESCE(CAST("raw amount" AS DECIMAL), 0)'
    assert result.semantic_type == "number"
    truncated = lower_expression(DateTruncExpression("month", TimestampLiteral("2026-08-23 12:34:56")))
    assert truncated.sql == "DATE_TRUNC('month', TIMESTAMP '2026-08-23 12:34:56')"
    assert truncated.required_capabilities == frozenset({"date_trunc"})
    assert truncated.semantic_type == "timestamp"
    assert lower_expression(DateLiteral("2026-08-23")).sql == "DATE '2026-08-23'"


def test_fixed_duration_arithmetic_is_explicitly_capability_gated() -> None:
    expression = DurationArithmetic(FieldPath(("created_at",)), "+", Duration(2, "day"))

    assert lower_expression(expression).sql == "created_at + INTERVAL '2' DAY"
    assert lower_expression(expression).required_capabilities == frozenset({"intervals"})

    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        lower_expression(expression, capabilities=frozenset())

    assert exc_info.value.diagnostic.feature == "dialect_capability"
    assert exc_info.value.diagnostic.required_capability == "intervals"


def test_case_coalesce_regex_functions_and_arrays() -> None:
    expression = CaseExpression(
        branches=(
            CaseBranch(
                RegexExpression(FunctionExpression("lower", (FieldPath(("name",)),)), "^a"),
                ArrayExpression((Literal("match"), Literal("review"))),
            ),
        ),
        otherwise=ArrayExpression((Literal("other"),)),
    )

    result = lower_expression(expression)

    assert result.sql == (
        "CASE WHEN REGEXP_LIKE(LOWER(name), '^a') THEN ARRAY['match', 'review'] ELSE ARRAY['other'] END"
    )
    assert result.required_capabilities == frozenset({"arrays", "regex"})


@pytest.mark.parametrize(
    "feature",
    [
        "safe_cast",
        "filter_string",
        "range",
        "raw_function",
        "parameter",
        "locality",
        "all",
        "exclude",
        "record",
    ],
)
def test_unsafe_or_unrepresented_forms_fail_closed(feature: str) -> None:
    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        lower_expression(RejectedExpression(feature))  # type: ignore[arg-type]

    assert exc_info.value.diagnostic.feature == feature
    assert exc_info.value.diagnostic.status == "rejected"


@pytest.mark.parametrize(
    ("expression", "feature"),
    [
        (FunctionExpression("vendor_magic", (FieldPath(("x",)),)), "function"),
        (FunctionExpression("replace", (FieldPath(("x",)),)), "function_arity"),
        (CastExpression(FieldPath(("x",)), "geography"), "cast_type"),
        (DateTruncExpression("millennium", FieldPath(("x",))), "date_trunc_unit"),
        (DurationArithmetic(FieldPath(("x",)), "+", Duration(1, "month")), "duration_unit"),
        (TimestampLiteral(datetime(2026, 8, 23, tzinfo=UTC)), "timestamp_timezone"),
    ],
)
def test_unfaithful_typed_forms_fail_with_structured_diagnostics(expression: object, feature: str) -> None:
    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        lower_expression(expression)  # type: ignore[arg-type]

    assert exc_info.value.diagnostic.feature == feature
    assert exc_info.value.diagnostic.status == "rejected"


def test_strings_are_sql_escaped_and_field_components_are_quoted() -> None:
    expression = BinaryExpression(FieldPath(("order details", "select")), "=", Literal("Nico's"))

    assert lower_expression(expression).sql == "\"order details\".\"select\" = 'Nico''s'"


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("duckdb", "REGEXP_MATCHES(\"order details\", '^a')"),
        ("postgres", "\"order details\" ~ '^a'"),
        ("bigquery", "REGEXP_CONTAINS(`order details`, '^a')"),
        ("snowflake", "REGEXP_LIKE(\"order details\", '^a')"),
    ],
)
def test_regex_and_identifier_rendering_is_dialect_aware(dialect: str, expected: str) -> None:
    expression = RegexExpression(FieldPath(("order details",), semantic_type="string"), "^a")

    assert lower_expression(expression, dialect=dialect).sql == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("duckdb", "created_at + INTERVAL '2' DAY"),
        ("postgres", "created_at + INTERVAL '2 DAY'"),
        ("bigquery", "DATE_ADD(created_at, INTERVAL '2' DAY)"),
        ("snowflake", "DATEADD(DAY, 2, created_at)"),
    ],
)
def test_duration_arithmetic_is_dialect_aware(dialect: str, expected: str) -> None:
    expression = DurationArithmetic(
        FieldPath(("created_at",), semantic_type="date"),
        "+",
        Duration(2, "day"),
    )

    result = lower_expression(expression, dialect=dialect)  # type: ignore[arg-type]

    assert result.sql == expected
    assert result.semantic_type == ("timestamp" if dialect in {"duckdb", "postgres"} else "date")
    assert parse_one(result.sql, read=dialect).sql(dialect=dialect) == result.sql


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("duckdb", "DATE_TRUNC('MONTH', created_at)"),
        ("postgres", "DATE_TRUNC('MONTH', created_at)"),
        ("bigquery", "DATE_TRUNC(created_at, MONTH)"),
        ("snowflake", "DATE_TRUNC('MONTH', created_at)"),
    ],
)
def test_date_truncation_is_dialect_aware(dialect: str, expected: str) -> None:
    expression = DateTruncExpression("month", FieldPath(("created_at",), semantic_type="date"))

    assert lower_expression(expression, dialect=dialect).sql == expected  # type: ignore[arg-type]


def test_casts_and_arrays_render_for_bigquery_and_snowflake() -> None:
    cast = CastExpression(FieldPath(("amount",), semantic_type="string"), "decimal")
    array = ArrayExpression((Literal("a"), Literal("b")))

    assert lower_expression(cast, dialect="bigquery").sql == "CAST(amount AS NUMERIC)"
    assert lower_expression(cast, dialect="snowflake").sql == "CAST(amount AS DECIMAL(38, 0))"
    assert lower_expression(array, dialect="bigquery").sql == "['a', 'b']"
    assert lower_expression(array, dialect="snowflake").sql == "['a', 'b']"


def test_bigquery_string_literals_use_backslash_escaping() -> None:
    expression = BinaryExpression(
        FieldPath(("name",), semantic_type="string"),
        "=",
        Literal("Nico's\\path"),
    )

    assert lower_expression(expression, dialect="bigquery").sql == "name = 'Nico\\'s\\\\path'"


def test_semantic_types_are_inferred_and_incompatible_operations_rejected() -> None:
    numeric = BinaryExpression(FieldPath(("amount",), semantic_type="number"), "+", Literal(2))
    predicate = BinaryExpression(numeric, ">", Literal(10))

    assert infer_expression_type(numeric) == "number"
    assert lower_expression(predicate).semantic_type == "boolean"

    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        lower_expression(BinaryExpression(FieldPath(("name",), semantic_type="string"), "+", Literal(2)))

    assert exc_info.value.diagnostic.feature == "binary_type"

    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        lower_expression(FunctionExpression("substring", (Literal("abc"), Literal("two"))))

    assert exc_info.value.diagnostic.feature == "function_type"


def test_executable_builder_constructs_validated_nodes_and_lowers_them() -> None:
    builder = MalloyExpressionBuilder(dialect="bigquery")
    created_at = builder.field("created_at", semantic_type="timestamp")
    expression = builder.binary(
        builder.date_trunc("month", created_at),
        "=",
        TimestampLiteral("2026-08-01 00:00:00"),
    )

    result = builder.lower(expression)

    assert result.sql == "TIMESTAMP_TRUNC(created_at, MONTH) = CAST('2026-08-01 00:00:00' AS TIMESTAMP)"
    assert result.semantic_type == "boolean"
    executable = builder.build(expression)
    assert isinstance(executable, exp.Expression)
    assert executable.sql(dialect="bigquery") == result.sql

    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        builder.regex(builder.field("amount", semantic_type="number"), "^[0-9]+$")

    assert exc_info.value.diagnostic.feature == "regex_type"


@pytest.mark.parametrize(
    ("dialect", "semantic_type", "unit", "expected_type"),
    [
        ("duckdb", "date", "day", "timestamp"),
        ("duckdb", "date", "hour", "timestamp"),
        ("postgres", "date", "day", "timestamp"),
        ("postgres", "date", "minute", "timestamp"),
        ("bigquery", "date", "day", "date"),
        ("bigquery", "timestamp", "hour", "timestamp"),
        ("snowflake", "date", "day", "date"),
        ("snowflake", "date", "second", "timestamp"),
        ("snowflake", "timestamp", "day", "timestamp"),
    ],
)
def test_temporal_duration_type_matrix(
    dialect: str,
    semantic_type: str,
    unit: str,
    expected_type: str,
) -> None:
    expression = DurationArithmetic(
        FieldPath(("value",), semantic_type=semantic_type),  # type: ignore[arg-type]
        "+",
        Duration(1, unit),
    )

    result = lower_expression(expression, dialect=dialect)  # type: ignore[arg-type]

    assert result.semantic_type == expected_type
    assert parse_one(result.sql, read=dialect).sql(dialect=dialect) == result.sql


@pytest.mark.parametrize("unit", ["hour", "minute", "second"])
def test_bigquery_date_rejects_sub_day_duration(unit: str) -> None:
    expression = DurationArithmetic(FieldPath(("value",), semantic_type="date"), "+", Duration(1, unit))

    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        lower_expression(expression, dialect="bigquery")

    assert exc_info.value.diagnostic.feature == "duration_type"


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("duckdb", "ARRAY_LENGTH(items)"),
        ("postgres", "CARDINALITY(items)"),
        ("bigquery", "ARRAY_LENGTH(items)"),
        ("snowflake", "ARRAY_SIZE(items)"),
    ],
)
def test_array_length_is_type_and_dialect_aware(dialect: str, expected: str) -> None:
    expression = FunctionExpression("length", (FieldPath(("items",), semantic_type="array"),))
    result = lower_expression(expression, dialect=dialect)  # type: ignore[arg-type]

    assert result.sql == expected
    assert parse_one(result.sql, read=dialect).sql(dialect=dialect) == result.sql
    assert (
        lower_expression(
            FunctionExpression("length", (FieldPath(("name",), semantic_type="string"),)),
            dialect=dialect,  # type: ignore[arg-type]
        ).sql
        == "LENGTH(name)"
    )


def test_dialect_length_rejects_unresolved_array_or_string_type() -> None:
    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        lower_expression(FunctionExpression("length", (FieldPath(("value",)),)), dialect="postgres")

    assert exc_info.value.diagnostic.feature == "function_type"


@pytest.mark.parametrize(
    ("operator", "expected"),
    [("=", "value IS NULL"), ("!=", "value IS NOT NULL"), ("<>", "value IS NOT NULL")],
)
def test_null_equality_uses_is_predicates(operator: str, expected: str) -> None:
    expression = BinaryExpression(FieldPath(("value",)), operator, Literal(None))

    result = lower_expression(expression, dialect="duckdb")

    assert result.sql == expected
    assert duckdb.sql(
        f"select {result.sql} from (values (null), (1)) t(value) order by value nulls first"
    ).fetchall() == [
        (operator == "=",),
        (operator != "=",),
    ]


def test_null_comparison_is_symmetric_and_ordered_null_is_rejected() -> None:
    assert (
        lower_expression(BinaryExpression(Literal(None), "=", FieldPath(("value",))), dialect="duckdb").sql
        == "value IS NULL"
    )

    with pytest.raises(MalloyExpressionLoweringError) as exc_info:
        lower_expression(BinaryExpression(FieldPath(("value",)), ">", Literal(None)), dialect="duckdb")

    assert exc_info.value.diagnostic.feature == "null_comparison"


def test_bigquery_identifier_backticks_use_backslash_escaping() -> None:
    result = lower_expression(FieldPath(("part`name",)), dialect="bigquery")

    assert result.sql == r"`part\`name`"
    assert isinstance(result.expression, exp.Column)


def test_duckdb_canonical_expression_executes_after_parse_roundtrip() -> None:
    expression = BinaryExpression(
        FunctionExpression("length", (ArrayExpression((Literal(1), Literal(2), Literal(3))),)),
        "=",
        Literal(3),
    )
    result = lower_expression(expression, dialect="duckdb")

    reparsed = parse_one(result.sql, read="duckdb")
    assert reparsed.sql(dialect="duckdb") == result.sql
    assert duckdb.sql(f"select {result.sql}").fetchone() == (True,)


def test_duckdb_duration_expression_executes_after_parse_roundtrip() -> None:
    expression = DurationArithmetic(DateLiteral("2026-08-23"), "+", Duration(2, "hour"))
    result = lower_expression(expression, dialect="duckdb")

    reparsed = parse_one(result.sql, read="duckdb")
    assert reparsed.sql(dialect="duckdb") == result.sql
    assert result.semantic_type == "timestamp"
    assert duckdb.sql(f"select {result.sql}").fetchone() == (datetime(2026, 8, 23, 2),)
