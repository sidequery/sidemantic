from __future__ import annotations

import datetime
import decimal
import json
from pathlib import Path

import duckdb
import pytest
import sqlglot

from sidemantic.interchange.ossie.portable import lower_ossie_sql

CORPUS = json.loads((Path(__file__).parent / "fixtures" / "portable_expressions.json").read_text())


def normalize_result(value):
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


@pytest.mark.parametrize("case", CORPUS, ids=lambda case: case["expression"])
def test_required_portable_functions_execute_with_independent_expected_results(case):
    sql = lower_ossie_sql(case["expression"], "duckdb")
    source = f" FROM {case['from_sql']}" if "from_sql" in case else ""
    with duckdb.connect() as connection:
        results = [normalize_result(row[0]) for row in connection.execute(f"SELECT {sql}{source}").fetchall()]
    if case.get("sort_results"):
        results.sort()
    assert len(results) == len(case["expected"])
    for result, expected in zip(results, case["expected"], strict=True):
        if isinstance(expected, float):
            assert result == pytest.approx(expected)
        else:
            assert result == expected


@pytest.mark.parametrize(
    "expression",
    ["CURRENT_DATE", "CURRENT_DATE()", "CURRENT_TIME", "CURRENT_TIME()", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"],
)
def test_current_date_and_time_forms(expression):
    sql = lower_ossie_sql(expression, "duckdb")
    with duckdb.connect() as connection:
        assert connection.execute(f"SELECT {sql} IS NOT NULL").fetchone() == (True,)


@pytest.mark.parametrize(
    "expression",
    [
        "SELECT x FROM t",
        "x IN (SELECT x FROM t)",
        "WITH t AS (SELECT 1) SELECT * FROM t",
        "DROP TABLE t",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "x; y",
        "x AS y",
        "*",
        "a.b.c",
        '"' + "a" * 129 + '"',
    ],
)
def test_rejects_disallowed_expression_constructs(expression):
    with pytest.raises(ValueError):
        lower_ossie_sql(expression, "duckdb")


@pytest.mark.parametrize("target", ["duckdb", "postgres", "snowflake", "bigquery", "databricks"])
def test_target_translations_do_not_drop_required_semantics(target):
    for expression in ["LOG(2, x)", "TRUNC(x, 2)", "DAYOFYEAR(d)", "TO_TIMESTAMP(s)", "CONTAINS(s,p)", "ENDSWITH(s,p)"]:
        sql = lower_ossie_sql(expression, target)
        assert sqlglot.parse_one(sql, read=target) is not None
        if expression == "LOG(2, x)":
            assert "LN(x)" in sql and "/ LN(2)" in sql
        if expression == "TRUNC(x, 2)":
            assert "ROUND(x, 2)" in sql and "0.01" in sql
        if expression == "CONTAINS(s,p)":
            assert "CONTAINS_SUBSTR" not in sql
        if expression == "TO_TIMESTAMP(s)":
            assert "CAST(s AS" in sql


def test_exact_percentiles_never_become_approximate():
    expression = "PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY x)"
    assert "PERCENTILE_CONT" in lower_ossie_sql(expression, "databricks")
    with pytest.raises(ValueError, match="query-level"):
        lower_ossie_sql(expression, "bigquery")
    with pytest.raises(ValueError, match="query-level"):
        lower_ossie_sql("MEDIAN(x)", "bigquery")


def test_unknown_extension_functions_are_preserved():
    assert lower_ossie_sql("MY_EXTENSION(x)", "duckdb") == "MY_EXTENSION(x)"


def test_week_truncation_uses_monday_independently_of_target_defaults():
    assert "WEEK(MONDAY)" in lower_ossie_sql("DATE_TRUNC('week',d)", "bigquery")
    assert "DAYOFWEEKISO" in lower_ossie_sql("DATE_TRUNC('week',d)", "snowflake")


def test_postgres_calendar_difference_arithmetic_keeps_parentheses():
    # The generated expression is ANSI arithmetic, executable in DuckDB too.
    # This verifies the calculation, without claiming PostgreSQL execution.
    sql = lower_ossie_sql("DATEDIFF(month, DATE '2023-12-31', DATE '2024-02-01')", "postgres")
    with duckdb.connect() as connection:
        assert connection.execute(f"SELECT {sql}").fetchone() == (2,)


@pytest.mark.parametrize(
    "unit, start, end, expected",
    [
        ("day", "2024-01-01 23:59:00", "2024-01-02 00:01:00", 1),
        ("hour", "2024-01-01 10:59:00", "2024-01-01 11:01:00", 1),
        ("minute", "2024-01-01 10:00:59", "2024-01-01 10:01:01", 1),
        ("second", "2024-01-01 10:00:00.999999", "2024-01-01 10:00:01.000001", 1),
        ("hour", "2024-01-01 10:01:00", "2024-01-01 10:59:00", 0),
        ("hour", "1969-12-31 23:59:00", "1970-01-01 00:01:00", 1),
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
def test_postgres_datediff_counts_boundaries(unit, start, end, expected, reverse):
    if reverse:
        start, end, expected = end, start, -expected
    sql = lower_ossie_sql(f"DATEDIFF({unit}, TIMESTAMP '{start}', TIMESTAMP '{end}')", "postgres")
    # Execute the PostgreSQL arithmetic without round-tripping it through a
    # transpiler. DuckDB supports the same EXTRACT/DATE_TRUNC timestamp operators.
    with duckdb.connect() as connection:
        assert connection.execute(f"SELECT {sql}").fetchone() == (expected,)


def test_ansi_value_window_frames_preserve_explicit_frames_and_other_source_functions():
    source = "ZEROIFNULL(NTH_VALUE(DAYOFYEAR(d), 2) OVER (ORDER BY d))"
    sql = lower_ossie_sql(source, "duckdb")
    with duckdb.connect() as connection:
        assert connection.execute(
            f"SELECT {sql} FROM (VALUES (DATE '2024-01-01'), (DATE '2024-02-01')) AS t(d)"
        ).fetchall() == [(0,), (32,)]
        explicit = lower_ossie_sql(
            "LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)", "duckdb"
        )
        assert connection.execute(f"SELECT {explicit} FROM (VALUES (1),(2)) AS t(x)").fetchall() == [(2,), (2,)]
