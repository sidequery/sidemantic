"""Replay Yardstick's measures.test end-to-end for parity confidence."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

import pytest
from sqlglot import exp

from sidemantic import SemanticLayer
from sidemantic.adapters.yardstick import YardstickAdapter
from tests.utils import fetch_rows


@dataclass
class _StatementBlock:
    line: int
    sql: str


@dataclass
class _QueryBlock:
    line: int
    header: str
    sql: str
    expected_rows: list[str]
    rowsort: bool


def _yardstick_measures_test_path() -> Path:
    override = os.environ.get("YARDSTICK_MEASURES_TEST_PATH")
    if override:
        return Path(override).expanduser()

    vendored = Path(__file__).resolve().parents[1] / "fixtures" / "sql" / "yardstick" / "measures.test"
    if vendored.exists():
        return vendored

    return Path("~/Code/yardstick/test/sql/measures.test").expanduser()


def _parse_measures_test(path: Path) -> tuple[list[_StatementBlock], list[_QueryBlock]]:
    lines = path.read_text().splitlines()
    statements: list[_StatementBlock] = []
    queries: list[_QueryBlock] = []

    i = 0
    while i < len(lines):
        stripped = lines[i].strip()

        if stripped.startswith("statement "):
            start_line = i + 1
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1

            sql_lines: list[str] = []
            while i < len(lines):
                sql_lines.append(lines[i])
                if lines[i].strip().endswith(";"):
                    i += 1
                    break
                i += 1

            sql = "\n".join(sql_lines).strip()
            if sql:
                statements.append(_StatementBlock(line=start_line, sql=sql))
            continue

        if stripped.startswith("query "):
            header = stripped
            start_line = i + 1
            rowsort = "rowsort" in header.split()
            i += 1

            sql_lines: list[str] = []
            while i < len(lines) and lines[i].strip() != "----":
                sql_lines.append(lines[i])
                i += 1

            if i >= len(lines):
                raise ValueError(f"Missing '----' separator for query at line {start_line}")
            i += 1  # Skip ----

            expected_rows: list[str] = []
            while i < len(lines):
                current = lines[i]
                current_stripped = current.strip()
                if not current_stripped:
                    while i < len(lines) and not lines[i].strip():
                        i += 1
                    break
                if current_stripped.startswith(("#", "statement ", "query ", "require ")):
                    break
                expected_rows.append(current.rstrip())
                i += 1

            sql = "\n".join(sql_lines).strip()
            queries.append(
                _QueryBlock(
                    line=start_line,
                    header=header,
                    sql=sql,
                    expected_rows=expected_rows,
                    rowsort=rowsort,
                )
            )
            continue

        i += 1

    return statements, queries


def _stringify_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    return str(value)


def _rows_to_lines(rows: list[tuple[object, ...]]) -> list[str]:
    return ["\t".join(_stringify_value(value) for value in row) for row in rows]


_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?(?:\d+\.\d*|\d*\.\d+|\d+[eE][+-]?\d+|\d+\.\d*[eE][+-]?\d+)$")


def _parse_expected_cell(token: str) -> object:
    if token == "NULL":
        return None
    if _INT_RE.match(token):
        return int(token)
    if _FLOAT_RE.match(token):
        return float(token)
    return token


def _cell_matches(actual: object, expected: object) -> bool:
    if expected is None:
        return actual is None

    if isinstance(expected, float):
        if actual is None:
            return False
        try:
            actual_float = float(actual)
        except (TypeError, ValueError):
            return False
        return actual_float == pytest.approx(expected, rel=1e-12, abs=1e-12)

    if isinstance(expected, int):
        if actual is None or isinstance(actual, bool):
            return False
        try:
            return float(actual) == float(expected)
        except (TypeError, ValueError):
            return False

    return str(actual) == str(expected)


def _assert_query_rows_match(query: _QueryBlock, actual_rows: list[tuple[object, ...]]) -> None:
    expected_rows = [row.split("\t") for row in query.expected_rows]

    if query.rowsort:
        actual_rows = sorted(actual_rows, key=lambda row: tuple(_stringify_value(value) for value in row))
        expected_rows = sorted(expected_rows)

    if len(actual_rows) != len(expected_rows):
        pytest.fail(
            "\n".join(
                [
                    f"Row count mismatch for query at line {query.line}: {query.header}",
                    f"Expected {len(expected_rows)} rows, got {len(actual_rows)} rows",
                    "SQL:",
                    query.sql,
                    "Expected:",
                    "\n".join(query.expected_rows),
                    "Actual:",
                    "\n".join(_rows_to_lines(actual_rows)),
                ]
            )
        )

    for row_index, (actual_row, expected_tokens) in enumerate(zip(actual_rows, expected_rows, strict=False), start=1):
        if len(actual_row) != len(expected_tokens):
            pytest.fail(
                "\n".join(
                    [
                        f"Column count mismatch in query at line {query.line}: {query.header}",
                        f"Row {row_index}: expected {len(expected_tokens)} columns, got {len(actual_row)} columns",
                        "SQL:",
                        query.sql,
                        f"Expected row: {' | '.join(expected_tokens)}",
                        f"Actual row: {' | '.join(_stringify_value(value) for value in actual_row)}",
                    ]
                )
            )

        for col_index, (actual_value, expected_token) in enumerate(
            zip(actual_row, expected_tokens, strict=False), start=1
        ):
            expected_value = _parse_expected_cell(expected_token)
            if _cell_matches(actual_value, expected_value):
                continue

            pytest.fail(
                "\n".join(
                    [
                        f"Value mismatch in query at line {query.line}: {query.header}",
                        f"Row {row_index}, column {col_index}",
                        "SQL:",
                        query.sql,
                        f"Expected token: {expected_token}",
                        f"Actual value: {_stringify_value(actual_value)}",
                        "Expected rows:",
                        "\n".join(query.expected_rows),
                        "Actual rows:",
                        "\n".join(_rows_to_lines(actual_rows)),
                    ]
                )
            )


def _apply_statement(layer: SemanticLayer, adapter: YardstickAdapter, sql: str) -> None:
    parsed = adapter._parse_statements(sql)
    if parsed:
        statement = parsed[0]
        if isinstance(statement, exp.Create) and (statement.args.get("kind") or "").upper() == "VIEW":
            select = statement.expression
            if isinstance(select, exp.Select):
                model = adapter._model_from_create_view(statement, select)
                if model is not None:
                    layer.add_model(model)
                    return
    layer.adapter.execute(sql)


def test_yardstick_measures_test_replay():
    path = _yardstick_measures_test_path()
    if not path.exists():
        pytest.skip(f"Yardstick measures.test not found: {path}")

    statements, queries = _parse_measures_test(path)
    assert len(queries) == 93, "Expected 93 query blocks from upstream measures.test"

    layer = SemanticLayer(connection="duckdb:///:memory:")
    adapter = YardstickAdapter()

    statement_index = 0
    for query in queries:
        while statement_index < len(statements) and statements[statement_index].line < query.line:
            statement = statements[statement_index]
            _apply_statement(layer, adapter, statement.sql)
            statement_index += 1

        actual_rows = fetch_rows(layer.sql(query.sql))
        _assert_query_rows_match(query, actual_rows)
