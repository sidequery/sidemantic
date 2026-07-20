"""Golden-query test runner (``sidemantic test``) and live schema checks.

Test files are YAML documents with a top-level ``tests`` list:

    tests:
      - name: total revenue 2024
        sql: SELECT orders.revenue FROM orders WHERE orders.created_at >= '2024-01-01'
        expect:
          value: 1200000.5
          tolerance: 0.01
      - name: status breakdown shape
        sql: SELECT orders.status, orders.revenue FROM orders
        expect:
          row_count: 3

``expect`` supports ``value`` (single row, single column), ``rows`` (full
result comparison), ``row_count``, optional ``columns`` (result column names),
and ``tolerance`` for numeric comparisons.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from numbers import Number
from pathlib import Path

import yaml

TEST_FILE_SUFFIXES = {".yml", ".yaml"}


@dataclass
class Expectation:
    value: object | None = None
    rows: list[list] | None = None
    row_count: int | None = None
    columns: list[str] | None = None
    tolerance: float = 0.0

    def assertions(self) -> list[str]:
        present = []
        if self.value is not None:
            present.append("value")
        if self.rows is not None:
            present.append("rows")
        if self.row_count is not None:
            present.append("row_count")
        if self.columns is not None:
            present.append("columns")
        return present


@dataclass
class TestCase:
    name: str
    sql: str
    expect: Expectation
    source: Path


@dataclass
class TestResult:
    case: TestCase
    passed: bool
    message: str | None = None


@dataclass
class TestRunReport:
    results: list[TestResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(result.passed for result in self.results)

    @property
    def failures(self) -> list[TestResult]:
        return [result for result in self.results if not result.passed]

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "total": len(self.results),
            "failed": len(self.failures),
            "tests": [
                {
                    "name": result.case.name,
                    "source": str(result.case.source),
                    "passed": result.passed,
                    "message": result.message,
                }
                for result in self.results
            ],
        }


def discover_test_files(root: Path) -> list[Path]:
    """Return the project's golden-test files (tests/*.yml under the root)."""

    tests_dir = root / "tests"
    if not tests_dir.is_dir():
        return []
    return sorted(path for path in tests_dir.iterdir() if path.is_file() and path.suffix.lower() in TEST_FILE_SUFFIXES)


def load_test_file(path: Path) -> list[TestCase]:
    """Parse one golden-test YAML file, validating its shape eagerly."""

    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict) or not isinstance(data.get("tests"), list):
        raise ValueError(f"{path}: expected a top-level 'tests' list")
    cases: list[TestCase] = []
    for index, entry in enumerate(data["tests"]):
        label = f"{path}: tests[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{label} must be a mapping")
        name = entry.get("name") or f"test {index + 1}"
        sql = entry.get("sql")
        if not sql or not isinstance(sql, str):
            raise ValueError(f"{label} ('{name}') is missing a 'sql' string")
        raw_expect = entry.get("expect")
        if not isinstance(raw_expect, dict):
            raise ValueError(f"{label} ('{name}') is missing an 'expect' mapping")
        unknown = set(raw_expect) - {"value", "rows", "row_count", "columns", "tolerance"}
        if unknown:
            raise ValueError(f"{label} ('{name}') has unknown expect keys: {', '.join(sorted(unknown))}")
        expect = Expectation(
            value=raw_expect.get("value"),
            rows=raw_expect.get("rows"),
            row_count=raw_expect.get("row_count"),
            columns=raw_expect.get("columns"),
            tolerance=float(raw_expect.get("tolerance") or 0.0),
        )
        if not expect.assertions():
            raise ValueError(f"{label} ('{name}') needs at least one of: value, rows, row_count, columns")
        cases.append(TestCase(name=name, sql=sql.strip(), expect=expect, source=path))
    return cases


def load_test_cases(paths: list[Path]) -> list[TestCase]:
    cases: list[TestCase] = []
    for path in paths:
        cases.extend(load_test_file(path))
    return cases


def _values_match(actual, expected, tolerance: float) -> bool:
    if isinstance(actual, Number) and isinstance(expected, Number) and not isinstance(actual, bool):
        return abs(float(actual) - float(expected)) <= max(tolerance, 1e-9)
    return actual == expected


def _check_case(case: TestCase, columns: list[str], rows: list[tuple]) -> str | None:
    expect = case.expect
    if expect.columns is not None and list(columns) != list(expect.columns):
        return f"expected columns {expect.columns}, got {list(columns)}"
    if expect.row_count is not None and len(rows) != expect.row_count:
        return f"expected {expect.row_count} row(s), got {len(rows)}"
    if expect.value is not None:
        if len(rows) != 1 or len(rows[0]) != 1:
            return f"expected a single value but query returned {len(rows)} row(s) x {len(rows[0]) if rows else 0} column(s)"
        if not _values_match(rows[0][0], expect.value, expect.tolerance):
            return f"expected value {expect.value!r}, got {rows[0][0]!r}"
    if expect.rows is not None:
        if len(rows) != len(expect.rows):
            return f"expected {len(expect.rows)} row(s), got {len(rows)}"
        for row_index, (actual_row, expected_row) in enumerate(zip(rows, expect.rows)):
            if len(actual_row) != len(expected_row):
                return f"row {row_index}: expected {len(expected_row)} column(s), got {len(actual_row)}"
            for column_index, (actual, expected) in enumerate(zip(actual_row, expected_row)):
                if not _values_match(actual, expected, expect.tolerance):
                    return f"row {row_index}, column {column_index}: expected {expected!r}, got {actual!r}"
    return None


def run_tests(layer, cases: list[TestCase]) -> TestRunReport:
    """Execute golden-query cases against a loaded semantic layer."""

    report = TestRunReport()
    for case in cases:
        try:
            cursor = layer.sql(case.sql)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
        except Exception as exc:
            report.results.append(TestResult(case=case, passed=False, message=f"query failed: {exc}"))
            continue
        failure = _check_case(case, columns, rows)
        report.results.append(TestResult(case=case, passed=failure is None, message=failure))
    return report


# --- Live schema drift checks (``sidemantic validate --live``) ---


def _bare_identifier(expression: str | None) -> str | None:
    if expression is None:
        return None
    candidate = expression.strip()
    if candidate.replace("_", "a").isalnum() and (candidate[0].isalpha() or candidate[0] == "_"):
        return candidate
    return None


def _referenced_columns(expression: str, dialect: str) -> set[str]:
    """Best-effort column references in a SQL expression fragment."""

    bare = _bare_identifier(expression)
    if bare is not None:
        return {bare}
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(f"SELECT {expression}", dialect=dialect)
        return {column.name for column in parsed.find_all(exp.Column) if not column.table}
    except Exception:
        return set()


@dataclass
class DriftReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checked_models: int = 0


def check_schema_drift(layer) -> DriftReport:
    """Compare loaded models against the live database schema.

    Catches dropped/renamed tables and columns that static validation cannot
    see: every table-backed model is probed with a zero-row select and every
    plainly-referenced column is checked against the live column list.
    """

    report = DriftReport()
    dialect = layer.adapter.dialect
    for model_name, model in sorted(layer.graph.models.items()):
        if model.table:
            probe = f"SELECT * FROM {model.table} LIMIT 0"
        elif model.sql:
            probe = f"SELECT * FROM ({model.sql}) AS _sidemantic_probe LIMIT 0"
        else:
            continue
        try:
            cursor = layer.adapter.execute(probe)
            live_columns = {description[0].lower() for description in cursor.description}
        except Exception as exc:
            source = model.table or "inline sql"
            report.errors.append(f"Model '{model_name}': cannot read {source}: {exc}")
            continue
        report.checked_models += 1

        def check(kind: str, label: str, expression: str | None, columns=live_columns, name=model_name):
            for column in _referenced_columns(expression, dialect) if expression else set():
                if column.lower() not in columns:
                    report.errors.append(f"Model '{name}': {kind} '{label}' references missing column '{column}'")

        for column in model.primary_key_columns:
            if column.lower() not in live_columns:
                report.errors.append(f"Model '{model_name}': primary key column '{column}' not found in database")
        for dimension in model.dimensions:
            check("dimension", dimension.name, dimension.sql or dimension.name)
        for metric in model.metrics:
            if metric.sql:
                check("metric", metric.name, metric.sql)
        for relationship in model.relationships:
            for column in relationship.foreign_key_columns:
                if column.lower() not in live_columns:
                    report.errors.append(
                        f"Model '{model_name}': relationship '{relationship.name}' foreign key '{column}' not found in database"
                    )
    return report
