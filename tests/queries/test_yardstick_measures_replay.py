"""Replay Yardstick's measures.test end-to-end for parity confidence."""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
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
    header: str
    sql: str
    expect_error: bool
    expected_error_lines: list[str]


@dataclass
class _QueryBlock:
    line: int
    header: str
    sql: str
    expected_rows: list[str]
    rowsort: bool


@dataclass
class _ExpectedDimension:
    name: str
    sql: str
    type: str
    granularity: str | None


@dataclass
class _ExpectedMetric:
    name: str
    agg: str | None
    sql: str | None
    filters: list[str] | None
    type: str | None


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _yardstick_measures_test_path() -> Path:
    override = os.environ.get("YARDSTICK_MEASURES_TEST_PATH")
    if override:
        return Path(override).expanduser()

    vendored = Path(__file__).resolve().parents[1] / "fixtures" / "sql" / "yardstick" / "measures.test"
    if vendored.exists():
        return vendored

    return Path("~/Code/yardstick/test/sql/measures.test").expanduser()


def _run_git(args: list[str], cwd: Path | None = None) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        pytest.fail("git is required to run live upstream Yardstick parity tests")
    except subprocess.CalledProcessError as exc:
        pytest.fail(
            "\n".join(
                [
                    "Failed to fetch live upstream Yardstick tests",
                    f"Command: git {' '.join(args)}",
                    f"stdout: {exc.stdout.strip()}",
                    f"stderr: {exc.stderr.strip()}",
                ]
            )
        )
    return result.stdout.strip()


def _yardstick_upstream_checkout() -> Path:
    override = os.environ.get("YARDSTICK_UPSTREAM_PATH")
    if override:
        path = Path(override).expanduser()
        if not path.exists():
            pytest.fail(f"YARDSTICK_UPSTREAM_PATH does not exist: {path}")
        return path

    if not _env_flag("SIDEMANTIC_YARDSTICK_UPSTREAM_TESTS"):
        pytest.skip("Set SIDEMANTIC_YARDSTICK_UPSTREAM_TESTS=1 to fetch and replay live upstream Yardstick tests")

    repo_url = os.environ.get("YARDSTICK_UPSTREAM_REPO", "https://github.com/sidequery/yardstick.git")
    ref = os.environ.get("YARDSTICK_UPSTREAM_REF", "main")
    checkout = Path(
        os.environ.get(
            "YARDSTICK_UPSTREAM_CACHE_DIR",
            str(Path(tempfile.gettempdir()) / "sidemantic-yardstick-upstream"),
        )
    ).expanduser()

    if not (checkout / ".git").exists():
        if checkout.exists() and any(checkout.iterdir()):
            pytest.fail(f"YARDSTICK_UPSTREAM_CACHE_DIR exists but is not a git checkout: {checkout}")
        checkout.parent.mkdir(parents=True, exist_ok=True)
        _run_git(["clone", "--depth", "1", repo_url, str(checkout)])

    _run_git(["fetch", "--depth", "1", "origin", ref], cwd=checkout)
    _run_git(["checkout", "--detach", "FETCH_HEAD"], cwd=checkout)
    return checkout


def _yardstick_upstream_sql_test_paths() -> list[Path]:
    checkout = _yardstick_upstream_checkout()
    paths = sorted((checkout / "test" / "sql").glob("*.test"))
    if not paths:
        pytest.fail(f"No upstream Yardstick SQL test files found under {checkout / 'test' / 'sql'}")
    return paths


def _parse_measures_test(path: Path) -> tuple[list[_StatementBlock], list[_QueryBlock]]:
    lines = path.read_text().splitlines()
    statements: list[_StatementBlock] = []
    queries: list[_QueryBlock] = []

    i = 0
    while i < len(lines):
        stripped = lines[i].strip()

        if stripped.startswith("statement "):
            header = stripped
            start_line = i + 1
            expect_error = "error" in header.split()
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

            expected_error_lines: list[str] = []
            if expect_error and i < len(lines) and lines[i].strip() == "----":
                i += 1
                while i < len(lines):
                    current = lines[i]
                    current_stripped = current.strip()
                    if not current_stripped:
                        while i < len(lines) and not lines[i].strip():
                            i += 1
                        break
                    if current_stripped.startswith(("#", "statement ", "query ", "require ")):
                        break
                    expected_error_lines.append(current.rstrip())
                    i += 1

            sql = "\n".join(sql_lines).strip()
            if sql:
                statements.append(
                    _StatementBlock(
                        line=start_line,
                        header=header,
                        sql=sql,
                        expect_error=expect_error,
                        expected_error_lines=expected_error_lines,
                    )
                )
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
    if token.lower() == "true":
        return True
    if token.lower() == "false":
        return False
    if _INT_RE.match(token):
        return int(token)
    if _FLOAT_RE.match(token):
        return float(token)
    return token


def _cell_matches(actual: object, expected: object) -> bool:
    if expected is None:
        return actual is None

    if isinstance(expected, str) and isinstance(actual, (date, datetime, time)):
        actual_text = _stringify_value(actual)
        if actual_text == expected:
            return True
        if isinstance(actual, date) and not isinstance(actual, datetime):
            return f"{actual.isoformat()} 00:00:00" == expected
        return False

    if isinstance(expected, bool):
        return actual is expected

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


def _execute_statement_sql(layer: SemanticLayer, adapter: YardstickAdapter, sql: str) -> None:
    try:
        parsed = adapter._parse_statements(sql)
    except Exception:
        parsed = None

    if parsed:
        statement = parsed[0]
        if isinstance(statement, exp.Create) and (statement.args.get("kind") or "").upper() == "VIEW":
            select = statement.expression
            if isinstance(select, exp.Select):
                model = adapter._model_from_create_view(statement, select)
                if model is not None:
                    layer.add_model(model)
                    if model.sql:
                        layer.adapter.execute(f"CREATE VIEW {model.name} AS {model.sql}")
                    elif model.table and model.table != model.name:
                        layer.adapter.execute(f"CREATE VIEW {model.name} AS SELECT * FROM {model.table}")
                    return

    statement_head = sql.lstrip().upper()
    if statement_head.startswith(("SEMANTIC ", "SELECT ", "WITH ")) or "AGGREGATE" in statement_head:
        layer.sql(sql)
        return

    layer.adapter.execute(sql)


def _create_view_model_from_statement(
    adapter: YardstickAdapter,
    sql: str,
) -> tuple[exp.Create, exp.Select, object] | None:
    try:
        parsed = adapter._parse_statements(sql)
    except Exception:
        return None

    if not parsed:
        return None

    statement = parsed[0]
    if not isinstance(statement, exp.Create) or (statement.args.get("kind") or "").upper() != "VIEW":
        return None

    select = statement.expression
    if not isinstance(select, exp.Select):
        return None

    model = adapter._model_from_create_view(statement, select)
    if model is None:
        return None

    return statement, select, model


def _expected_model_source(select: exp.Select, dialect: str) -> tuple[str | None, str | None]:
    from_clause = select.args.get("from")
    joins = select.args.get("joins") or []
    where_clause = select.args.get("where")
    with_clause = select.args.get("with")

    if (
        isinstance(from_clause, exp.From)
        and isinstance(from_clause.this, exp.Table)
        and not joins
        and where_clause is None
        and with_clause is None
    ):
        table_expr = from_clause.this
        if isinstance(table_expr.this, exp.Identifier) and table_expr.args.get("alias") is None:
            return table_expr.sql(dialect=dialect), None

    if from_clause is None:
        return None, None

    base_relation = exp.select("*")
    if with_clause is not None:
        base_relation.set("with", with_clause.copy())
    base_relation.set("from", from_clause.copy())
    if joins:
        base_relation.set("joins", [join.copy() for join in joins])
    if where_clause is not None:
        base_relation.set("where", where_clause.copy())

    return None, base_relation.sql(dialect=dialect)


def _expected_dimension_type(expression: exp.Expression) -> tuple[str, str | None]:
    if isinstance(expression, exp.Boolean):
        return "boolean", None
    if isinstance(expression, exp.Literal):
        if expression.is_number:
            return "numeric", None
        return "categorical", None
    if isinstance(expression, exp.Column):
        column_name = expression.name.lower()
        if "timestamp" in column_name:
            return "time", "second"
        if "date" in column_name:
            return "time", "day"
        if "time" in column_name:
            return "time", "second"
        return "categorical", None
    if isinstance(expression, exp.Func):
        granularity_by_func = {
            "date": "day",
            "date_trunc": "day",
            "year": "year",
            "quarter": "quarter",
            "month": "month",
            "week": "week",
            "day": "day",
            "hour": "hour",
            "minute": "minute",
        }
        function_name = (expression.name or "").lower()
        if function_name in granularity_by_func:
            return "time", granularity_by_func[function_name]
    return "categorical", None


_EXPECTED_SIMPLE_AGGREGATIONS: tuple[tuple[type[exp.Expression], str], ...] = (
    (exp.Sum, "sum"),
    (exp.Avg, "avg"),
    (exp.Min, "min"),
    (exp.Max, "max"),
    (exp.Median, "median"),
    (exp.Stddev, "stddev"),
    (exp.StddevPop, "stddev_pop"),
    (exp.Variance, "variance"),
    (exp.VariancePop, "variance_pop"),
)
_EXPECTED_SUPPORTED_FUNCTION_AGGS = {
    "avg",
    "max",
    "median",
    "min",
    "stddev",
    "stddev_pop",
    "sum",
    "variance",
    "variance_pop",
}
_EXPECTED_ANONYMOUS_AGGREGATIONS = {
    "entropy",
    "geometric_mean",
    "kurtosis",
    "mode",
    "product",
    "skewness",
    "weighted_avg",
}


def _expected_count_aggregation(expression: exp.Expression, dialect: str) -> tuple[str, str] | None:
    count_expr = None
    if isinstance(expression, exp.Count):
        count_expr = expression.this
    elif isinstance(expression, exp.Func) and (expression.name or "").lower() == "count":
        count_expr = expression.this or (expression.expressions[0] if expression.expressions else None)
    else:
        return None

    if isinstance(count_expr, exp.Distinct):
        if count_expr.expressions:
            return "count_distinct", ", ".join(expr.sql(dialect=dialect) for expr in count_expr.expressions)
        return "count_distinct", count_expr.sql(dialect=dialect)

    if count_expr is None or isinstance(count_expr, exp.Star):
        return "count", "*"
    return "count", count_expr.sql(dialect=dialect)


def _expected_supported_aggregation(expression: exp.Expression, dialect: str) -> tuple[str, str] | None:
    count_aggregation = _expected_count_aggregation(expression, dialect)
    if count_aggregation is not None:
        return count_aggregation

    for expression_type, aggregation_name in _EXPECTED_SIMPLE_AGGREGATIONS:
        if isinstance(expression, expression_type):
            inner_expression = expression.this
            if inner_expression is None:
                return aggregation_name, "*"
            return aggregation_name, inner_expression.sql(dialect=dialect)

    if isinstance(expression, exp.Func):
        function_name = (expression.name or "").lower()
        if function_name in _EXPECTED_SUPPORTED_FUNCTION_AGGS:
            inner_expression = expression.this or (expression.expressions[0] if expression.expressions else None)
            if inner_expression is None:
                return function_name, "*"
            return function_name, inner_expression.sql(dialect=dialect)

    return None


def _expected_filtered_aggregation(
    expression: exp.Expression,
    dialect: str,
) -> tuple[str, str, list[str] | None] | None:
    if not isinstance(expression, exp.Filter):
        return None

    aggregation = _expected_supported_aggregation(expression.this, dialect)
    if aggregation is None:
        return None

    agg, inner_sql = aggregation
    where_expression = expression.args.get("expression")
    if isinstance(where_expression, exp.Where):
        filter_sql = where_expression.this.sql(dialect=dialect)
    elif isinstance(where_expression, exp.Expression):
        filter_sql = where_expression.sql(dialect=dialect)
    else:
        filter_sql = ""

    return agg, inner_sql, [filter_sql] if filter_sql else None


def _expected_has_aggregate_semantics(expression: exp.Expression) -> bool:
    if any(isinstance(node, exp.AggFunc) for node in expression.walk()):
        return True

    for node in expression.walk():
        if isinstance(node, exp.List):
            return True
        if isinstance(node, exp.Anonymous) and (node.name or "").lower() in _EXPECTED_ANONYMOUS_AGGREGATIONS:
            return True
    return False


def _expected_references_other_measures(
    name: str,
    expression: exp.Expression,
    all_measure_names: set[str],
) -> bool:
    measure_lookup = {
        measure_name.lower() for measure_name in all_measure_names if measure_name.lower() != name.lower()
    }
    referenced_columns = {column.name.lower() for column in expression.find_all(exp.Column)}
    return bool(referenced_columns & measure_lookup)


def _expected_metric_from_expression(
    name: str,
    expression: exp.Expression,
    all_measure_names: set[str],
    dialect: str,
) -> _ExpectedMetric:
    expression_sql = expression.sql(dialect=dialect)

    if _expected_references_other_measures(name, expression, all_measure_names):
        return _ExpectedMetric(name=name, agg=None, sql=expression_sql, filters=None, type="derived")

    filtered_aggregation = _expected_filtered_aggregation(expression, dialect)
    if filtered_aggregation is not None:
        agg, inner_sql, filters = filtered_aggregation
        return _ExpectedMetric(name=name, agg=agg, sql=inner_sql, filters=filters, type=None)

    simple_aggregation = _expected_supported_aggregation(expression, dialect)
    if simple_aggregation is not None:
        agg, inner_sql = simple_aggregation
        return _ExpectedMetric(name=name, agg=agg, sql=inner_sql, filters=None, type=None)

    if _expected_has_aggregate_semantics(expression):
        return _ExpectedMetric(name=name, agg=None, sql=expression_sql, filters=None, type=None)

    return _ExpectedMetric(name=name, agg=None, sql=expression_sql, filters=None, type="derived")


def _expected_projections(
    select: exp.Select,
    dialect: str,
) -> tuple[list[_ExpectedDimension], list[_ExpectedMetric]]:
    measure_aliases = {
        projection.output_name
        for projection in select.expressions
        if isinstance(projection, exp.Alias) and projection.args.get("yardstick_measure")
    }
    expected_dimensions: list[_ExpectedDimension] = []
    expected_metrics: list[_ExpectedMetric] = []

    for projection in select.expressions:
        output_name = projection.output_name
        if not output_name:
            continue

        expression = projection.this if isinstance(projection, exp.Alias) else projection
        if output_name in measure_aliases:
            expected_metrics.append(
                _expected_metric_from_expression(
                    output_name,
                    expression,
                    all_measure_names=set(measure_aliases),
                    dialect=dialect,
                )
            )
            continue

        if isinstance(expression, exp.Star):
            continue

        dimension_type, granularity = _expected_dimension_type(expression)
        expected_dimensions.append(
            _ExpectedDimension(
                name=output_name,
                sql=expression.sql(dialect=dialect),
                type=dimension_type,
                granularity=granularity,
            )
        )
    return expected_dimensions, expected_metrics


def _assert_definition_blocks_match(path: Path) -> None:
    statements, _queries = _parse_measures_test(path)
    adapter = YardstickAdapter()
    checked_models: list[str] = []

    for statement_block in statements:
        parsed_model = _create_view_model_from_statement(adapter, statement_block.sql)
        if parsed_model is None:
            continue

        create_statement, select, model = parsed_model
        expected_source_table, expected_source_sql = _expected_model_source(select, adapter.dialect)
        expected_dimensions, expected_metrics = _expected_projections(select, adapter.dialect)
        view_name = create_statement.this.name if isinstance(create_statement.this, exp.Table) else None
        context = f"{path}:{statement_block.line}"
        if view_name:
            context = f"{context} ({view_name})"

        assert model.name == view_name, context
        assert model.primary_key == (expected_dimensions[0].name if expected_dimensions else "id"), context
        assert model.table == (expected_source_table or (view_name if expected_source_sql is None else None)), context
        assert model.sql == expected_source_sql, context

        yardstick_metadata = (model.metadata or {}).get("yardstick")
        assert isinstance(yardstick_metadata, dict), context
        assert yardstick_metadata.get("view_sql") == select.sql(dialect=adapter.dialect), context
        assert yardstick_metadata.get("base_table") == expected_source_table, context
        assert yardstick_metadata.get("base_relation_sql") == expected_source_sql, context

        assert len(model.dimensions) == len(expected_dimensions), context
        for actual_dimension, expected_dimension in zip(model.dimensions, expected_dimensions, strict=True):
            assert actual_dimension.name == expected_dimension.name, context
            assert actual_dimension.sql == expected_dimension.sql, context
            assert actual_dimension.type == expected_dimension.type, context
            assert actual_dimension.granularity == expected_dimension.granularity, context

        assert len(model.metrics) == len(expected_metrics), context
        for actual_metric, expected_metric in zip(model.metrics, expected_metrics, strict=True):
            assert actual_metric.name == expected_metric.name, context
            assert actual_metric.agg == expected_metric.agg, context
            assert actual_metric.sql == expected_metric.sql, context
            assert actual_metric.filters == expected_metric.filters, context
            assert actual_metric.type == expected_metric.type, context

        checked_models.append(model.name)

    assert checked_models, f"No Yardstick CREATE VIEW ... AS MEASURE definitions found in {path}"


def _apply_statement(layer: SemanticLayer, adapter: YardstickAdapter, statement: _StatementBlock) -> None:
    if not statement.expect_error:
        _execute_statement_sql(layer, adapter, statement.sql)
        return

    with pytest.raises(Exception) as exc_info:
        _execute_statement_sql(layer, adapter, statement.sql)

    if statement.expected_error_lines:
        expected_text = "\n".join(statement.expected_error_lines)
        actual_text = str(exc_info.value)
        if expected_text not in actual_text:
            pytest.fail(
                "\n".join(
                    [
                        f"Error text mismatch for statement at line {statement.line}: {statement.header}",
                        "SQL:",
                        statement.sql,
                        "Expected error text:",
                        expected_text,
                        "Actual error text:",
                        actual_text,
                    ]
                )
            )


def _replay_yardstick_sql_test(path: Path) -> None:
    if not path.exists():
        pytest.skip(f"Yardstick SQL test file not found: {path}")

    statements, queries = _parse_measures_test(path)
    assert queries, f"No query blocks parsed from Yardstick SQL test: {path}"

    layer = SemanticLayer(connection="duckdb:///:memory:")
    adapter = YardstickAdapter()

    statement_index = 0
    for query in queries:
        while statement_index < len(statements) and statements[statement_index].line < query.line:
            statement = statements[statement_index]
            _apply_statement(layer, adapter, statement)
            statement_index += 1

        try:
            actual_rows = fetch_rows(layer.sql(query.sql))
        except Exception as exc:
            pytest.fail(
                "\n".join(
                    [
                        f"Execution failed for query at {path}:{query.line}: {query.header}",
                        "SQL:",
                        query.sql,
                        f"Error: {exc}",
                    ]
                )
            )
        _assert_query_rows_match(query, actual_rows)


def test_yardstick_measures_test_replay():
    _replay_yardstick_sql_test(_yardstick_measures_test_path())


@pytest.mark.yardstick_upstream
def test_yardstick_upstream_create_view_definitions():
    for path in _yardstick_upstream_sql_test_paths():
        _assert_definition_blocks_match(path)


@pytest.mark.yardstick_upstream
def test_yardstick_upstream_sql_replay():
    for path in _yardstick_upstream_sql_test_paths():
        _replay_yardstick_sql_test(path)
