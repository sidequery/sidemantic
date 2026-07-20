"""Tests for the golden-test loader/runner and live schema drift (sidemantic/testing.py)."""

from __future__ import annotations

from pathlib import Path

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.testing import (
    check_schema_drift,
    load_test_file,
    run_tests,
)

# --- load_test_file shape validation -----------------------------------------


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "cases.yml"
    path.write_text(body)
    return path


def test_load_test_file_parses_valid_cases(tmp_path: Path):
    path = _write(
        tmp_path,
        """
tests:
  - name: revenue
    sql: SELECT orders.revenue FROM orders
    expect:
      value: 250.0
      tolerance: 0.01
""",
    )
    cases = load_test_file(path)
    assert len(cases) == 1
    assert cases[0].name == "revenue"
    assert cases[0].expect.value == 250.0
    assert cases[0].expect.tolerance == 0.01


def test_load_test_file_missing_tests_list(tmp_path: Path):
    path = _write(tmp_path, "not_tests: []\n")
    with pytest.raises(ValueError, match="expected a top-level 'tests' list"):
        load_test_file(path)


def test_load_test_file_missing_sql(tmp_path: Path):
    path = _write(tmp_path, "tests:\n  - name: x\n    expect:\n      value: 1\n")
    with pytest.raises(ValueError, match="missing a 'sql' string"):
        load_test_file(path)


def test_load_test_file_missing_expect(tmp_path: Path):
    path = _write(tmp_path, "tests:\n  - name: x\n    sql: SELECT 1\n")
    with pytest.raises(ValueError, match="missing an 'expect' mapping"):
        load_test_file(path)


def test_load_test_file_unknown_expect_keys(tmp_path: Path):
    path = _write(
        tmp_path,
        "tests:\n  - name: x\n    sql: SELECT 1\n    expect:\n      value: 1\n      bogus: 2\n",
    )
    with pytest.raises(ValueError, match="unknown expect keys: bogus"):
        load_test_file(path)


def test_load_test_file_needs_at_least_one_assertion(tmp_path: Path):
    path = _write(
        tmp_path,
        "tests:\n  - name: x\n    sql: SELECT 1\n    expect:\n      tolerance: 0.1\n",
    )
    with pytest.raises(ValueError, match="needs at least one of"):
        load_test_file(path)


# --- run_tests ----------------------------------------------------------------


@pytest.fixture
def orders_layer() -> SemanticLayer:
    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE orders (id INTEGER, status VARCHAR, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO orders VALUES (1, 'paid', 100.0), (2, 'paid', 150.0), (3, 'pending', 50.0)")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )
    return layer


def _cases(tmp_path: Path, body: str):
    path = tmp_path / "t.yml"
    path.write_text(body)
    return load_test_file(path)


def test_run_tests_value_with_tolerance_passes(orders_layer: SemanticLayer, tmp_path: Path):
    cases = _cases(
        tmp_path,
        "tests:\n  - name: revenue\n    sql: SELECT orders.revenue FROM orders\n    expect:\n      value: 300.0\n      tolerance: 0.5\n",
    )
    report = run_tests(orders_layer, cases)
    assert report.passed
    assert report.results[0].message is None


def test_run_tests_row_count_passes(orders_layer: SemanticLayer, tmp_path: Path):
    cases = _cases(
        tmp_path,
        "tests:\n  - name: shape\n    sql: SELECT orders.status, orders.revenue FROM orders\n    expect:\n      row_count: 2\n",
    )
    report = run_tests(orders_layer, cases)
    assert report.passed


def test_run_tests_rows_mismatch_message(orders_layer: SemanticLayer, tmp_path: Path):
    cases = _cases(
        tmp_path,
        "tests:\n  - name: rows\n    sql: SELECT orders.status, orders.revenue FROM orders ORDER BY orders.status\n"
        "    expect:\n      rows:\n        - [paid, 999.0]\n        - [pending, 50.0]\n",
    )
    report = run_tests(orders_layer, cases)
    assert not report.passed
    assert "expected 999.0" in report.results[0].message


def test_run_tests_columns_assertion(orders_layer: SemanticLayer, tmp_path: Path):
    cases = _cases(
        tmp_path,
        "tests:\n  - name: cols\n    sql: SELECT orders.status, orders.revenue FROM orders\n"
        "    expect:\n      columns: [status, revenue]\n",
    )
    report = run_tests(orders_layer, cases)
    assert report.passed, report.results[0].message


def test_run_tests_columns_mismatch_message(orders_layer: SemanticLayer, tmp_path: Path):
    cases = _cases(
        tmp_path,
        "tests:\n  - name: cols\n    sql: SELECT orders.status FROM orders\n    expect:\n      columns: [wrong_name]\n",
    )
    report = run_tests(orders_layer, cases)
    assert not report.passed
    assert "expected columns" in report.results[0].message


def test_run_tests_query_failure_is_reported(orders_layer: SemanticLayer, tmp_path: Path):
    cases = _cases(
        tmp_path,
        "tests:\n  - name: broken\n    sql: SELECT orders.nonexistent FROM orders\n    expect:\n      value: 1\n",
    )
    report = run_tests(orders_layer, cases)
    assert not report.passed
    assert report.results[0].message.startswith("query failed:")


def test_run_report_to_dict_summarizes(orders_layer: SemanticLayer, tmp_path: Path):
    cases = _cases(
        tmp_path,
        "tests:\n  - name: ok\n    sql: SELECT orders.revenue FROM orders\n    expect:\n      value: 300.0\n"
        "  - name: bad\n    sql: SELECT orders.revenue FROM orders\n    expect:\n      value: 1.0\n",
    )
    report = run_tests(orders_layer, cases)
    payload = report.to_dict()
    assert payload["passed"] is False
    assert payload["total"] == 2
    assert payload["failed"] == 1


# --- check_schema_drift -------------------------------------------------------


def test_check_schema_drift_happy_path_with_model_objects():
    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE orders (id INTEGER, status VARCHAR, amount DOUBLE)")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    report = check_schema_drift(layer)
    assert report.errors == []
    assert report.checked_models == 1


def test_check_schema_drift_reports_missing_dimension_column():
    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="ghost", type="categorical", sql="does_not_exist")],
            metrics=[Metric(name="c", agg="count")],
        )
    )
    report = check_schema_drift(layer)
    assert report.checked_models == 1
    assert any("does_not_exist" in error for error in report.errors)


def test_check_schema_drift_reports_missing_primary_key():
    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE orders (status VARCHAR)")
    layer.add_model(Model(name="orders", table="orders", primary_key="id", metrics=[Metric(name="c", agg="count")]))
    report = check_schema_drift(layer)
    assert any("primary key" in error for error in report.errors)


def test_check_schema_drift_reports_missing_relationship_fk():
    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE orders (id INTEGER)")
    layer.adapter.execute("CREATE TABLE customers (id INTEGER)")
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            metrics=[Metric(name="c", agg="count")],
        )
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
            metrics=[Metric(name="c", agg="count")],
        )
    )
    report = check_schema_drift(layer)
    assert any("foreign key" in error and "customer_id" in error for error in report.errors)


def test_check_schema_drift_missing_table_is_an_error():
    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.add_model(Model(name="orders", table="orders", primary_key="id", metrics=[Metric(name="c", agg="count")]))
    report = check_schema_drift(layer)
    # Probe of the missing table fails, so the model is not counted as checked.
    assert report.checked_models == 0
    assert any("cannot read" in error for error in report.errors)
