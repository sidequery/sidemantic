"""Tests for model bootstrapping from a live schema (sidemantic/bootstrap.py)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from sidemantic.bootstrap import (
    CATEGORICAL_CARDINALITY_LIMIT,
    ColumnInfo,
    TableInfo,
    bootstrap_models,
    generate_model_dict,
    introspect_connection,
    write_model_files,
)


@pytest.fixture
def sample_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE customers (id INTEGER, region VARCHAR, signup_date TIMESTAMP, active BOOLEAN)")
        con.execute("INSERT INTO customers VALUES (1, 'North', now(), true), (2, 'South', now(), false)")
        con.execute(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), status VARCHAR, created_at TIMESTAMP)"
        )
        con.execute("INSERT INTO orders VALUES (1, 1, 10.0, 'paid', now()), (2, 2, 20.0, 'pending', now())")
    finally:
        con.close()
    return db_path


def test_introspect_connection_reads_tables_and_columns(sample_db: Path):
    tables = introspect_connection(f"duckdb:///{sample_db}")

    by_name = {table.name: table for table in tables}
    assert {"customers", "orders"} <= set(by_name)
    order_columns = {column.name for column in by_name["orders"].columns}
    assert order_columns == {"id", "customer_id", "amount", "status", "created_at"}


def test_introspect_connection_profiles_duckdb(sample_db: Path):
    tables = {table.name: table for table in introspect_connection(f"duckdb:///{sample_db}", profile=True)}
    orders = tables["orders"]
    assert orders.row_count == 2
    # Profiling records distinct counts for id/string/*_id columns.
    assert "status" in orders.distinct_counts


def test_introspect_connection_can_skip_profiling(sample_db: Path):
    tables = {table.name: table for table in introspect_connection(f"duckdb:///{sample_db}", profile=False)}
    assert tables["orders"].row_count is None
    assert tables["orders"].distinct_counts == {}


def test_generate_model_dict_id_is_primary_key():
    table = TableInfo(name="widgets", columns=[ColumnInfo("id", "INTEGER"), ColumnInfo("label", "VARCHAR")])
    model = generate_model_dict(table, {"widgets"})
    assert model["primary_key"] == "id"


def test_generate_model_dict_foreign_key_becomes_relationship():
    table = TableInfo(
        name="orders",
        columns=[ColumnInfo("id", "INTEGER"), ColumnInfo("customer_id", "INTEGER")],
    )
    model = generate_model_dict(table, {"orders", "customers"})
    assert model["relationships"] == [{"name": "customers", "type": "many_to_one", "foreign_key": "customer_id"}]
    # The foreign key is not also emitted as a dimension or metric.
    assert "dimensions" not in model


def test_generate_model_dict_foreign_key_without_target_is_not_a_relationship():
    table = TableInfo(
        name="orders",
        columns=[ColumnInfo("id", "INTEGER"), ColumnInfo("vendor_id", "INTEGER")],
    )
    model = generate_model_dict(table, {"orders"})
    assert "relationships" not in model


def test_generate_model_dict_timestamp_is_day_time_dimension():
    table = TableInfo(name="events", columns=[ColumnInfo("id", "INTEGER"), ColumnInfo("occurred_at", "TIMESTAMP")])
    model = generate_model_dict(table, {"events"})
    assert {"name": "occurred_at", "type": "time", "granularity": "day"} in model["dimensions"]


def test_generate_model_dict_low_cardinality_string_is_categorical():
    table = TableInfo(
        name="orders",
        columns=[ColumnInfo("id", "INTEGER"), ColumnInfo("status", "VARCHAR")],
        row_count=1000,
        distinct_counts={"status": 3},
    )
    model = generate_model_dict(table, {"orders"})
    assert {"name": "status", "type": "categorical"} in model["dimensions"]


def test_generate_model_dict_high_cardinality_string_excluded_when_profiled():
    table = TableInfo(
        name="events",
        columns=[ColumnInfo("id", "INTEGER"), ColumnInfo("token", "VARCHAR")],
        row_count=1000,
        distinct_counts={"token": CATEGORICAL_CARDINALITY_LIMIT + 1},
    )
    model = generate_model_dict(table, {"events"})
    assert "dimensions" not in model


def test_generate_model_dict_numeric_gets_sum_metric_and_record_count():
    table = TableInfo(
        name="orders",
        columns=[ColumnInfo("id", "INTEGER"), ColumnInfo("amount", "DECIMAL(10,2)")],
    )
    model = generate_model_dict(table, {"orders"})
    metric_names = {metric["name"] for metric in model["metrics"]}
    assert "record_count" in metric_names
    assert {"name": "total_amount", "agg": "sum", "sql": "amount"} in model["metrics"]


def test_generate_model_dict_record_count_always_present_even_without_numerics():
    table = TableInfo(name="tags", columns=[ColumnInfo("id", "INTEGER"), ColumnInfo("name", "VARCHAR")])
    model = generate_model_dict(table, {"tags"})
    assert model["metrics"][0] == {"name": "record_count", "agg": "count"}


def test_bootstrap_models_emits_notes(sample_db: Path):
    result = bootstrap_models(f"duckdb:///{sample_db}")
    assert set(result.model_dicts) == {"customers", "orders"}
    notes_text = "\n".join(result.notes)
    assert "customers" in notes_text
    assert "orders" in notes_text
    # The orders note mentions the inferred join to customers.
    assert "joins customers" in notes_text


def test_write_model_files_writes_one_file_per_model(sample_db: Path, tmp_path: Path):
    result = bootstrap_models(f"duckdb:///{sample_db}")
    models_dir = tmp_path / "models"

    written = write_model_files(result, models_dir)

    names = {path.name for path in written}
    assert names == {"customers.yml", "orders.yml"}
    for path in written:
        assert path.is_file()


def test_write_model_files_conflict_without_force_raises(sample_db: Path, tmp_path: Path):
    result = bootstrap_models(f"duckdb:///{sample_db}")
    models_dir = tmp_path / "models"
    write_model_files(result, models_dir)

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_model_files(result, models_dir)

    # With force it overwrites cleanly.
    assert write_model_files(result, models_dir, force=True)
