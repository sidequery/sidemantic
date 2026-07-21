"""End-to-end behavior tests for the Python widget transport."""

from __future__ import annotations

import base64
from datetime import datetime
from decimal import Decimal

import duckdb
import pyarrow as pa

from sidemantic.widget import MetricsExplorer
from sidemantic.widget._auto_model import build_auto_model, compute_cardinality
from sidemantic.widget._data_registry import register_data, to_arrow_table
from sidemantic.widget._widget import _table_to_ipc, _table_to_ipc_bytes


def _read_ipc(payload: bytes) -> pa.Table:
    return pa.ipc.open_file(pa.BufferReader(payload)).read_all()


def test_arrow_registry_round_trips_ipc_and_materializes_duckdb_table():
    source = pa.table({"category": ["a", "b"], "amount": [10, 20]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, source.schema) as writer:
        writer.write_table(source)

    restored = to_arrow_table(sink.getvalue().to_pybytes())
    connection = duckdb.connect(":memory:")

    assert restored.equals(source)
    assert register_data(restored, connection, "sales data") == "sales data"
    assert connection.execute('SELECT SUM(amount) FROM "sales data"').fetchone() == (30,)


def test_auto_model_classifies_fields_and_honors_cardinality_threshold():
    schema = pa.schema(
        [
            pa.field("occurred_at", pa.timestamp("us")),
            pa.field("amount", pa.decimal128(12, 2)),
            pa.field("active", pa.bool_()),
            pa.field("customer_id", pa.string()),
        ]
    )

    graph, time_dimension = build_auto_model(
        schema,
        table_name="events",
        max_dimension_cardinality=100,
        cardinality_map={"customer_id": 1000},
    )
    model = graph.get_model("events")

    assert time_dimension == "occurred_at"
    assert [dimension.name for dimension in model.dimensions] == ["occurred_at", "amount", "active"]
    assert model.get_dimension("occurred_at").granularity == "day"
    assert model.get_dimension("amount").type == "numeric"
    assert model.get_dimension("active").type == "boolean"
    assert model.get_metric("sum_amount").agg == "sum"
    assert model.get_metric("avg_amount").agg == "avg"


def test_compute_cardinality_isolated_column_failures():
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE events (region VARCHAR)")
    connection.execute("INSERT INTO events VALUES ('west'), ('west'), ('east')")

    cardinalities = compute_cardinality(connection, "events", ["region", "missing"])

    assert cardinalities == {"region": 2, "missing": 0}


def test_decimal_ipc_transport_supports_float_and_string_modes():
    table = pa.table({"amount": pa.array([Decimal("12.34")], type=pa.decimal128(10, 2)), "category": ["west"]})

    float_table = _read_ipc(_table_to_ipc_bytes(table, decimal_mode="float"))
    string_table = _read_ipc(base64.b64decode(_table_to_ipc(table, decimal_mode="string")))

    assert float_table.schema.field("amount").type == pa.float64()
    assert float_table["amount"].to_pylist() == [12.34]
    assert string_table.schema.field("amount").type == pa.string()
    assert string_table["amount"].to_pylist() == ["12.34"]


def test_dataframe_widget_loads_query_results_and_filter_state():
    source = pa.table(
        {
            "occurred_at": pa.array(
                [datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 3)],
                type=pa.timestamp("us"),
            ),
            "region": ["west", "east", "west"],
            "amount": [10.0, 20.0, 30.0],
        }
    )

    widget = MetricsExplorer(
        source,
        metrics=["widget_data.sum_amount"],
        dimensions=["widget_data.region"],
        time_dimension="occurred_at",
    )

    series = _read_ipc(base64.b64decode(widget.metric_series_data))
    leaderboard = _read_ipc(base64.b64decode(widget.dimension_data["region"]))

    assert widget.status == "ready"
    assert widget.error == ""
    assert widget.metric_totals == {"sum_amount": 60.0}
    assert series.num_rows == 3
    assert leaderboard.to_pylist() == [
        {"region": "west", "sum_amount": 40.0},
        {"region": "east", "sum_amount": 20.0},
    ]

    widget.filters = {"region": ["west", None]}
    expressions = widget._build_filters()

    assert any("widget_data.region = 'west'" in expression for expression in expressions)
    assert any("widget_data.region IS NULL" in expression for expression in expressions)
