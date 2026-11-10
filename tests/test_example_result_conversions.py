"""Result conversions used by the shipped examples and README notebooks.

The examples read ``SemanticLayer.query()`` and ``SemanticLayer.sql()`` results
with these calls, so they must work on every supported DuckDB version. DuckDB 2
removed ``.pl()``, ``.arrow()`` and ``.fetch_arrow_table()`` from query results;
the notebooks now go through ``layer.adapter.fetch_record_batch()`` instead.
"""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer

EXPECTED_ROWS = {("completed", 200.0, 2), ("pending", 50.0, 1)}
COLUMNS = ["status", "revenue", "order_count"]


@pytest.fixture
def orders_layer():
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute(
        """
        create table orders as
        select * from (values
            (1, 'completed', 120.0::double),
            (2, 'completed', 80.0::double),
            (3, 'pending', 50.0::double)
        ) as t(order_id, status, amount)
        """
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )
    return layer


def run_query(layer: SemanticLayer, api: str):
    if api == "query":
        return layer.query(metrics=["orders.revenue", "orders.order_count"], dimensions=["orders.status"])
    return layer.sql("select status, revenue, order_count from orders")


@pytest.mark.parametrize("api", ["query", "sql"])
def test_result_converts_to_arrow_and_polars(orders_layer, api):
    pytest.importorskip("pyarrow")
    pl = pytest.importorskip("polars")

    table = orders_layer.adapter.fetch_record_batch(run_query(orders_layer, api)).read_all()
    frame = pl.from_arrow(table)

    assert table.column_names == COLUMNS
    assert set(frame.select(COLUMNS).iter_rows()) == EXPECTED_ROWS


@pytest.mark.parametrize("api", ["query", "sql"])
def test_result_fetches_rows_and_pandas(orders_layer, api):
    pytest.importorskip("pandas")

    assert set(run_query(orders_layer, api).fetchall()) == EXPECTED_ROWS

    frame = run_query(orders_layer, api).fetchdf()
    assert list(frame.columns) == COLUMNS
    assert set(frame.itertuples(index=False, name=None)) == EXPECTED_ROWS
