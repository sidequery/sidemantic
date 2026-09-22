"""SQL projections retain their requested order across raw and rollup plans."""

import pytest

from sidemantic import Dimension, Metric, Model, PreAggregation, SemanticLayer


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("rollup", [False, True])
@pytest.mark.parametrize(
    "wrapper", ["{query}", "SELECT * FROM ({query}) AS totals", "WITH totals AS ({query}) SELECT * FROM totals"]
)
def test_metric_first_projection_keeps_aliases_grouping_and_order(engine, rollup, wrapper):
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False, use_preaggregations=rollup)
    try:
        layer.adapter.execute("CREATE TABLE orders(status VARCHAR, amount INTEGER)")
        layer.adapter.execute("INSERT INTO orders VALUES ('b', 2), ('a', 3), ('a', 4)")
        layer.add_model(
            Model(
                name="orders",
                table="orders",
                dimensions=[Dimension(name="status", type="categorical")],
                metrics=[Metric(name="revenue", agg="sum", sql="amount")],
                pre_aggregations=[PreAggregation(name="status", dimensions=["status"], measures=["revenue"])],
            )
        )
        layer.adapter.execute(
            "CREATE TABLE orders_preagg_status AS SELECT status, SUM(amount) AS revenue_raw FROM orders GROUP BY status"
        )
        query = wrapper.format(query='SELECT revenue AS "Total", status AS "Status" FROM orders') + ' ORDER BY "Status"'
        result = layer.sql(query)
        assert [column[0] for column in result.description] == ["Total", "Status"]
        assert result.fetchall() == [(7, "a"), (2, "b")]
    finally:
        layer.adapter.close()
