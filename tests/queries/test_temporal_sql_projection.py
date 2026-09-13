import duckdb
import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.query_rewriter import QueryRewriter


@pytest.mark.parametrize(
    "projection,columns",
    [
        ("monthly.sale_date__month, revenue_mom", ["sale_date__month", "revenue_mom"]),
        ("revenue_mom, monthly.sale_date__month", ["revenue_mom", "sale_date__month"]),
        ("monthly.sale_date__month as month, revenue_mom as change", ["month", "change"]),
        (
            "monthly.sale_date__month, monthly.revenue, revenue_mom",
            ["sale_date__month", "revenue", "revenue_mom"],
        ),
    ],
)
@pytest.mark.parametrize("order_by_alias", [False, True])
def test_temporal_sql_exposes_only_requested_projection(projection, columns, order_by_alias):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="monthly",
            table="monthly",
            primary_key="sale_date",
            dimensions=[Dimension(name="sale_date", type="time")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    graph.add_metric(
        Metric(
            name="revenue_mom",
            type="time_comparison",
            base_metric="monthly.revenue",
            comparison_type="mom",
            calculation="difference",
        )
    )
    order_field = columns[-1] if order_by_alias else "monthly.sale_date__month"
    sql = QueryRewriter(graph).rewrite(f"select {projection} from metrics order by {order_field}")
    with duckdb.connect() as connection:
        connection.execute("create table monthly(sale_date date, amount integer)")
        connection.execute("insert into monthly values ('2024-01-01', 100), ('2024-03-01', 150)")
        result = connection.execute(sql)
        assert [column[0] for column in result.description] == columns
        rows = result.fetchall()
        assert len(rows) == 2
        metric_name = "change" if "change" in columns else "revenue_mom"
        assert all(row[columns.index(metric_name)] is None for row in rows)


@pytest.mark.parametrize("order_field", ["first_total", "second_total", "orders.revenue"])
def test_repeated_metric_keeps_each_projection_alias(order_field):
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", metrics=[Metric(name="revenue", agg="sum", sql="amount")]))
    sql = QueryRewriter(graph).rewrite(
        "select orders.revenue as first_total, orders.revenue as second_total "
        f"from metrics order by {order_field} desc limit 1"
    )
    with duckdb.connect() as connection:
        connection.execute("create table orders(amount integer)")
        connection.execute("insert into orders values (100), (140)")
        result = connection.execute(sql)
        assert [column[0] for column in result.description] == ["first_total", "second_total"]
        assert result.fetchall() == [(240, 240)]
