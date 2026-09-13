import duckdb
import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


@pytest.fixture
def ratio_graph():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="customer_count", agg="count")],
        )
    )
    graph.add_metric(
        Metric(
            name="revenue_per_customer",
            type="ratio",
            numerator="orders.revenue",
            denominator="customers.customer_count",
        )
    )
    graph.add_metric(Metric(name="doubled_ratio", type="derived", sql="revenue_per_customer * 2"))
    graph.add_metric(Metric(name="added", type="derived", sql="orders.revenue + customers.customer_count"))
    graph.add_metric(Metric(name="double_added", type="derived", sql="added * 2"))
    graph.get_model("orders").metrics.append(
        Metric(name="customers_proxy", type="derived", sql="customers.customer_count")
    )
    graph.get_model("orders").metrics.append(
        Metric(name="local_sum", type="derived", sql="revenue + customers.customer_count")
    )
    graph.get_model("orders").metrics.append(Metric(name="max_amount", agg="max", sql="amount"))
    graph.add_metric(
        Metric(name="proxy_ratio", type="ratio", numerator="orders.revenue", denominator="orders.customers_proxy")
    )
    return graph


@pytest.mark.parametrize(
    "query, expected",
    [
        ({"metrics": ["revenue_per_customer"]}, [(80.0,)]),
        ({"metrics": ["doubled_ratio"]}, [(160.0,)]),
        ({"metrics": ["proxy_ratio"]}, [(80.0,)]),
        ({"metrics": ["double_added"]}, [(486,)]),
        ({"metrics": ["orders.local_sum"]}, [(243,)]),
        ({"metrics": ["doubled_ratio"], "filters": ["revenue_per_customer > 90"]}, []),
        (
            {
                "metrics": ["revenue_per_customer"],
                "aliases": {"orders.revenue": "total"},
                "filters": ["orders.revenue > 90"],
            },
            [(80.0,)],
        ),
        ({"metrics": ["revenue_per_customer"], "filters": ["orders.max_amount > 90"]}, [(80.0,)]),
        ({"metrics": ["revenue_per_customer"], "filters": ["orders.customers_proxy > 2"]}, [(80.0,)]),
        (
            {
                "metrics": ["revenue_per_customer", "orders.revenue", "customers.customer_count"],
                "aliases": {"revenue_per_customer": "ratio", "orders.revenue": "total"},
            },
            [(80.0, 240, 3)],
        ),
        (
            {"metrics": ["revenue_per_customer"], "dimensions": ["customers.region"], "order_by": ["customers.region"]},
            [("east", 120.0), ("west", None)],
        ),
        (
            {
                "metrics": ["revenue_per_customer"],
                "dimensions": ["customers.region"],
                "aliases": {"customers.region": "location"},
                "filters": ["revenue_per_customer > 90 OR customers.region = 'west'"],
                "order_by": ["customers.region"],
            },
            [("east", 120.0), ("west", None)],
        ),
        (
            {"metrics": ["revenue_per_customer"], "filters": ["orders.amount > 60"]},
            [(200.0,)],
        ),
        (
            {"metrics": ["revenue_per_customer"], "filters": ["revenue_per_customer > 90"]},
            [],
        ),
        (
            {"metrics": ["revenue_per_customer"], "filters": ["orders.amount > 60 AND revenue_per_customer > 90"]},
            [(200.0,)],
        ),
    ],
)
def test_calculated_metrics_preserve_each_source_grain(ratio_graph, query, expected):
    with duckdb.connect() as con:
        con.execute("create table orders(id integer, customer_id integer, amount integer)")
        con.execute("insert into orders values (1, 1, 100), (2, 1, 100), (3, 2, 40)")
        con.execute("create table customers(id integer, region varchar)")
        # Customer 3 has no orders and still belongs to the unfiltered denominator.
        con.execute("insert into customers values (1, 'east'), (2, 'east'), (3, 'west')")
        sql = SQLGenerator(ratio_graph).generate(use_preaggregations=False, **query)
        assert con.execute(sql).fetchall() == expected


def test_filtered_ratio_rejects_unknown_customer_grain(ratio_graph):
    ratio_graph.get_model("customers").primary_key = None
    ratio_graph.get_model("orders").relationships[0].primary_key = "id"
    ratio_graph.build_adjacency()
    with pytest.raises(ValueError, match="primary.key"):
        SQLGenerator(ratio_graph).generate(
            metrics=["revenue_per_customer"], filters=["orders.amount > 60"], use_preaggregations=False
        )


def test_mixed_row_and_calculation_or_rejected(ratio_graph):
    with pytest.raises(ValueError, match="Cannot combine row-level fields and calculated aggregate metrics"):
        SQLGenerator(ratio_graph).generate(
            metrics=["revenue_per_customer"],
            filters=["revenue_per_customer > 90 OR orders.amount > 60"],
            use_preaggregations=False,
        )


def test_cross_model_calculation_output_name_collisions(ratio_graph):
    for model_name, multiplier in (("orders", 1), ("customers", 2)):
        ratio_graph.get_model(model_name).metrics.append(
            Metric(name="same", type="derived", sql=f"orders.revenue / customers.customer_count * {multiplier}")
        )
    sql = SQLGenerator(ratio_graph).generate(metrics=["orders.same", "customers.same"], use_preaggregations=False)
    with duckdb.connect() as con:
        con.execute("create table orders(id integer, customer_id integer, amount integer)")
        con.execute("insert into orders values (1, 1, 100), (2, 1, 100), (3, 2, 40)")
        con.execute("create table customers(id integer, region varchar)")
        con.execute("insert into customers values (1, 'east'), (2, 'east')")
        result = con.execute(sql)
        assert [column[0] for column in result.description] == ["orders_same", "customers_same"]
        assert result.fetchall() == [(120.0, 240.0)]
