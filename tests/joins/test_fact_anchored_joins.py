"""The model that owns the selected measures is the preserved side of a join.

Dimension models are LEFT JOINed onto it, so a fact whose key is NULL or matches
no dimension row lands in a NULL group and every breakdown adds up to the
ungrouped total. Expected rows come from hand-written SQL on the same data.
"""

import pytest

from sidemantic import Dimension, Explore, Metric, Model, Relationship, Segment


@pytest.fixture
def shop(layer):
    """Customers, orders, order items and tickets with NULL and unmatched keys.

    Order 7 has no customer, order 8 names a customer that does not exist, item
    10 names an order that does not exist and customer 4 has no orders.
    """
    layer.conn.execute("""
        create table customers as select * from (values
            (1, 'EU'), (2, 'US'), (3, 'US'), (4, 'APAC')) t(id, region);
        create table orders as select * from (values
            (1, 1, 'completed', 100), (2, 1, 'completed', 50), (3, 2, 'completed', 200),
            (4, 2, 'cancelled', 80), (5, 3, 'pending', 40), (6, 3, 'completed', 60),
            (7, null, 'completed', 10), (8, 99, 'completed', 5)) t(id, customer_id, status, amount);
        create table order_items as select * from (values
            (1, 1, 60), (2, 1, 40), (3, 2, 50), (4, 3, 200), (5, 4, 80), (6, 5, 40),
            (7, 6, 60), (8, 7, 10), (9, 8, 5), (10, 77, 1)) t(id, order_id, price);
        create table tickets as select * from (values
            (1, 1), (2, 1), (3, 1), (4, 2), (5, 4)) t(id, customer_id);
    """)
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            segments=[Segment(name="us", sql="{model}.region = 'US'")],
            metrics=[Metric(name="customer_count", agg="count")],
        )
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount"), Metric(name="order_count", agg="count")],
        )
    )
    layer.add_model(
        Model(
            name="order_items",
            table="order_items",
            primary_key="id",
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
            metrics=[Metric(name="gmv", agg="sum", sql="price"), Metric(name="item_count", agg="count")],
        )
    )
    layer.add_model(
        Model(
            name="tickets",
            table="tickets",
            primary_key="id",
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
            metrics=[Metric(name="ticket_count", agg="count")],
        )
    )
    return layer


def _rows(result):
    return sorted(result.fetchall(), key=repr)


def _hand_sql(layer, sql):
    return sorted(layer.conn.execute(sql).fetchall(), key=repr)


def test_breakdown_by_joined_dimension_adds_up_to_total(shop):
    rows = _rows(shop.query(metrics=["orders.revenue", "orders.order_count"], dimensions=["customers.region"]))

    assert rows == _hand_sql(
        shop,
        """
        select customers.region, sum(orders.amount), count(orders.id)
        from orders left join customers on orders.customer_id = customers.id
        group by 1
        """,
    )
    # The NULL group holds the order without a customer and the unmatched one.
    assert rows == [("EU", 150, 2), ("US", 380, 4), (None, 15, 2)]
    total = shop.query(metrics=["orders.revenue", "orders.order_count"]).fetchall()
    assert total == [(545, 8)]
    assert (sum(row[1] for row in rows), sum(row[2] for row in rows)) == total[0]


def test_multi_hop_breakdown_adds_up_to_total(shop):
    """order_items -> orders -> customers keeps every item, whichever hop finds no match."""
    rows = _rows(shop.query(metrics=["order_items.gmv", "order_items.item_count"], dimensions=["customers.region"]))

    assert rows == _hand_sql(
        shop,
        """
        select customers.region, sum(order_items.price), count(order_items.id)
        from order_items
        left join orders on order_items.order_id = orders.id
        left join customers on orders.customer_id = customers.id
        group by 1
        """,
    )
    assert rows == [("EU", 150, 3), ("US", 380, 4), (None, 16, 3)]
    assert shop.query(metrics=["order_items.gmv", "order_items.item_count"]).fetchall() == [(546, 10)]


def test_dimensions_from_two_models_keep_every_fact(shop):
    rows = _rows(shop.query(metrics=["order_items.gmv"], dimensions=["orders.status", "customers.region"]))

    assert rows == _hand_sql(
        shop,
        """
        select orders.status, customers.region, sum(order_items.price)
        from order_items
        left join orders on order_items.order_id = orders.id
        left join customers on orders.customer_id = customers.id
        group by 1, 2
        """,
    )
    assert sum(row[2] for row in rows) == 546
    assert (None, None, 1) in rows


@pytest.mark.parametrize("with_second_fact", [False, True])
def test_cell_does_not_depend_on_other_selected_measures(shop, with_second_fact):
    """Adding a measure from another model must not change the revenue of any region."""
    metrics = ["orders.revenue"]
    if with_second_fact:
        metrics.append("tickets.ticket_count")
    rows = shop.query(metrics=metrics, dimensions=["customers.region"]).fetchall()

    revenue = {row[0]: row[1] for row in rows if row[1] is not None}
    assert revenue == {"EU": 150, "US": 380, None: 15}


def test_graph_level_measure_keeps_the_rows_of_the_model_it_aggregates(shop):
    """A graph metric has no owning model; the model named in its SQL is the preserved side."""
    shop.add_metric(Metric(name="graph_revenue", agg="sum", sql="orders.amount"))
    shop.add_metric(Metric(name="doubled_revenue", type="derived", sql="orders.revenue * 2"))

    rows = _rows(shop.query(metrics=["graph_revenue", "doubled_revenue"], dimensions=["customers.region"]))

    assert rows == [("EU", 150, 300), ("US", 380, 760), (None, 15, 30)]


def test_computed_dimension_stays_null_for_unmatched_facts(shop):
    """COALESCE applies to customers that exist; an order without a customer row has no such value."""
    shop.conn.execute("insert into customers values (5, null); insert into orders values (9, 5, 'completed', 7)")
    shop.graph.models["customers"].dimensions.append(
        Dimension(name="market", type="categorical", sql="COALESCE(region, 'none')")
    )

    rows = _rows(shop.query(metrics=["orders.revenue"], dimensions=["customers.market"]))

    assert rows == _hand_sql(
        shop,
        """
        select customers.market, sum(orders.amount)
        from orders
        left join (select id, coalesce(region, 'none') as market from customers) as customers
          on orders.customer_id = customers.id
        group by 1
        """,
    )
    assert rows == [("EU", 150), ("US", 380), ("none", 7), (None, 15)]


def test_measure_on_one_side_keeps_its_rows_and_is_not_fanned_out(shop):
    """Customers anchor their own count; the customer without orders has a NULL status."""
    rows = _rows(shop.query(metrics=["customers.customer_count"], dimensions=["orders.status"]))

    assert rows == _hand_sql(
        shop,
        """
        select orders.status, count(distinct customers.id)
        from customers left join orders on orders.customer_id = customers.id
        group by 1
        """,
    )
    assert rows == [("cancelled", 1), ("completed", 3), ("pending", 1), (None, 1)]


@pytest.mark.parametrize(
    "restriction",
    [{"filters": ["customers.region = 'US'"]}, {"segments": ["customers.us"]}],
    ids=["filter", "segment"],
)
def test_restriction_on_dimension_model_removes_unmatched_facts(shop, restriction):
    """A predicate on a dimension column cannot hold for a fact without that dimension row."""
    assert shop.query(metrics=["orders.revenue"], dimensions=["customers.region"], **restriction).fetchall() == [
        ("US", 380)
    ]
    assert shop.query(metrics=["orders.revenue"], **restriction).fetchall() == [(380,)]


def test_sql_query_keeps_unmatched_facts(shop):
    rows = _rows(shop.sql("select customers.region, orders.revenue from orders"))

    assert rows == [("EU", 150), ("US", 380), (None, 15)]


def test_explore_on_dimension_model_lists_members_without_facts(shop):
    """An Explore names the anchor explicitly: every customer region, matched or not."""
    shop.graph.add_explore(Explore(name="by_customer", model="customers"))
    rows = _rows(
        shop.query(explore="by_customer", metrics=["orders.revenue", "orders.order_count"], dimensions=["region"])
    )

    assert rows == _hand_sql(
        shop,
        """
        select customers.region, sum(orders.amount), count(orders.id)
        from customers left join orders on orders.customer_id = customers.id
        group by 1
        """,
    )
    assert rows == [("APAC", None, 0), ("EU", 150, 2), ("US", 380, 4)]


def test_measure_from_dimension_model_keeps_both_populations(shop):
    """Selecting a customers measure as well keeps regions without orders and orders without a region."""
    rows = _rows(shop.query(metrics=["orders.revenue", "customers.customer_count"], dimensions=["customers.region"]))

    assert rows == [("APAC", None, 1), ("EU", 150, 1), ("US", 380, 2), (None, 15, 0)]


def test_dimension_only_query_starts_from_first_dimension(shop):
    """Without a measure there is no fact to preserve; the first dimension's model anchors."""
    assert _rows(shop.query(dimensions=["customers.region", "orders.status"])) == [
        ("APAC", None),
        ("EU", "completed"),
        ("US", "cancelled"),
        ("US", "completed"),
        ("US", "pending"),
    ]
    assert ("completed", None) in shop.query(dimensions=["orders.status", "customers.region"]).fetchall()
