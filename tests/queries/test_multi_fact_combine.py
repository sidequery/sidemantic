"""Independently aggregated fact sources combine on their shared dimensions."""

import pytest
import sqlglot
from sqlglot import exp

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from tests.utils import fetch_rows

ENGINES = ["python", "rust"]


def _shop_layer(engine: str, dialect: str | None = None) -> SemanticLayer:
    if engine == "rust":
        pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False, dialect=dialect)
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="customer_count", agg="count")],
        )
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="created_at", type="time", granularity="day"),
            ],
            metrics=[Metric(name="total_amount", agg="sum", sql="amount")],
        )
    )
    layer.add_model(
        Model(
            name="order_items",
            table="order_items",
            primary_key="id",
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
            metrics=[Metric(name="total_qty", agg="sum", sql="qty")],
        )
    )
    return layer


def _load_shop(layer: SemanticLayer) -> None:
    """One order without a customer, one customer without orders, one item without an order."""
    layer.adapter.execute("CREATE TABLE customers (id INTEGER, region VARCHAR)")
    layer.adapter.execute("INSERT INTO customers VALUES (1, 'EU'), (2, 'US'), (3, 'US'), (4, NULL), (5, 'APAC')")
    layer.adapter.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, amount INTEGER, created_at TIMESTAMP)"
    )
    layer.adapter.execute(
        """
        INSERT INTO orders VALUES
            (1, 1, 'completed', 100, '2024-01-05 10:00:00'),
            (2, 1, 'pending', 50, '2024-01-20 11:00:00'),
            (3, 2, 'completed', 75, '2024-02-03 09:30:00'),
            (4, 3, 'cancelled', 20, '2024-02-10 14:00:00'),
            (5, NULL, 'completed', 33, '2024-02-15 08:00:00'),
            (6, 4, 'completed', 60, '2024-03-01 12:00:00')
        """
    )
    layer.adapter.execute("CREATE TABLE order_items (id INTEGER, order_id INTEGER, qty INTEGER)")
    layer.adapter.execute(
        "INSERT INTO order_items VALUES (1, 1, 2), (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 5, 1), (6, 6, 2), (7, NULL, 4)"
    )


@pytest.fixture(params=ENGINES)
def shop(request):
    layer = _shop_layer(request.param)
    _load_shop(layer)
    yield layer
    layer.adapter.close()


def _full_join_conditions(sql: str, dialect: str) -> dict[str, list[exp.Expression]]:
    """Conjuncts of every FULL JOIN condition, keyed by the joined relation."""
    conditions = {}
    for join in sqlglot.parse_one(sql, read=dialect).find_all(exp.Join):
        if join.side == "FULL":
            condition = join.args["on"]
            conditions[join.alias_or_name] = (
                list(condition.flatten()) if isinstance(condition, exp.And) else [condition]
            )
    return conditions


def _is_null_safe_combine_key(conjuncts: list[exp.Expression]) -> bool:
    """One generated key: equal as array elements, and equally NULL."""
    if len(conjuncts) != 2 or not all(type(conjunct) is exp.EQ for conjunct in conjuncts):
        return False
    elements, null_flags = conjuncts
    wrapped = [side.expressions[0] for side in (elements.left, elements.right) if isinstance(side, exp.Array)]
    flagged = [
        side.unnest().this
        for side in (null_flags.left, null_flags.right)
        if isinstance(side.unnest(), exp.Is) and isinstance(side.unnest().expression, exp.Null)
    ]
    return len(wrapped) == 2 and wrapped == flagged


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize(
    ("metrics", "dimensions", "full_joins"),
    [
        (["orders.total_amount", "order_items.total_qty"], ["customers.region"], 1),
        (
            ["orders.total_amount", "order_items.total_qty", "customers.customer_count"],
            ["customers.region", "orders.created_at__month"],
            2,
        ),
    ],
    ids=["two-sources", "three-sources-two-dimensions"],
)
def test_postgres_full_join_uses_only_equality_conditions(engine, metrics, dimensions, full_joins):
    """PostgreSQL rejects FULL JOIN conditions that are not hash- or merge-joinable."""
    layer = _shop_layer(engine, dialect="postgres")
    try:
        structured = layer.compile(metrics=metrics, dimensions=dimensions)
        semantic_sql = f"SELECT {', '.join(dimensions + metrics)} FROM orders"
        rewritten = layer.explain_sql(semantic_sql).rewritten_sql
        for sql in (structured, rewritten):
            joins = _full_join_conditions(sql, "postgres")
            assert len(joins) == full_joins, sql
            for conjuncts in joins.values():
                assert all(type(conjunct) is exp.EQ for conjunct in conjuncts), sql
                # A bare column equality would drop the NULL dimension bucket.
                keys = [conjuncts[index : index + 2] for index in range(0, len(conjuncts), 2)]
                assert len(keys) == len(dimensions), sql
                assert all(_is_null_safe_combine_key(key) for key in keys), sql
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("dialect", ["postgres", "duckdb"])
def test_authored_full_join_condition_is_emitted_as_written(engine, dialect):
    """Only generated combine keys are lowered for PostgreSQL, never the query's own joins.

    Wrapping an array-valued operand would make NULL and the empty array compare as equal.
    """
    layer = _shop_layer(engine, dialect=dialect)
    try:
        rewritten = layer.explain_sql(
            "SELECT s.region, s.total_amount, s.total_qty, labels.name"
            " FROM (SELECT customers.region, orders.total_amount, order_items.total_qty FROM orders) AS s"
            " FULL OUTER JOIN labels ON s.total_amount = labels.amount AND s.region IS NOT DISTINCT FROM labels.tags"
        ).rewritten_sql
        joins = _full_join_conditions(rewritten, dialect)
        assert set(joins) == {"labels", "order_items_preagg"}, rewritten

        authored = joins["labels"]
        assert [type(conjunct) for conjunct in authored] == [exp.EQ, exp.NullSafeEQ], rewritten
        assert authored[1].sql(dialect=dialect) == "s.region IS NOT DISTINCT FROM labels.tags"
        assert not list(authored[1].find_all(exp.Array)), rewritten

        generated = joins["order_items_preagg"]
        if dialect == "postgres":
            assert _is_null_safe_combine_key(generated), rewritten
        else:
            assert [type(conjunct) for conjunct in generated] == [exp.NullSafeEQ], rewritten
    finally:
        layer.adapter.close()


def test_three_sources_share_null_and_unmatched_dimension_groups(shop):
    rows = fetch_rows(
        shop.sql(
            "SELECT customers.region, orders.total_amount, order_items.total_qty, customers.customer_count FROM orders"
        )
    )
    assert sorted(rows, key=repr) == sorted(
        [
            # Orders 5 and 6 and the orderless item all have no region.
            (None, 93, 7, 1),
            ("APAC", None, None, 1),
            ("EU", 150, 5, 1),
            ("US", 95, 3, 2),
        ],
        key=repr,
    )


def test_two_dimensions_including_time_grain_align_across_sources(shop):
    rows = fetch_rows(
        shop.sql(
            "SELECT orders.created_at__month, customers.region, orders.total_amount, order_items.total_qty FROM orders"
        )
    )
    months = [(month.isoformat()[:10] if month else None, region, amount, qty) for month, region, amount, qty in rows]
    assert sorted(months, key=repr) == sorted(
        [
            ("2024-01-01", "EU", 150, 5),
            ("2024-02-01", "US", 95, 3),
            # The NULL region is its own group inside each month.
            ("2024-02-01", None, 33, 1),
            ("2024-03-01", None, 60, 2),
            (None, None, None, 4),
        ],
        key=repr,
    )


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT customers.region, orders.total_amount, order_items.total_qty FROM orders ORDER BY 2 DESC",
            [("EU", 150, 5), ("US", 95, 3), (None, 93, 7)],
        ),
        (
            # A position refers to the written projection, not the generated one.
            "SELECT orders.total_amount, customers.region, order_items.total_qty FROM orders ORDER BY 1 DESC",
            [(150, "EU", 5), (95, "US", 3), (93, None, 7)],
        ),
        (
            "SELECT customers.region, orders.total_amount, order_items.total_qty FROM orders ORDER BY 3, 2",
            [("US", 95, 3), ("EU", 150, 5), (None, 93, 7)],
        ),
        (
            "SELECT customers.region AS r, orders.total_amount AS amt, order_items.total_qty AS q"
            " FROM orders ORDER BY amt DESC",
            [("EU", 150, 5), ("US", 95, 3), (None, 93, 7)],
        ),
        (
            "SELECT customers.region, orders.total_amount, order_items.total_qty FROM orders"
            " ORDER BY 2 DESC LIMIT 1 OFFSET 1",
            [("US", 95, 3)],
        ),
    ],
    ids=["metric-position", "written-projection-position", "two-positions", "select-alias", "position-with-limit"],
)
def test_multi_fact_sql_orders_by_position_and_alias(shop, query, expected):
    assert fetch_rows(shop.sql(query)) == expected


def test_multi_fact_sql_orders_dimension_by_position(shop):
    rows = fetch_rows(
        shop.sql("SELECT customers.region, orders.total_amount, order_items.total_qty FROM orders ORDER BY 1")
    )
    # Engines place the NULL bucket differently; the named regions must be sorted.
    assert [row for row in rows if row[0] is not None] == [("EU", 150, 5), ("US", 95, 3)]
    assert (None, 93, 7) in rows and len(rows) == 3


def test_multi_fact_structured_query_orders_by_position(shop):
    result = shop.query(
        metrics=["orders.total_amount", "order_items.total_qty"],
        dimensions=["customers.region"],
        order_by=["2 DESC", "1"],
        limit=2,
    )
    assert fetch_rows(result) == [("EU", 150, 5), ("US", 95, 3)]
