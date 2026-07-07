"""Tests for security policy enforcement in the compile/query path (work item A2).

Covers:
- row filter on a JOINED (non-base) model lands inside that model's own CTE;
- access=false raises SecurityError before any SQL is produced;
- a security block with user_attributes=None raises SecurityError (deny-by-default);
- an injection-style attribute value is neutralized (stays a single quoted literal);
- enforce_visibility hides and rejects non-public fields;
- a row-filtered query bypasses pre-aggregation routing (compiles against raw tables);
- end-to-end DuckDB execution: a scoped query returns strictly fewer rows than unscoped.
"""

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from sidemantic import Dimension, Metric, Model, Relationship, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_layer import SecurityError


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            region VARCHAR,
            email VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO customers VALUES
        (1, 'US', 'a@us.com'),
        (2, 'US', 'b@us.com'),
        (3, 'EU', 'c@eu.com')
        """
    )
    conn.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DECIMAL(10, 2)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders VALUES
        (10, 1, 100.0),
        (11, 1, 50.0),
        (12, 2, 25.0),
        (13, 3, 200.0),
        (14, 3, 300.0)
        """
    )
    return conn


def _layer(db, **kwargs):
    from sidemantic.db.duckdb import DuckDBAdapter

    adapter = DuckDBAdapter()
    adapter.conn = db
    return SemanticLayer(connection=adapter, auto_register=False, engine="python", **kwargs)


def _customers_model(**security_kwargs):
    return Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[
            Dimension(name="id", type="numeric"),
            Dimension(name="region", type="categorical"),
            Dimension(name="email", type="categorical"),
        ],
        metrics=[Metric(name="customer_count", agg="count")],
        security=SecurityPolicy(**security_kwargs) if security_kwargs else None,
    )


def _orders_model():
    return Model(
        name="orders",
        table="orders",
        primary_key="id",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[
            Dimension(name="id", type="numeric"),
            Dimension(name="customer_id", type="numeric"),
        ],
        metrics=[Metric(name="order_count", agg="count"), Metric(name="total_amount", agg="sum", sql="amount")],
    )


def test_row_filter_on_joined_model_lands_in_that_models_cte(db):
    """A row filter on a joined (non-base) model must be injected inside that model's CTE."""
    layer = _layer(db)
    layer.add_model(_customers_model(access=True, row_filters=["region = '{{ user.region }}'"]))
    layer.add_model(_orders_model())

    # orders is the base model here (metric comes from orders); customers is joined.
    sql = layer.compile(
        metrics=["orders.order_count"],
        dimensions=["customers.region"],
        user_attributes={"region": "US"},
    )

    # The filter must appear inside the customers CTE, before the join/aggregation.
    assert "customers_cte AS (" in sql
    customers_cte = sql.split("customers_cte AS (", 1)[1].split("\n)", 1)[0]
    assert "region = 'US'" in customers_cte, f"row filter not scoped inside customers CTE:\n{sql}"
    # And it must not appear only in the outer query.
    assert "WHERE" in customers_cte


def test_access_false_raises_before_sql(db):
    """access evaluating falsy raises SecurityError naming the model, before any SQL."""
    layer = _layer(db)
    layer.add_model(_customers_model(access="{{ user.role == 'admin' }}"))

    with pytest.raises(SecurityError, match="customers"):
        layer.compile(metrics=["customers.customer_count"], user_attributes={"role": "viewer"})

    # Literal False also denies.
    layer2 = _layer(db)
    layer2.add_model(_customers_model(access=False))
    with pytest.raises(SecurityError, match="customers"):
        layer2.compile(metrics=["customers.customer_count"], user_attributes={})


def test_security_block_with_none_attributes_denies(db):
    """A model with a security policy and user_attributes=None is denied by default."""
    layer = _layer(db)
    layer.add_model(_customers_model(access=True, row_filters=["region = '{{ user.region }}'"]))

    with pytest.raises(SecurityError, match="no user_attributes|security policy"):
        layer.compile(metrics=["customers.customer_count"], user_attributes=None)

    # Empty dict is "provided but empty" -> not deny-by-default; here it triggers the
    # undefined-attribute guard from the row filter instead.
    with pytest.raises(SecurityError, match="undefined user attribute"):
        layer.compile(metrics=["customers.customer_count"], user_attributes={})


def test_injection_value_is_neutralized(db):
    """A dangerous attribute value must stay a single quoted literal, not a boolean condition."""
    layer = _layer(db)
    layer.add_model(_customers_model(access=True, row_filters=["email = '{{ user.email }}'"]))

    injection = "x' OR '1'='1"
    sql = layer.compile(metrics=["customers.customer_count"], user_attributes={"email": injection})

    parsed = sqlglot.parse_one(sql, dialect="duckdb")
    string_literals = [node.this for node in parsed.find_all(exp.Literal) if node.is_string]
    assert injection in string_literals, f"injection value not preserved as a single literal:\n{sql}"
    # The dangerous OR must not have leaked in as a boolean condition.
    assert not list(parsed.find_all(exp.Or)), f"OR condition leaked from injection value:\n{sql}"


def test_enforce_visibility_hides_and_rejects_non_public_fields(db):
    """enforce_visibility=True rejects requests for non-public fields and omits them from listings."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="id", type="numeric"), Dimension(name="customer_id", type="numeric")],
        metrics=[
            Metric(name="order_count", agg="count"),
            Metric(name="margin", agg="sum", sql="amount", public=False),
        ],
    )
    layer = _layer(db, enforce_visibility=True)
    layer.add_model(model)

    # Requesting the non-public field raises a clear error.
    with pytest.raises(SecurityError, match=r"Field 'orders\.margin' is not public"):
        layer.compile(metrics=["orders.margin"])

    # Public field still compiles.
    layer.compile(metrics=["orders.order_count"])

    # Introspection listing omits the non-public metric.
    described = layer.describe_models()
    orders_desc = next(m for m in described["models"] if m["name"] == "orders")
    metric_names = {m["name"] for m in orders_desc["metrics"]}
    assert "order_count" in metric_names
    assert "margin" not in metric_names

    # Catalog listing omits it too.
    from sidemantic.core.catalog import get_catalog_metadata

    catalog = get_catalog_metadata(layer.graph, enforce_visibility=True)
    catalog_cols = {(c["table_name"], c["column_name"]) for c in catalog["columns"]}
    assert ("orders", "order_count") in catalog_cols
    assert ("orders", "margin") not in catalog_cols


def test_enforce_visibility_default_off_allows_non_public(db):
    """Default (enforce_visibility=False) leaves library users unaffected."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="id", type="numeric")],
        metrics=[Metric(name="margin", agg="sum", sql="amount", public=False)],
    )
    layer = _layer(db)  # enforce_visibility defaults to False
    layer.add_model(model)
    # Non-public field compiles fine when enforcement is off.
    layer.compile(metrics=["orders.margin"])


def test_row_filtered_query_bypasses_preaggregation(db):
    """When a participating model has active row filters, pre-agg routing is disabled."""
    from sidemantic.core.pre_aggregation import PreAggregation

    model = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[Dimension(name="id", type="numeric"), Dimension(name="region", type="categorical")],
        metrics=[Metric(name="customer_count", agg="count")],
        pre_aggregations=[
            PreAggregation(
                name="by_region",
                measures=["customer_count"],
                dimensions=["region"],
            )
        ],
        security=SecurityPolicy(access=True, row_filters=["region = '{{ user.region }}'"]),
    )
    layer = _layer(db, use_preaggregations=True)
    layer.add_model(model)

    sql = layer.compile(
        metrics=["customers.customer_count"],
        dimensions=["customers.region"],
        user_attributes={"region": "US"},
    )
    # Must compile against the raw table, not a rollup, and must carry the row filter.
    assert "used_preagg=true" not in sql
    assert "FROM customers" in sql
    assert "region = 'US'" in sql


def test_row_filter_scopes_rows_end_to_end(db):
    """Execute against DuckDB: a scoped query returns strictly fewer rows than unscoped."""
    layer = _layer(db)
    layer.add_model(_customers_model(access=True, row_filters=["region = '{{ user.region }}'"]))
    layer.add_model(_orders_model())

    # Unscoped baseline (no security): a plain model without a policy, same data.
    plain = _layer(db)
    plain.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric"), Dimension(name="region", type="categorical")],
            metrics=[Metric(name="customer_count", agg="count")],
        )
    )
    unscoped_total = plain.query(metrics=["customers.customer_count"]).fetchall()[0][0]
    assert unscoped_total == 3

    # Scoped to US -> only the 2 US customers.
    scoped = layer.query(
        metrics=["customers.customer_count"],
        user_attributes={"region": "US"},
    ).fetchall()[0][0]
    assert scoped == 2
    assert scoped < unscoped_total

    # Row filter on the JOINED model scopes orders through the customer join. Requesting a
    # customers dimension pulls customers into the query so its policy applies: US customers
    # (1, 2) own orders 10, 11, 12 -> 3 orders total (not all 5). Sum the per-region counts.
    order_rows = layer.query(
        metrics=["orders.order_count"],
        dimensions=["customers.region"],
        user_attributes={"region": "US"},
    ).fetchall()
    total_scoped_orders = sum(row[-1] for row in order_rows)
    assert total_scoped_orders == 3, order_rows
