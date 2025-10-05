"""Tests for Postgres catalog metadata export."""

from sidemantic import Dimension, Metric, Model, Relationship


def test_basic_catalog_metadata(layer):
    """Test basic catalog metadata export."""
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[
            Dimension(name="status", sql="status", type="categorical"),
            Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count"),
        ],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[
            Dimension(name="name", sql="name", type="categorical"),
            Dimension(name="region", sql="region", type="categorical"),
        ],
        metrics=[
            Metric(name="customer_count", agg="count"),
        ],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    catalog = layer.get_catalog_metadata()

    # Check tables
    assert len(catalog["tables"]) == 2
    table_names = {t["table_name"] for t in catalog["tables"]}
    assert table_names == {"orders", "customers"}

    orders_table = next(t for t in catalog["tables"] if t["table_name"] == "orders")
    assert orders_table["table_schema"] == "public"
    assert orders_table["table_type"] == "BASE TABLE"
    assert orders_table["is_insertable_into"] == "NO"

    # Check columns for orders
    orders_cols = [c for c in catalog["columns"] if c["table_name"] == "orders"]
    col_names = {c["column_name"] for c in orders_cols}

    # Should have: id (PK), customer_id (FK), status, order_date, revenue, order_count
    assert col_names == {"id", "status", "order_date", "revenue", "order_count"}

    # Check primary key
    pk_col = next(c for c in orders_cols if c["column_name"] == "id")
    assert pk_col["is_primary_key"] is True
    assert pk_col["is_nullable"] == "NO"
    assert pk_col["data_type"] == "BIGINT"

    # Check dimension types
    status_col = next(c for c in orders_cols if c["column_name"] == "status")
    assert status_col["data_type"] == "VARCHAR"
    assert status_col["is_metric"] is False

    date_col = next(c for c in orders_cols if c["column_name"] == "order_date")
    assert date_col["data_type"] == "DATE"  # day granularity → DATE
    assert date_col["is_metric"] is False

    # Check metric types
    revenue_col = next(c for c in orders_cols if c["column_name"] == "revenue")
    assert revenue_col["data_type"] == "NUMERIC"
    assert revenue_col["is_metric"] is True
    assert revenue_col["aggregation"] == "sum"

    count_col = next(c for c in orders_cols if c["column_name"] == "order_count")
    assert count_col["data_type"] == "BIGINT"  # count → BIGINT
    assert count_col["is_metric"] is True
    assert count_col["aggregation"] == "count"

    # Check customers columns
    customer_cols = [c for c in catalog["columns"] if c["table_name"] == "customers"]
    customer_col_names = {c["column_name"] for c in customer_cols}
    assert customer_col_names == {"id", "name", "region", "customer_count"}


def test_foreign_key_constraints(layer):
    """Test foreign key constraint metadata."""
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[Dimension(name="customer_id", sql="customer_id", type="numeric")],
        metrics=[],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[],
        metrics=[],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    catalog = layer.get_catalog_metadata()

    # Check constraints
    constraints = catalog["constraints"]
    fk_constraints = [c for c in constraints if c["constraint_type"] == "FOREIGN KEY"]
    assert len(fk_constraints) == 1

    fk = fk_constraints[0]
    assert fk["table_name"] == "orders"
    assert "customer_id_fkey" in fk["constraint_name"]

    # Check key column usage
    fk_usage = [k for k in catalog["key_column_usage"] if "fkey" in k["constraint_name"]]
    assert len(fk_usage) == 1

    fk_col = fk_usage[0]
    assert fk_col["table_name"] == "orders"
    assert fk_col["column_name"] == "customer_id"
    assert fk_col["referenced_table_name"] == "customers"
    assert fk_col["referenced_column_name"] == "id"

    # Check that foreign key column is marked
    customer_id_col = next(
        c for c in catalog["columns"] if c["table_name"] == "orders" and c["column_name"] == "customer_id"
    )
    assert customer_id_col["is_foreign_key"] is True


def test_type_mappings(layer):
    """Test all type mappings."""
    test_model = Model(
        name="test",
        table="test",
        primary_key="id",
        dimensions=[
            Dimension(name="cat_field", sql="cat", type="categorical"),
            Dimension(name="num_field", sql="num", type="numeric"),
            Dimension(name="bool_field", sql="bool", type="boolean"),
            Dimension(name="date_field", sql="date", type="time", granularity="day"),
            Dimension(name="timestamp_field", sql="ts", type="time", granularity="hour"),
        ],
        metrics=[
            Metric(name="sum_metric", agg="sum", sql="value"),
            Metric(name="avg_metric", agg="avg", sql="value"),
            Metric(name="count_metric", agg="count"),
            Metric(name="count_distinct_metric", agg="count_distinct", sql="user_id"),
            Metric(name="min_metric", agg="min", sql="value"),
            Metric(name="max_metric", agg="max", sql="value"),
        ],
    )

    layer.add_model(test_model)
    catalog = layer.get_catalog_metadata()

    cols = {c["column_name"]: c for c in catalog["columns"] if c["table_name"] == "test"}

    # Dimension types
    assert cols["cat_field"]["data_type"] == "VARCHAR"
    assert cols["num_field"]["data_type"] == "NUMERIC"
    assert cols["bool_field"]["data_type"] == "BOOLEAN"
    assert cols["date_field"]["data_type"] == "DATE"
    assert cols["timestamp_field"]["data_type"] == "TIMESTAMP"

    # Metric types
    assert cols["sum_metric"]["data_type"] == "NUMERIC"
    assert cols["avg_metric"]["data_type"] == "NUMERIC"
    assert cols["count_metric"]["data_type"] == "BIGINT"
    assert cols["count_distinct_metric"]["data_type"] == "BIGINT"
    assert cols["min_metric"]["data_type"] == "NUMERIC"
    assert cols["max_metric"]["data_type"] == "NUMERIC"


def test_custom_schema(layer):
    """Test custom schema name."""
    model = Model(
        name="test",
        table="test",
        primary_key="id",
        dimensions=[],
        metrics=[],
    )

    layer.add_model(model)
    catalog = layer.get_catalog_metadata(schema="analytics")

    assert catalog["tables"][0]["table_schema"] == "analytics"
    assert catalog["columns"][0]["table_schema"] == "analytics"


def test_ordinal_positions(layer):
    """Test column ordinal positions."""
    model = Model(
        name="test",
        table="test",
        primary_key="id",
        dimensions=[
            Dimension(name="dim1", sql="d1", type="categorical"),
            Dimension(name="dim2", sql="d2", type="categorical"),
        ],
        metrics=[
            Metric(name="metric1", agg="sum", sql="m1"),
            Metric(name="metric2", agg="count"),
        ],
    )

    layer.add_model(model)
    catalog = layer.get_catalog_metadata()

    cols = sorted(
        [c for c in catalog["columns"] if c["table_name"] == "test"],
        key=lambda c: c["ordinal_position"],
    )

    # Should be ordered: id (PK), dim1, dim2, metric1, metric2
    assert cols[0]["column_name"] == "id"
    assert cols[0]["ordinal_position"] == 1

    assert cols[1]["column_name"] == "dim1"
    assert cols[1]["ordinal_position"] == 2

    assert cols[2]["column_name"] == "dim2"
    assert cols[2]["ordinal_position"] == 3

    assert cols[3]["column_name"] == "metric1"
    assert cols[3]["ordinal_position"] == 4

    assert cols[4]["column_name"] == "metric2"
    assert cols[4]["ordinal_position"] == 5


def test_metadata_fields(layer):
    """Test description and label metadata."""
    model = Model(
        name="test",
        table="test",
        primary_key="id",
        dimensions=[
            Dimension(
                name="status",
                sql="status",
                type="categorical",
                description="Order status",
                label="Status",
            )
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                description="Total revenue",
                label="Revenue (USD)",
            )
        ],
    )

    layer.add_model(model)
    catalog = layer.get_catalog_metadata()

    status_col = next(c for c in catalog["columns"] if c["column_name"] == "status")
    assert status_col["description"] == "Order status"
    assert status_col["label"] == "Status"

    revenue_col = next(c for c in catalog["columns"] if c["column_name"] == "revenue")
    assert revenue_col["description"] == "Total revenue"
    assert revenue_col["label"] == "Revenue (USD)"
