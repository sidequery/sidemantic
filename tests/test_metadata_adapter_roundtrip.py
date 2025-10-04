"""Test that metadata fields round-trip through adapters."""

import tempfile
from pathlib import Path

from sidemantic import Dimension, Metric, Model, Segment
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_graph import SemanticGraph


def test_metadata_roundtrip_sidemantic_adapter():
    """Test metadata fields survive export/import through Sidemantic adapter."""
    # Create a model with all metadata fields populated
    original = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        description="Order data",
        dimensions=[
            Dimension(
                name="status",
                type="categorical",
                description="Order status",
                label="Status",
                format=None,
                value_format_name=None
            ),
            Dimension(
                name="discount_rate",
                type="numeric",
                description="Discount percentage",
                label="Discount %",
                format="0.0%",
                value_format_name="percent"
            ),
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                description="Total revenue",
                label="Revenue",
                format="$#,##0.00",
                value_format_name="usd",
                drill_fields=["status", "customer_id"],
                default_time_dimension="created_at",
                default_grain="day"
            ),
            Metric(
                name="avg_order_value",
                agg="avg",
                sql="amount",
                description="Average order value",
                format="$#,##0.00",
                non_additive_dimension="created_at"
            ),
        ],
        segments=[
            Segment(
                name="completed",
                sql="{model}.status = 'completed'",
                description="Completed orders only"
            ),
        ]
    )

    # Create a graph with the model
    graph = SemanticGraph()
    graph.add_model(original)

    adapter = SidemanticAdapter()

    # Export to YAML
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)

        # Import back
        imported_graph = adapter.parse(temp_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()

    # Should have one model
    assert len(imported_graph.models) == 1
    imported = list(imported_graph.models.values())[0]

    # Verify model basics
    assert imported.name == "orders"
    assert imported.table == "orders_table"
    assert imported.description == "Order data"

    # Verify dimensions preserved
    assert len(imported.dimensions) == 2

    status_dim = imported.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"
    assert status_dim.description == "Order status"
    assert status_dim.label == "Status"

    discount_dim = imported.get_dimension("discount_rate")
    assert discount_dim is not None
    assert discount_dim.format == "0.0%"
    assert discount_dim.value_format_name == "percent"

    # Verify metrics preserved with all metadata
    assert len(imported.metrics) == 2

    revenue = imported.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"
    assert revenue.sql == "amount"
    assert revenue.description == "Total revenue"
    assert revenue.label == "Revenue"
    assert revenue.format == "$#,##0.00"
    assert revenue.value_format_name == "usd"
    assert revenue.drill_fields == ["status", "customer_id"]
    assert revenue.default_time_dimension == "created_at"
    assert revenue.default_grain == "day"

    avg_value = imported.get_metric("avg_order_value")
    assert avg_value is not None
    assert avg_value.format == "$#,##0.00"
    assert avg_value.non_additive_dimension == "created_at"

    # Verify segments preserved
    assert len(imported.segments) == 1
    completed_seg = imported.get_segment("completed")
    assert completed_seg is not None
    assert completed_seg.sql == "{model}.status = 'completed'"
    assert completed_seg.description == "Completed orders only"


def test_partial_metadata_roundtrip():
    """Test that models with only some metadata fields set work correctly."""
    # Model with minimal metadata
    minimal = Model(
        name="products",
        table="products",
        metrics=[
            Metric(
                name="price",
                agg="avg",
                sql="price",
                # Only format set, no other metadata
                format="$0.00"
            ),
        ],
        dimensions=[
            Dimension(
                name="category",
                type="categorical",
                # Only label set
                label="Product Category"
            ),
        ]
    )

    graph = SemanticGraph()
    graph.add_model(minimal)

    adapter = SidemanticAdapter()

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)
        imported_graph = adapter.parse(temp_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()

    imported = list(imported_graph.models.values())[0]
    price = imported.get_metric("price")
    assert price.format == "$0.00"
    assert price.value_format_name is None
    assert price.drill_fields is None

    category = imported.get_dimension("category")
    assert category.label == "Product Category"
    assert category.format is None


def test_empty_metadata_roundtrip():
    """Test models with no metadata fields set."""
    basic = Model(
        name="users",
        table="users",
        metrics=[
            Metric(name="user_count", agg="count"),
        ],
        dimensions=[
            Dimension(name="status", type="categorical"),
        ]
    )

    graph = SemanticGraph()
    graph.add_model(basic)

    adapter = SidemanticAdapter()

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)
        imported_graph = adapter.parse(temp_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()

    imported = list(imported_graph.models.values())[0]

    user_count = imported.get_metric("user_count")
    assert user_count.format is None
    assert user_count.drill_fields is None
    assert user_count.default_grain is None

    status = imported.get_dimension("status")
    assert status.format is None
    assert status.value_format_name is None
