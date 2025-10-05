"""Test inheritance for models and metrics."""

from sidemantic import Dimension, Metric, Model, Segment
from sidemantic.core.inheritance import merge_metric, merge_model, resolve_model_inheritance


def test_model_inheritance_basic():
    """Test basic model inheritance."""
    parent = Model(
        name="base_sales",
        table="sales_table",
        primary_key="id",
        dimensions=[
            Dimension(name="date", type="time", granularity="day"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    child = Model(
        name="filtered_sales",
        extends="base_sales",
        dimensions=[
            Dimension(name="customer_id", type="categorical"),
        ],
        segments=[
            Segment(name="completed", sql="{model}.status = 'completed'"),
        ],
    )

    merged = merge_model(child, parent)

    # Should have parent's table
    assert merged.table == "sales_table"
    # Should have combined dimensions
    assert len(merged.dimensions) == 3
    assert any(d.name == "date" for d in merged.dimensions)
    assert any(d.name == "region" for d in merged.dimensions)
    assert any(d.name == "customer_id" for d in merged.dimensions)
    # Should have parent's metrics
    assert len(merged.metrics) == 1
    assert merged.metrics[0].name == "revenue"
    # Should have child's segments
    assert len(merged.segments) == 1


def test_model_inheritance_override():
    """Test that child can override parent fields."""
    parent = Model(
        name="base",
        table="parent_table",
        primary_key="id",
        description="Parent model",
    )

    child = Model(
        name="child",
        extends="base",
        table="child_table",
        description="Child model",
    )

    merged = merge_model(child, parent)

    # Child values should override
    assert merged.table == "child_table"
    assert merged.description == "Child model"
    assert merged.name == "child"


def test_metric_inheritance_basic():
    """Test basic metric inheritance."""
    parent = Metric(name="base_revenue", agg="sum", sql="amount", description="Base revenue metric")

    child = Metric(
        name="filtered_revenue",
        extends="base_revenue",
        filters=["{model}.status = 'completed'"],
    )

    merged = merge_metric(child, parent)

    # Should inherit from parent
    assert merged.agg == "sum"
    assert merged.sql == "amount"
    assert merged.description == "Base revenue metric"
    # Should have child's filters
    assert merged.filters == ["{model}.status = 'completed'"]
    assert merged.name == "filtered_revenue"


def test_metric_inheritance_adds_filters():
    """Test that child can add to parent's filters."""
    parent = Metric(name="base_revenue", agg="sum", sql="amount", filters=["{model}.amount > 0"])

    child = Metric(
        name="completed_revenue",
        extends="base_revenue",
        filters=["{model}.status = 'completed'"],
    )

    merged = merge_metric(child, parent)

    # Should have both parent and child filters
    assert len(merged.filters) == 2
    assert "{model}.amount > 0" in merged.filters
    assert "{model}.status = 'completed'" in merged.filters


def test_resolve_model_inheritance():
    """Test resolving inheritance for multiple models."""
    models = {
        "base": Model(
            name="base",
            table="base_table",
            dimensions=[Dimension(name="id", type="categorical")],
        ),
        "child1": Model(
            name="child1",
            extends="base",
            dimensions=[Dimension(name="name", type="categorical")],
        ),
        "child2": Model(
            name="child2",
            extends="base",
            dimensions=[Dimension(name="amount", type="numeric")],
        ),
    }

    resolved = resolve_model_inheritance(models)

    # All should have base's table
    assert resolved["base"].table == "base_table"
    assert resolved["child1"].table == "base_table"
    assert resolved["child2"].table == "base_table"

    # Each child should have base + their own dimensions
    assert len(resolved["child1"].dimensions) == 2
    assert len(resolved["child2"].dimensions) == 2


def test_multi_level_inheritance():
    """Test multi-level inheritance (grandparent -> parent -> child)."""
    models = {
        "grandparent": Model(
            name="grandparent",
            table="grand_table",
            dimensions=[Dimension(name="a", type="categorical")],
        ),
        "parent": Model(
            name="parent",
            extends="grandparent",
            dimensions=[Dimension(name="b", type="categorical")],
        ),
        "child": Model(
            name="child",
            extends="parent",
            dimensions=[Dimension(name="c", type="categorical")],
        ),
    }

    resolved = resolve_model_inheritance(models)

    # Child should have all three dimensions
    child = resolved["child"]
    assert len(child.dimensions) == 3
    assert any(d.name == "a" for d in child.dimensions)
    assert any(d.name == "b" for d in child.dimensions)
    assert any(d.name == "c" for d in child.dimensions)
    # Child should inherit table from grandparent
    assert child.table == "grand_table"


def test_circular_inheritance_detection():
    """Test that circular inheritance is detected."""
    models = {
        "a": Model(name="a", table="a_table", extends="b"),
        "b": Model(name="b", table="b_table", extends="a"),
    }

    try:
        resolve_model_inheritance(models)
        assert False, "Should have detected circular inheritance"
    except ValueError as e:
        assert "Circular inheritance" in str(e)


def test_missing_parent_detection():
    """Test that missing parent is detected."""
    models = {
        "child": Model(name="child", table="child_table", extends="nonexistent"),
    }

    try:
        resolve_model_inheritance(models)
        assert False, "Should have detected missing parent"
    except ValueError as e:
        assert "not found" in str(e)


def test_metric_inheritance_override():
    """Test that child metric can override parent fields."""
    parent = Metric(
        name="parent",
        agg="sum",
        sql="amount",
        format="$#,##0.00",
    )

    child = Metric(
        name="child",
        extends="parent",
        agg="avg",  # Override aggregation
        format="$#,##0",  # Override format
    )

    merged = merge_metric(child, parent)

    assert merged.agg == "avg"
    assert merged.format == "$#,##0"
    assert merged.sql == "amount"  # Still inherited


def test_model_dimension_override():
    """Test that child can override parent's dimension with same name."""
    parent = Model(
        name="parent",
        table="parent_table",
        dimensions=[
            Dimension(name="status", type="categorical", description="Parent status"),
        ],
    )

    child = Model(
        name="child",
        extends="parent",
        dimensions=[
            Dimension(name="status", type="categorical", description="Child status"),
        ],
    )

    merged = merge_model(child, parent)

    # Should have only one status dimension with child's description
    status_dims = [d for d in merged.dimensions if d.name == "status"]
    assert len(status_dims) == 1
    assert status_dims[0].description == "Child status"
