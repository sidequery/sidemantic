"""Tests for Malloy adapter - annotations."""

from pathlib import Path

from sidemantic.adapters.malloy import MalloyAdapter


def test_model_annotations():
    """Test that model-level annotations are parsed as descriptions."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/annotations.malloy"))

    # Customers should have description from ## annotations
    customers = graph.get_model("customers")
    assert customers is not None
    assert customers.description is not None
    assert "CRM" in customers.description or "Customer" in customers.description

    # Orders should have description
    orders = graph.get_model("orders")
    assert orders is not None
    assert orders.description is not None
    assert "Order" in orders.description or "transaction" in orders.description


def test_dimension_annotations():
    """Test that dimension-level annotations are parsed as descriptions."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/annotations.malloy"))

    customers = graph.get_model("customers")

    # email dimension should have description
    email_dim = customers.get_dimension("email")
    assert email_dim is not None
    assert email_dim.description is not None
    assert "email" in email_dim.description.lower()

    # name dimension should have description
    name_dim = customers.get_dimension("name")
    assert name_dim is not None
    assert name_dim.description is not None
    assert "name" in name_dim.description.lower()

    # region dimension should have description
    region_dim = customers.get_dimension("region")
    assert region_dim is not None
    assert region_dim.description is not None

    # customer_id has no annotation - should have no description
    id_dim = customers.get_dimension("customer_id")
    assert id_dim is not None
    assert id_dim.description is None


def test_measure_annotations():
    """Test that measure-level annotations are parsed as descriptions."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/annotations.malloy"))

    customers = graph.get_model("customers")

    # customer_count should have description
    count_metric = customers.get_metric("customer_count")
    assert count_metric is not None
    assert count_metric.description is not None
    assert "customer" in count_metric.description.lower()

    orders = graph.get_model("orders")

    # total_revenue should have description
    revenue_metric = orders.get_metric("total_revenue")
    assert revenue_metric is not None
    assert revenue_metric.description is not None
    assert "order" in revenue_metric.description.lower() or "total" in revenue_metric.description.lower()

    # avg_order_value should have description
    avg_metric = orders.get_metric("avg_order_value")
    assert avg_metric is not None
    assert avg_metric.description is not None


def test_annotation_format():
    """Test that # desc: annotation is parsed correctly."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/annotations.malloy"))

    customers = graph.get_model("customers")
    assert customers.description is not None
    # Should contain the description
    assert "CRM" in customers.description or "Customer" in customers.description


def test_annotation_roundtrip():
    """Test that annotations survive export and re-parse."""
    import tempfile

    adapter = MalloyAdapter()
    graph1 = adapter.parse(Path("tests/fixtures/malloy/annotations.malloy"))

    # Export to temp file
    with tempfile.NamedTemporaryFile(suffix=".malloy", delete=False) as f:
        output_path = Path(f.name)

    try:
        adapter.export(graph1, output_path)

        # Re-parse
        graph2 = adapter.parse(output_path)

        # Check model descriptions survived
        customers1 = graph1.get_model("customers")
        customers2 = graph2.get_model("customers")
        assert customers2.description is not None
        assert customers1.description == customers2.description

        orders1 = graph1.get_model("orders")
        orders2 = graph2.get_model("orders")
        assert orders2.description is not None
        assert orders1.description == orders2.description

        # Check dimension descriptions survived
        email1 = customers1.get_dimension("email")
        email2 = customers2.get_dimension("email")
        assert email2.description is not None
        assert email1.description == email2.description

        # Check measure descriptions survived
        count1 = customers1.get_metric("customer_count")
        count2 = customers2.get_metric("customer_count")
        assert count2.description is not None
        assert count1.description == count2.description

    finally:
        output_path.unlink()
