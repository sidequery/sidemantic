"""Tests for Malloy adapter - parsing."""

from pathlib import Path

from sidemantic.adapters.malloy import MalloyAdapter


def test_malloy_adapter_flights():
    """Test Malloy adapter with flights example."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/flights.malloy"))

    # Check all models were imported
    assert "flights" in graph.models
    assert "carriers" in graph.models
    assert "airports" in graph.models

    # Check flights model
    flights = graph.get_model("flights")
    assert flights.primary_key == "id"

    # Check dimensions
    assert len(flights.dimensions) >= 6

    carrier_dim = flights.get_dimension("carrier")
    assert carrier_dim is not None
    assert carrier_dim.type == "categorical"

    flight_date_dim = flights.get_dimension("flight_date")
    assert flight_date_dim is not None
    assert flight_date_dim.type == "time"

    is_delayed_dim = flights.get_dimension("is_delayed")
    assert is_delayed_dim is not None
    assert is_delayed_dim.type == "boolean"

    distance_tier_dim = flights.get_dimension("distance_tier")
    assert distance_tier_dim is not None
    assert distance_tier_dim.type == "categorical"
    # Check that pick/when was transformed to CASE
    assert "CASE" in distance_tier_dim.sql or "case" in distance_tier_dim.sql.lower()

    # Check measures
    assert len(flights.metrics) >= 4

    flight_count = flights.get_metric("flight_count")
    assert flight_count is not None
    assert flight_count.agg == "count"

    total_distance = flights.get_metric("total_distance")
    assert total_distance is not None
    assert total_distance.agg == "sum"
    assert total_distance.sql == "distance"

    avg_delay = flights.get_metric("avg_delay")
    assert avg_delay is not None
    assert avg_delay.agg == "avg"

    # Check filtered measure
    delayed_flights = flights.get_metric("delayed_flights")
    assert delayed_flights is not None
    assert delayed_flights.agg == "count"
    # Note: filter parsing may vary

    # Check relationships
    assert len(flights.relationships) >= 3

    carrier_rel = next((r for r in flights.relationships if r.name == "carriers"), None)
    assert carrier_rel is not None
    assert carrier_rel.type == "many_to_one"
    assert carrier_rel.foreign_key == "carrier"


def test_malloy_adapter_ecommerce():
    """Test Malloy adapter with ecommerce example."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/ecommerce.malloy"))

    # Check all models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models
    assert "order_items" in graph.models
    assert "products" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.primary_key == "order_id"

    # Check time dimensions
    order_date_dim = orders.get_dimension("order_date")
    assert order_date_dim is not None
    assert order_date_dim.type == "time"
    assert order_date_dim.granularity == "day"

    order_month_dim = orders.get_dimension("order_month")
    assert order_month_dim is not None
    assert order_month_dim.type == "time"
    assert order_month_dim.granularity == "month"

    # Check orders measures
    revenue = orders.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"
    assert revenue.sql == "amount"

    avg_order = orders.get_metric("avg_order_value")
    assert avg_order is not None
    assert avg_order.agg == "avg"

    # Check relationships
    customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"

    items_rel = next((r for r in orders.relationships if r.name == "order_items"), None)
    assert items_rel is not None
    assert items_rel.type == "one_to_many"

    # Check customers model
    customers = graph.get_model("customers")

    is_premium_dim = customers.get_dimension("is_premium")
    assert is_premium_dim is not None
    assert is_premium_dim.type == "boolean"

    unique_customers = customers.get_metric("unique_customers")
    assert unique_customers is not None
    # count_distinct should be parsed correctly
    assert unique_customers.agg == "count_distinct" or "count_distinct" in str(unique_customers.sql or "")


def test_malloy_adapter_directory():
    """Test Malloy adapter parsing entire directory."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy"))

    # Should have parsed both files
    assert len(graph.models) >= 7  # 3 from flights + 4 from ecommerce


def test_malloy_adapter_export():
    """Test Malloy adapter export functionality."""
    import tempfile

    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/flights.malloy"))

    # Export to temp file
    with tempfile.NamedTemporaryFile(suffix=".malloy", delete=False) as f:
        output_path = Path(f.name)

    try:
        adapter.export(graph, output_path)

        # Verify file was created
        assert output_path.exists()

        # Read and check content
        content = output_path.read_text()
        assert "source: flights" in content
        assert "dimension:" in content
        assert "measure:" in content
        assert "flight_count is count()" in content

        # Parse the exported file to verify it's valid
        graph2 = adapter.parse(output_path)
        assert "flights" in graph2.models
    finally:
        output_path.unlink()


def _is_passthrough_dimension(dim, primary_key: str) -> bool:
    """Check if a dimension is a passthrough (sql == name) that won't be exported."""
    if dim.name == primary_key:
        return True
    sql = (dim.sql or dim.name).replace("{model}.", "").strip()
    return sql == dim.name


def test_malloy_adapter_roundtrip():
    """Test that parse -> export -> parse produces equivalent models.

    Note: Passthrough dimensions (where sql == name) are not exported to Malloy
    because Malloy auto-exposes table columns. We only compare non-passthrough
    dimensions in the roundtrip check.
    """
    import tempfile

    adapter = MalloyAdapter()

    # Parse original
    graph1 = adapter.parse(Path("tests/fixtures/malloy/flights.malloy"))
    flights1 = graph1.get_model("flights")

    # Export
    with tempfile.NamedTemporaryFile(suffix=".malloy", delete=False) as f:
        output_path = Path(f.name)

    try:
        adapter.export(graph1, output_path)

        # Parse exported
        graph2 = adapter.parse(output_path)
        flights2 = graph2.get_model("flights")

        # Compare key attributes
        assert flights1.name == flights2.name
        assert flights1.primary_key == flights2.primary_key

        # Compare non-passthrough dimensions only (passthroughs are not exported)
        dims1_non_passthrough = [
            d for d in flights1.dimensions if not _is_passthrough_dimension(d, flights1.primary_key)
        ]
        dims2_non_passthrough = [
            d for d in flights2.dimensions if not _is_passthrough_dimension(d, flights2.primary_key)
        ]
        assert len(dims1_non_passthrough) == len(dims2_non_passthrough)

        # Compare metrics (all metrics are exported)
        assert len(flights1.metrics) == len(flights2.metrics)

        # Check non-passthrough dimension names match
        dim_names1 = {d.name for d in dims1_non_passthrough}
        dim_names2 = {d.name for d in dims2_non_passthrough}
        assert dim_names1 == dim_names2

        # Check metric names match
        metric_names1 = {m.name for m in flights1.metrics}
        metric_names2 = {m.name for m in flights2.metrics}
        assert metric_names1 == metric_names2
    finally:
        output_path.unlink()
