"""Tests for LookML adapter."""

from pathlib import Path

from sidemantic.adapters.lookml import LookMLAdapter


def test_lookml_adapter_basic():
    """Test LookML adapter with basic orders example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/orders.lkml"))

    # Check models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"
    assert orders.primary_key == "id"
    assert len(orders.dimensions) > 0
    assert len(orders.metrics) > 0

    # Check dimensions
    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    customer_id_dim = orders.get_dimension("customer_id")
    assert customer_id_dim is not None
    assert customer_id_dim.type == "numeric"

    # Check time dimension groups were parsed
    created_date = orders.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"
    assert created_date.granularity == "day"

    created_week = orders.get_dimension("created_week")
    assert created_week is not None
    assert created_week.type == "time"
    assert created_week.granularity == "week"

    created_month = orders.get_dimension("created_month")
    assert created_month is not None
    assert created_month.type == "time"
    assert created_month.granularity == "month"

    # Check basic measures
    count_measure = orders.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    revenue_measure = orders.get_metric("revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"

    avg_measure = orders.get_metric("avg_order_value")
    assert avg_measure is not None
    assert avg_measure.agg == "avg"

    # Check filtered measure
    completed_revenue = orders.get_metric("completed_revenue")
    assert completed_revenue is not None
    assert completed_revenue.agg == "sum"
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Check derived measure (ratio metric)
    conversion_rate = orders.get_metric("conversion_rate")
    assert conversion_rate is not None
    assert conversion_rate.type == "derived"
    assert conversion_rate.sql is not None

    # Check segments
    assert len(orders.segments) > 0
    high_value_segment = next((s for s in orders.segments if s.name == "high_value"), None)
    assert high_value_segment is not None

    # Check customers model
    customers = graph.get_model("customers")
    assert customers.table == "public.customers"
    assert customers.primary_key == "id"


def test_lookml_adapter_ecommerce():
    """Test LookML adapter with comprehensive ecommerce example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/ecommerce.lkml"))

    # Check all models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models
    assert "products" in graph.models
    assert "order_items" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "ecommerce.orders"

    # Test various dimension types
    amount_dim = orders.get_dimension("amount")
    assert amount_dim is not None
    assert amount_dim.type == "numeric"

    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    # Test dimension group with multiple timeframes
    created_time = orders.get_dimension("created_time")
    assert created_time is not None
    assert created_time.granularity == "hour"

    created_date = orders.get_dimension("created_date")
    assert created_date is not None
    assert created_date.granularity == "day"

    created_week = orders.get_dimension("created_week")
    assert created_week is not None
    assert created_week.granularity == "week"

    created_quarter = orders.get_dimension("created_quarter")
    assert created_quarter is not None
    assert created_quarter.granularity == "quarter"

    # Test various measure types
    count_measure = orders.get_metric("count")
    assert count_measure.agg == "count"

    sum_measure = orders.get_metric("total_revenue")
    assert sum_measure.agg == "sum"

    avg_measure = orders.get_metric("avg_order_value")
    assert avg_measure.agg == "avg"

    min_measure = orders.get_metric("min_order_value")
    assert min_measure.agg == "min"

    max_measure = orders.get_metric("max_order_value")
    assert max_measure.agg == "max"

    # Test filtered measures
    delivered_orders = orders.get_metric("delivered_orders")
    assert delivered_orders is not None
    assert delivered_orders.agg == "count"
    assert delivered_orders.filters is not None

    high_value_orders = orders.get_metric("high_value_orders")
    assert high_value_orders is not None
    assert high_value_orders.filters is not None

    # Test derived measure
    avg_discount = orders.get_metric("avg_discount_percentage")
    assert avg_discount is not None
    assert avg_discount.type == "derived"

    # Test segments
    assert len(orders.segments) == 3
    segment_names = [s.name for s in orders.segments]
    assert "delivered" in segment_names
    assert "high_value" in segment_names
    assert "international" in segment_names

    # Check customers model
    customers = graph.get_model("customers")
    assert customers.table == "ecommerce.customers"

    # Test count_distinct measure
    count_active = customers.get_metric("count_active")
    assert count_active is not None
    assert count_active.agg == "count_distinct"
    assert count_active.filters is not None

    # Check products model
    products = graph.get_model("products")
    assert products.table == "ecommerce.products"

    # Check order_items model
    order_items = graph.get_model("order_items")
    assert order_items.table == "ecommerce.order_items"

    # Test count_distinct on order_items
    distinct_products = order_items.get_metric("distinct_products_sold")
    assert distinct_products is not None
    assert distinct_products.agg == "count_distinct"


def test_lookml_adapter_advanced_measures():
    """Test LookML adapter with advanced measures example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/advanced_measures.lkml"))

    # Check models
    assert "sales_analytics" in graph.models
    assert "marketing_campaigns" in graph.models

    # Check sales_analytics model
    sales = graph.get_model("sales_analytics")
    assert sales.table == "analytics.sales"

    # Test count_distinct measures
    unique_customers = sales.get_metric("unique_customers")
    assert unique_customers is not None
    assert unique_customers.agg == "count_distinct"

    unique_products = sales.get_metric("unique_products")
    assert unique_products is not None
    assert unique_products.agg == "count_distinct"

    # Test multiple filtered measures
    online_sales = sales.get_metric("online_sales")
    assert online_sales is not None
    assert online_sales.agg == "sum"
    assert online_sales.filters is not None

    online_large_sales = sales.get_metric("online_large_sales")
    assert online_large_sales is not None
    assert online_large_sales.filters is not None
    # Should have 2 filters (channel and sale_amount)
    assert len(online_large_sales.filters) == 2

    # Test derived/ratio measures
    gross_profit = sales.get_metric("gross_profit")
    assert gross_profit is not None
    assert gross_profit.type == "derived"

    profit_margin = sales.get_metric("profit_margin")
    assert profit_margin is not None
    assert profit_margin.type == "derived"

    # Check marketing_campaigns model
    campaigns = graph.get_model("marketing_campaigns")
    assert campaigns.table == "analytics.campaigns"

    # Test ratio metrics in campaigns
    ctr = campaigns.get_metric("click_through_rate")
    assert ctr is not None
    assert ctr.type == "derived"

    conversion_rate = campaigns.get_metric("conversion_rate")
    assert conversion_rate is not None
    assert conversion_rate.type == "derived"


def test_lookml_adapter_explores():
    """Test LookML adapter with explores and joins."""
    adapter = LookMLAdapter()

    # Create a temporary directory with just the files we need
    import shutil
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Copy the files we need for this test
        shutil.copy("examples/lookml/orders.lkml", tmpdir_path / "orders.lkml")
        shutil.copy("examples/lookml/orders.explore.lkml", tmpdir_path / "orders.explore.lkml")

        # Parse the directory
        graph = adapter.parse(tmpdir_path)

        # Check that all models exist
        assert "orders" in graph.models
        assert "customers" in graph.models

        # Check orders model has relationships
        orders = graph.get_model("orders")

        # The explores are in a separate file, so relationships should be parsed
        assert len(orders.relationships) > 0

        # Find customer relationship
        customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
        assert customer_rel is not None
        assert customer_rel.type == "many_to_one"
        assert customer_rel.foreign_key == "customer_id"


def test_lookml_adapter_explores_multi_join():
    """Test LookML adapter with multiple joins in explores."""
    adapter = LookMLAdapter()

    # Create a temporary directory with just the ecommerce files
    import shutil
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Copy the ecommerce files
        shutil.copy("examples/lookml/ecommerce.lkml", tmpdir_path / "ecommerce.lkml")
        shutil.copy("examples/lookml/ecommerce_explores.lkml", tmpdir_path / "ecommerce_explores.lkml")

        # Parse the directory
        graph = adapter.parse(tmpdir_path)

        # Check that ecommerce models were parsed
        assert "orders" in graph.models
        assert "customers" in graph.models
        assert "products" in graph.models
        assert "order_items" in graph.models

        # Check orders relationships
        orders = graph.get_model("orders")
        assert len(orders.relationships) > 0

        # Check for various join types
        relationships_by_name = {r.name: r for r in orders.relationships}

        # Verify customers relationship is many_to_one
        assert "customers" in relationships_by_name
        assert relationships_by_name["customers"].type == "many_to_one"
        assert relationships_by_name["customers"].foreign_key == "customer_id"

        # Verify order_items relationship is one_to_many
        assert "order_items" in relationships_by_name
        assert relationships_by_name["order_items"].type == "one_to_many"
        assert relationships_by_name["order_items"].foreign_key == "id"

        # Verify products relationship exists (through order_items)
        assert "products" in relationships_by_name
        assert relationships_by_name["products"].type == "many_to_one"

        # Check customers explore
        customers = graph.get_model("customers")
        if len(customers.relationships) > 0:
            customer_rels = {r.name: r for r in customers.relationships}
            # Customers should have one_to_many to orders
            if "orders" in customer_rels:
                assert customer_rels["orders"].type == "one_to_many"


def test_lookml_adapter_derived_tables():
    """Test LookML adapter with derived tables."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/derived_tables.lkml"))

    # Check model was created from derived table
    assert "customer_summary" in graph.models

    customer_summary = graph.get_model("customer_summary")

    # Should not have a table name, but should have SQL
    assert customer_summary.table is None
    assert customer_summary.sql is not None
    assert "SELECT" in customer_summary.sql.upper()

    # Check primary key
    assert customer_summary.primary_key == "customer_id"

    # Check dimensions
    customer_id_dim = customer_summary.get_dimension("customer_id")
    assert customer_id_dim is not None

    order_count_dim = customer_summary.get_dimension("order_count")
    assert order_count_dim is not None
    assert order_count_dim.type == "numeric"

    # Check time dimension group
    last_order_date = customer_summary.get_dimension("last_order_date")
    assert last_order_date is not None
    assert last_order_date.type == "time"

    # Check measures
    total_customers = customer_summary.get_metric("total_customers")
    assert total_customers is not None
    assert total_customers.agg == "count"

    avg_orders = customer_summary.get_metric("avg_orders_per_customer")
    assert avg_orders is not None
    assert avg_orders.agg == "avg"


def test_lookml_adapter_export():
    """Test LookML adapter export functionality."""
    adapter = LookMLAdapter()

    # Parse a simple example
    graph = adapter.parse(Path("examples/lookml/orders.lkml"))

    # Export to a temporary file
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        output_path = f.name

    try:
        adapter.export(graph, output_path)

        # Read back and verify structure
        adapter2 = LookMLAdapter()
        graph2 = adapter2.parse(output_path)

        # Check that models were preserved
        assert "orders" in graph2.models
        assert "customers" in graph2.models

        # Check basic structure is preserved
        orders = graph2.get_model("orders")
        assert orders.table == "public.orders"
        assert len(orders.dimensions) > 0
        assert len(orders.metrics) > 0
    finally:
        # Clean up
        import os

        if os.path.exists(output_path):
            os.remove(output_path)


def test_lookml_thelook_orders():
    """Test LookML adapter with real thelook orders example from looker-open-source."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_orders.view.lkml"))

    # Check model was imported
    assert "orders" in graph.models
    orders = graph.get_model("orders")

    # Check primary key
    assert orders.primary_key == "id"

    # Check dimensions
    id_dim = orders.get_dimension("id")
    assert id_dim is not None
    assert id_dim.type == "numeric"

    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    traffic_source_dim = orders.get_dimension("traffic_source")
    assert traffic_source_dim is not None

    user_id_dim = orders.get_dimension("user_id")
    assert user_id_dim is not None
    assert user_id_dim.type == "numeric"

    # Check complex dimensions with SQL
    total_amount_dim = orders.get_dimension("total_amount_of_order_usd")
    assert total_amount_dim is not None
    assert total_amount_dim.type == "numeric"
    assert "SELECT SUM" in total_amount_dim.sql

    order_profit_dim = orders.get_dimension("order_profit")
    assert order_profit_dim is not None
    # Should reference other dimensions
    assert "total_amount_of_order_usd" in order_profit_dim.sql or "total_cost_of_order" in order_profit_dim.sql

    # Check yesno dimension
    is_first_purchase_dim = orders.get_dimension("is_first_purchase")
    assert is_first_purchase_dim is not None
    assert is_first_purchase_dim.type == "categorical"  # yesno maps to categorical

    # Check time dimension group
    created_date = orders.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"
    assert created_date.granularity == "day"

    created_week = orders.get_dimension("created_week")
    assert created_week is not None
    assert created_week.granularity == "week"

    created_month = orders.get_dimension("created_month")
    assert created_month is not None
    assert created_month.granularity == "month"

    # Check measures
    count_measure = orders.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    revenue_measure = orders.get_metric("total_revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"


def test_lookml_thelook_users():
    """Test LookML adapter with real thelook users example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_users.view.lkml"))

    assert "users" in graph.models
    users = graph.get_model("users")

    # Check primary key
    assert users.primary_key == "id"

    # Check various dimension types
    age_dim = users.get_dimension("age")
    assert age_dim is not None
    assert age_dim.type == "numeric"

    city_dim = users.get_dimension("city")
    assert city_dim is not None
    assert city_dim.type == "categorical"

    email_dim = users.get_dimension("email")
    assert email_dim is not None

    # Check geographic dimensions
    lat_dim = users.get_dimension("lat")
    assert lat_dim is not None
    assert lat_dim.type == "numeric"

    lng_dim = users.get_dimension("lng")
    assert lng_dim is not None
    assert lng_dim.type == "numeric"

    # Check zipcode dimension (special type)
    zipcode_dim = users.get_dimension("zipcode")
    assert zipcode_dim is not None

    # Check time dimension group
    created_date = users.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"

    # Check measure
    count_measure = users.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"


def test_lookml_thelook_products():
    """Test LookML adapter with real thelook products example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_products.view.lkml"))

    assert "products" in graph.models
    products = graph.get_model("products")

    # Check dimensions
    brand_dim = products.get_dimension("brand")
    assert brand_dim is not None

    category_dim = products.get_dimension("category")
    assert category_dim is not None

    department_dim = products.get_dimension("department")
    assert department_dim is not None

    retail_price_dim = products.get_dimension("retail_price")
    assert retail_price_dim is not None
    assert retail_price_dim.type == "numeric"

    sku_dim = products.get_dimension("sku")
    assert sku_dim is not None

    # Check measure
    count_measure = products.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"


def test_lookml_thelook_order_items():
    """Test LookML adapter with real thelook order_items example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_order_items.view.lkml"))

    assert "order_items" in graph.models
    order_items = graph.get_model("order_items")

    # Check primary key
    assert order_items.primary_key == "id"

    # Check foreign key dimensions
    inventory_item_id_dim = order_items.get_dimension("inventory_item_id")
    assert inventory_item_id_dim is not None
    assert inventory_item_id_dim.type == "numeric"

    order_id_dim = order_items.get_dimension("order_id")
    assert order_id_dim is not None
    assert order_id_dim.type == "numeric"

    # Check time dimensions with multiple granularities
    created_date = order_items.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"

    created_week = order_items.get_dimension("created_week")
    assert created_week is not None

    # Check returned time dimension group
    returned_date = order_items.get_dimension("returned_date")
    assert returned_date is not None
    assert returned_date.type == "time"

    # Check numeric dimension
    sale_price_dim = order_items.get_dimension("sale_price")
    assert sale_price_dim is not None
    assert sale_price_dim.type == "numeric"

    # Check measures
    count_measure = order_items.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    total_sale_price = order_items.get_metric("total_sale_price")
    assert total_sale_price is not None
    assert total_sale_price.agg == "sum"

    avg_sale_price = order_items.get_metric("average_sale_price")
    assert avg_sale_price is not None
    assert avg_sale_price.agg == "avg"


def test_lookml_thelook_inventory_items():
    """Test LookML adapter with real thelook inventory_items example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_inventory_items.view.lkml"))

    assert "inventory_items" in graph.models
    inventory = graph.get_model("inventory_items")

    # Check dimensions
    cost_dim = inventory.get_dimension("cost")
    assert cost_dim is not None
    assert cost_dim.type == "numeric"

    product_id_dim = inventory.get_dimension("product_id")
    assert product_id_dim is not None
    assert product_id_dim.type == "numeric"

    # Check time dimensions with convert_tz: no
    created_date = inventory.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"

    sold_date = inventory.get_dimension("sold_date")
    assert sold_date is not None
    assert sold_date.type == "time"

    # Check measure
    count_measure = inventory.get_metric("count")
    assert count_measure is not None


def test_lookml_bq_thelook_events():
    """Test LookML adapter with BigQuery thelook events example (compact syntax)."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/bq_thelook_events.view.lkml"))

    assert "events" in graph.models
    events = graph.get_model("events")

    # Check sql_table_name was parsed
    assert events.table == "thelook_web_analytics.events"

    # Check primary key with compact syntax
    assert events.primary_key == "id"

    # Check compact dimension definitions (no sql: ${TABLE}.field)
    browser_dim = events.get_dimension("browser")
    assert browser_dim is not None

    city_dim = events.get_dimension("city")
    assert city_dim is not None

    event_type_dim = events.get_dimension("event_type")
    assert event_type_dim is not None

    # Check numeric dimensions
    latitude_dim = events.get_dimension("latitude")
    assert latitude_dim is not None
    assert latitude_dim.type == "numeric"

    sequence_num_dim = events.get_dimension("sequence_number")
    assert sequence_num_dim is not None
    assert sequence_num_dim.type == "numeric"

    # Check dimension with complex SQL (REGEXP_EXTRACT)
    user_id_dim = events.get_dimension("user_id")
    assert user_id_dim is not None
    assert user_id_dim.type == "numeric"
    assert "REGEXP_EXTRACT" in user_id_dim.sql or "CAST" in user_id_dim.sql

    # Check time dimension (defaults to "date" when no timeframes specified)
    created_date = events.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"
    assert created_date.granularity == "day"

    # Check basic measure
    count_measure = events.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    # Check derived/custom measures with SQL only (no type:)
    min_time = events.get_metric("minimum_time")
    assert min_time is not None
    # These are "derived" because they have custom SQL but no agg type
    assert min_time.type == "derived"

    max_time = events.get_metric("max_time")
    assert max_time is not None
    assert max_time.type == "derived"

    # Check measures with array aggregations
    ip_addresses = events.get_metric("ip_addresses")
    assert ip_addresses is not None
    assert "ARRAY_TO_STRING" in ip_addresses.sql or "ARRAY_AGG" in ip_addresses.sql

    event_types = events.get_metric("event_types")
    assert event_types is not None

    # Check min aggregation type
    first_user_id = events.get_metric("first_user_id")
    assert first_user_id is not None
    assert first_user_id.agg == "min"


def test_lookml_bq_thelook_inventory_compact():
    """Test LookML adapter with compact syntax (no ${TABLE} references)."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/bq_thelook_inventory.view.lkml"))

    assert "inventory_items" in graph.models
    inventory = graph.get_model("inventory_items")

    # Check table name
    assert inventory.table == "thelook_web_analytics.inventory_items"

    # Check compact dimensions
    product_brand_dim = inventory.get_dimension("product_brand")
    assert product_brand_dim is not None

    product_category_dim = inventory.get_dimension("product_category")
    assert product_category_dim is not None

    product_name_dim = inventory.get_dimension("product_name")
    assert product_name_dim is not None

    # Check numeric dimensions
    cost_dim = inventory.get_dimension("cost")
    assert cost_dim is not None
    assert cost_dim.type == "numeric"

    retail_price_dim = inventory.get_dimension("product_retail_price")
    assert retail_price_dim is not None
    assert retail_price_dim.type == "numeric"

    # Check measure
    count_measure = inventory.get_metric("count")
    assert count_measure is not None


def test_lookml_bq_thelook_distribution_centers():
    """Test LookML adapter with geographic dimensions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/bq_thelook_distribution_centers.view.lkml"))

    assert "distribution_centers" in graph.models
    dc = graph.get_model("distribution_centers")

    # Check table
    assert dc.table == "thelook_web_analytics.distribution_centers"

    # Check geographic dimensions
    latitude_dim = dc.get_dimension("latitude")
    assert latitude_dim is not None
    assert latitude_dim.type == "numeric"

    longitude_dim = dc.get_dimension("longitude")
    assert longitude_dim is not None
    assert longitude_dim.type == "numeric"

    name_dim = dc.get_dimension("name")
    assert name_dim is not None

    # Check measure
    count_measure = dc.get_metric("count")
    assert count_measure is not None
