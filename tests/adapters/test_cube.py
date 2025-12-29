"""Tests for Cube adapter."""

from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter


def test_cube_adapter():
    """Test Cube adapter with example YAML."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/orders.yml"))

    # Check models were imported
    assert "orders" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"
    assert len(orders.dimensions) > 0
    assert len(orders.metrics) > 0

    # Check dimensions
    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    created_dim = orders.get_dimension("created_at")
    assert created_dim is not None
    assert created_dim.type == "time"

    # Check measures
    count_measure = orders.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    revenue_measure = orders.get_metric("revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"

    # Check segments imported
    assert len(orders.segments) > 0
    completed_segment = next((s for s in orders.segments if s.name == "completed"), None)
    assert completed_segment is not None


def test_cube_adapter_join_discovery():
    """Test that Cube adapter enables join discovery."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/orders.yml"))

    # Check that relationships were imported
    orders = graph.get_model("orders")
    assert len(orders.relationships) > 0
    # Note: The Cube example only has one model, so no actual join path can be tested
    # but we verify that the relationship structure was imported correctly


def test_cube_adapter_pre_aggregations():
    """Test Cube adapter with pre-aggregations."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/orders_with_preagg.yml"))

    orders = graph.get_model("orders")
    assert orders is not None

    # Check pre-aggregations were parsed
    # Note: Pre-aggregations are not stored as first-class objects in SemanticGraph
    # but the adapter should handle them gracefully during parsing
    assert len(orders.dimensions) > 0
    assert len(orders.metrics) > 0
    assert len(orders.segments) == 2

    # Verify time dimensions
    completed_at = orders.get_dimension("completed_at")
    assert completed_at is not None
    assert completed_at.type == "time"

    created_at = orders.get_dimension("created_at")
    assert created_at is not None
    assert created_at.type == "time"


def test_cube_adapter_multi_cube():
    """Test Cube adapter with multiple related cubes."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/ecommerce_multi_cube.yml"))

    # Check all models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models
    assert "line_items" in graph.models
    assert "products" in graph.models

    # Check relationships
    orders = graph.get_model("orders")
    assert len(orders.relationships) == 2

    # Check many_to_one relationship to customers
    customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"

    # Check one_to_many relationship to line_items
    line_items_rel = next((r for r in orders.relationships if r.name == "line_items"), None)
    assert line_items_rel is not None
    assert line_items_rel.type == "one_to_many"

    # Check drill members (if supported)
    count_metric = orders.get_metric("count")
    assert count_metric is not None
    if hasattr(count_metric, "drill_fields") and count_metric.drill_fields:
        # Verify drill fields include cross-cube references
        assert any("customers" in str(field) for field in count_metric.drill_fields)


def test_cube_adapter_segments():
    """Test Cube adapter segment parsing with SQL transformations."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/ecommerce_multi_cube.yml"))

    # Test orders segments
    orders = graph.get_model("orders")
    completed_segment = next((s for s in orders.segments if s.name == "completed"), None)
    assert completed_segment is not None
    # Check that ${CUBE} was replaced with {model}
    assert "{model}" in completed_segment.sql or "orders" in completed_segment.sql.lower()

    # Test customers segments
    customers = graph.get_model("customers")
    assert len(customers.segments) == 2

    sf_segment = next((s for s in customers.segments if s.name == "sf_customers"), None)
    assert sf_segment is not None

    ca_segment = next((s for s in customers.segments if s.name == "ca_customers"), None)
    assert ca_segment is not None


def test_cube_adapter_drill_members():
    """Test Cube adapter drill member parsing."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/ecommerce_multi_cube.yml"))

    orders = graph.get_model("orders")
    count_metric = orders.get_metric("count")
    assert count_metric is not None

    # Note: drill_members parsing is not yet implemented in Cube adapter
    # This test will validate when the feature is added
    # For now, we just verify the metric exists and has correct basic properties
    assert count_metric.name == "count"
    assert count_metric.agg == "count"


def test_cube_saas_analytics():
    """Test Cube adapter with SaaS analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/saas_analytics.yml"))

    # Check all models were imported
    assert "users" in graph.models
    assert "subscriptions" in graph.models
    assert "events" in graph.models

    # Check users model
    users = graph.get_model("users")
    assert users.table == "public.users"
    assert len(users.dimensions) >= 5
    assert len(users.metrics) >= 2
    assert len(users.segments) == 2

    # Check user dimensions
    email_dim = users.get_dimension("email")
    assert email_dim is not None
    assert email_dim.type == "categorical"

    city_dim = users.get_dimension("city")
    assert city_dim is not None

    signup_date_dim = users.get_dimension("signup_date")
    assert signup_date_dim is not None
    assert signup_date_dim.type == "time"

    # Check user measures
    count_measure = users.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    # Check user segments
    sf_segment = next((s for s in users.segments if s.name == "sf_users"), None)
    assert sf_segment is not None
    assert "San Francisco" in sf_segment.sql or "CA" in sf_segment.sql

    # Check subscriptions model
    subscriptions = graph.get_model("subscriptions")
    assert subscriptions.table == "public.subscriptions"
    assert len(subscriptions.relationships) == 1

    # Check subscription relationship
    user_rel = next((r for r in subscriptions.relationships if r.name == "users"), None)
    assert user_rel is not None
    assert user_rel.type == "many_to_one"

    # Check subscription dimensions
    plan_dim = subscriptions.get_dimension("plan")
    assert plan_dim is not None

    status_dim = subscriptions.get_dimension("status")
    assert status_dim is not None

    # Check subscription measures
    active_subs = subscriptions.get_metric("active_subscriptions")
    assert active_subs is not None
    assert active_subs.filters is not None

    # Check subscription pre-aggregations
    assert len(subscriptions.pre_aggregations) == 1
    preagg = subscriptions.pre_aggregations[0]
    assert preagg.name == "subscriptions_by_plan"
    assert preagg.granularity == "day"
    assert preagg.partition_granularity == "month"

    # Check events model
    events = graph.get_model("events")
    assert events.table == "public.events"
    assert len(events.segments) == 2

    # Check event measures
    unique_users = events.get_metric("unique_users")
    assert unique_users is not None
    assert unique_users.agg == "count_distinct"


def test_cube_retail_analytics():
    """Test Cube adapter with retail analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/retail_analytics.yml"))

    # Check all models were imported
    assert "products" in graph.models
    assert "line_items" in graph.models
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Check products model
    products = graph.get_model("products")
    assert products.table == "public.products"

    # Check product dimensions
    category_dim = products.get_dimension("category")
    assert category_dim is not None

    price_dim = products.get_dimension("price")
    assert price_dim is not None
    assert price_dim.type == "numeric"

    # Check product measures
    avg_price = products.get_metric("avg_price")
    assert avg_price is not None
    assert avg_price.agg == "avg"

    min_price = products.get_metric("min_price")
    assert min_price is not None
    assert min_price.agg == "min"

    # Check product segments
    electronics_segment = next((s for s in products.segments if s.name == "electronics"), None)
    assert electronics_segment is not None

    # Check line_items model
    line_items = graph.get_model("line_items")
    assert line_items.table == "public.line_items"
    assert len(line_items.relationships) == 2

    # Check line_items joins
    product_rel = next((r for r in line_items.relationships if r.name == "products"), None)
    assert product_rel is not None
    assert product_rel.type == "many_to_one"

    orders_rel = next((r for r in line_items.relationships if r.name == "orders"), None)
    assert orders_rel is not None
    assert orders_rel.type == "many_to_one"

    # Check line_items pre-aggregations
    assert len(line_items.pre_aggregations) == 1
    preagg = line_items.pre_aggregations[0]
    assert preagg.refresh_key is not None
    assert preagg.refresh_key.every == "1 day"

    # Check orders model
    orders = graph.get_model("orders")
    assert len(orders.relationships) == 2

    # Check orders pre-aggregations with indexes
    assert len(orders.pre_aggregations) == 1
    orders_preagg = orders.pre_aggregations[0]
    assert orders_preagg.refresh_key is not None
    assert orders_preagg.refresh_key.incremental is True
    assert orders_preagg.refresh_key.update_window == "7 day"
    assert len(orders_preagg.indexes) == 1
    assert orders_preagg.indexes[0].name == "status_idx"


def test_cube_web_analytics():
    """Test Cube adapter with web analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/web_analytics.yml"))

    # Check all models were imported
    assert "page_views" in graph.models
    assert "sessions" in graph.models
    assert "conversions" in graph.models

    # Check page_views model
    page_views = graph.get_model("page_views")
    assert page_views.table == "analytics.page_views"

    # Check page_views dimensions
    url_dim = page_views.get_dimension("url")
    assert url_dim is not None

    duration_dim = page_views.get_dimension("duration")
    assert duration_dim is not None
    assert duration_dim.type == "numeric"

    # Check page_views measures
    unique_sessions = page_views.get_metric("unique_sessions")
    assert unique_sessions is not None
    assert unique_sessions.agg == "count_distinct"

    avg_duration = page_views.get_metric("avg_duration")
    assert avg_duration is not None
    assert avg_duration.agg == "avg"

    # Check page_views segments
    assert len(page_views.segments) >= 3
    homepage_segment = next((s for s in page_views.segments if s.name == "homepage"), None)
    assert homepage_segment is not None

    # Check page_views pre-aggregation with incremental refresh
    assert len(page_views.pre_aggregations) == 1
    preagg = page_views.pre_aggregations[0]
    assert preagg.granularity == "hour"
    assert preagg.partition_granularity == "day"
    assert preagg.refresh_key.incremental is True

    # Check sessions model
    sessions = graph.get_model("sessions")
    assert sessions.table == "analytics.sessions"
    assert len(sessions.relationships) == 2

    # Check session dimensions
    device_dim = sessions.get_dimension("device_type")
    assert device_dim is not None

    utm_source_dim = sessions.get_dimension("utm_source")
    assert utm_source_dim is not None

    # Check session segments
    assert len(sessions.segments) >= 4
    mobile_segment = next((s for s in sessions.segments if s.name == "mobile"), None)
    assert mobile_segment is not None

    # Check conversions model
    conversions = graph.get_model("conversions")
    assert conversions.table == "analytics.conversions"

    # Check conversion measures
    purchases = conversions.get_metric("purchases")
    assert purchases is not None
    assert purchases.filters is not None

    # Check conversion pre-aggregation with indexes
    assert len(conversions.pre_aggregations) == 1
    conv_preagg = conversions.pre_aggregations[0]
    assert len(conv_preagg.indexes) == 1


def test_cube_financial_analytics():
    """Test Cube adapter with financial analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/financial_analytics.yml"))

    # Check all models were imported
    assert "transactions" in graph.models
    assert "accounts" in graph.models
    assert "categories" in graph.models
    assert "customers" in graph.models

    # Check transactions model
    transactions = graph.get_model("transactions")
    assert transactions.table == "finance.transactions"
    assert len(transactions.relationships) == 2

    # Check transaction dimensions
    transaction_type_dim = transactions.get_dimension("transaction_type")
    assert transaction_type_dim is not None

    is_recurring_dim = transactions.get_dimension("is_recurring")
    assert is_recurring_dim is not None
    assert is_recurring_dim.type == "categorical"  # boolean maps to categorical

    # Check transaction measures with filters
    credit_amount = transactions.get_metric("credit_amount")
    assert credit_amount is not None
    assert credit_amount.filters is not None
    assert credit_amount.agg == "sum"

    debit_amount = transactions.get_metric("debit_amount")
    assert debit_amount is not None
    assert debit_amount.filters is not None

    # Check transaction segments
    assert len(transactions.segments) >= 4
    large_tx_segment = next((s for s in transactions.segments if s.name == "large_transactions"), None)
    assert large_tx_segment is not None

    # Check transaction pre-aggregation
    assert len(transactions.pre_aggregations) == 1
    preagg = transactions.pre_aggregations[0]
    assert preagg.refresh_key.incremental is True
    assert preagg.refresh_key.update_window == "3 day"

    # Check accounts model
    accounts = graph.get_model("accounts")
    assert accounts.table == "finance.accounts"
    assert len(accounts.relationships) == 2

    # Check account measures
    active_accounts = accounts.get_metric("active_accounts")
    assert active_accounts is not None
    assert active_accounts.filters is not None


def test_cube_logistics_shipping():
    """Test Cube adapter with logistics/shipping example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/logistics_shipping.yml"))

    # Check all models were imported
    assert "shipments" in graph.models
    assert "warehouses" in graph.models
    assert "carriers" in graph.models
    assert "orders" in graph.models

    # Check shipments model
    shipments = graph.get_model("shipments")
    assert shipments.table == "logistics.shipments"
    assert len(shipments.relationships) == 3

    # Check shipment dimensions
    status_dim = shipments.get_dimension("status")
    assert status_dim is not None

    weight_dim = shipments.get_dimension("weight")
    assert weight_dim is not None
    assert weight_dim.type == "numeric"

    # Check shipment measures
    total_weight = shipments.get_metric("total_weight")
    assert total_weight is not None
    assert total_weight.agg == "sum"

    delayed = shipments.get_metric("delayed")
    assert delayed is not None
    assert delayed.filters is not None

    # Check shipment segments
    assert len(shipments.segments) >= 5
    heavy_segment = next((s for s in shipments.segments if s.name == "heavy_shipments"), None)
    assert heavy_segment is not None

    # Check shipment pre-aggregation with indexes
    assert len(shipments.pre_aggregations) == 1
    preagg = shipments.pre_aggregations[0]
    assert len(preagg.indexes) == 1
    assert preagg.indexes[0].name == "status_carrier_idx"
    assert len(preagg.indexes[0].columns) == 2

    # Check warehouses model
    warehouses = graph.get_model("warehouses")
    assert warehouses.table == "logistics.warehouses"

    # Check warehouse dimensions
    capacity_dim = warehouses.get_dimension("capacity")
    assert capacity_dim is not None
    assert capacity_dim.type == "numeric"

    # Check carriers model
    carriers = graph.get_model("carriers")
    assert carriers.table == "logistics.carriers"

    # Check carrier segments
    assert len(carriers.segments) >= 3


def test_cube_iot_sensors():
    """Test Cube adapter with IoT/sensor data example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/iot_sensors.yml"))

    # Check all models were imported
    assert "sensor_readings" in graph.models
    assert "devices" in graph.models
    assert "locations" in graph.models
    assert "alerts" in graph.models

    # Check sensor_readings model
    sensor_readings = graph.get_model("sensor_readings")
    assert sensor_readings.table == "iot.sensor_readings"
    assert len(sensor_readings.relationships) == 2

    # Check sensor dimensions
    sensor_type_dim = sensor_readings.get_dimension("sensor_type")
    assert sensor_type_dim is not None

    value_dim = sensor_readings.get_dimension("value")
    assert value_dim is not None
    assert value_dim.type == "numeric"

    # Check sensor measures
    avg_value = sensor_readings.get_metric("avg_value")
    assert avg_value is not None
    assert avg_value.agg == "avg"

    avg_temp = sensor_readings.get_metric("avg_temperature")
    assert avg_temp is not None
    assert avg_temp.filters is not None

    # Check sensor segments
    assert len(sensor_readings.segments) >= 5

    # Check sensor pre-aggregations
    assert len(sensor_readings.pre_aggregations) == 1

    hourly_preagg = next((p for p in sensor_readings.pre_aggregations if p.name == "readings_hourly"), None)
    assert hourly_preagg is not None
    assert hourly_preagg.granularity == "hour"
    assert hourly_preagg.refresh_key.every == "1 hour"
    assert len(hourly_preagg.indexes) == 1

    # Check devices model
    devices = graph.get_model("devices")
    assert devices.table == "iot.devices"

    # Check device measures
    maintenance_needed = devices.get_metric("maintenance_needed")
    assert maintenance_needed is not None
    assert maintenance_needed.filters is not None

    # Check alerts model
    alerts = graph.get_model("alerts")
    assert alerts.table == "iot.alerts"

    # Check alert measures
    critical_alerts = alerts.get_metric("critical_alerts")
    assert critical_alerts is not None
    assert critical_alerts.filters is not None


def test_cube_healthcare_patients():
    """Test Cube adapter with healthcare analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/healthcare_patients.yml"))

    # Check all models were imported
    assert "patients" in graph.models
    assert "appointments" in graph.models
    assert "treatments" in graph.models
    assert "physicians" in graph.models
    assert "departments" in graph.models

    # Check patients model
    patients = graph.get_model("patients")
    assert patients.table == "healthcare.patients"
    assert len(patients.relationships) == 2

    # Check patient dimensions
    age_dim = patients.get_dimension("age")
    assert age_dim is not None
    assert age_dim.type == "numeric"

    blood_type_dim = patients.get_dimension("blood_type")
    assert blood_type_dim is not None

    # Check patient measures
    avg_age = patients.get_metric("avg_age")
    assert avg_age is not None
    assert avg_age.agg == "avg"

    pediatric = patients.get_metric("pediatric_patients")
    assert pediatric is not None
    assert pediatric.filters is not None

    # Check patient segments
    assert len(patients.segments) >= 4

    # Check appointments model
    appointments = graph.get_model("appointments")
    assert appointments.table == "healthcare.appointments"
    assert len(appointments.relationships) == 3

    # Check appointment dimensions
    appointment_type_dim = appointments.get_dimension("appointment_type")
    assert appointment_type_dim is not None

    duration_dim = appointments.get_dimension("duration_minutes")
    assert duration_dim is not None
    assert duration_dim.type == "numeric"

    # Check appointment measures
    completed = appointments.get_metric("completed_appointments")
    assert completed is not None
    assert completed.filters is not None

    no_show = appointments.get_metric("no_show_appointments")
    assert no_show is not None
    assert no_show.filters is not None

    # Check appointment segments
    assert len(appointments.segments) >= 5

    # Check appointment pre-aggregation
    assert len(appointments.pre_aggregations) == 1
    preagg = appointments.pre_aggregations[0]
    assert preagg.name == "appointments_daily"
    assert len(preagg.measures) >= 3

    # Check treatments model
    treatments = graph.get_model("treatments")
    assert treatments.table == "healthcare.treatments"

    # Check treatment measures
    total_cost = treatments.get_metric("total_cost")
    assert total_cost is not None
    assert total_cost.agg == "sum"

    # Check physicians model
    physicians = graph.get_model("physicians")
    assert physicians.table == "healthcare.physicians"

    # Check departments model
    departments = graph.get_model("departments")
    assert departments.table == "healthcare.departments"


def test_cube_adapter_empty_sections():
    """Test Cube adapter handles empty YAML sections gracefully.

    GitHub issue: Cube generates models with empty pre-aggregation sections.
    """
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/edge_cases.yml"))

    # Check that model with empty sections was parsed successfully
    assert "empty_sections" in graph.models
    empty_sections = graph.get_model("empty_sections")

    # Verify empty sections didn't cause errors
    assert empty_sections.pre_aggregations == []
    assert empty_sections.segments == []
    assert empty_sections.relationships == []

    # Verify dimensions and measures were still parsed
    assert len(empty_sections.dimensions) >= 2
    assert len(empty_sections.metrics) >= 1


def test_cube_adapter_cube_syntax_variants():
    """Test Cube adapter handles {CUBE} syntax variants.

    GitHub issue: {CUBE} syntax (without dollar sign) should also be handled.
    """
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/edge_cases.yml"))

    # Check cube_syntax_variants model
    assert "cube_syntax_variants" in graph.models
    model = graph.get_model("cube_syntax_variants")

    # Verify {CUBE} syntax (without dollar) was normalized to {model}
    name_dim = model.get_dimension("name")
    assert name_dim is not None
    assert "{model}" in name_dim.sql
    assert "{CUBE}" not in name_dim.sql

    # Verify ${CUBE} syntax was normalized
    price_dim = model.get_dimension("price")
    assert price_dim is not None
    assert "{model}" in price_dim.sql
    assert "${CUBE}" not in price_dim.sql

    # Verify measure SQL was normalized
    total_price = model.get_metric("total_price")
    assert total_price is not None
    assert "{model}" in total_price.sql
    assert "{CUBE}" not in total_price.sql

    # Verify filter SQL was normalized
    filtered_count = model.get_metric("filtered_count")
    assert filtered_count is not None
    assert filtered_count.filters is not None
    assert len(filtered_count.filters) == 1
    assert "{model}" in filtered_count.filters[0]
    assert "{CUBE}" not in filtered_count.filters[0]

    # Verify segment SQL was normalized
    expensive_segment = next((s for s in model.segments if s.name == "expensive"), None)
    assert expensive_segment is not None
    assert "{model}" in expensive_segment.sql
    assert "{CUBE}" not in expensive_segment.sql


def test_cube_adapter_cube_name_reference():
    """Test Cube adapter handles ${cube_name} and {cube_name} syntax.

    GitHub issue: References to cube by name should also be normalized.
    """
    adapter = CubeAdapter()
    graph = adapter.parse(Path("tests/fixtures/cube/edge_cases.yml"))

    # Check custom_cube_ref model
    assert "custom_cube_ref" in graph.models
    model = graph.get_model("custom_cube_ref")

    # Verify ${cube_name} syntax was normalized
    id_dim = model.get_dimension("id")
    assert id_dim is not None
    assert "{model}" in id_dim.sql
    assert "${custom_cube_ref}" not in id_dim.sql

    # Verify {cube_name} syntax (without dollar) was normalized
    value_dim = model.get_dimension("value")
    assert value_dim is not None
    assert "{model}" in value_dim.sql
    assert "{custom_cube_ref}" not in value_dim.sql

    # Verify measure SQL was normalized
    total = model.get_metric("total")
    assert total is not None
    assert "{model}" in total.sql
    assert "${custom_cube_ref}" not in total.sql

    # Verify segment SQL was normalized
    high_value_segment = next((s for s in model.segments if s.name == "high_value"), None)
    assert high_value_segment is not None
    assert "{model}" in high_value_segment.sql
    assert "${custom_cube_ref}" not in high_value_segment.sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
