"""Tests for Malloy adapter - domain fixtures."""

from pathlib import Path

from sidemantic.adapters.malloy import MalloyAdapter


class TestRetailAnalytics:
    """Test retail analytics domain fixture."""

    def setup_method(self):
        """Parse the retail analytics fixture."""
        adapter = MalloyAdapter()
        self.graph = adapter.parse(Path("tests/fixtures/malloy/retail_analytics.malloy"))

    def test_all_models_parsed(self):
        """Verify all retail models are parsed."""
        expected_models = {"products", "customers", "orders", "order_items", "inventory"}
        assert set(self.graph.models.keys()) == expected_models

    def test_products_model(self):
        """Test products model structure."""
        products = self.graph.get_model("products")
        assert products is not None
        assert products.table == "products.parquet"
        assert products.primary_key == "product_id"

        # Check dimensions
        assert len(products.dimensions) == 12
        dim_names = {d.name for d in products.dimensions}
        assert "product_id" in dim_names
        assert "sku" in dim_names
        assert "category" in dim_names
        assert "is_active" in dim_names
        assert "created_date" in dim_names

        # Check boolean dimension
        is_active = products.get_dimension("is_active")
        assert is_active is not None
        assert is_active.type == "boolean"

        # Check time dimension
        created_date = products.get_dimension("created_date")
        assert created_date is not None
        assert created_date.type == "time"

        # Check measures
        assert len(products.metrics) == 5
        metric_names = {m.name for m in products.metrics}
        assert "product_count" in metric_names
        assert "avg_unit_cost" in metric_names
        assert "active_products" in metric_names

        # Check aggregation types
        product_count = products.get_metric("product_count")
        assert product_count.agg == "count"

        avg_cost = products.get_metric("avg_unit_cost")
        assert avg_cost.agg == "avg"

        # Check filtered measure
        active_products = products.get_metric("active_products")
        assert active_products.agg == "count"
        # Filter may or may not be parsed depending on implementation

    def test_customers_model(self):
        """Test customers model with pick/when dimension."""
        customers = self.graph.get_model("customers")
        assert customers is not None
        assert customers.primary_key == "customer_id"

        # Check pick/when dimension is transformed to CASE
        customer_segment = customers.get_dimension("customer_segment")
        assert customer_segment is not None
        assert customer_segment.type == "categorical"
        # Should have been transformed to CASE
        assert "CASE" in customer_segment.sql or "case" in customer_segment.sql.lower()

        # Check time dimensions with granularity
        signup_month = customers.get_dimension("signup_month")
        assert signup_month is not None
        assert signup_month.type == "time"
        assert signup_month.granularity == "month"

        signup_year = customers.get_dimension("signup_year")
        assert signup_year is not None
        assert signup_year.type == "time"
        assert signup_year.granularity == "year"

        # Check count_distinct measure
        unique_customers = customers.get_metric("unique_customers")
        assert unique_customers is not None
        assert unique_customers.agg == "count_distinct"

    def test_orders_model_relationships(self):
        """Test orders model with relationships."""
        orders = self.graph.get_model("orders")
        assert orders is not None
        assert orders.primary_key == "order_id"

        # Check relationship to customers
        assert len(orders.relationships) == 1
        customer_rel = orders.relationships[0]
        assert customer_rel.name == "customers"
        assert customer_rel.type == "many_to_one"
        assert customer_rel.foreign_key == "customer_id"

        # Check various time granularities
        order_month = orders.get_dimension("order_month")
        assert order_month.granularity == "month"

        order_quarter = orders.get_dimension("order_quarter")
        assert order_quarter.granularity == "quarter"

        order_year = orders.get_dimension("order_year")
        assert order_year.granularity == "year"

    def test_order_items_multiple_joins(self):
        """Test order_items model with multiple join_one relationships."""
        order_items = self.graph.get_model("order_items")
        assert order_items is not None

        # Should have two join_one relationships
        assert len(order_items.relationships) == 2
        rel_names = {r.name for r in order_items.relationships}
        assert "orders" in rel_names
        assert "products" in rel_names

        # All should be many_to_one
        for rel in order_items.relationships:
            assert rel.type == "many_to_one"


class TestSaasMetrics:
    """Test SaaS metrics domain fixture."""

    def setup_method(self):
        """Parse the SaaS metrics fixture."""
        adapter = MalloyAdapter()
        self.graph = adapter.parse(Path("tests/fixtures/malloy/saas_metrics.malloy"))

    def test_all_models_parsed(self):
        """Verify all SaaS models are parsed."""
        expected_models = {"users", "companies", "subscriptions", "events", "invoices"}
        assert set(self.graph.models.keys()) == expected_models

    def test_users_model_complex_dimensions(self):
        """Test users model with complex dimension logic."""
        users = self.graph.get_model("users")
        assert users is not None
        assert users.primary_key == "user_id"

        # Check user_status pick/when
        user_status = users.get_dimension("user_status")
        assert user_status is not None
        assert "CASE" in user_status.sql or "case" in user_status.sql.lower()

        # Check boolean dimensions
        is_admin = users.get_dimension("is_admin")
        assert is_admin.type == "boolean"

        is_active = users.get_dimension("is_active")
        assert is_active.type == "boolean"

        # Check filtered measures
        active_users = users.get_metric("active_users")
        assert active_users.agg == "count"

        churned_users = users.get_metric("churned_users")
        assert churned_users.agg == "count"

    def test_companies_join_many(self):
        """Test companies model with join_many relationship."""
        companies = self.graph.get_model("companies")
        assert companies is not None

        # Check join_many relationship
        assert len(companies.relationships) == 1
        users_rel = companies.relationships[0]
        assert users_rel.name == "users"
        assert users_rel.type == "one_to_many"

    def test_subscriptions_derived_dimensions(self):
        """Test subscriptions model with derived dimensions."""
        subs = self.graph.get_model("subscriptions")
        assert subs is not None

        # arr is derived from mrr
        arr = subs.get_dimension("arr")
        assert arr is not None
        # arr = mrr * 12, should be numeric type
        assert arr.type == "numeric"

        # Check total_arr measure
        total_arr = subs.get_metric("total_arr")
        assert total_arr.agg == "sum"

    def test_events_model_many_measures(self):
        """Test events model with many filtered measures."""
        events = self.graph.get_model("events")
        assert events is not None

        # Check we have all expected measures
        metric_names = {m.name for m in events.metrics}
        expected_metrics = {
            "event_count",
            "unique_sessions",
            "unique_users",
            "page_views",
            "clicks",
            "signups",
            "logins",
            "mobile_events",
            "desktop_events",
        }
        assert expected_metrics.issubset(metric_names)

        # Check count_distinct measures
        unique_sessions = events.get_metric("unique_sessions")
        assert unique_sessions.agg == "count_distinct"

        unique_users = events.get_metric("unique_users")
        assert unique_users.agg == "count_distinct"

    def test_invoices_multiple_relationships(self):
        """Test invoices model with multiple relationships."""
        invoices = self.graph.get_model("invoices")
        assert invoices is not None

        # Should have two relationships
        assert len(invoices.relationships) == 2
        rel_names = {r.name for r in invoices.relationships}
        assert "subscriptions" in rel_names
        assert "companies" in rel_names
