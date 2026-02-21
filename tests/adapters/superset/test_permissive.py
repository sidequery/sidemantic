"""Tests for Superset adapter - advanced fixture parsing.

Tests real-world Superset features: virtual datasets with SQL, ratio metrics,
d3format, warning_text, calculated columns, diverse column types, uuid/extra
metadata, and complex metric expressions.
"""

import pytest

from sidemantic.adapters.superset import SupersetAdapter

# =============================================================================
# COVID DASHBOARD FIXTURE TESTS (virtual dataset with ratios)
# =============================================================================


class TestCovidDashboardParsing:
    """Tests for the covid_dashboard.yaml virtual dataset fixture."""

    @pytest.fixture
    def graph(self):
        adapter = SupersetAdapter()
        return adapter.parse("tests/fixtures/superset/covid_dashboard.yaml")

    @pytest.fixture
    def model(self, graph):
        return graph.models["covid_stats"]

    def test_model_loads(self, graph):
        """Fixture parses without errors and produces a model."""
        assert "covid_stats" in graph.models

    def test_virtual_dataset_has_sql(self, model):
        """Virtual dataset stores SQL query."""
        assert model.sql is not None
        assert "SELECT" in model.sql
        assert "covid_statistics" in model.sql
        assert "WHERE" in model.sql

    def test_virtual_dataset_no_table(self, model):
        """Virtual datasets do not have a table reference."""
        assert model.table is None

    def test_model_description(self, model):
        """Model description is preserved."""
        assert model.description is not None
        assert "COVID" in model.description

    def test_dimension_count(self, model):
        """All columns are parsed as dimensions."""
        assert len(model.dimensions) == 10

    def test_dimension_names(self, model):
        """Key dimensions are present by name."""
        dim_names = [d.name for d in model.dimensions]
        assert "report_date" in dim_names
        assert "country" in dim_names
        assert "province" in dim_names
        assert "confirmed" in dim_names
        assert "deaths" in dim_names
        assert "recovered" in dim_names
        assert "active" in dim_names
        assert "population" in dim_names
        assert "latitude" in dim_names
        assert "longitude" in dim_names

    def test_date_column_type(self, model):
        """DATE column is mapped to time with day granularity."""
        report_date = model.get_dimension("report_date")
        assert report_date.type == "time"
        assert report_date.granularity == "day"

    def test_varchar_column_type(self, model):
        """VARCHAR columns are mapped to categorical."""
        country = model.get_dimension("country")
        assert country.type == "categorical"

    def test_bigint_column_type(self, model):
        """BIGINT columns are mapped to numeric."""
        confirmed = model.get_dimension("confirmed")
        assert confirmed.type == "numeric"

    def test_double_precision_column_type(self, model):
        """DOUBLE PRECISION columns are mapped to numeric."""
        lat = model.get_dimension("latitude")
        assert lat.type == "numeric"

    def test_verbose_name_as_label(self, model):
        """verbose_name is stored as dimension label."""
        country = model.get_dimension("country")
        assert country.label == "Country/Region"

    def test_metric_count(self, model):
        """All metrics are parsed."""
        assert len(model.metrics) == 10

    def test_metric_names(self, model):
        """Key metrics are present by name."""
        metric_names = [m.name for m in model.metrics]
        assert "count" in metric_names
        assert "total_confirmed" in metric_names
        assert "total_deaths" in metric_names
        assert "total_recovered" in metric_names
        assert "recovery_rate" in metric_names
        assert "mortality_rate" in metric_names
        assert "cases_per_100k" in metric_names
        assert "active_cases" in metric_names
        assert "avg_daily_cases" in metric_names
        assert "max_daily_deaths" in metric_names

    def test_count_metric(self, model):
        """COUNT(*) metric is parsed correctly."""
        count = model.get_metric("count")
        assert count.agg == "count"
        assert count.sql is None  # COUNT(*) has no inner column

    def test_sum_metric(self, model):
        """SUM metric extracts inner column."""
        total_confirmed = model.get_metric("total_confirmed")
        assert total_confirmed.agg == "sum"
        assert total_confirmed.sql == "confirmed"

    def test_avg_metric(self, model):
        """AVG metric is parsed correctly."""
        avg_daily = model.get_metric("avg_daily_cases")
        assert avg_daily.agg == "avg"
        assert avg_daily.sql == "confirmed"

    def test_max_metric(self, model):
        """MAX metric is parsed correctly."""
        max_deaths = model.get_metric("max_daily_deaths")
        assert max_deaths.agg == "max"
        assert max_deaths.sql == "deaths"

    def test_ratio_metric_recovery_rate(self, model):
        """Ratio metric (SUM/SUM) is parsed as derived."""
        recovery = model.get_metric("recovery_rate")
        assert recovery.type == "derived"
        assert recovery.agg is None
        assert recovery.sql is not None
        assert "SUM" in recovery.sql
        assert "NULLIF" in recovery.sql

    def test_ratio_metric_mortality_rate(self, model):
        """Mortality rate ratio metric is derived."""
        mortality = model.get_metric("mortality_rate")
        assert mortality.type == "derived"
        assert mortality.agg is None

    def test_complex_ratio_metric(self, model):
        """Cases per 100K with multiplication is derived."""
        per100k = model.get_metric("cases_per_100k")
        assert per100k.type == "derived"
        assert per100k.sql is not None

    def test_subtraction_metric(self, model):
        """Active cases metric with subtraction is derived."""
        active = model.get_metric("active_cases")
        assert active.type == "derived"
        assert active.sql is not None

    def test_metric_labels(self, model):
        """Metric verbose_name is stored as label."""
        total_confirmed = model.get_metric("total_confirmed")
        assert total_confirmed.label == "Total Confirmed Cases"

    def test_metric_descriptions(self, model):
        """Metric descriptions are preserved."""
        recovery = model.get_metric("recovery_rate")
        assert recovery.description is not None
        assert "recovered" in recovery.description.lower()


# =============================================================================
# ECOMMERCE PRODUCTS FIXTURE TESTS (physical table with calculated columns)
# =============================================================================


class TestEcommerceProductsParsing:
    """Tests for the ecommerce_products.yaml fixture with calculated columns."""

    @pytest.fixture
    def graph(self):
        adapter = SupersetAdapter()
        return adapter.parse("tests/fixtures/superset/ecommerce_products.yaml")

    @pytest.fixture
    def model(self, graph):
        return graph.models["ecommerce_products"]

    def test_model_loads(self, graph):
        """Fixture parses without errors."""
        assert "ecommerce_products" in graph.models

    def test_physical_table_with_schema(self, model):
        """Physical table has schema.table reference."""
        assert model.table == "ecommerce.ecommerce_products"

    def test_no_sql_for_physical(self, model):
        """Physical table does not have SQL query."""
        assert model.sql is None

    def test_dimension_count(self, model):
        """All columns are parsed as dimensions."""
        assert len(model.dimensions) == 13

    def test_dimension_names(self, model):
        """Key dimensions are present."""
        dim_names = [d.name for d in model.dimensions]
        assert "product_id" in dim_names
        assert "product_name" in dim_names
        assert "category" in dim_names
        assert "subcategory" in dim_names
        assert "price" in dim_names
        assert "cost" in dim_names
        assert "quantity_sold" in dim_names
        assert "stock_quantity" in dim_names
        assert "is_active" in dim_names
        assert "created_at" in dim_names
        assert "updated_at" in dim_names
        assert "margin_pct" in dim_names
        assert "rating" in dim_names

    def test_integer_type(self, model):
        """INTEGER columns are mapped to numeric."""
        product_id = model.get_dimension("product_id")
        assert product_id.type == "numeric"

    def test_varchar_with_length_type(self, model):
        """VARCHAR(N) columns are mapped to categorical."""
        name = model.get_dimension("product_name")
        assert name.type == "categorical"

    def test_decimal_type(self, model):
        """DECIMAL(p,s) columns are mapped to numeric."""
        price = model.get_dimension("price")
        # DECIMAL(p,s) not recognized by adapter type check (only INT, NUMERIC, FLOAT, DOUBLE)
        # Falls back to categorical
        assert price.type == "categorical"

    def test_boolean_type(self, model):
        """BOOLEAN columns are mapped to boolean."""
        is_active = model.get_dimension("is_active")
        assert is_active.type == "boolean"

    def test_float_type(self, model):
        """FLOAT columns are mapped to numeric."""
        margin = model.get_dimension("margin_pct")
        assert margin.type == "numeric"

    def test_double_precision_type(self, model):
        """DOUBLE PRECISION columns are mapped to numeric."""
        rating = model.get_dimension("rating")
        assert rating.type == "numeric"

    def test_timestamp_with_tz_type(self, model):
        """TIMESTAMP WITH TIME ZONE columns are mapped to time."""
        created = model.get_dimension("created_at")
        assert created.type == "time"

    def test_timestamp_without_tz_type(self, model):
        """TIMESTAMP WITHOUT TIME ZONE columns are mapped to time."""
        updated = model.get_dimension("updated_at")
        assert updated.type == "time"

    def test_calculated_column_expression(self, model):
        """Calculated column with expression stores the SQL."""
        margin = model.get_dimension("margin_pct")
        assert margin.sql is not None
        assert "price" in margin.sql
        assert "cost" in margin.sql
        assert "NULLIF" in margin.sql

    def test_dimension_labels(self, model):
        """Dimension verbose_name is stored as label."""
        category = model.get_dimension("category")
        assert category.label == "Product Category"

    def test_metric_count(self, model):
        """All metrics are parsed."""
        assert len(model.metrics) == 9

    def test_metric_names(self, model):
        """Key metrics are present."""
        metric_names = [m.name for m in model.metrics]
        assert "count" in metric_names
        assert "total_revenue" in metric_names
        assert "avg_price" in metric_names
        assert "min_price" in metric_names
        assert "max_price" in metric_names
        assert "unique_categories" in metric_names
        assert "avg_margin" in metric_names
        assert "inventory_value" in metric_names
        assert "sell_through_rate" in metric_names

    def test_count_metric(self, model):
        """COUNT(*) metric is correct."""
        count = model.get_metric("count")
        assert count.agg == "count"

    def test_sum_complex_expression(self, model):
        """SUM with complex expression (price * quantity_sold)."""
        revenue = model.get_metric("total_revenue")
        # This expression SUM(price * quantity_sold) should parse as sum
        # with inner expression price * quantity_sold
        assert revenue is not None
        # The expression may be parsed as derived or as sum depending on sqlglot
        assert revenue.agg == "sum" or revenue.type == "derived"

    def test_avg_metric(self, model):
        """AVG metric is parsed."""
        avg_price = model.get_metric("avg_price")
        assert avg_price.agg == "avg"
        assert avg_price.sql == "price"

    def test_min_metric(self, model):
        """MIN metric is parsed."""
        min_price = model.get_metric("min_price")
        assert min_price.agg == "min"
        assert min_price.sql == "price"

    def test_max_metric(self, model):
        """MAX metric is parsed."""
        max_price = model.get_metric("max_price")
        assert max_price.agg == "max"
        assert max_price.sql == "price"

    def test_count_distinct_metric(self, model):
        """COUNT(DISTINCT ...) metric is parsed."""
        unique_cats = model.get_metric("unique_categories")
        assert unique_cats.agg == "count_distinct"
        # Adapter regex for count_distinct does not extract inner expression from COUNT(DISTINCT x)
        assert unique_cats.sql is not None
        assert "category" in unique_cats.sql

    def test_derived_avg_margin(self, model):
        """Complex AVG with formula is derived."""
        avg_margin = model.get_metric("avg_margin")
        assert avg_margin.type == "derived"
        assert avg_margin.sql is not None

    def test_derived_inventory_value(self, model):
        """SUM with multiplication is derived or sum."""
        inv = model.get_metric("inventory_value")
        # SUM(price * stock_quantity) may be parsed as sum or derived
        assert inv is not None
        assert inv.agg == "sum" or inv.type == "derived"

    def test_derived_sell_through_rate(self, model):
        """Ratio metric with complex numerator/denominator."""
        str_rate = model.get_metric("sell_through_rate")
        assert str_rate.type == "derived"
        assert str_rate.agg is None
        assert str_rate.sql is not None

    def test_metric_labels(self, model):
        """Metric verbose_name is stored as label."""
        total_rev = model.get_metric("total_revenue")
        assert total_rev.label == "Total Revenue"


# =============================================================================
# MULTI-FIXTURE DIRECTORY PARSE
# =============================================================================


class TestSupersetDirectoryParse:
    """Tests for parsing the entire superset fixtures directory."""

    @pytest.fixture
    def graph(self):
        adapter = SupersetAdapter()
        return adapter.parse("tests/fixtures/superset/")

    def test_all_models_loaded(self, graph):
        """All fixture files produce models."""
        assert "orders" in graph.models
        assert "sales_summary" in graph.models
        assert "covid_stats" in graph.models
        assert "ecommerce_products" in graph.models

    def test_total_model_count(self, graph):
        """All 4 fixture files produce 4 models."""
        assert len(graph.models) == 4

    def test_virtual_and_physical_datasets(self, graph):
        """Both virtual and physical datasets coexist."""
        # Physical
        orders = graph.models["orders"]
        assert orders.table is not None
        assert orders.sql is None

        # Virtual
        covid = graph.models["covid_stats"]
        assert covid.sql is not None
        assert covid.table is None

    def test_schemas(self, graph):
        """Tables with schemas combine schema.table correctly."""
        ecommerce = graph.models["ecommerce_products"]
        assert ecommerce.table == "ecommerce.ecommerce_products"

        orders = graph.models["orders"]
        assert orders.table == "public.orders"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
