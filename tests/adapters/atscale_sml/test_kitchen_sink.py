"""Kitchen sink tests for AtScale SML adapter."""

from pathlib import Path

import pytest

from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter


@pytest.fixture
def adapter():
    return AtScaleSMLAdapter()


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent.parent.parent / "fixtures" / "atscale_sml_kitchen_sink"


class TestAtScaleSMLKitchenSink:
    def test_parse_models_and_tables(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)

        assert "order_model" in graph.models
        assert "return_model" in graph.models
        assert "dim_customers" in graph.models
        assert "dim_dates" in graph.models
        assert "dim_promos" in graph.models

        order_model = graph.models["order_model"]
        return_model = graph.models["return_model"]
        dim_customers = graph.models["dim_customers"]
        dim_dates = graph.models["dim_dates"]
        dim_promos = graph.models["dim_promos"]

        assert order_model.table == "warehouse.analytics.orders"
        assert return_model.table == "warehouse.analytics.returns"
        assert dim_customers.table == "warehouse.analytics.customers"
        assert dim_promos.table == "warehouse.analytics.promos"
        assert dim_dates.sql == "select * from analytics.dates"

    def test_parse_dimensions(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)

        order_model = graph.models["order_model"]
        return_model = graph.models["return_model"]
        dim_customers = graph.models["dim_customers"]
        dim_dates = graph.models["dim_dates"]

        order_year = dim_dates.get_dimension("order_year")
        assert order_year is not None
        assert order_year.type == "time"
        assert order_year.granularity == "year"

        order_month = dim_dates.get_dimension("order_month")
        assert order_month is not None
        assert order_month.parent == "order_year"

        order_date = dim_dates.get_dimension("order_date")
        assert order_date is not None
        assert order_date.parent == "order_month"

        email = dim_customers.get_dimension("email")
        assert email is not None
        assert email.parent == "customer_id"

        customer_alias = dim_customers.get_dimension("customer_alias")
        assert customer_alias is not None
        assert customer_alias.parent == "customer_name"

        order_status_orders = order_model.get_dimension("order_status")
        order_status_returns = return_model.get_dimension("order_status")
        assert order_status_orders is not None
        assert order_status_returns is not None
        assert order_status_orders.type == "categorical"

    def test_parse_metrics(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)

        order_model = graph.models["order_model"]
        return_model = graph.models["return_model"]
        dim_customers = graph.models["dim_customers"]

        total_amount = order_model.get_metric("total_amount")
        assert total_amount is not None
        assert total_amount.agg == "sum"
        assert total_amount.sql == "total_amount"
        assert total_amount.format == "currency"

        order_count = order_model.get_metric("order_count")
        assert order_count is not None
        assert order_count.agg == "count"
        assert order_count.sql == "order_id"

        unique_customers = order_model.get_metric("unique_customers")
        assert unique_customers is not None
        assert unique_customers.agg == "count_distinct"

        total_amount_distinct = order_model.get_metric("total_amount_distinct")
        assert total_amount_distinct is not None
        assert total_amount_distinct.type == "derived"
        assert total_amount_distinct.sql == "SUM(DISTINCT total_amount)"

        amount_stddev = order_model.get_metric("amount_stddev")
        assert amount_stddev is not None
        assert amount_stddev.type == "derived"
        assert amount_stddev.sql == "STDDEV_SAMP(total_amount)"

        amount_median = order_model.get_metric("amount_median")
        assert amount_median is not None
        assert amount_median.agg == "median"
        assert amount_median.sql == "total_amount"

        amount_p90 = order_model.get_metric("amount_p90")
        assert amount_p90 is not None
        assert amount_p90.type == "derived"
        assert amount_p90.sql == "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_amount)"

        avg_order_value = order_model.get_metric("avg_order_value")
        assert avg_order_value is not None
        assert avg_order_value.type == "derived"
        assert "total_amount" in avg_order_value.sql

        customer_count = dim_customers.get_metric("customer_count")
        assert customer_count is not None
        assert customer_count.agg == "count_distinct"

        return_count = return_model.get_metric("return_count")
        assert return_count is not None
        assert return_count.agg == "count"

    def test_parse_relationships_and_rollups(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)
        order_model = graph.models["order_model"]

        rel_names = {rel.name: rel for rel in order_model.relationships}
        assert "dim_customers" in rel_names
        assert "dim_dates" in rel_names
        assert "dim_promos" in rel_names

        customer_rel = rel_names["dim_customers"]
        assert customer_rel.type == "many_to_one"
        assert customer_rel.foreign_key == "customer_id"
        assert customer_rel.primary_key == "customer_id"

        promo_rel = rel_names["dim_promos"]
        assert promo_rel.type == "many_to_many"
        assert promo_rel.foreign_key == "promo_id"

        assert len(order_model.pre_aggregations) == 1
        preagg = order_model.pre_aggregations[0]
        assert preagg.name == "orders_rollup"
        assert "total_amount" in preagg.measures
        assert "order_count" in preagg.measures
        assert "order_year" in preagg.dimensions
        assert "promo_name" in preagg.dimensions

        total_amount = order_model.get_metric("total_amount")
        assert total_amount.drill_fields is not None
        assert "customer_name" in total_amount.drill_fields
        assert "promo_name" in total_amount.drill_fields


class TestAtScaleSMLAdvancedFeatures:
    """Tests for advanced SML features derived from Apache 2.0 spec.

    Fixtures sourced from the Apache 2.0 licensed SML spec at
    github.com/semanticdatalayer/sml (sml-reference/ docs).
    Exercises: role-playing relationships, perspectives, aggregates
    with partitioning, constraint translation, row security,
    and custom empty members.
    """

    def test_internet_sales_model_created(self, adapter, fixtures_dir):
        """Internet Sales model with role-playing relationships parses."""
        graph = adapter.parse(fixtures_dir)

        assert "Internet Sales" in graph.models
        model = graph.models["Internet Sales"]
        assert model.table == "warehouse.analytics.internet_sales"

    def test_role_playing_relationships(self, adapter, fixtures_dir):
        """Multiple role-playing rels to Date Dimension produce relationships."""
        graph = adapter.parse(fixtures_dir)
        model = graph.models["Internet Sales"]

        rel_names = {rel.name for rel in model.relationships}
        # Both role-played Date rels and Product rel resolve to target models
        assert "dimdate" in rel_names
        assert "dimproduct" in rel_names

        date_rel = next(r for r in model.relationships if r.name == "dimdate")
        assert date_rel.foreign_key == "orderdatekey"
        assert date_rel.primary_key == "datekey"

        product_rel = next(r for r in model.relationships if r.name == "dimproduct")
        assert product_rel.foreign_key == "productkey"
        assert product_rel.primary_key == "productkey"

    def test_constraint_translation_does_not_break(self, adapter, fixtures_dir):
        """Relationships with constraint_translation parse without error."""
        graph = adapter.parse(fixtures_dir)
        # The constraint_translation property is silently ignored by the
        # adapter but should not cause parse failures.
        assert "Internet Sales" in graph.models

    def test_aggregate_with_partition_attribute(self, adapter, fixtures_dir):
        """Aggregate with partition attribute on DateYear parses."""
        graph = adapter.parse(fixtures_dir)
        model = graph.models["Internet Sales"]

        assert len(model.pre_aggregations) == 1
        preagg = model.pre_aggregations[0]
        assert preagg.name == "sales_by_date_agg"
        assert "orderquantity" in preagg.measures
        assert "salesamount" in preagg.measures
        assert "DateYear" in preagg.dimensions
        assert "Product Name" in preagg.dimensions

    def test_perspectives_do_not_break(self, adapter, fixtures_dir):
        """Model with perspectives property parses without error."""
        graph = adapter.parse(fixtures_dir)
        # Perspectives are not mapped to the semantic graph but should
        # not cause parse failures.
        assert "Internet Sales" in graph.models

    def test_partitions_do_not_break(self, adapter, fixtures_dir):
        """Model-level partitions property parses without error."""
        graph = adapter.parse(fixtures_dir)
        assert "Internet Sales" in graph.models

    def test_drillthroughs_on_internet_sales(self, adapter, fixtures_dir):
        """Drillthroughs from Internet Sales model attach to metrics."""
        graph = adapter.parse(fixtures_dir)
        model = graph.models["Internet Sales"]

        salesamount = model.get_metric("salesamount")
        assert salesamount is not None
        assert salesamount.drill_fields is not None
        assert "Product Name" in salesamount.drill_fields

        orderquantity = model.get_metric("orderquantity")
        assert orderquantity is not None
        assert orderquantity.drill_fields is not None
        assert "StateCity" in orderquantity.drill_fields
        assert "City" in orderquantity.drill_fields

    def test_metrics_on_internet_sales(self, adapter, fixtures_dir):
        """Metrics on Internet Sales model from factinternetsales dataset."""
        graph = adapter.parse(fixtures_dir)
        model = graph.models["Internet Sales"]

        salesamount = model.get_metric("salesamount")
        assert salesamount is not None
        assert salesamount.agg == "sum"
        assert salesamount.sql == "salesamount"
        assert salesamount.format == "standard"

        orderquantity = model.get_metric("orderquantity")
        assert orderquantity is not None
        assert orderquantity.agg == "sum"
        assert orderquantity.sql == "orderquantity"

    def test_row_security_object_skipped_gracefully(self, adapter, fixtures_dir):
        """row_security object type is not in supported types and is skipped."""
        graph = adapter.parse(fixtures_dir)
        # The row_security file should not create a model or cause errors
        assert "Country Security Filter" not in graph.models

    def test_row_security_relationship_skipped(self, adapter, fixtures_dir):
        """Dimension relationship targeting row_security is skipped."""
        graph = adapter.parse(fixtures_dir)
        # The Geography Dimension has a relationship to row_security.
        # Since to.dimension is None, the adapter skips it.
        geo = graph.models.get("dim_geo_country")
        assert geo is not None
        # No relationship should be created for the row_security link
        row_sec_rels = [r for r in geo.relationships if r.name == "Country Security Filter"]
        assert len(row_sec_rels) == 0

    def test_custom_empty_member_does_not_break(self, adapter, fixtures_dir):
        """Dimensions with custom_empty_member parse correctly."""
        graph = adapter.parse(fixtures_dir)

        dimproduct = graph.models.get("dimproduct")
        assert dimproduct is not None

        product_name = dimproduct.get_dimension("Product Name")
        assert product_name is not None
        assert product_name.type == "categorical"

        # Secondary attributes with custom_empty_member also parse
        product_color = dimproduct.get_dimension("product_color")
        assert product_color is not None
        assert product_color.type == "categorical"

    def test_date_dimension_hierarchy_and_granularity(self, adapter, fixtures_dir):
        """Date Dimension with constraint_translation_rank parses hierarchy."""
        graph = adapter.parse(fixtures_dir)

        dimdate = graph.models.get("dimdate")
        assert dimdate is not None

        date_year = dimdate.get_dimension("DateYear")
        assert date_year is not None
        assert date_year.type == "time"
        assert date_year.granularity == "year"

        date_month = dimdate.get_dimension("DateMonth")
        assert date_month is not None
        assert date_month.type == "time"
        assert date_month.granularity == "month"
        assert date_month.parent == "DateYear"

        day_month = dimdate.get_dimension("DayMonth")
        assert day_month is not None
        assert day_month.type == "time"
        assert day_month.granularity == "day"
        assert day_month.parent == "DateMonth"

    def test_geography_dimension_hierarchy(self, adapter, fixtures_dir):
        """Geography dimension with nested hierarchy and secondary attrs."""
        graph = adapter.parse(fixtures_dir)

        geo = graph.models.get("dim_geo_country")
        assert geo is not None

        country = geo.get_dimension("CountryCity")
        assert country is not None
        assert country.type == "categorical"

        state = geo.get_dimension("StateCity")
        assert state is not None
        assert state.parent == "CountryCity"

        city = geo.get_dimension("City")
        assert city is not None
        assert city.parent == "StateCity"

        # zip_code is a secondary attribute on the City level
        zip_code = geo.get_dimension("zip_code")
        assert zip_code is not None
        assert zip_code.parent == "City"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
