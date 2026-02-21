"""Tests for parsing the Snowflake Cortex Analyst revenue_timeseries fixture.

This fixture is based on the official Snowflake tutorial example from
Snowflake-Labs/sfguide-getting-started-with-cortex-analyst. It exercises:
- Multi-table semantic model (fact + dimension tables)
- Cortex Analyst `measures` key (with default_aggregation, synonyms)
- time_dimensions
- Composite primary keys
- Relationships with join_type and relationship_type
- sample_values on dimensions
- cortex_search_service_name on dimensions
- verified_queries section
"""

from pathlib import Path

import pytest

from sidemantic.adapters.snowflake import SnowflakeAdapter


@pytest.fixture
def adapter():
    return SnowflakeAdapter()


@pytest.fixture
def fixture_path():
    return Path(__file__).parent.parent.parent / "fixtures" / "snowflake" / "revenue_timeseries.yaml"


@pytest.fixture
def graph(adapter, fixture_path):
    return adapter.parse(fixture_path)


class TestRevenueTimeseriesParseWithoutErrors:
    """Parsing the fixture must not raise exceptions."""

    def test_parse_does_not_error(self, graph):
        """The adapter should parse the full file without exceptions."""
        assert graph is not None


class TestRevenueTimeseriesTableCounts:
    """Verify table/model counts."""

    def test_four_tables_parsed(self, graph):
        """The fixture defines 4 tables: daily_revenue, product, region, product_dimension."""
        assert len(graph.models) == 4

    def test_table_names(self, graph):
        expected = {"daily_revenue", "product", "region", "product_dimension"}
        assert set(graph.models.keys()) == expected


class TestRevenueTimeseriesBaseTable:
    """Verify base_table parsing produces correct fully-qualified table names."""

    def test_daily_revenue_fqn(self, graph):
        model = graph.models["daily_revenue"]
        assert model.table == "cortex_analyst_demo.revenue_timeseries.daily_revenue"

    def test_product_fqn(self, graph):
        model = graph.models["product"]
        assert model.table == "cortex_analyst_demo.revenue_timeseries.product_dim"

    def test_region_fqn(self, graph):
        model = graph.models["region"]
        assert model.table == "cortex_analyst_demo.revenue_timeseries.region_dim"

    def test_product_dimension_fqn(self, graph):
        model = graph.models["product_dimension"]
        assert model.table == "cortex_analyst_demo.revenue_timeseries.product_dim"


class TestRevenueTimeseriesDescriptions:
    """Verify descriptions are preserved."""

    def test_daily_revenue_description(self, graph):
        model = graph.models["daily_revenue"]
        assert model.description is not None
        assert "revenue" in model.description.lower()

    def test_product_description(self, graph):
        model = graph.models["product"]
        assert model.description is not None
        assert "product" in model.description.lower()

    def test_region_description(self, graph):
        model = graph.models["region"]
        assert model.description is not None
        assert "region" in model.description.lower()


class TestRevenueTimeseriesPrimaryKey:
    """Verify primary key parsing (composite key uses first column)."""

    def test_daily_revenue_composite_pk_uses_first(self, graph):
        """Composite PK (date, product_id, region_id) should use first: date."""
        model = graph.models["daily_revenue"]
        assert model.primary_key == "date"

    def test_product_pk(self, graph):
        model = graph.models["product"]
        assert model.primary_key == "product_id"

    def test_region_pk(self, graph):
        model = graph.models["region"]
        assert model.primary_key == "region_id"


class TestRevenueTimeseriesTimeDimensions:
    """Verify time_dimensions are parsed."""

    def test_daily_revenue_has_time_dimension(self, graph):
        model = graph.models["daily_revenue"]
        date_dim = model.get_dimension("date")
        assert date_dim is not None
        assert date_dim.type == "time"

    def test_time_dimension_has_expr(self, graph):
        model = graph.models["daily_revenue"]
        date_dim = model.get_dimension("date")
        assert date_dim.sql == "date"

    def test_time_dimension_has_description(self, graph):
        model = graph.models["daily_revenue"]
        date_dim = model.get_dimension("date")
        assert date_dim.description is not None


class TestRevenueTimeseriesDimensions:
    """Verify regular dimensions are parsed."""

    def test_daily_revenue_dimensions(self, graph):
        model = graph.models["daily_revenue"]
        dim_names = {d.name for d in model.dimensions if d.type != "time"}
        assert "product_id" in dim_names
        assert "region_id" in dim_names

    def test_product_dimensions(self, graph):
        model = graph.models["product"]
        dim_names = {d.name for d in model.dimensions}
        assert "product_id" in dim_names
        assert "product_line" in dim_names

    def test_region_dimensions(self, graph):
        model = graph.models["region"]
        dim_names = {d.name for d in model.dimensions}
        assert "region_id" in dim_names
        assert "sales_region" in dim_names

    def test_product_dimension_table_dimensions(self, graph):
        model = graph.models["product_dimension"]
        dim_names = {d.name for d in model.dimensions}
        assert "product_line" in dim_names

    def test_numeric_dimension_type(self, graph):
        """product_id with data_type: number should be numeric."""
        model = graph.models["daily_revenue"]
        pid = model.get_dimension("product_id")
        assert pid is not None
        assert pid.type == "numeric"

    def test_varchar_dimension_type(self, graph):
        """product_line with data_type: varchar should be categorical."""
        model = graph.models["product"]
        pl = model.get_dimension("product_line")
        assert pl is not None
        assert pl.type == "categorical"


class TestRevenueTimeseriesMeasures:
    """Verify measures parsing.

    The Cortex Analyst format uses `measures` (not `facts`). The adapter
    currently only looks for `facts` and `metrics`, so measures from the
    Cortex Analyst format are not imported. These tests are marked xfail
    to document the gap.
    """

    @pytest.mark.xfail(reason="Adapter parses 'facts' key, not 'measures' (Cortex Analyst format)")
    def test_daily_revenue_has_measures(self, graph):
        model = graph.models["daily_revenue"]
        metric_names = {m.name for m in model.metrics}
        assert "daily_revenue" in metric_names

    @pytest.mark.xfail(reason="Adapter parses 'facts' key, not 'measures' (Cortex Analyst format)")
    def test_daily_revenue_measure_count(self, graph):
        """daily_revenue table defines 5 measures."""
        model = graph.models["daily_revenue"]
        assert len(model.metrics) == 5

    @pytest.mark.xfail(reason="Adapter parses 'facts' key, not 'measures' (Cortex Analyst format)")
    def test_daily_cogs_measure(self, graph):
        model = graph.models["daily_revenue"]
        cogs = model.get_metric("daily_cogs")
        assert cogs is not None
        assert cogs.agg == "sum"
        assert cogs.sql == "cogs"

    @pytest.mark.xfail(reason="Adapter parses 'facts' key, not 'measures' (Cortex Analyst format)")
    def test_daily_profit_computed_measure(self, graph):
        """daily_profit has expr 'revenue - cogs' and no default_aggregation."""
        model = graph.models["daily_revenue"]
        profit = model.get_metric("daily_profit")
        assert profit is not None

    @pytest.mark.xfail(reason="Adapter parses 'facts' key, not 'measures' (Cortex Analyst format)")
    def test_forecast_error_avg_aggregation(self, graph):
        """daily_forecast_abs_error has default_aggregation: avg."""
        model = graph.models["daily_revenue"]
        error = model.get_metric("daily_forecast_abs_error")
        assert error is not None
        assert error.agg == "avg"


class TestRevenueTimeseriesRelationships:
    """Verify relationships are parsed."""

    def test_daily_revenue_has_relationships(self, graph):
        model = graph.models["daily_revenue"]
        assert len(model.relationships) == 2

    def test_revenue_to_product_relationship(self, graph):
        model = graph.models["daily_revenue"]
        rel_names = {r.name for r in model.relationships}
        assert "product" in rel_names

    def test_revenue_to_region_relationship(self, graph):
        model = graph.models["daily_revenue"]
        rel_names = {r.name for r in model.relationships}
        assert "region" in rel_names

    def test_relationship_type(self, graph):
        model = graph.models["daily_revenue"]
        product_rel = next(r for r in model.relationships if r.name == "product")
        assert product_rel.type == "many_to_one"

    def test_relationship_foreign_key(self, graph):
        model = graph.models["daily_revenue"]
        product_rel = next(r for r in model.relationships if r.name == "product")
        assert product_rel.foreign_key == "product_id"

    def test_relationship_primary_key(self, graph):
        model = graph.models["daily_revenue"]
        product_rel = next(r for r in model.relationships if r.name == "product")
        assert product_rel.primary_key == "product_id"

    def test_region_relationship_keys(self, graph):
        model = graph.models["daily_revenue"]
        region_rel = next(r for r in model.relationships if r.name == "region")
        assert region_rel.foreign_key == "region_id"
        assert region_rel.primary_key == "region_id"


class TestRevenueTimeseriesUnsupportedFeatures:
    """Test features present in the fixture that the adapter does not yet handle.

    These are marked xfail to document what a Cortex Analyst model can contain
    that sidemantic does not yet import.
    """

    @pytest.mark.xfail(reason="verified_queries not imported by adapter")
    def test_verified_queries_imported(self, graph):
        """The fixture has 2 verified_queries; adapter should expose them."""
        # SemanticGraph has no verified_queries attribute yet
        assert hasattr(graph, "verified_queries")
        assert len(graph.verified_queries) == 2

    @pytest.mark.xfail(reason="synonyms on measures not imported by adapter")
    def test_measure_synonyms(self, graph):
        """daily_revenue measure has synonyms ['sales', 'income']."""
        model = graph.models["daily_revenue"]
        rev = model.get_metric("daily_revenue")
        assert hasattr(rev, "synonyms")
        assert "sales" in rev.synonyms

    @pytest.mark.xfail(reason="sample_values on dimensions not imported by adapter")
    def test_dimension_sample_values(self, graph):
        """product_line dimension has sample_values."""
        model = graph.models["product"]
        pl = model.get_dimension("product_line")
        assert hasattr(pl, "sample_values")
        assert "Electronics" in pl.sample_values

    @pytest.mark.xfail(reason="cortex_search_service_name not imported by adapter")
    def test_cortex_search_service_name(self, graph):
        """product_dimension table has cortex_search_service_name on product_line."""
        model = graph.models["product_dimension"]
        pl = model.get_dimension("product_line")
        assert hasattr(pl, "cortex_search_service_name")
        assert pl.cortex_search_service_name == "product_line_search_service"
