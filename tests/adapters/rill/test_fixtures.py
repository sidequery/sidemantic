"""Tests for real-world Rill fixtures based on rill-examples patterns.

These fixtures come from actual Rill deployments found in rilldata/rill-examples:
- cost_monitoring: rill-cost-monitoring/metrics_margin_metrics.yaml
- medicaid_spending: medicaid_provider_spending.yaml
- mobile_events: rill-app-engagement/mobile_events_metrics.yaml

Tests are permissive: parse without errors, check counts, verify key names.
"""

import pytest

from sidemantic.adapters.rill import RillAdapter

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def cost_monitoring():
    """Parse cost_monitoring fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/cost_monitoring.yaml")
    return graph.models["cost_monitoring"]


@pytest.fixture
def medicaid_spending():
    """Parse medicaid_spending fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/medicaid_spending.yaml")
    return graph.models["medicaid_spending"]


@pytest.fixture
def mobile_events():
    """Parse mobile_events fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/mobile_events.yaml")
    return graph.models["mobile_events"]


# =============================================================================
# COST MONITORING (currency/percentage formats, derived measures, security)
# =============================================================================


class TestCostMonitoringParsing:
    """Tests for cost_monitoring.yaml based on rill-cost-monitoring."""

    def test_parses_without_error(self):
        """Fixture parses successfully."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/cost_monitoring.yaml")
        assert "cost_monitoring" in graph.models

    def test_model_name_from_filename(self, cost_monitoring):
        """Model name derived from filename since no 'name' key in YAML."""
        # The fixture has no explicit 'name' field, so it uses the file stem
        assert cost_monitoring.table == "metrics_margin_model"

    def test_dimension_count(self, cost_monitoring):
        """8 explicit dimensions + 1 auto-created __time timeseries = 9."""
        assert len(cost_monitoring.dimensions) == 9

    def test_dimension_names(self, cost_monitoring):
        """All 8 business dimensions present."""
        dim_names = {d.name for d in cost_monitoring.dimensions}
        expected = {
            "customer",
            "plan_name",
            "location",
            "component",
            "app_name",
            "sku_description",
            "pipeline",
            "environment",
        }
        assert expected.issubset(dim_names)

    def test_dimension_labels(self, cost_monitoring):
        """display_name maps to label."""
        dims = {d.name: d for d in cost_monitoring.dimensions}
        assert dims["customer"].label == "Customer"
        assert dims["location"].label == "Cost by Region"
        assert dims["sku_description"].label == "Cost by SKU"

    def test_dimension_descriptions(self, cost_monitoring):
        """Descriptions are preserved."""
        dims = {d.name: d for d in cost_monitoring.dimensions}
        assert "name of the customer" in dims["customer"].description.lower()

    def test_dimension_column_mapping(self, cost_monitoring):
        """column field maps to sql."""
        dims = {d.name: d for d in cost_monitoring.dimensions}
        # customer.column = company
        assert dims["customer"].sql == "company"

    def test_timeseries_auto_created(self, cost_monitoring):
        """__time timeseries dimension auto-created since not in explicit dims."""
        time_dims = [d for d in cost_monitoring.dimensions if d.type == "time"]
        assert len(time_dims) == 1
        assert time_dims[0].name == "__time"

    def test_default_time_dimension(self, cost_monitoring):
        """timeseries field sets default_time_dimension."""
        assert cost_monitoring.default_time_dimension == "__time"

    def test_default_grain(self, cost_monitoring):
        """smallest_time_grain: day maps to default_grain."""
        assert cost_monitoring.default_grain == "day"

    def test_metric_count(self, cost_monitoring):
        """5 measures parsed."""
        assert len(cost_monitoring.metrics) == 5

    def test_metric_names(self, cost_monitoring):
        """All measure names present."""
        metric_names = {m.name for m in cost_monitoring.metrics}
        expected = {
            "total_cost",
            "total_revenue",
            "net_revenue",
            "gross_margin_percent",
            "unique_customers",
        }
        assert metric_names == expected

    def test_simple_sum_metrics(self, cost_monitoring):
        """SUM(cost) and SUM(revenue) decomposed correctly."""
        metrics = {m.name: m for m in cost_monitoring.metrics}
        assert metrics["total_cost"].agg == "sum"
        assert metrics["total_cost"].sql == "cost"
        assert metrics["total_revenue"].agg == "sum"
        assert metrics["total_revenue"].sql == "revenue"

    def test_count_distinct_metric(self, cost_monitoring):
        """COUNT(DISTINCT company) parsed as count_distinct."""
        metrics = {m.name: m for m in cost_monitoring.metrics}
        assert metrics["unique_customers"].agg == "count_distinct"
        assert metrics["unique_customers"].sql == "company"

    def test_derived_arithmetic_net_revenue(self, cost_monitoring):
        """SUM(revenue) - SUM(cost) kept as full expression (not decomposed)."""
        metrics = {m.name: m for m in cost_monitoring.metrics}
        m = metrics["net_revenue"]
        # Multi-agg expressions can't be decomposed into a single agg
        assert m.agg is None
        assert "SUM(revenue)" in m.sql
        assert "SUM(cost)" in m.sql

    def test_derived_arithmetic_gross_margin(self, cost_monitoring):
        """(SUM(revenue) - SUM(cost))/SUM(revenue) kept as full expression."""
        metrics = {m.name: m for m in cost_monitoring.metrics}
        m = metrics["gross_margin_percent"]
        assert m.agg is None
        assert "SUM(revenue)" in m.sql
        assert "SUM(cost)" in m.sql

    def test_format_preset_currency(self, cost_monitoring):
        """currency_usd format_preset maps to 'usd' value_format_name."""
        metrics = {m.name: m for m in cost_monitoring.metrics}
        assert metrics["total_cost"].value_format_name == "usd"
        assert metrics["total_revenue"].value_format_name == "usd"
        assert metrics["net_revenue"].value_format_name == "usd"

    def test_format_preset_percentage(self, cost_monitoring):
        """percentage format_preset maps to 'percent' value_format_name."""
        metrics = {m.name: m for m in cost_monitoring.metrics}
        assert metrics["gross_margin_percent"].value_format_name == "percent"

    def test_format_preset_humanize(self, cost_monitoring):
        """humanize format_preset maps to 'decimal_0' value_format_name."""
        metrics = {m.name: m for m in cost_monitoring.metrics}
        assert metrics["unique_customers"].value_format_name == "decimal_0"

    def test_security_section_ignored_gracefully(self):
        """The security: access: true section does not break parsing."""
        # This test is implicit in the fixture parsing, but explicit is better
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/cost_monitoring.yaml")
        assert len(graph.models) == 1

    def test_version_field_ignored_gracefully(self):
        """The version: 1 field does not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/cost_monitoring.yaml")
        assert len(graph.models) == 1


# =============================================================================
# MEDICAID SPENDING (expression dimensions, NULLIF, 20+ dimensions)
# =============================================================================


class TestMedicaidSpendingParsing:
    """Tests for medicaid_spending.yaml based on medicaid_provider_spending."""

    def test_parses_without_error(self):
        """Fixture parses successfully."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/medicaid_spending.yaml")
        assert "medicaid_spending" in graph.models

    def test_model_description(self, medicaid_spending):
        """Model description is preserved."""
        assert "Medicaid provider spending" in medicaid_spending.description

    def test_dimension_count(self, medicaid_spending):
        """20 dimensions total (claim_month is both explicit and timeseries)."""
        assert len(medicaid_spending.dimensions) == 20

    def test_column_based_dimensions(self, medicaid_spending):
        """Column-based dimensions have correct sql."""
        dims = {d.name: d for d in medicaid_spending.dimensions}
        assert dims["hcpcs_code"].sql == "hcpcs_code"
        assert dims["billing_provider_npi"].sql == "billing_provider_npi"
        assert dims["servicing_provider_name"].sql == "servicing_provider_name"
        assert dims["procedure_description"].sql == "procedure_description"

    def test_expression_dimension_case_when(self, medicaid_spending):
        """Expression-based dimensions with CASE WHEN are preserved."""
        dims = {d.name: d for d in medicaid_spending.dimensions}

        same = dims["same_provider"]
        assert "CASE WHEN" in same.sql
        assert "billing_provider_npi = servicing_provider_npi" in same.sql
        assert same.label == "Same Billing & Servicing Provider"

    def test_expression_dimension_boolean_case(self, medicaid_spending):
        """Boolean CASE WHEN expression preserved."""
        dims = {d.name: d for d in medicaid_spending.dimensions}
        excl = dims["is_excluded"]
        assert "CASE WHEN is_excluded" in excl.sql

    def test_expression_dimension_comparison(self, medicaid_spending):
        """Comparison expression with != preserved."""
        dims = {d.name: d for d in medicaid_spending.dimensions}
        ms = dims["multi_state"]
        assert "billing_provider_state != servicing_provider_state" in ms.sql

    def test_expression_dimension_multiline_case(self, medicaid_spending):
        """Multiline CASE expression with buckets preserved."""
        dims = {d.name: d for d in medicaid_spending.dimensions}
        pr = dims["payment_range"]
        assert "CASE" in pr.sql
        assert "10000" in pr.sql

    def test_expression_dimension_numeric_comparison(self, medicaid_spending):
        """CASE WHEN with numeric comparison (> 10000) preserved."""
        dims = {d.name: d for d in medicaid_spending.dimensions}
        hcf = dims["high_cost_flag"]
        assert "total_paid > 10000" in hcf.sql

    def test_timeseries_dimension_is_time_type(self, medicaid_spending):
        """claim_month is recognized as time dimension."""
        dims = {d.name: d for d in medicaid_spending.dimensions}
        assert dims["claim_month"].type == "time"

    def test_default_time_dimension(self, medicaid_spending):
        """timeseries: claim_month sets default_time_dimension."""
        assert medicaid_spending.default_time_dimension == "claim_month"

    def test_default_grain_month(self, medicaid_spending):
        """smallest_time_grain: month maps correctly."""
        assert medicaid_spending.default_grain == "month"

    def test_metric_count(self, medicaid_spending):
        """7 measures parsed."""
        assert len(medicaid_spending.metrics) == 7

    def test_simple_sum_metric(self, medicaid_spending):
        """SUM(total_paid) decomposed correctly."""
        metrics = {m.name: m for m in medicaid_spending.metrics}
        assert metrics["total_paid"].agg == "sum"
        assert metrics["total_paid"].sql == "total_paid"

    def test_count_star_metric(self, medicaid_spending):
        """COUNT(*) decomposed correctly."""
        metrics = {m.name: m for m in medicaid_spending.metrics}
        assert metrics["total_claims"].agg == "count"

    def test_count_distinct_metrics(self, medicaid_spending):
        """Multiple COUNT(DISTINCT ...) measures parsed."""
        metrics = {m.name: m for m in medicaid_spending.metrics}
        assert metrics["unique_providers"].agg == "count_distinct"
        assert metrics["unique_providers"].sql == "billing_provider_npi"
        assert metrics["unique_servicing_providers"].agg == "count_distinct"
        assert metrics["unique_servicing_providers"].sql == "servicing_provider_npi"
        assert metrics["unique_procedures"].agg == "count_distinct"
        assert metrics["unique_procedures"].sql == "hcpcs_code"

    def test_nullif_ratio_metric(self, medicaid_spending):
        """SUM(total_paid) / NULLIF(SUM(total_claims), 0) as full expression."""
        metrics = {m.name: m for m in medicaid_spending.metrics}
        m = metrics["avg_paid_per_claim"]
        # Complex multi-agg expression stays as full SQL
        assert m.agg is None
        assert "NULLIF" in m.sql
        assert "SUM(total_paid)" in m.sql

    def test_count_star_nullif_ratio(self, medicaid_spending):
        """COUNT(*) / NULLIF(COUNT(DISTINCT ...), 0) as full expression."""
        metrics = {m.name: m for m in medicaid_spending.metrics}
        m = metrics["avg_claims_per_provider"]
        assert m.agg is None
        assert "COUNT(*)" in m.sql
        assert "NULLIF" in m.sql

    def test_format_presets(self, medicaid_spending):
        """Format presets mapped correctly."""
        metrics = {m.name: m for m in medicaid_spending.metrics}
        assert metrics["total_paid"].value_format_name == "usd"
        assert metrics["total_claims"].value_format_name == "decimal_0"
        assert metrics["avg_paid_per_claim"].value_format_name == "usd"
        assert metrics["unique_providers"].value_format_name == "decimal_0"

    def test_metric_descriptions(self, medicaid_spending):
        """Metric descriptions are preserved."""
        metrics = {m.name: m for m in medicaid_spending.metrics}
        assert "Total Medicaid payments" in metrics["total_paid"].description
        assert "distinct billing provider" in metrics["unique_providers"].description.lower()

    def test_dimension_descriptions(self, medicaid_spending):
        """Dimension descriptions are preserved for column-based and expression-based."""
        dims = {d.name: d for d in medicaid_spending.dimensions}
        assert "procedure" in dims["hcpcs_code"].description.lower()
        assert "billing and servicing" in dims["same_provider"].description.lower()

    def test_many_provider_dimensions(self, medicaid_spending):
        """Multiple provider NPI/name/type/state/city dimensions present."""
        dim_names = {d.name for d in medicaid_spending.dimensions}
        billing_dims = {n for n in dim_names if n.startswith("billing_provider_")}
        servicing_dims = {n for n in dim_names if n.startswith("servicing_provider_")}
        assert len(billing_dims) >= 4
        assert len(servicing_dims) >= 4


# =============================================================================
# MOBILE EVENTS (funnel metrics, *1.0 pattern, hour grain, count with space)
# =============================================================================


class TestMobileEventsParsing:
    """Tests for mobile_events.yaml based on rill-app-engagement."""

    def test_parses_without_error(self):
        """Fixture parses successfully."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/mobile_events.yaml")
        assert "mobile_events" in graph.models

    def test_dimension_count(self, mobile_events):
        """7 explicit dimensions + 1 auto-created event_time = 8."""
        assert len(mobile_events.dimensions) == 8

    def test_dimension_names(self, mobile_events):
        """All business dimensions present."""
        dim_names = {d.name for d in mobile_events.dimensions}
        expected = {
            "new_existing",
            "referral_source",
            "device_type",
            "os_version",
            "campaign_name",
            "country",
            "app_version",
        }
        assert expected.issubset(dim_names)

    def test_timeseries_auto_created(self, mobile_events):
        """event_time auto-created as time dimension."""
        time_dims = [d for d in mobile_events.dimensions if d.type == "time"]
        assert len(time_dims) == 1
        assert time_dims[0].name == "event_time"

    def test_default_time_dimension(self, mobile_events):
        """timeseries: event_time sets default_time_dimension."""
        assert mobile_events.default_time_dimension == "event_time"

    def test_hour_grain(self, mobile_events):
        """smallest_time_grain: hour maps correctly."""
        assert mobile_events.default_grain == "hour"

    def test_metric_count(self, mobile_events):
        """8 measures parsed."""
        assert len(mobile_events.metrics) == 8

    def test_metric_names(self, mobile_events):
        """All measure names present."""
        metric_names = {m.name for m in mobile_events.metrics}
        expected = {
            "total_page_views",
            "total_clicks",
            "total_downloads",
            "unique_visitors",
            "site_conversion_rate",
            "opt_in_rate",
            "completion_rate",
            "avg_session_duration",
        }
        assert metric_names == expected

    def test_lowercase_sum(self, mobile_events):
        """sum(...) with lowercase parsed correctly."""
        metrics = {m.name: m for m in mobile_events.metrics}
        assert metrics["total_page_views"].agg == "sum"
        assert metrics["total_page_views"].sql == "landing_page_view_cnt"

    def test_lowercase_count_distinct(self, mobile_events):
        """count(distinct ...) with lowercase parsed correctly."""
        metrics = {m.name: m for m in mobile_events.metrics}
        assert metrics["total_downloads"].agg == "count_distinct"
        assert metrics["total_downloads"].sql == "download_id"

    def test_count_space_distinct(self, mobile_events):
        """count (distinct ...) with space before paren parsed correctly."""
        metrics = {m.name: m for m in mobile_events.metrics}
        # The adapter should handle the space between count and (
        assert metrics["unique_visitors"].agg == "count_distinct"
        assert metrics["unique_visitors"].sql == "visitor_id"

    def test_avg_metric(self, mobile_events):
        """AVG(session_duration_seconds) decomposed correctly."""
        metrics = {m.name: m for m in mobile_events.metrics}
        assert metrics["avg_session_duration"].agg == "avg"
        assert metrics["avg_session_duration"].sql == "session_duration_seconds"

    def test_conversion_rate_expression(self, mobile_events):
        """sum(x)*1.0/sum(y)*1.0 kept as full expression."""
        metrics = {m.name: m for m in mobile_events.metrics}
        m = metrics["site_conversion_rate"]
        assert m.agg is None
        assert "*1.0" in m.sql

    def test_opt_in_rate_expression(self, mobile_events):
        """count(distinct x)*1.0/count(distinct y)*1.0 kept as full expression."""
        metrics = {m.name: m for m in mobile_events.metrics}
        m = metrics["opt_in_rate"]
        assert m.agg is None
        assert "count(distinct" in m.sql.lower()

    def test_completion_rate_expression(self, mobile_events):
        """count (distinct x)*1.0/count (distinct y)*1.0 with spaces kept as expression."""
        metrics = {m.name: m for m in mobile_events.metrics}
        m = metrics["completion_rate"]
        assert m.agg is None
        assert "*1.0" in m.sql

    def test_format_preset_percentage(self, mobile_events):
        """Funnel/conversion metrics have percentage format."""
        metrics = {m.name: m for m in mobile_events.metrics}
        assert metrics["site_conversion_rate"].value_format_name == "percent"
        assert metrics["opt_in_rate"].value_format_name == "percent"
        assert metrics["completion_rate"].value_format_name == "percent"

    def test_format_preset_humanize(self, mobile_events):
        """Count metrics have humanize (decimal_0) format."""
        metrics = {m.name: m for m in mobile_events.metrics}
        assert metrics["total_page_views"].value_format_name == "decimal_0"
        assert metrics["total_clicks"].value_format_name == "decimal_0"
        assert metrics["total_downloads"].value_format_name == "decimal_0"
        assert metrics["unique_visitors"].value_format_name == "decimal_0"

    def test_security_section_ignored(self):
        """security: access: true does not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/mobile_events.yaml")
        assert len(graph.models) == 1

    def test_version_field_ignored(self):
        """version: 1 does not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/mobile_events.yaml")
        assert len(graph.models) == 1

    def test_metric_labels(self, mobile_events):
        """display_name maps to label on metrics."""
        metrics = {m.name: m for m in mobile_events.metrics}
        assert metrics["total_page_views"].label == "Page Views"
        assert metrics["site_conversion_rate"].label == "Site Conversion Rate"
        assert metrics["completion_rate"].label == "Completion Rate"

    def test_dimension_labels(self, mobile_events):
        """display_name maps to label on dimensions."""
        dims = {d.name: d for d in mobile_events.dimensions}
        assert dims["new_existing"].label == "New vs Existing"
        assert dims["referral_source"].label == "Referral Source"
        assert dims["device_type"].label == "Device Type"
