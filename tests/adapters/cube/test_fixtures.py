"""Tests for Cube adapter with real-world patterns.

Fixtures sourced from:
- Cube.js official Stripe schema: https://github.com/cube-js/stripe-schema
- Cube schema compiler test fixtures: https://github.com/cube-js/cube
- Cube documentation examples: https://cube.dev/docs

These tests are intentionally permissive: they verify the adapter can parse
without errors and check key properties, documenting what works and what
the adapter silently ignores.
"""

from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter

FIXTURES_DIR = Path("tests/fixtures/cube")


# ---------------------------------------------------------------------------
# Stripe SaaS Metrics
# ---------------------------------------------------------------------------


class TestStripeSaaSMetrics:
    """Stripe SaaS metrics: rolling windows, derived measures, format, drill_members."""

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "stripe_saas_metrics.yml")

    def test_parses_without_error(self, graph):
        """The adapter should parse the fixture without raising."""
        assert graph is not None

    def test_all_cubes_imported(self, graph):
        assert "stripe_customers" in graph.models
        assert "stripe_charges" in graph.models
        assert "stripe_saas_metrics" in graph.models

    def test_stripe_customers_dimensions(self, graph):
        customers = graph.get_model("stripe_customers")
        assert customers.table == "public.customers"
        assert len(customers.dimensions) >= 4
        dim_names = {d.name for d in customers.dimensions}
        assert {"id", "email", "created", "currency"} <= dim_names

    def test_stripe_charges_dimensions_with_format(self, graph):
        charges = graph.get_model("stripe_charges")
        assert charges.table == "public.charges"

        amount_dim = charges.get_dimension("amount")
        assert amount_dim is not None
        assert amount_dim.type == "numeric"
        assert amount_dim.format == "currency"

        amount_refunded = charges.get_dimension("amount_refunded")
        assert amount_refunded is not None
        assert amount_refunded.format == "currency"

    def test_stripe_charges_boolean_dimensions(self, graph):
        charges = graph.get_model("stripe_charges")
        captured = charges.get_dimension("captured")
        assert captured is not None
        # boolean maps to categorical in sidemantic
        assert captured.type == "categorical"

        paid = charges.get_dimension("paid")
        assert paid is not None
        assert paid.type == "categorical"

    def test_stripe_charges_join(self, graph):
        charges = graph.get_model("stripe_charges")
        assert len(charges.relationships) == 1
        rel = charges.relationships[0]
        assert rel.name == "stripe_customers"
        assert rel.type == "many_to_one"

    def test_stripe_charges_filtered_measures(self, graph):
        charges = graph.get_model("stripe_charges")

        refunded_count = charges.get_metric("refunded_count")
        assert refunded_count is not None
        assert refunded_count.filters is not None
        assert len(refunded_count.filters) >= 1

        total_failed = charges.get_metric("total_failed_amount")
        assert total_failed is not None
        assert total_failed.filters is not None
        assert total_failed.format == "currency"

    def test_stripe_charges_derived_net_revenue(self, graph):
        charges = graph.get_model("stripe_charges")
        net_revenue = charges.get_metric("total_net_revenue")
        assert net_revenue is not None
        assert net_revenue.description is not None
        assert net_revenue.format == "currency"
        # Derived measure should be type=derived or have sql referencing other measures
        assert net_revenue.type == "derived" or net_revenue.sql is not None

    def test_saas_metrics_sql_table(self, graph):
        """stripe_saas_metrics uses sql: (not sql_table:)."""
        saas = graph.get_model("stripe_saas_metrics")
        assert saas.table is None
        assert saas.sql is not None

    def test_saas_metrics_composite_primary_key_via_concat(self, graph):
        """Composite PK via SQL concatenation: dd || mrr_type || customer_id."""
        saas = graph.get_model("stripe_saas_metrics")
        id_dim = saas.get_dimension("id")
        assert id_dim is not None
        # The SQL should contain concatenation operators
        assert id_dim.sql is not None
        assert "||" in id_dim.sql

    def test_saas_metrics_rolling_window_measures(self, graph):
        saas = graph.get_model("stripe_saas_metrics")

        mrr = saas.get_metric("mrr")
        assert mrr is not None
        assert mrr.format == "currency"
        # rolling_window makes this cumulative
        assert mrr.type == "cumulative"

        active_customers = saas.get_metric("active_customers")
        assert active_customers is not None
        assert active_customers.type == "cumulative"

    def test_saas_metrics_derived_measures(self, graph):
        saas = graph.get_model("stripe_saas_metrics")

        arr = saas.get_metric("arr")
        assert arr is not None
        assert arr.format == "currency"

        arpa = saas.get_metric("arpa")
        assert arpa is not None
        assert arpa.format == "currency"

        ltv = saas.get_metric("ltv")
        assert ltv is not None
        assert ltv.format == "currency"

    def test_saas_metrics_percent_format(self, graph):
        saas = graph.get_model("stripe_saas_metrics")
        churn_rate = saas.get_metric("mrr_churn_rate")
        assert churn_rate is not None
        assert churn_rate.format == "percent"

    def test_saas_metrics_filtered_measures(self, graph):
        saas = graph.get_model("stripe_saas_metrics")

        churned = saas.get_metric("churned_mrr")
        assert churned is not None
        assert churned.filters is not None

        new_mrr = saas.get_metric("new_mrr")
        assert new_mrr is not None
        assert new_mrr.filters is not None

        expansion = saas.get_metric("expansion_mrr")
        assert expansion is not None
        assert expansion.filters is not None

        contraction = saas.get_metric("contraction_mrr")
        assert contraction is not None
        assert contraction.filters is not None

    def test_saas_metrics_measure_count(self, graph):
        """Verify we parse a reasonable number of measures (15+ in the fixture)."""
        saas = graph.get_model("stripe_saas_metrics")
        assert len(saas.metrics) >= 14


# ---------------------------------------------------------------------------
# Diamond Join Pattern
# ---------------------------------------------------------------------------


class TestDiamondJoin:
    """Diamond join: A -> B -> D, A -> C -> D."""

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "diamond_join.yml")

    def test_parses_without_error(self, graph):
        assert graph is not None

    def test_all_cubes_imported(self, graph):
        assert "a" in graph.models
        assert "b" in graph.models
        assert "c" in graph.models
        assert "d" in graph.models

    def test_cube_a_joins(self, graph):
        a = graph.get_model("a")
        assert len(a.relationships) == 2
        join_names = {r.name for r in a.relationships}
        assert join_names == {"b", "c"}

    def test_cube_a_join_types(self, graph):
        a = graph.get_model("a")
        for rel in a.relationships:
            assert rel.type == "one_to_one"

    def test_cube_b_joins_d(self, graph):
        b = graph.get_model("b")
        assert len(b.relationships) == 1
        assert b.relationships[0].name == "d"
        assert b.relationships[0].type == "one_to_one"

    def test_cube_c_joins_d(self, graph):
        c = graph.get_model("c")
        assert len(c.relationships) == 1
        assert c.relationships[0].name == "d"
        assert c.relationships[0].type == "one_to_one"

    def test_cube_d_standalone(self, graph):
        d = graph.get_model("d")
        assert len(d.relationships) == 0

    def test_path_qualified_dimensions(self, graph):
        """Cube A has dimensions referencing paths through the diamond."""
        a = graph.get_model("a")
        d_via_b = a.get_dimension("d_via_b")
        assert d_via_b is not None
        assert d_via_b.type == "numeric"

        d_via_c = a.get_dimension("d_via_c")
        assert d_via_c is not None
        assert d_via_c.type == "numeric"

    def test_inline_sql(self, graph):
        """All cubes use inline SQL (not sql_table)."""
        for name in ["a", "b", "c", "d"]:
            model = graph.get_model(name)
            assert model.table is None
            assert model.sql is not None
            assert "SELECT" in model.sql


# ---------------------------------------------------------------------------
# Views with includes/excludes/prefix/alias
# ---------------------------------------------------------------------------


class TestViewsIncludesExcludes:
    """Views: includes/excludes/prefix/alias.

    The Cube adapter currently only parses the cubes: section.
    Views are a Cube-specific concept not yet mapped to sidemantic models.
    These tests document that the cubes parse correctly and that the views
    section is silently ignored (permissive parsing).
    """

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "views_includes_excludes.yml")

    def test_parses_without_error(self, graph):
        """File with views: section should parse without crashing."""
        assert graph is not None

    def test_cubes_imported(self, graph):
        assert "base_orders" in graph.models
        assert "line_items" in graph.models
        assert "products" in graph.models
        assert "users" in graph.models

    def test_views_not_imported_as_models(self, graph):
        """Views are not cubes; they should not appear as models."""
        assert "orders_view" not in graph.models
        assert "minimal_orders_view" not in graph.models

    def test_base_orders_dimensions(self, graph):
        orders = graph.get_model("base_orders")
        assert orders.table == "public.orders"
        dim_names = {d.name for d in orders.dimensions}
        assert {"id", "status", "created_date"} <= dim_names

    def test_base_orders_measures(self, graph):
        orders = graph.get_model("base_orders")
        metric_names = {m.name for m in orders.metrics}
        assert {"count", "total_amount", "average_order_value"} <= metric_names

    def test_base_orders_joins(self, graph):
        orders = graph.get_model("base_orders")
        assert len(orders.relationships) == 2
        join_names = {r.name for r in orders.relationships}
        assert join_names == {"line_items", "users"}

    def test_line_items_joins_products(self, graph):
        li = graph.get_model("line_items")
        assert len(li.relationships) == 1
        assert li.relationships[0].name == "products"
        assert li.relationships[0].type == "many_to_one"

    def test_users_model(self, graph):
        users = graph.get_model("users")
        assert users.table == "public.users"
        dim_names = {d.name for d in users.dimensions}
        assert {"id", "name", "city", "company"} <= dim_names


# ---------------------------------------------------------------------------
# Multi-Stage Calculations with time_shift
# ---------------------------------------------------------------------------


class TestMultiStageTimeShift:
    """Multi-stage measures, time_shift, rolling_window to_date, switch dimensions.

    Many of these features (multi_stage, time_shift, group_by, rank, switch)
    are Cube-specific and the adapter maps them best-effort. Tests verify
    parsing does not crash and checks what does get mapped.
    """

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "multi_stage_time_shift.yml")

    def test_parses_without_error(self, graph):
        assert graph is not None

    def test_all_cubes_imported(self, graph):
        assert "calendar_orders" in graph.models
        assert "prior_date" in graph.models
        assert "percent_of_total" in graph.models
        assert "ranking" in graph.models
        assert "orders_with_switch" in graph.models

    def test_calendar_orders_join(self, graph):
        cal = graph.get_model("calendar_orders")
        assert len(cal.relationships) == 1
        assert cal.relationships[0].name == "custom_calendar"
        assert cal.relationships[0].type == "many_to_one"

    def test_calendar_orders_dimensions_with_meta(self, graph):
        """Dimensions with meta: should parse (meta is on the YAML but not mapped)."""
        cal = graph.get_model("calendar_orders")
        status = cal.get_dimension("status")
        assert status is not None
        assert status.type == "categorical"

    def test_calendar_orders_rolling_window(self, graph):
        cal = graph.get_model("calendar_orders")
        total = cal.get_metric("total")
        assert total is not None
        assert total.type == "cumulative"

    def test_calendar_orders_filtered_measure(self, graph):
        cal = graph.get_model("calendar_orders")
        completed = cal.get_metric("completed_count")
        assert completed is not None
        assert completed.filters is not None

    def test_calendar_orders_percent_format(self, graph):
        cal = graph.get_model("calendar_orders")
        pct = cal.get_metric("completed_percentage")
        assert pct is not None
        assert pct.format == "percent"

    def test_prior_date_measures(self, graph):
        pd_model = graph.get_model("prior_date")
        assert pd_model.sql is not None

        revenue = pd_model.get_metric("revenue")
        assert revenue is not None
        assert revenue.agg == "sum"

        # revenue_ytd has rolling_window with type: to_date
        # The adapter treats any rolling_window as cumulative
        revenue_ytd = pd_model.get_metric("revenue_ytd")
        assert revenue_ytd is not None
        assert revenue_ytd.type == "cumulative"

    def test_prior_date_time_shift_measures_parse(self, graph):
        """time_shift measures should parse without error.

        The adapter may not fully map time_shift semantics, but it should
        not crash. These measures have type: number, so they become derived.
        """
        pd_model = graph.get_model("prior_date")
        prior_year = pd_model.get_metric("revenue_prior_year")
        assert prior_year is not None

        prior_year_ytd = pd_model.get_metric("revenue_prior_year_ytd")
        assert prior_year_ytd is not None

    def test_percent_of_total_measures(self, graph):
        pot = graph.get_model("percent_of_total")
        dim_names = {d.name for d in pot.dimensions}
        assert {"product", "country"} <= dim_names

        revenue = pot.get_metric("revenue")
        assert revenue is not None
        assert revenue.format == "currency"

        # country_revenue has group_by (not mapped by adapter, but should parse)
        country_rev = pot.get_metric("country_revenue")
        assert country_rev is not None

    def test_ranking_measures(self, graph):
        rank_model = graph.get_model("ranking")
        revenue = rank_model.get_metric("revenue")
        assert revenue is not None
        assert revenue.format == "currency"

        # product_rank has type: rank (not a standard agg, adapter handles it)
        product_rank = rank_model.get_metric("product_rank")
        assert product_rank is not None

    def test_switch_dimension_parses(self, graph):
        """Switch dimensions (type: switch) should parse without error.

        The adapter maps unknown dimension types to 'categorical'.
        """
        switch_model = graph.get_model("orders_with_switch")
        currency = switch_model.get_dimension("currency")
        assert currency is not None
        # switch is not a recognized type, should fall back to categorical
        assert currency.type == "categorical"

    def test_switch_model_measures(self, graph):
        switch_model = graph.get_model("orders_with_switch")
        assert len(switch_model.metrics) >= 4

        amount_usd = switch_model.get_metric("amount_usd")
        assert amount_usd is not None
        assert amount_usd.agg == "sum"

        # amount_in_currency has case/switch/when (not standard, but should parse)
        aic = switch_model.get_metric("amount_in_currency")
        assert aic is not None


# ---------------------------------------------------------------------------
# Extends (Cube Inheritance) and Hierarchies
# ---------------------------------------------------------------------------


class TestExtendsAndHierarchies:
    """Extends, hierarchies, accessPolicy, count_distinct.

    The adapter parses cubes individually. The extends: field and hierarchies:
    section are Cube-specific; the adapter should not crash on them.
    """

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "extends_and_hierarchies.yml")

    def test_parses_without_error(self, graph):
        assert graph is not None

    def test_all_cubes_imported(self, graph):
        assert "orders_base" in graph.models
        assert "orders_ext" in graph.models
        assert "order_users" in graph.models

    def test_orders_base_dimensions(self, graph):
        base = graph.get_model("orders_base")
        assert base.table == "public.orders"
        dim_names = {d.name for d in base.dimensions}
        assert {"id", "user_id", "status", "created_at", "completed_at"} <= dim_names

    def test_orders_base_join(self, graph):
        base = graph.get_model("orders_base")
        assert len(base.relationships) == 1
        assert base.relationships[0].name == "order_users"
        assert base.relationships[0].type == "many_to_one"

    def test_orders_base_segment(self, graph):
        base = graph.get_model("orders_base")
        assert len(base.segments) == 1
        seg = base.segments[0]
        assert seg.name == "sf_users"
        assert seg.description == "SF users segment"

    def test_orders_base_pre_aggregation(self, graph):
        base = graph.get_model("orders_base")
        assert len(base.pre_aggregations) == 1
        preagg = base.pre_aggregations[0]
        assert preagg.name == "count_created_at"
        assert preagg.type == "rollup"
        assert preagg.granularity == "day"
        assert preagg.partition_granularity == "month"
        assert preagg.refresh_key is not None
        assert preagg.refresh_key.every == "1 hour"
        assert preagg.scheduled_refresh is True

    def test_orders_ext_extends_field(self, graph):
        """The extends: field should be preserved if the adapter reads it."""
        ext = graph.get_model("orders_ext")
        # The adapter does not currently map extends: to the Model,
        # but the cube should still parse. orders_ext won't have a table
        # because it inherits from orders_base (which the adapter doesn't resolve).
        assert ext is not None

    def test_orders_ext_own_dimensions(self, graph):
        ext = graph.get_model("orders_ext")
        city = ext.get_dimension("city")
        assert city is not None
        assert city.type == "categorical"

    def test_orders_ext_count_distinct(self, graph):
        ext = graph.get_model("orders_ext")
        cd = ext.get_metric("count_distinct_status")
        assert cd is not None
        assert cd.agg == "count_distinct"

    def test_orders_ext_joins(self, graph):
        ext = graph.get_model("orders_ext")
        assert len(ext.relationships) >= 1
        join_names = {r.name for r in ext.relationships}
        assert "line_items" in join_names

    def test_orders_ext_segment(self, graph):
        ext = graph.get_model("orders_ext")
        assert len(ext.segments) == 1
        seg = ext.segments[0]
        assert seg.name == "another_status"

    def test_orders_ext_pre_aggregation(self, graph):
        ext = graph.get_model("orders_ext")
        assert len(ext.pre_aggregations) == 1
        preagg = ext.pre_aggregations[0]
        assert preagg.name == "main_pre_aggs"
        assert preagg.type == "rollup"

    def test_order_users_model(self, graph):
        users = graph.get_model("order_users")
        assert users.table == "public.users"
        dim_names = {d.name for d in users.dimensions}
        assert {"id", "name", "city", "state", "age"} <= dim_names

    def test_order_users_measure(self, graph):
        users = graph.get_model("order_users")
        count = users.get_metric("count")
        assert count is not None
        assert count.agg == "count"
