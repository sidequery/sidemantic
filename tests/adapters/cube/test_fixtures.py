"""Tests for Cube adapter with real-world patterns.

Fixtures sourced from:
- Cube.js official Stripe schema: https://github.com/cube-js/stripe-schema
- Cube schema compiler test fixtures: https://github.com/cube-js/cube
- Cube documentation examples: https://cube.dev/docs
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

    def test_drill_members_imported(self, graph):
        """drill_members on measures should map to drill_fields."""
        charges = graph.get_model("stripe_charges")
        count = charges.get_metric("count")
        assert count.drill_fields == ["id", "amount", "paid", "refunded", "created"]

    def test_shown_false(self, graph):
        """shown: false should map to public=False."""
        saas = graph.get_model("stripe_saas_metrics")
        new_subs = saas.get_metric("new_subscriptions")
        assert new_subs is not None
        assert new_subs.public is False

        churned_30d = saas.get_metric("churned_movement_30days")
        assert churned_30d is not None
        assert churned_30d.public is False

        mrr_30d = saas.get_metric("mrr_30days_ago")
        assert mrr_30d is not None
        assert mrr_30d.public is False


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
    """Views: includes/excludes/prefix/alias are resolved into composite Models."""

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

    def test_views_imported_as_models(self, graph):
        """Views are resolved into composite Models."""
        assert "orders_view" in graph.models
        assert "minimal_orders_view" in graph.models

    def test_view_metadata(self, graph):
        """Views have cube_type=view in meta."""
        view = graph.get_model("orders_view")
        assert view.meta == {"cube_type": "view"}

    def test_orders_view_selected_members(self, graph):
        """orders_view includes selected dims/metrics from base_orders."""
        view = graph.get_model("orders_view")
        dim_names = {d.name for d in view.dimensions}
        metric_names = {m.name for m in view.metrics}
        assert {"status", "created_date"} <= dim_names
        assert {"total_amount", "count", "average_order_value"} <= metric_names

    def test_orders_view_alias(self, graph):
        """products.name is aliased to 'product' via alias."""
        view = graph.get_model("orders_view")
        dim_names = {d.name for d in view.dimensions}
        assert "product" in dim_names

    def test_orders_view_prefixed_users(self, graph):
        """users dimensions are prefixed with users_ and company is excluded."""
        view = graph.get_model("orders_view")
        dim_names = {d.name for d in view.dimensions}
        assert "users_id" in dim_names
        assert "users_name" in dim_names
        assert "users_city" in dim_names
        assert "users_company" not in dim_names

    def test_minimal_orders_view(self, graph):
        """minimal_orders_view includes only count and status."""
        view = graph.get_model("minimal_orders_view")
        dim_names = {d.name for d in view.dimensions}
        metric_names = {m.name for m in view.metrics}
        assert "status" in dim_names
        assert "count" in metric_names

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
    """Multi-stage measures, time_shift, rolling_window to_date, switch dimensions."""

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

    def test_calendar_orders_meta_on_dimensions(self, graph):
        """meta: on dimensions is now imported."""
        cal = graph.get_model("calendar_orders")
        status = cal.get_dimension("status")
        assert status is not None
        assert status.type == "categorical"
        assert status.meta == {"addDesc": "The status of order", "moreNum": 42}

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

        revenue_ytd = pd_model.get_metric("revenue_ytd")
        assert revenue_ytd is not None
        assert revenue_ytd.type == "cumulative"

    def test_revenue_ytd_grain_to_date(self, graph):
        """rolling_window with type: to_date maps to grain_to_date."""
        pd_model = graph.get_model("prior_date")
        revenue_ytd = pd_model.get_metric("revenue_ytd")
        assert revenue_ytd.grain_to_date == "year"

    def test_time_shift_maps_to_time_comparison(self, graph):
        """time_shift with type: prior maps to time_comparison metric."""
        pd_model = graph.get_model("prior_date")
        prior_year = pd_model.get_metric("revenue_prior_year")
        assert prior_year is not None
        assert prior_year.type == "time_comparison"
        assert prior_year.comparison_type == "yoy"
        assert prior_year.time_offset == "1 year"
        assert prior_year.base_metric == "prior_date.revenue"

    def test_percent_of_total_measures(self, graph):
        pot = graph.get_model("percent_of_total")
        dim_names = {d.name for d in pot.dimensions}
        assert {"product", "country"} <= dim_names

        revenue = pot.get_metric("revenue")
        assert revenue is not None
        assert revenue.format == "currency"

        country_rev = pot.get_metric("country_revenue")
        assert country_rev is not None

    def test_rank_preserves_metadata(self, graph):
        """type: rank measures store rank metadata and are queryable."""
        rank_model = graph.get_model("ranking")
        product_rank = rank_model.get_metric("product_rank")
        assert product_rank is not None
        # count as executable fallback
        assert product_rank.agg == "count"
        # rank metadata preserved for special handling
        assert product_rank.meta is not None
        assert product_rank.meta.get("cube_type") == "rank"
        assert product_rank.meta.get("order_by") is not None
        assert product_rank.meta.get("reduce_by") is not None

    def test_ranking_measures(self, graph):
        rank_model = graph.get_model("ranking")
        revenue = rank_model.get_metric("revenue")
        assert revenue is not None
        assert revenue.format == "currency"

    def test_switch_dimension_parses(self, graph):
        """Switch dimensions (type: switch) parse as categorical."""
        switch_model = graph.get_model("orders_with_switch")
        currency = switch_model.get_dimension("currency")
        assert currency is not None
        assert currency.type == "categorical"

    def test_switch_dimension_values_stored(self, graph):
        """The switch dimension's values list is preserved in meta."""
        switch_model = graph.get_model("orders_with_switch")
        currency = switch_model.get_dimension("currency")
        assert currency.meta is not None
        assert currency.meta.get("switch_values") == ["USD", "EUR", "GBP"]

    def test_multi_stage_group_by_stored(self, graph):
        """Multi-stage group_by (percent-of-total) is preserved in meta."""
        pot = graph.get_model("percent_of_total")
        country_rev = pot.get_metric("country_revenue")
        assert country_rev is not None
        assert country_rev.meta is not None
        assert country_rev.meta.get("group_by") == ["country"]

    def test_switch_model_measures(self, graph):
        switch_model = graph.get_model("orders_with_switch")
        assert len(switch_model.metrics) >= 4

        amount_usd = switch_model.get_metric("amount_usd")
        assert amount_usd is not None
        assert amount_usd.agg == "sum"

        aic = switch_model.get_metric("amount_in_currency")
        assert aic is not None


# ---------------------------------------------------------------------------
# Extends (Cube Inheritance) and Hierarchies
# ---------------------------------------------------------------------------


class TestExtendsAndHierarchies:
    """Extends resolves inheritance; hierarchies set Dimension.parent chains."""

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

    def test_orders_ext_inherits_table(self, graph):
        """extends: resolves inheritance, so orders_ext gets parent's table."""
        ext = graph.get_model("orders_ext")
        assert ext is not None
        # After inheritance resolution, extends is cleared and table is inherited
        assert ext.table == "public.orders"

    def test_orders_ext_inherits_dimensions(self, graph):
        """orders_ext inherits parent's dims plus its own city."""
        ext = graph.get_model("orders_ext")
        dim_names = {d.name for d in ext.dimensions}
        # From parent: id, user_id, status, created_at, completed_at
        # Own: city
        assert {"id", "user_id", "status", "created_at", "completed_at", "city"} <= dim_names

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

    def test_orders_ext_inherits_measures(self, graph):
        """orders_ext inherits count from parent plus own count_distinct_status."""
        ext = graph.get_model("orders_ext")
        metric_names = {m.name for m in ext.metrics}
        assert "count" in metric_names
        assert "count_distinct_status" in metric_names

    def test_orders_ext_joins(self, graph):
        ext = graph.get_model("orders_ext")
        assert len(ext.relationships) >= 1
        join_names = {r.name for r in ext.relationships}
        assert "line_items" in join_names
        # Also inherits order_users join from parent
        assert "order_users" in join_names

    def test_orders_ext_inherits_segments(self, graph):
        """orders_ext inherits sf_users from parent plus own another_status."""
        ext = graph.get_model("orders_ext")
        seg_names = {s.name for s in ext.segments}
        assert "sf_users" in seg_names
        assert "another_status" in seg_names

    def test_orders_ext_inherits_pre_aggregations(self, graph):
        """orders_ext inherits count_created_at from parent plus own main_pre_aggs."""
        ext = graph.get_model("orders_ext")
        preagg_names = {p.name for p in ext.pre_aggregations}
        assert "count_created_at" in preagg_names
        assert "main_pre_aggs" in preagg_names

    def test_hierarchy_sets_parent(self, graph):
        """Hierarchy levels create Dimension.parent chains."""
        ext = graph.get_model("orders_ext")
        city = ext.get_dimension("city")
        assert city is not None
        # orders_ext hierarchy "ehlo": levels [status, city]
        assert city.parent == "status"

    def test_order_users_hierarchy_parent(self, graph):
        """order_users hierarchy: levels [age, city] -> city.parent = age."""
        users = graph.get_model("order_users")
        city = users.get_dimension("city")
        assert city is not None
        assert city.parent == "age"

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


# ---------------------------------------------------------------------------
# Case/Switch Dimensions
# ---------------------------------------------------------------------------


class TestCaseSwitchDimensions:
    """Case dimensions generate SQL CASE WHEN expressions."""

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "case_switch_ownership.yaml")

    def test_parses_without_error(self, graph):
        assert graph is not None

    def test_case_dimension_generates_sql(self, graph):
        """case/when/else blocks are converted to SQL CASE expressions."""
        users = graph.get_model("users")
        owned_case = users.get_dimension("ownedCase")
        assert owned_case is not None
        assert owned_case.sql is not None
        assert "CASE" in owned_case.sql
        assert "WHEN" in owned_case.sql
        assert "Admin" in owned_case.sql
        assert "User" in owned_case.sql
        assert "ELSE" in owned_case.sql
        assert "Unknown" in owned_case.sql

    def test_case_dimension_with_cross_cube_ref(self, graph):
        """Case dimensions with cross-cube references preserve the references."""
        users = graph.get_model("users")
        not_owned = users.get_dimension("notOwnedCase")
        assert not_owned is not None
        assert not_owned.sql is not None
        assert "CASE" in not_owned.sql

    def test_views_from_case_fixture(self, graph):
        """Views in case_switch_ownership.yaml are parsed."""
        assert "users_to_orders" in graph.models
        view = graph.get_model("users_to_orders")
        assert view.meta == {"cube_type": "view"}


# ---------------------------------------------------------------------------
# Switch Dimension (standalone fixture)
# ---------------------------------------------------------------------------


class TestSwitchDimension:
    """type: switch dimension with a values list (enum-like)."""

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "switch_dimension.yml")

    def test_parses_without_error(self, graph):
        assert graph is not None
        assert "orders" in graph.models

    def test_switch_dimension_is_categorical(self, graph):
        orders = graph.get_model("orders")
        currency = orders.get_dimension("currency")
        assert currency is not None
        assert currency.type == "categorical"

    def test_switch_values_preserved(self, graph):
        orders = graph.get_model("orders")
        currency = orders.get_dimension("currency")
        assert currency.meta is not None
        assert currency.meta.get("switch_values") == ["USD", "EUR", "GBP"]

    def test_non_switch_dimensions_have_no_switch_values(self, graph):
        orders = graph.get_model("orders")
        status = orders.get_dimension("status")
        assert status is not None
        # status is a plain string dimension; no switch metadata added
        assert not (status.meta and "switch_values" in status.meta)


# ---------------------------------------------------------------------------
# Folders, View Extends (standalone fixture)
# ---------------------------------------------------------------------------


class TestViewFoldersAndExtends:
    """View folders (nested member grouping) and view-level extends."""

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "folders.yml")

    def test_all_views_imported(self, graph):
        for view_name in ("test_view", "test_view2", "test_view3", "test_view4"):
            assert view_name in graph.models, f"missing {view_name}"
            assert graph.get_model(view_name).meta.get("cube_type") == "view"

    def test_view_folders_stored(self, graph):
        view = graph.get_model("test_view")
        folders = view.meta.get("folders")
        assert folders is not None
        names = [f.get("name") for f in folders]
        assert names == ["folder1", "folder2"]

    def test_nested_folder_structure_preserved(self, graph):
        """Nested folders (folder containing sub-folders) parse verbatim."""
        view = graph.get_model("test_view4")
        folders = view.meta.get("folders")
        folder3 = next(f for f in folders if f.get("name") == "folder3")
        # folder3 has nested folder dicts inside its includes
        nested = [inc for inc in folder3["includes"] if isinstance(inc, dict)]
        nested_names = {inc["name"] for inc in nested}
        assert {"inner folder 4", "inner folder 5"} <= nested_names

    def test_view_extends_inherits_members(self, graph):
        """A view extending another inherits its projected members."""
        parent = graph.get_model("test_view3")
        child = graph.get_model("test_view4")
        # test_view4 only adds a folder; all members come from test_view3
        parent_dim_names = {d.name for d in parent.dimensions}
        child_dim_names = {d.name for d in child.dimensions}
        assert parent_dim_names <= child_dim_names

    def test_view_extends_inherits_folders(self, graph):
        """Extending a view also inherits its folders (parent first)."""
        child = graph.get_model("test_view4")
        folder_names = [f.get("name") for f in child.meta.get("folders")]
        # folder1 from test_view2, folder2 from test_view3, folder3 from test_view4
        assert folder_names == ["folder1", "folder2", "folder3"]


# ---------------------------------------------------------------------------
# Tesseract-era measure/dimension params and view metadata
# ---------------------------------------------------------------------------


class TestTesseractFeatures:
    """number_agg / non-numeric measures, mask, currency, view_groups, default filters."""

    @pytest.fixture()
    def graph(self):
        adapter = CubeAdapter()
        return adapter.parse(FIXTURES_DIR / "tesseract_features.yml")

    def test_parses_without_error(self, graph):
        assert graph is not None
        assert "orders" in graph.models

    def test_number_agg_measure(self, graph):
        """number_agg measures keep the full SQL aggregate; agg stays None."""
        orders = graph.get_model("orders")
        p95 = orders.get_metric("amount_p95")
        assert p95 is not None
        assert p95.agg is None
        assert p95.type is None
        assert p95.sql is not None
        assert "PERCENTILE_CONT" in p95.sql
        assert p95.meta.get("cube_type") == "number_agg"

    def test_non_numeric_measure_types(self, graph):
        """string / time / boolean measures preserve sql and record cube_type."""
        orders = graph.get_model("orders")
        for mname, ctype in (("last_status", "string"), ("last_order_time", "time"), ("any_completed", "boolean")):
            m = orders.get_metric(mname)
            assert m is not None, mname
            assert m.agg is None, mname
            assert m.meta.get("cube_type") == ctype
            assert m.sql is not None

    def test_aggregate_expression_measures_preserved_verbatim(self, graph):
        """Measures whose sql is itself a top-level aggregate must not be re-parsed.

        Without sql_is_complete, Metric.handle_expr_and_parse_agg would extract the
        aggregation (agg=max) and rewrite the sql, corrupting the {model} placeholder
        (e.g. into "{'model': model}.created_at"). The import must stay lossless.
        """
        orders = graph.get_model("orders")

        latest = orders.get_metric("latest_order_at")
        assert latest is not None
        assert latest.agg is None
        assert latest.type is None
        assert latest.meta.get("cube_type") == "time"
        # sql preserved verbatim with the {model} placeholder intact
        assert latest.sql == "MAX({model}.created_at)"

        total_agg = orders.get_metric("amount_total_agg")
        assert total_agg is not None
        assert total_agg.agg is None
        assert total_agg.type is None
        assert total_agg.meta.get("cube_type") == "number_agg"
        assert total_agg.sql == "SUM({model}.amount)"

    def test_complete_sql_measures_have_no_dependencies(self, graph):
        """sql_is_complete measures are opaque - never dependency-expanded.

        A plain-column measure like ``last_status`` (sql: status) must NOT treat
        ``status`` as a metric reference, otherwise compilation raises
        "Metric status not found".
        """
        orders = graph.get_model("orders")
        for mname in ("last_status", "last_order_time", "amount_p95", "latest_order_at"):
            m = orders.get_metric(mname)
            assert m.sql_is_complete is True, mname
            assert m.get_dependencies(graph, "orders") == set(), mname

    def test_aggregate_complete_measures_queryable(self, graph):
        """number_agg / aggregate string|time|boolean measures compile and execute.

        Regression: the {model} placeholder must not be collected as a real column
        named "model" in the CTE (which produced "SELECT amount AS amount, model AS
        model FROM public.orders" and failed on tables without a model column).
        """
        import duckdb

        from sidemantic import SemanticLayer
        from sidemantic.sql.generator import SQLGenerator

        gen = SQLGenerator(graph)
        con = duckdb.connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS public")
        con.execute(
            "CREATE TABLE public.orders AS SELECT * FROM (VALUES "
            "(1,'completed',100.0,TIMESTAMP '2024-01-01'),"
            "(2,'pending',50.0,TIMESTAMP '2024-02-01'),"
            "(3,'completed',200.0,TIMESTAMP '2024-03-01')) "
            "t(id,status,amount,created_at)"
        )

        for mref in ("orders.amount_p95", "orders.latest_order_at", "orders.any_completed", "orders.amount_total_agg"):
            sql = gen.generate(metrics=[mref]).split("-- sidemantic")[0]
            assert "model AS model" not in sql, mref
            # Must execute against a real table that has no "model" column.
            con.execute(sql).fetchall()

        # Also exercise the public SemanticLayer.compile path used by the CLI.
        layer = SemanticLayer()
        layer.add_model(graph.get_model("orders"))
        layer.compile(metrics=["orders.amount_total_agg"])

    def test_plain_non_numeric_measures_queryable(self, graph):
        """type: string / time measures with plain-column sql are queryable.

        Regression: these import as type=None, agg=None, sql_is_complete=True and
        previously raised "Metric status not found" during dependency expansion.
        """
        import duckdb

        from sidemantic.sql.generator import SQLGenerator

        gen = SQLGenerator(graph)
        con = duckdb.connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS public")
        con.execute(
            "CREATE TABLE public.orders AS SELECT * FROM (VALUES "
            "(1,'completed',TIMESTAMP '2024-01-01'),"
            "(2,'pending',TIMESTAMP '2024-02-01')) t(id,status,created_at)"
        )

        # Ungrouped: plain column projected directly.
        for mref in ("orders.last_status", "orders.last_order_time"):
            sql = gen.generate(metrics=[mref], ungrouped=True).split("-- sidemantic")[0]
            con.execute(sql).fetchall()

        # Grouped by the same dimension also compiles and executes.
        sql = gen.generate(metrics=["orders.last_status"], dimensions=["orders.status"]).split("-- sidemantic")[0]
        con.execute(sql).fetchall()

    def test_measure_mask_and_currency(self, graph):
        orders = graph.get_model("orders")
        total = orders.get_metric("total_amount")
        assert total is not None
        assert total.agg == "sum"
        assert total.meta.get("mask") == 0
        assert total.meta.get("currency") == "USD"

    def test_dimension_mask(self, graph):
        orders = graph.get_model("orders")
        ssn = orders.get_dimension("ssn")
        assert ssn is not None
        assert ssn.meta is not None
        assert "mask" in ssn.meta

    def test_dimension_currency(self, graph):
        orders = graph.get_model("orders")
        amount = orders.get_dimension("amount")
        assert amount is not None
        assert amount.meta.get("currency") == "USD"

    def test_view_folders(self, graph):
        view = graph.get_model("orders_view")
        names = [f.get("name") for f in view.meta.get("folders")]
        assert names == ["Identifiers", "Financials"]

    def test_view_default_filters(self, graph):
        view = graph.get_model("orders_view")
        df = view.meta.get("default_filters")
        assert df is not None
        assert df[0]["member"] == "status"
        assert df[0]["operator"] == "equals"

    def test_view_default_ui_filters(self, graph):
        view = graph.get_model("orders_view")
        duf = view.meta.get("default_ui_filters")
        assert duf is not None
        assert duf[0]["member"] == "status"

    def test_view_extends_appends_folders(self, graph):
        view = graph.get_model("orders_view_extended")
        names = [f.get("name") for f in view.meta.get("folders")]
        # inherited Identifiers/Financials plus own Extra
        assert names == ["Identifiers", "Financials", "Extra"]

    def test_view_groups_preserved(self, graph):
        groups = graph.metadata.get("cube_view_groups")
        assert groups is not None
        assert groups[0]["name"] == "Sales"
        assert "orders_view" in groups[0]["views"]
