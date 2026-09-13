"""Nested SQL boundaries use the installed Rust compiler without fallback."""

from pathlib import Path

import pytest

from sidemantic import SecurityPolicy, SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.core.transport_security import rewrite_transport_sql
from sidemantic.rust_bridge import rewrite_semantic_input
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def layer():
    rust = pytest.importorskip("sidemantic_rs", reason="Nested policy SQL requires the installed Rust extension")
    assert callable(rust.rewrite_with_semantic_input_context)
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False, enforce_visibility=True)
    layer.graph = SidemanticAdapter().parse(FIXTURES / "policy_aggregate.yml")
    layer.adapter.execute((FIXTURES / "policy_aggregate.sql").read_text())
    try:
        yield layer
    finally:
        layer.adapter.close()


def rows(layer, sql, *, tenant=1):
    generated = rewrite_transport_sql(
        layer, sql, user_attributes={"tenant": tenant}, transport="nested policy acceptance"
    )
    assert layer.last_engine_selection["engine"] == "rust"
    return layer.adapter.execute(generated).fetchall()


def test_outer_filter_projection_order_and_pagination_follow_policies(layer):
    # Only purchases 1/2/3 survive tenant, account-active and deleted predicates.
    assert rows(
        layer,
        """
        select s.purchase, s.total * 2 as doubled
        from (select purchases.id as purchase, purchases.revenue as total from metrics) as s
        where s.total >= 60 order by doubled desc limit 1 offset 1
        """,
    ) == [(3, 120)]


@pytest.mark.parametrize("tenant,expected", [(1, [(180,)]), (2, [(1000,)]), (3, [(None,)])])
def test_each_caller_gets_only_its_authorized_nested_population(layer, tenant, expected):
    assert (
        rows(
            layer,
            "select q.total from (select purchases.revenue as total from metrics) q",
            tenant=tenant,
        )
        == expected
    )


def test_chained_ctes_and_column_aliases_preserve_outer_aggregation(layer):
    assert rows(
        layer,
        """
        with visible(purchase, total) as (
            select purchases.id, purchases.revenue from metrics
        ), summarized as (
            select sum(total) as total, count(*) as n from visible where purchase > 1
        )
        select total, n from summarized
        """,
    ) == [(80, 2)]


@pytest.mark.parametrize("outer_source,expected", [("visible", [(180,)]), ("nested", [(6,)])])
def test_nested_cte_shadowing_is_scoped(layer, outer_source, expected):
    assert (
        rows(
            layer,
            f"""
        with visible as (select purchases.revenue as total from metrics),
        nested as (
            with visible as (select accounts.quota as total from metrics)
            select visible.total from visible
        )
        select total from {outer_source}
        """,
        )
        == expected
    )


@pytest.mark.parametrize("name", ["metrics", '"MeTrIcS"', "purchases", "accounts_cte", '"Visible Values"'])
def test_cte_binding_takes_precedence_over_semantic_names(layer, name):
    assert rows(
        layer,
        f"with {name} as (select accounts.quota as total from metrics) select {name}.total from {name}",
    ) == [(6,)]


def test_cte_named_like_physical_source_cannot_capture_compiler_read(layer):
    assert rows(
        layer,
        """
        with aggregate_purchases as (select accounts.quota as total from metrics)
        select purchases.revenue from metrics
        """,
    ) == [(180,)]


def test_internal_cte_names_do_not_collide_with_trusted_sql_sources(layer):
    layer.adapter.execute("alter table aggregate_purchases rename to __sidemantic_input_cte_0")
    purchases = layer.graph.models["purchases"]
    purchases.table = None
    purchases.sql = "select * from __sidemantic_input_cte_0"
    assert rows(
        layer,
        """
        with visible as (select accounts.quota as total from metrics)
        select purchases.revenue from metrics
        """,
    ) == [(180,)]


@pytest.mark.parametrize("policy_kind", ["security", "invariant"])
def test_user_cte_cannot_capture_private_policy_entitlement_source(layer, policy_kind):
    # The first candidate is reserved by the input alias. The next candidate
    # names a real entitlement source declared only in the private policy map.
    layer.adapter.execute("create table __sidemantic_input_cte_1(tenant integer)")
    layer.adapter.execute("insert into __sidemantic_input_cte_1 values (1)")
    accounts = layer.graph.models["accounts"]
    predicate = "tenant in (select tenant from __sidemantic_input_cte_1)"
    accounts.security = SecurityPolicy(row_filters=[predicate]) if policy_kind == "security" else None
    if policy_kind == "invariant":
        accounts.invariant_filters.append(predicate)
    sql = """
        with hostile as (
            select purchases.id as tenant from metrics
        )
        select purchases.revenue as __sidemantic_input_cte_0 from metrics
    """
    # The hostile CTE includes purchase ID 2, which would authorize tenant 2
    # if it captured the entitlement table. Its real policy permits only 1.
    generated = rewrite_semantic_input(layer.graph, sql, user_attributes={}, enforce_visibility=True)
    assert layer.adapter.execute(generated).fetchall() == [(180,)]


@pytest.mark.parametrize(
    "leaf",
    [
        "select purchases.secret_cost from metrics",
        "select purchases.revenue from metrics where purchases.cost > 0",
        "select purchases.revenue from metrics order by purchases.secret_cost",
        "select purchases.revenue as total from metrics order by purchases.cost",
    ],
)
def test_hidden_fields_remain_denied_in_nested_leaves(layer, leaf):
    with pytest.raises(SecurityError):
        rows(layer, f"select * from ({leaf}) as q")


@pytest.mark.parametrize("unused", [False, True])
def test_every_semantic_leaf_checks_model_access_including_unused_ctes(layer, unused):
    layer.graph.models["purchases"].security = SecurityPolicy(access=False)
    sql = (
        "with denied as (select purchases.revenue from metrics) select accounts.quota from metrics"
        if unused
        else "select * from (select purchases.revenue from metrics) q"
    )
    with pytest.raises(SecurityError):
        rows(layer, sql)


@pytest.mark.parametrize(
    "sql",
    [
        "select * from aggregate_purchases",
        "select purchases.revenue from purchases",
        "select purchases.revenue from main.metrics",
        "select * from (select * from aggregate_purchases) q",
        "with visible as (select * from aggregate_purchases) select * from visible",
        "select purchases.revenue, (select max(amount) from aggregate_purchases) from metrics",
        "select * from (select purchases.revenue from metrics) q where exists (select 1 from aggregate_purchases)",
        "select sum((select max(amount) from aggregate_purchases)) from (select purchases.revenue from metrics) q",
        "select coalesce((select max(amount) from aggregate_purchases), 0) from (select purchases.revenue from metrics) q",
        "select case when q.revenue > 0 then (select max(amount) from aggregate_purchases) else 0 end "
        "from (select purchases.revenue from metrics) q",
        "select * from (select purchases.revenue from metrics) q "
        "where coalesce((select max(amount) from aggregate_purchases), 0) > 0",
        "select * from (select purchases.revenue from metrics) q "
        "where case when q.revenue > 0 then (select max(amount) from aggregate_purchases) else 0 end > 0",
        "select sum(q.revenue) from (select purchases.revenue from metrics) q "
        "having sum((select max(amount) from aggregate_purchases)) > 0",
        "select * from (select purchases.revenue from metrics) q "
        "order by coalesce((select max(amount) from aggregate_purchases), q.revenue)",
        "select * from read_parquet('private.parquet')",
        "delete from aggregate_purchases",
        "create table leaked as select purchases.revenue from metrics",
        "select purchases.revenue into leaked from metrics",
        "select purchases.revenue from metrics; select purchases.revenue from metrics",
        "with recursive q as (select purchases.revenue from metrics) select * from q",
        "select purchases.revenue from metrics union all select purchases.revenue from metrics",
        "select * from (select purchases.revenue from metrics) a join (select accounts.quota from metrics) b on true",
    ],
)
def test_native_context_boundary_rejects_unsecured_or_unqualified_shapes(layer, sql):
    # Call Rust directly so Python's transport preflight cannot mask a bypass.
    with pytest.raises(UnsupportedSemanticFeaturesError, match=r"rewrite\.(policy|scoped)_select_shape"):
        rewrite_semantic_input(layer.graph, sql, user_attributes={"tenant": 1}, enforce_visibility=True)
