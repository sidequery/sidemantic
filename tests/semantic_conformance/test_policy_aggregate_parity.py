"""Independent population contracts for policies on cross-model aggregates."""

from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import SecurityError

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Population acceptance requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, auto_register=False, enforce_visibility=True)
    try:
        layer.graph = SidemanticAdapter().parse(FIXTURES / "policy_aggregate.yml")
        layer.adapter.execute((FIXTURES / "policy_aggregate.sql").read_text())
        yield layer
    finally:
        layer.adapter.close()


def assert_result(layer, query, columns, rows):
    result = layer.adapter.execute(layer.compile(**query))
    assert [column[0] for column in result.description] == columns
    assert result.fetchall() == rows


@pytest.mark.parametrize("tenant,ratio,derived", [(1, 30.0, 174), (2, 100.0, 990)])
def test_policy_and_invariants_constrain_both_metric_populations(layer, tenant, ratio, derived):
    # Tenant 1: (100+20+60)/(2+4)=30, with quota counted once per account.
    # Tenant 2: 1000/10=100. Deleted purchases and inactive accounts never count.
    assert_result(
        layer,
        {"metrics": ["revenue_per_quota", "net_of_quota"], "user_attributes": {"tenant": tenant}},
        ["revenue_per_quota", "net_of_quota"],
        [(ratio, derived)],
    )


def test_grouped_policy_aggregate_keeps_each_tier_population(layer):
    assert_result(
        layer,
        {
            "metrics": ["revenue_per_quota", "net_of_quota"],
            "dimensions": ["accounts.tier"],
            "order_by": ["accounts.tier"],
            "user_attributes": {"tenant": 1},
        },
        ["tier", "revenue_per_quota", "net_of_quota"],
        [("business", 15.0, 56), ("retail", 60.0, 118)],
    )


def test_joined_invariant_and_policy_scope_purchase_revenue(layer):
    # Follows test_row_filter_scopes_rows_end_to_end: selecting the account
    # dimension scopes purchases through authorized account rows.
    assert_result(
        layer,
        {
            "metrics": ["purchases.revenue"],
            "dimensions": ["accounts.tier"],
            "order_by": ["accounts.tier"],
            "user_attributes": {"tenant": 1},
        },
        ["tier", "revenue"],
        [("business", 60), ("retail", 120)],
    )


def test_account_invariant_excludes_inactive_denominator_rows(layer):
    assert_result(
        layer,
        {"metrics": ["accounts.quota", "accounts.account_count"], "user_attributes": {"tenant": 1}},
        ["quota", "account_count"],
        [(6, 2)],
    )


@pytest.mark.parametrize("attributes", [None, {}])
def test_derived_dependencies_require_policy_context(layer, attributes):
    with pytest.raises(SecurityError):
        layer.compile(metrics=["net_of_quota"], user_attributes=attributes)


@pytest.mark.parametrize(
    "query",
    [
        {"metrics": ["purchases.secret_cost"]},
        {"metrics": ["purchases.revenue"], "dimensions": ["purchases.cost"]},
        {"metrics": ["purchases.revenue"], "filters": ["purchases.cost > 0"]},
        {"metrics": ["purchases.revenue"], "order_by": ["purchases.secret_cost"]},
    ],
    ids=["metric", "dimension", "filter", "order"],
)
def test_visibility_denies_hidden_fields_in_every_query_position(layer, query):
    # Existing visibility enforcement applies to filters and ordering too;
    # neither query position is a way to access a hidden value indirectly.
    with pytest.raises(SecurityError, match="not public"):
        layer.compile(**query, user_attributes={"tenant": 1})
