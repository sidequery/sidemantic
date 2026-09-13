"""Positive migration contracts for policies, roles, and time calculations.

These synthetic cases require real compiler results. Rust feature rejection is
a failure here, unlike the explicitly unsupported baseline corpus.
"""

from datetime import date, datetime
from pathlib import Path

import pytest

from sidemantic import SecurityPolicy, SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.validation import QueryValidationError

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(params=["python", "rust"])
def engine(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Migration acceptance requires the real Rust extension")
    return request.param


@pytest.fixture
def layer_for(engine):
    layers = []

    def create(source):
        layer = SemanticLayer(engine=engine, auto_register=False)
        layer.graph = SidemanticAdapter().parse(FIXTURES / f"{source}.yml")
        layer.adapter.execute((FIXTURES / "migration_seed.sql").read_text())
        layers.append(layer)
        return layer

    yield create
    for layer in layers:
        layer.adapter.close()


def assert_result(layer, query, columns, expected):
    result = layer.adapter.execute(layer.compile(**query))
    assert [column[0] for column in result.description] == columns
    rows = [
        tuple(value.isoformat() if isinstance(value, (date, datetime)) else value for value in row)
        for row in result.fetchall()
    ]
    assert rows == expected


@pytest.mark.parametrize("model", ["quoted", "unquoted"])
@pytest.mark.parametrize("subject,expected", [("alice", 10), ("O'Brien", 9), ("x' OR '1'='1", 7)])
def test_policy_values_remain_sql_literals(layer_for, model, subject, expected):
    layer = layer_for("policy_literals")
    assert_result(
        layer,
        {"metrics": [f"{model}.total"], "user_attributes": {"subject": subject}},
        ["total"],
        [(expected,)],
    )


@pytest.mark.parametrize("model", ["quoted", "unquoted"])
@pytest.mark.parametrize("attributes", [None, {}])
def test_missing_policy_attribute_denies(layer_for, model, attributes):
    layer = layer_for("policy_literals")
    with pytest.raises(SecurityError):
        layer.compile(metrics=[f"{model}.total"], user_attributes=attributes)


def test_empty_context_is_provided_but_missing_context_denies(layer_for):
    layer = layer_for("policy_literals")
    with pytest.raises(SecurityError):
        layer.compile(metrics=["allowed.total"])
    assert_result(layer, {"metrics": ["allowed.total"], "user_attributes": {}}, ["total"], [(46,)])


@pytest.mark.parametrize("model,attributes", [("denied", {}), ("admin_only", {"role": "viewer"})])
def test_access_denial_precedes_sql(layer_for, model, attributes):
    layer = layer_for("policy_literals")
    with pytest.raises(SecurityError):
        layer.compile(metrics=[f"{model}.total"], user_attributes=attributes)


def test_access_expression_allows_authorized_user(layer_for):
    layer = layer_for("policy_literals")
    assert_result(layer, {"metrics": ["admin_only.total"], "user_attributes": {"role": "admin"}}, ["total"], [(46,)])


def test_joined_policy_scopes_purchases_to_authorized_accounts(layer_for):
    layer = layer_for("policy_join")
    # Match test_row_filter_scopes_rows_end_to_end in
    # tests/core/test_security_enforcement.py: requesting a joined model's
    # dimension applies its policy to the base rows through the join. Purchases
    # without an authorized account must not survive with a null account label.
    assert_result(
        layer,
        {
            "metrics": ["purchases.total"],
            "dimensions": ["purchases.id", "accounts.label"],
            "order_by": ["purchases.id"],
            "user_attributes": {"tenant": 1},
        },
        ["id", "label", "total"],
        [(1, "visible", 10)],
    )


def test_explicit_left_join_preserves_purchases_with_filtered_account_details(layer_for):
    layer = layer_for("policy_join")
    layer.graph.models["purchases"].relationships[0].metadata = {"bsl_how": "left"}
    assert_result(
        layer,
        {
            "metrics": ["purchases.total"],
            "dimensions": ["purchases.id", "accounts.label"],
            "order_by": ["purchases.id"],
            "user_attributes": {"tenant": 1},
        },
        ["id", "label", "total"],
        [(1, "visible", 10), (2, None, 20), (3, None, 30), (4, None, 40)],
    )


def test_two_roles_preserve_unmatched_rows_and_exclude_inactive_join(layer_for):
    layer = layer_for("migration_roles")
    assert_result(
        layer,
        {
            "metrics": ["journeys.journey_count"],
            "dimensions": ["journeys.id", "origin.city", "destination.city"],
            "order_by": ["journeys.id"],
        },
        ["id", "origin_city", "destination_city", "journey_count"],
        [(10, "SFO", "LAX", 1), (11, "SFO", "JFK", 1), (12, "LAX", None, 1)],
    )


def test_inactive_relationship_cannot_be_requested(layer_for):
    layer = layer_for("migration_roles")
    with pytest.raises(QueryValidationError):
        layer.compile(metrics=["journeys.journey_count"], dimensions=["archived.city"])


def test_sparse_comparison_uses_calendar_month_and_category(layer_for):
    layer = layer_for("migration_time")
    assert_result(
        layer,
        {
            "metrics": ["month_change"],
            "dimensions": ["sales.day__month", "sales.category"],
            "order_by": ["sales.category", "sales.day__month"],
        },
        ["day__month", "category", "revenue", "month_change"],
        [
            ("2024-01-01", "a", 100, None),
            ("2024-03-01", "a", 180, None),
            ("2024-04-01", "a", 210, 30),
            ("2024-01-01", "b", 10, None),
            ("2024-03-01", "b", 30, None),
            ("2024-04-01", "b", 50, 20),
        ],
    )


def test_named_calendar_comparison_without_declared_granularity(layer_for):
    layer = layer_for("migration_time")
    layer.graph.models["sales"].dimensions[0].granularity = None
    assert_result(
        layer,
        {
            "metrics": ["month_change"],
            "dimensions": ["sales.day", "sales.category"],
            "order_by": ["sales.category", "sales.day"],
        },
        ["day", "category", "revenue", "month_change"],
        [
            ("2024-01-01", "a", 100, None),
            ("2024-03-01", "a", 180, None),
            ("2024-04-01", "a", 210, 30),
            ("2024-01-01", "b", 10, None),
            ("2024-03-01", "b", 30, None),
            ("2024-04-01", "b", 50, 20),
        ],
    )


def test_rolling_sum_partitions_categories(layer_for):
    layer = layer_for("migration_time")
    assert_result(
        layer,
        {
            "metrics": ["rolling_revenue"],
            "dimensions": ["sales.day__month", "sales.category"],
            "order_by": ["sales.category", "sales.day__month"],
        },
        ["day__month", "category", "revenue", "rolling_revenue"],
        [
            ("2024-01-01", "a", 100, 100),
            ("2024-03-01", "a", 180, 280),
            ("2024-04-01", "a", 210, 390),
            ("2024-01-01", "b", 10, 10),
            ("2024-03-01", "b", 30, 40),
            ("2024-04-01", "b", 50, 80),
        ],
    )


def test_policy_survives_temporal_child_queries(layer_for):
    layer = layer_for("migration_time")
    layer.graph.models["sales"].security = SecurityPolicy(row_filters=["category = {{ user.category }}"])
    assert_result(
        layer,
        {
            "metrics": ["rolling_revenue", "month_change"],
            "dimensions": ["sales.day__month"],
            "order_by": ["sales.day__month"],
            "user_attributes": {"category": "a"},
        },
        ["day__month", "revenue", "rolling_revenue", "month_change"],
        [
            ("2024-01-01", 100, 100, None),
            ("2024-03-01", 180, 280, None),
            ("2024-04-01", 210, 390, 30),
        ],
    )
