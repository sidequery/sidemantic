"""Focused tests for consumption-contract compilation and validation."""

import pytest

from sidemantic import (
    Dimension,
    Explore,
    Metric,
    Model,
    Parameter,
    Relationship,
    SavedQuery,
    Segment,
    SemanticLayer,
)
from sidemantic.core.consumption import expression_field_references, qualify_expression_fields
from sidemantic.validation import validate_explore, validate_saved_query


def _layer() -> SemanticLayer:
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            owner="analytics",
            domain="commerce",
            category="sales",
            tags=["tier-1", "finance"],
            status="active",
            certification="certified",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="created_at", type="time", granularity="day"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount", owner="finance"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )
    layer.graph.add_explore(
        Explore(
            name="revenue_overview",
            model="orders",
            label="Revenue overview",
            allowed_dimensions=["status", "created_at__month"],
            allowed_metrics=["revenue", "order_count"],
            allowed_filter_fields=["status"],
            allowed_order_by=["revenue"],
            default_dimensions=["status"],
            default_metrics=["revenue"],
            filters=["orders.status != 'deleted'"],
            default_filters=["orders.status = 'paid'"],
            default_order_by=["orders.revenue DESC"],
            default_limit=25,
            max_limit=100,
            owner="analytics",
            domain="commerce",
            certification="verified",
        )
    )
    layer.graph.add_saved_query(
        SavedQuery(
            name="paid_revenue",
            explore="revenue_overview",
            dimensions=["status"],
            metrics=["revenue"],
            filters=["orders.status = 'paid'"],
            order_by=["orders.revenue DESC"],
            limit=10,
        )
    )
    return layer


def test_explore_defaults_compile_and_mandatory_filters_apply():
    sql = _layer().compile(explore="revenue_overview")

    assert "SUM(orders_cte.revenue_raw)" in sql
    assert "status <> 'deleted'" in sql
    assert "status = 'paid'" in sql
    assert "ORDER BY" in sql
    assert "LIMIT 25" in sql


def test_explore_qualifies_relative_filter_and_order_expressions():
    layer = _layer()
    layer.graph.add_explore(
        Explore(
            name="relative_contract",
            model="orders",
            default_metrics=["revenue"],
            filters=["status != 'deleted'"],
            default_filters=["status = 'paid'"],
            default_order_by=["revenue DESC"],
        )
    )

    sql = layer.compile(explore="relative_contract")

    assert "status <> 'deleted'" in sql
    assert "status = 'paid'" in sql
    assert "status AS status" in sql
    assert "revenue DESC" in sql


def test_explore_filter_qualification_skips_subquery_columns():
    expression = "status IN (SELECT status FROM allowed_statuses)"

    assert qualify_expression_fields([expression], "orders") == [
        "orders.status IN (SELECT status FROM allowed_statuses)"
    ]
    assert expression_field_references([expression], "orders") == {"orders.status"}

    correlated = "EXISTS (SELECT 1 FROM allowed_statuses AS allowed WHERE allowed.status = orders.status)"
    assert expression_field_references([correlated], "orders", graph_models={"orders"}) == {"orders.status"}


def test_explore_queries_remain_anchored_to_the_base_model():
    layer = _layer()
    layer.graph.models["orders"].relationships.append(
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id")
    )
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="customer_id",
            dimensions=[Dimension(name="region", type="categorical")],
        )
    )
    layer.graph.add_explore(
        Explore(
            name="orders_by_customer",
            model="orders",
            allowed_dimensions=["customers.region"],
        )
    )

    sql = layer.compile(explore="orders_by_customer", dimensions=["customers.region"])

    assert "FROM orders_cte" in sql
    assert "JOIN customers_cte" in sql


def test_explore_enforces_allowlists_and_max_limit():
    layer = _layer()

    with pytest.raises(ValueError, match="does not allow dimension"):
        layer.compile(explore="revenue_overview", dimensions=["orders.order_id"])
    with pytest.raises(ValueError, match="does not allow filter field"):
        layer.compile(explore="revenue_overview", filters=["orders.created_at > '2026-01-01'"])
    with pytest.raises(ValueError, match="does not allow filter field"):
        layer.compile(
            explore="revenue_overview",
            filters=["EXISTS (SELECT 1 WHERE orders.created_at > '2026-01-01')"],
        )
    with pytest.raises(ValueError, match="does not allow ordering"):
        layer.compile(explore="revenue_overview", order_by=["orders.status"])
    with pytest.raises(ValueError, match="exceeds max_limit"):
        layer.compile(explore="revenue_overview", limit=101)

    layer.graph.add_explore(Explore(name="choose_revenue", model="orders", allowed_metrics=["revenue"]))
    with pytest.raises(ValueError, match="must select at least one metric or dimension"):
        layer.compile(explore="choose_revenue")
    assert "SUM" in layer.compile(explore="choose_revenue", metrics=["revenue"])


def test_saved_query_is_immutable_and_compiles_through_its_explore():
    layer = _layer()

    sql = layer.compile(saved_query="paid_revenue")
    assert "LIMIT 10" in sql
    assert "status <> 'deleted'" in sql
    with pytest.raises(ValueError, match="immutable"):
        layer.compile(saved_query="paid_revenue", metrics=["orders.order_count"])
    with pytest.raises(ValueError, match="offset"):
        layer.compile(saved_query="paid_revenue", offset=5)
    with pytest.raises(ValueError, match="ungrouped"):
        layer.compile(saved_query="paid_revenue", ungrouped=True)
    with pytest.raises(ValueError, match="timezone"):
        layer.compile(saved_query="paid_revenue", timezone="America/Los_Angeles")
    with pytest.raises(ValueError, match="explore"):
        layer.compile(saved_query="paid_revenue", explore="revenue_overview")

    layer.graph.add_saved_query(SavedQuery(name="all_revenue", metrics=["orders.revenue"]))
    with pytest.raises(ValueError, match="explore"):
        layer.compile(saved_query="all_revenue", explore="revenue_overview")


def test_validation_accepts_metric_filters_and_preflights_saved_query_explore_constraints():
    layer = _layer()
    layer.graph.models["orders"].metrics.append(Metric(name="cost", agg="sum", sql="cost"))
    explore = layer.graph.explores["revenue_overview"]
    explore.allowed_filter_fields = ["status", "revenue"]
    errors, _warnings = validate_explore(explore, layer.graph)
    assert errors == []

    invalid = SavedQuery(
        name="invalid_contract",
        explore="revenue_overview",
        metrics=["cost"],
        dimensions=["created_at__month"],
        filters=["orders.created_at > '2026-01-01'"],
        order_by=["orders.status"],
        limit=101,
    )
    errors, _warnings = validate_saved_query(invalid, layer.graph)
    assert any("metric(s) not allowed" in error for error in errors)
    assert any("filters on field(s) not allowed" in error for error in errors)
    assert any("orders by field(s) not allowed" in error for error in errors)
    assert any("exceeds Explore" in error for error in errors)


def test_validation_preflights_saved_query_segments():
    layer = _layer()
    layer.graph.models["orders"].segments.append(Segment(name="paid", sql="{model}.status = 'paid'"))

    valid = SavedQuery(
        name="valid_segment",
        explore="revenue_overview",
        metrics=["revenue"],
        segments=["paid"],
    )
    errors, _warnings = validate_saved_query(valid, layer.graph)
    assert errors == []
    layer.graph.add_saved_query(valid)
    assert "status = 'paid'" in layer.compile(saved_query="valid_segment")

    invalid = valid.model_copy(update={"name": "invalid_segment", "segments": ["orders.missing"]})
    errors, _warnings = validate_saved_query(invalid, layer.graph)
    assert errors == [
        "Saved query 'invalid_segment' references segment 'missing' which doesn't exist on model 'orders'"
    ]


def test_validation_preflights_consumption_filter_and_order_fields():
    layer = _layer()
    explore = layer.graph.explores["revenue_overview"].model_copy(
        update={"name": "invalid_defaults", "default_order_by": ["orders.missing DESC"]}
    )
    errors, _warnings = validate_explore(explore, layer.graph)
    assert "Explore 'invalid_defaults' ordering field 'orders.missing' is not a metric or dimension" in errors

    saved_query = SavedQuery(
        name="invalid_expressions",
        metrics=["orders.revenue"],
        filters=["orders.missing > 0"],
        order_by=["orders.unknown DESC"],
    )
    errors, _warnings = validate_saved_query(saved_query, layer.graph)
    assert "Saved query 'invalid_expressions' filter field 'orders.missing' is not a metric or dimension" in errors
    assert "Saved query 'invalid_expressions' ordering field 'orders.unknown' is not a metric or dimension" in errors


def test_validation_rejects_expression_models_without_a_join_path():
    layer = _layer()
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            dimensions=[Dimension(name="region", type="categorical")],
        )
    )
    explore = Explore(
        name="disconnected_filter",
        model="orders",
        default_metrics=["revenue"],
        filters=["customers.region = 'West'"],
    )
    errors, _warnings = validate_explore(explore, layer.graph)
    assert any("filter expression is incompatible" in error and "No join path found" in error for error in errors)

    saved_query = SavedQuery(
        name="disconnected_saved_query",
        explore="revenue_overview",
        metrics=["revenue"],
        filters=["customers.region = 'West'"],
    )
    errors, _warnings = validate_saved_query(saved_query, layer.graph)
    assert any("filter expression is incompatible" in error and "No join path found" in error for error in errors)

    disconnected_selection = Explore(
        name="disconnected_selection",
        model="orders",
        default_dimensions=["customers.region"],
    )
    errors, _warnings = validate_explore(disconnected_selection, layer.graph)
    assert (
        "Explore 'disconnected_selection' has no join path from base model 'orders' to selected model 'customers'"
        in errors
    )


def test_validation_checks_saved_query_with_mandatory_explore_filters():
    layer = _layer()
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            metrics=[Metric(name="customer_count", agg="count")],
        )
    )
    layer.graph.add_explore(
        Explore(
            name="mandatory_orders_filter",
            model="orders",
            filters=["status = 'paid'"],
        )
    )
    saved_query = SavedQuery(
        name="disconnected_from_mandatory_filter",
        explore="mandatory_orders_filter",
        metrics=["customers.customer_count"],
    )

    errors, _warnings = validate_saved_query(saved_query, layer.graph)

    assert any(
        "inherited Explore 'mandatory_orders_filter' filter expression is incompatible" in error
        and "No join path found" in error
        for error in errors
    )

    layer.graph.add_explore(Explore(name="unfiltered_orders", model="orders"))
    disconnected_without_filter = SavedQuery(
        name="disconnected_without_filter",
        explore="unfiltered_orders",
        metrics=["customers.customer_count"],
    )
    errors, _warnings = validate_saved_query(disconnected_without_filter, layer.graph)
    assert (
        "Saved query 'disconnected_without_filter' has no join path from base model 'orders' "
        "to selected model 'customers'" in errors
    )

    layer.graph.models["customers"].segments.append(Segment(name="vip", sql="region = 'VIP'"))
    disconnected_segment = SavedQuery(
        name="disconnected_segment",
        explore="unfiltered_orders",
        metrics=["revenue"],
        segments=["customers.vip"],
    )
    errors, _warnings = validate_saved_query(disconnected_segment, layer.graph)
    assert (
        "Saved query 'disconnected_segment' has no join path from base model 'orders' to selected model 'customers'"
        in errors
    )


def test_validation_interpolates_saved_query_parameters():
    layer = _layer()
    with pytest.warns(DeprecationWarning):
        layer.graph.add_parameter(Parameter(name="status", type="string"))
    saved_query = SavedQuery(
        name="parameterized_status",
        explore="revenue_overview",
        metrics=["revenue"],
        filters=["orders.status = {{ status }}"],
        parameters={"status": "paid"},
    )

    errors, _warnings = validate_saved_query(saved_query, layer.graph)

    assert errors == []
    layer.graph.add_saved_query(saved_query)
    assert "status = 'paid'" in layer.compile(saved_query="parameterized_status")


def test_validation_requires_order_fields_in_default_or_saved_selection():
    layer = _layer()
    explore = Explore(
        name="unselected_default_order",
        model="orders",
        default_metrics=["revenue"],
        default_order_by=["status"],
    )
    errors, _warnings = validate_explore(explore, layer.graph)
    assert (
        "Explore 'unselected_default_order' default ordering field(s) must be selected by the query: orders.status"
        in errors
    )

    saved_query = SavedQuery(
        name="unselected_saved_order",
        explore="revenue_overview",
        metrics=["revenue"],
        order_by=["status"],
    )
    errors, _warnings = validate_saved_query(saved_query, layer.graph)
    assert (
        "Saved query 'unselected_saved_order' ordering field(s) must be selected by the query: orders.status" in errors
    )


def test_explore_order_override_requires_selected_field():
    layer = _layer()
    layer.graph.add_explore(
        Explore(
            name="order_override",
            model="orders",
            allowed_metrics=["revenue"],
            allowed_order_by=["status"],
            default_metrics=["revenue"],
        )
    )

    with pytest.raises(
        ValueError,
        match="Explore 'order_override' ordering field\\(s\\) must be selected by the query: orders.status",
    ):
        layer.compile(explore="order_override", order_by=["status"])
