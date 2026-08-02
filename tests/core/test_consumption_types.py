"""Tests for consumption-contract schemas and reusable governance metadata."""

import pytest
from pydantic import ValidationError

from sidemantic import Deprecation, Explore, Metric, Model, SavedQuery, View
from sidemantic.core.consumption import (
    expression_field_references,
    qualify_expression_fields,
    qualify_order_by_fields,
)
from sidemantic.core.governance import governance_dict


def test_governance_defaults_and_serialization():
    model = Model(name="orders", table="orders")

    assert model.owner is None
    assert model.tags == []
    assert model.visibility == "public"
    assert governance_dict(model) == {}

    governed = Model(
        name="legacy_orders",
        table="legacy_orders",
        owner="analytics",
        tags=["legacy"],
        status="deprecated",
        deprecation=Deprecation(message="Use orders", replaced_by="orders"),
        visibility="internal",
    )
    assert governance_dict(governed) == {
        "owner": "analytics",
        "tags": ["legacy"],
        "status": "deprecated",
        "deprecation": {"message": "Use orders", "replaced_by": "orders"},
        "visibility": "internal",
    }


def test_model_and_metric_governance_preserve_public_compatibility():
    assert Model(name="orders", table="orders", domain="commerce").domain == "commerce"

    hidden = Metric(name="hidden_revenue", agg="sum", sql="amount", public=False)
    internal = Metric(name="internal_revenue", agg="sum", sql="amount", visibility="internal")
    visible = Metric(name="revenue", agg="sum", sql="amount")

    assert hidden.public is False
    assert hidden.visibility == "private"
    assert internal.public is False
    assert internal.visibility == "internal"
    assert visible.public is True
    assert visible.visibility == "public"


def test_explore_defaults_must_respect_allowlists_and_limits():
    valid = Explore(
        name="revenue_overview",
        model="orders",
        allowed_dimensions=["status"],
        allowed_metrics=["orders.revenue"],
        allowed_filter_fields=["status"],
        allowed_order_by=["revenue"],
        default_dimensions=["orders.status"],
        default_metrics=["revenue"],
        default_filters=["status = 'paid'"],
        default_order_by=["revenue DESC"],
        default_limit=25,
        max_limit=100,
    )

    assert View is Explore
    assert valid.default_metrics == ["revenue"]

    invalid_values = [
        ({"allowed_dimensions": ["status"], "default_dimensions": ["created_at"]}, "default_dimensions"),
        ({"allowed_metrics": ["revenue"], "default_metrics": ["order_count"]}, "default_metrics"),
        (
            {"allowed_filter_fields": ["status"], "default_filters": ["created_at > '2026-01-01'"]},
            "default_filters",
        ),
        ({"allowed_order_by": ["revenue"], "default_order_by": ["status ASC"]}, "default_order_by"),
        ({"default_limit": 101, "max_limit": 100}, "default_limit cannot exceed max_limit"),
    ]
    for values, message in invalid_values:
        with pytest.raises(ValidationError, match=message):
            Explore(name="invalid", model="orders", **values)


def test_saved_query_rejects_invalid_limits_and_unknown_fields():
    query = SavedQuery(
        name="paid_revenue",
        explore="revenue_overview",
        metrics=["revenue"],
        filters=["status = 'paid'"],
        limit=10,
    )
    assert query.visibility == "public"

    with pytest.raises(ValidationError):
        SavedQuery(name="negative_limit", limit=-1)
    with pytest.raises(ValidationError, match="extra_forbidden"):
        SavedQuery(name="unknown_field", unknown=True)


def test_expression_helpers_qualify_semantic_fields_and_skip_subqueries():
    expression = "status IN (SELECT status FROM allowed_statuses)"

    assert qualify_expression_fields([expression], "orders") == [
        "orders.status IN (SELECT status FROM allowed_statuses)"
    ]
    assert expression_field_references([expression], "orders") == {"orders.status"}

    correlated = "EXISTS (SELECT 1 FROM allowed_statuses AS allowed WHERE allowed.status = orders.status)"
    assert expression_field_references([correlated], "orders", graph_models={"orders"}) == {"orders.status"}

    assert qualify_expression_fields(["revenue > 0 AND status = 'paid'"], "orders", graph_metrics={"revenue"}) == [
        "revenue > 0 AND orders.status = 'paid'"
    ]
    assert qualify_order_by_fields(["revenue DESC, status ASC"], "orders", graph_metrics={"revenue"}) == [
        "revenue DESC",
        "orders.status ASC",
    ]
