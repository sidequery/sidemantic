"""Tests for SemanticLayer.explain() and PreAggregationMatcher.explain_matching()."""

from sidemantic import Dimension, Metric, Model, PreAggregation, SemanticLayer
from sidemantic.core.preagg_matcher import PreAggregationMatcher


def make_layer_with_preaggs():
    """Create a SemanticLayer with a model that has pre-aggregations."""
    layer = SemanticLayer(preagg_schema="preagg", use_preaggregations=True)

    events = Model(
        name="events",
        table="events",
        primary_key="event_id",
        dimensions=[
            Dimension(name="event_type", type="categorical", sql="event_type"),
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="event_date", type="time", sql="event_date", granularity="day"),
        ],
        metrics=[
            Metric(name="event_count", agg="count"),
            Metric(name="total_amount", agg="sum", sql="amount"),
            Metric(name="unique_users", agg="count_distinct", sql="user_id"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_by_type",
                measures=["event_count", "total_amount"],
                dimensions=["event_type"],
                time_dimension="event_date",
                granularity="day",
            ),
            PreAggregation(
                name="daily_by_region",
                measures=["event_count", "total_amount"],
                dimensions=["region"],
                time_dimension="event_date",
                granularity="day",
            ),
            PreAggregation(
                name="monthly_summary",
                measures=["event_count", "total_amount"],
                dimensions=["event_type", "region"],
                time_dimension="event_date",
                granularity="month",
            ),
        ],
    )

    layer.add_model(events)
    return layer


class TestExplainMatching:
    """Test PreAggregationMatcher.explain_matching()."""

    def test_matching_candidate_selected(self):
        layer = make_layer_with_preaggs()
        model = layer.get_model("events")
        matcher = PreAggregationMatcher(model)

        candidates = matcher.explain_matching(
            metrics=["event_count", "total_amount"],
            dimensions=["event_type"],
        )

        assert len(candidates) == 3
        selected = [c for c in candidates if c.selected]
        assert len(selected) == 1
        assert selected[0].name == "daily_by_type"
        assert selected[0].matched is True
        assert selected[0].score is not None

    def test_non_matching_shows_failure_reason(self):
        layer = make_layer_with_preaggs()
        model = layer.get_model("events")
        matcher = PreAggregationMatcher(model)

        candidates = matcher.explain_matching(
            metrics=["event_count"],
            dimensions=["event_type"],
        )

        # daily_by_region should fail on dimensions
        region_candidate = next(c for c in candidates if c.name == "daily_by_region")
        assert region_candidate.matched is False
        dim_check = next(ch for ch in region_candidate.checks if ch.name == "dimensions")
        assert dim_check.passed is False
        assert "event_type" in dim_check.detail

    def test_count_distinct_not_derivable(self):
        layer = make_layer_with_preaggs()
        model = layer.get_model("events")
        matcher = PreAggregationMatcher(model)

        candidates = matcher.explain_matching(
            metrics=["unique_users"],
            dimensions=["event_type"],
        )

        # All candidates should fail on measures (count_distinct not derivable)
        for c in candidates:
            assert c.matched is False
            measure_check = next(ch for ch in c.checks if ch.name == "measures")
            assert measure_check.passed is False
            assert "count_distinct" in measure_check.detail

    def test_all_checks_pass_details(self):
        layer = make_layer_with_preaggs()
        model = layer.get_model("events")
        matcher = PreAggregationMatcher(model)

        candidate = matcher.explain_query(
            preagg=model.pre_aggregations[0],  # daily_by_type
            query_metrics=["event_count"],
            query_dimensions=["event_type"],
        )

        assert candidate.matched is True
        assert all(ch.passed for ch in candidate.checks)
        check_names = {ch.name for ch in candidate.checks}
        assert check_names == {"dimensions", "measures", "granularity", "filters"}


class TestSemanticLayerExplain:
    """Test SemanticLayer.explain()."""

    def test_explain_with_matching_preagg(self):
        layer = make_layer_with_preaggs()
        plan = layer.explain(
            metrics=["events.event_count", "events.total_amount"],
            dimensions=["events.event_type"],
        )

        assert plan.used_preaggregation is True
        assert plan.selected_preagg == "daily_by_type"
        assert plan.model == "events"
        assert "daily_by_type" in plan.routing_reason
        assert len(plan.candidates) == 3

    def test_explain_no_match(self):
        layer = make_layer_with_preaggs()
        plan = layer.explain(
            metrics=["events.unique_users"],
            dimensions=["events.event_type"],
        )

        assert plan.used_preaggregation is False
        assert plan.selected_preagg is None
        assert "no pre-aggregation matched" in plan.routing_reason
        assert len(plan.candidates) == 3

    def test_explain_preaggs_disabled(self):
        layer = make_layer_with_preaggs()
        layer.use_preaggregations = False
        plan = layer.explain(
            metrics=["events.event_count"],
            dimensions=["events.event_type"],
        )

        assert plan.used_preaggregation is False
        assert "not enabled" in plan.routing_reason
        assert plan.candidates == []

    def test_explain_no_preaggs_defined(self):
        layer = SemanticLayer(use_preaggregations=True)
        layer.add_model(
            Model(
                name="simple",
                table="simple",
                dimensions=[Dimension(name="status", type="categorical", sql="status")],
                metrics=[Metric(name="count", agg="count")],
            )
        )

        plan = layer.explain(
            metrics=["simple.count"],
            dimensions=["simple.status"],
        )

        assert plan.used_preaggregation is False
        assert "no pre-aggregations defined" in plan.routing_reason

    def test_explain_ungrouped(self):
        layer = make_layer_with_preaggs()
        plan = layer.explain(
            metrics=["events.event_count"],
            dimensions=["events.event_type"],
            ungrouped=True,
        )

        assert plan.used_preaggregation is False
        assert "ungrouped" in plan.routing_reason

    def test_explain_str_output(self):
        layer = make_layer_with_preaggs()
        plan = layer.explain(
            metrics=["events.event_count"],
            dimensions=["events.event_type"],
        )

        output = str(plan)
        assert "Query Plan" in output
        assert "Model: events" in output
        assert "daily_by_type" in output
        assert "SQL:" in output

    def test_explain_with_time_granularity(self):
        layer = make_layer_with_preaggs()
        plan = layer.explain(
            metrics=["events.event_count"],
            dimensions=["events.event_type", "events.event_date__month"],
        )

        assert plan.used_preaggregation is True
        # daily_by_type should be selected (day can roll up to month)
        assert plan.selected_preagg == "daily_by_type"

    def test_explain_sql_present(self):
        layer = make_layer_with_preaggs()
        plan = layer.explain(
            metrics=["events.event_count"],
            dimensions=["events.event_type"],
        )

        assert "preagg" in plan.sql.lower() or "events" in plan.sql.lower()
        assert plan.sql.strip() != ""

    def test_explain_cross_model_filter_detected(self):
        """Filters referencing another model should trigger multi-model detection."""
        layer = make_layer_with_preaggs()

        # Add a second model so the cross-model filter is valid
        from sidemantic import Relationship

        customers = Model(
            name="customers",
            table="customers",
            primary_key="customer_id",
            dimensions=[
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[Metric(name="customer_count", agg="count")],
        )
        layer.add_model(customers)

        # Update events model to have a relationship to customers
        events = layer.get_model("events")
        events.relationships = [
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
        ]

        plan = layer.explain(
            metrics=["events.event_count"],
            dimensions=["events.event_type"],
            filters=["customers.status = 'vip'"],
        )

        assert plan.used_preaggregation is False
        assert "multi-model" in plan.routing_reason
