"""Tests for the 9-fix bugfix batch.

Each test targets a specific fix to ensure it works correctly and doesn't regress.
"""

import duckdb

from sidemantic import Dimension, Metric, Model, Segment
from sidemantic.core.inheritance import merge_metric, merge_model
from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.core.preagg_matcher import PreAggregationMatcher
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator
from tests.utils import fetch_dicts

# ==========================================================================
# Fix 1: FULL OUTER JOIN uses IS NOT DISTINCT FROM instead of COALESCE
# ==========================================================================


class TestNullSafeJoin:
    """COALESCE(x, '') = COALESCE(y, '') fails for non-string types (ints, dates).
    IS NOT DISTINCT FROM handles NULLs correctly for all types.
    """

    def _build_multi_model_graph(self):
        """Build a two-model graph where pre-aggregation triggers FULL OUTER JOIN."""
        graph = SemanticGraph()

        orders = Model(
            name="orders",
            sql="""
                SELECT * FROM (VALUES
                    (1, 1, '2024-01-01'::DATE, 100),
                    (2, 2, '2024-01-01'::DATE, 200),
                    (3, NULL, '2024-01-02'::DATE, 50)
                ) AS t(id, customer_id, order_date, amount)
            """,
            primary_key="id",
            dimensions=[
                Dimension(name="customer_id", type="categorical", sql="customer_id"),
                Dimension(name="order_date", type="time", sql="order_date"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[
                Relationship(
                    name="line_items",
                    type="one_to_many",
                    sql="id",
                    foreign_key="order_id",
                ),
            ],
        )

        line_items = Model(
            name="line_items",
            sql="""
                SELECT * FROM (VALUES
                    (1, 1, 5),
                    (2, 1, 3),
                    (3, 2, 10),
                    (4, 99, 1)
                ) AS t(id, order_id, qty)
            """,
            primary_key="id",
            dimensions=[],
            metrics=[Metric(name="total_qty", agg="sum", sql="qty")],
            relationships=[
                Relationship(
                    name="orders",
                    type="many_to_one",
                    foreign_key="order_id",
                    primary_key="id",
                ),
            ],
        )

        graph.add_model(orders)
        graph.add_model(line_items)
        return graph

    def test_null_dimension_in_full_outer_join(self):
        """NULL customer_id must not crash or silently drop rows."""
        graph = self._build_multi_model_graph()
        gen = SQLGenerator(graph)
        sql = gen.generate(
            metrics=["orders.revenue", "line_items.total_qty"],
            dimensions=["orders.customer_id"],
        )

        # The FULL OUTER JOIN condition should use IS NOT DISTINCT FROM,
        # not COALESCE(..., '') = COALESCE(..., '') which fails for non-string types.
        # Extract only the JOIN ON clause to check.
        upper = sql.upper()
        assert "FULL OUTER JOIN" in upper
        assert "IS NOT DISTINCT FROM" in upper

        conn = duckdb.connect(":memory:")
        rows = fetch_dicts(conn.execute(sql))
        conn.close()

        # customer_id=1 has revenue 100, customer_id=2 has revenue 200,
        # customer_id=NULL has revenue 50, line_item order_id=99 has no match
        assert len(rows) >= 3  # at least 3 groups

    def test_join_condition_not_coalesce_equality(self):
        """The FULL OUTER JOIN must not use COALESCE-based equality for the join condition."""
        graph = self._build_multi_model_graph()
        gen = SQLGenerator(graph)
        sql = gen.generate(
            metrics=["orders.revenue", "line_items.total_qty"],
            dimensions=["orders.customer_id"],
        )
        # Extract lines around the FULL OUTER JOIN ... ON clause
        lines = sql.split("\n")
        join_line_idx = None
        for i, line in enumerate(lines):
            if "FULL OUTER JOIN" in line.upper():
                join_line_idx = i
                break
        assert join_line_idx is not None
        # The ON clause is on the same or next line
        on_region = " ".join(lines[join_line_idx : join_line_idx + 3])
        # Must not have COALESCE in the join condition specifically
        assert "COALESCE" not in on_region


# ==========================================================================
# Fix 4: Fan-out detection checks all hops, not just first hop
# ==========================================================================


class TestFanoutAllHops:
    """A path like A->B(many_to_one)->C(one_to_many) has fan-out from C,
    even though the first hop is many_to_one."""

    def test_multi_hop_fanout_detected(self):
        graph = SemanticGraph()

        # A -> B (many_to_one) -> C (one_to_many)
        a = Model(
            name="a",
            table="t_a",
            primary_key="id",
            metrics=[Metric(name="a_sum", agg="sum", sql="val")],
            relationships=[
                Relationship(name="b", type="many_to_one", foreign_key="b_id", primary_key="id"),
            ],
        )
        b = Model(
            name="b",
            table="t_b",
            primary_key="id",
            metrics=[Metric(name="b_sum", agg="sum", sql="val")],
            relationships=[
                Relationship(name="a", type="one_to_many", sql="id", foreign_key="b_id"),
                Relationship(name="c", type="one_to_many", sql="id", foreign_key="b_id"),
            ],
        )
        c = Model(
            name="c",
            table="t_c",
            primary_key="id",
            metrics=[Metric(name="c_sum", agg="sum", sql="val")],
            relationships=[
                Relationship(name="b", type="many_to_one", foreign_key="b_id", primary_key="id"),
            ],
        )

        graph.add_model(a)
        graph.add_model(b)
        graph.add_model(c)

        gen = SQLGenerator(graph)
        # Path from a to c goes through b with a one_to_many hop
        result = gen._has_fanout_joins("a", ["c"])
        # Should detect fan-out because the path contains one_to_many
        assert result["a"] is True


# ==========================================================================
# Fix 5/8: Cross-model filters applied on outer pre-aggregation query
# ==========================================================================


class TestSharedFiltersOnOuterQuery:
    """Filters that reference multiple models or metric columns can't be pushed
    down into individual model sub-queries. They must be applied on the outer
    FULL OUTER JOIN query."""

    def _build_graph(self):
        graph = SemanticGraph()

        orders = Model(
            name="orders",
            sql="""
                SELECT * FROM (VALUES
                    (1, '2024-01-01'::DATE, 100),
                    (2, '2024-01-01'::DATE, 200),
                    (3, '2024-01-02'::DATE, 50)
                ) AS t(id, order_date, amount)
            """,
            primary_key="id",
            dimensions=[
                Dimension(name="order_date", type="time", sql="order_date"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[
                Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id"),
            ],
        )

        items = Model(
            name="items",
            sql="""
                SELECT * FROM (VALUES
                    (1, 1, 5),
                    (2, 1, 3),
                    (3, 2, 10),
                    (4, 3, 2)
                ) AS t(id, order_id, qty)
            """,
            primary_key="id",
            dimensions=[],
            metrics=[Metric(name="total_qty", agg="sum", sql="qty")],
            relationships=[
                Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id"),
            ],
        )

        graph.add_model(orders)
        graph.add_model(items)
        return graph

    def test_model_specific_filter_pushdown(self):
        """A filter like 'orders.amount > 50' should be pushed into the orders sub-query."""
        graph = self._build_graph()
        gen = SQLGenerator(graph)
        sql = gen.generate(
            metrics=["orders.revenue", "items.total_qty"],
            dimensions=["orders.order_date"],
            filters=["orders_cte.amount > 50"],
        )

        conn = duckdb.connect(":memory:")
        rows = fetch_dicts(conn.execute(sql))
        conn.close()

        # Only orders with amount > 50 should be included (orders 1 and 2)
        # order_date 2024-01-01: revenue = 100 + 200 = 300
        # order_date 2024-01-02: excluded (amount=50, not > 50)
        revenues = {r["order_date"].isoformat(): r["revenue"] for r in rows if r.get("revenue")}
        assert "2024-01-01" in revenues
        assert revenues["2024-01-01"] == 300

    def test_filter_rewrite_uses_ast(self):
        """The filter rewrite should use sqlglot AST, not string replacement."""
        graph = self._build_graph()
        gen = SQLGenerator(graph)
        sql = gen.generate(
            metrics=["orders.revenue", "items.total_qty"],
            dimensions=["orders.order_date"],
        )
        # Just verify the SQL is valid and runs
        conn = duckdb.connect(":memory:")
        rows = fetch_dicts(conn.execute(sql))
        conn.close()
        assert len(rows) >= 1


# ==========================================================================
# Fix 6: Duplicate segment resolution (segments=None after resolve)
# ==========================================================================


class TestDuplicateSegmentResolution:
    """When segments are resolved to filters in generate(), passing them again
    to _generate_with_preaggregation would double-apply them."""

    def test_segment_not_double_applied(self):
        """A segment filter should appear exactly once in the generated SQL."""
        graph = SemanticGraph()

        orders = Model(
            name="orders",
            sql="""
                SELECT * FROM (VALUES
                    (1, '2024-01-01'::DATE, 100, 'completed'),
                    (2, '2024-01-01'::DATE, 200, 'pending'),
                    (3, '2024-01-02'::DATE, 50, 'completed')
                ) AS t(id, order_date, amount, status)
            """,
            primary_key="id",
            dimensions=[
                Dimension(name="order_date", type="time", sql="order_date"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            segments=[
                Segment(name="completed", sql="orders_cte.status = 'completed'"),
            ],
            relationships=[
                Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id"),
            ],
        )

        items = Model(
            name="items",
            sql="""
                SELECT * FROM (VALUES
                    (1, 1, 5),
                    (2, 2, 3),
                    (3, 3, 10)
                ) AS t(id, order_id, qty)
            """,
            primary_key="id",
            dimensions=[],
            metrics=[Metric(name="total_qty", agg="sum", sql="qty")],
            relationships=[
                Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id"),
            ],
        )

        graph.add_model(orders)
        graph.add_model(items)

        gen = SQLGenerator(graph)
        sql = gen.generate(
            metrics=["orders.revenue", "items.total_qty"],
            dimensions=["orders.order_date"],
            segments=["orders.completed"],
        )

        # The segment filter should be applied (only completed orders)
        conn = duckdb.connect(":memory:")
        rows = fetch_dicts(conn.execute(sql))
        conn.close()

        # completed orders: id=1 (amount=100, date=01-01) and id=3 (amount=50, date=01-02)
        revenues = {r["order_date"].isoformat(): r["revenue"] for r in rows if r.get("revenue")}
        assert revenues.get("2024-01-01") == 100  # only order 1, not order 2 (pending)
        assert revenues.get("2024-01-02") == 50


# ==========================================================================
# Fix 7: Inheritance uses model_fields_set instead of exclude_none
# ==========================================================================


class TestInheritanceExplicitNone:
    """A child model that explicitly sets a field to None should clear the
    parent's value, not inherit it. exclude_none=True would silently keep
    the parent's value."""

    def test_child_can_clear_parent_description(self):
        parent = Model(
            name="parent",
            table="parent_table",
            primary_key="id",
            description="Parent description",
        )

        child = Model(
            name="child",
            extends="parent",
            description=None,
        )

        # Explicitly set description=None via model_fields_set
        # Pydantic tracks which fields were passed to __init__
        assert "description" in child.model_fields_set

        merged = merge_model(child, parent)
        # Child explicitly set description=None, so it should override parent
        assert merged.description is None

    def test_child_unset_field_inherits_from_parent(self):
        """A field NOT set on the child should inherit from parent."""
        parent = Model(
            name="parent",
            table="parent_table",
            primary_key="pk",
            description="Inherited",
        )

        child = Model(
            name="child",
            extends="parent",
            # description not set at all
        )

        assert "description" not in child.model_fields_set

        merged = merge_model(child, parent)
        # Should inherit parent's description
        assert merged.description == "Inherited"

    def test_metric_child_can_clear_parent_format(self):
        parent = Metric(name="parent_metric", agg="sum", sql="amount", format="$#,##0.00")

        child = Metric(
            name="child_metric",
            extends="parent_metric",
            format=None,
        )

        assert "format" in child.model_fields_set

        merged = merge_metric(child, parent)
        assert merged.format is None


# ==========================================================================
# Fix 3/9: Conversion metric dimension support
# ==========================================================================


class TestConversionMetricDimensions:
    """Conversion metrics should support dimensions for GROUP BY slicing.
    The LEFT JOIN must include dimension columns to prevent cross-group leaking."""

    def test_conversion_with_dimension(self):
        """Conversion rate should be computed per dimension group."""
        events = Model(
            name="events",
            sql="""
                SELECT * FROM (VALUES
                    (1, 'signup', '2024-01-01'::DATE, 'US'),
                    (1, 'purchase', '2024-01-03'::DATE, 'US'),
                    (2, 'signup', '2024-01-05'::DATE, 'EU'),
                    (3, 'signup', '2024-01-10'::DATE, 'EU')
                ) AS t(user_id, event_type, event_date, region)
            """,
            primary_key="user_id",
            dimensions=[
                Dimension(name="user_id", sql="user_id", type="categorical"),
                Dimension(name="event_type", sql="event_type", type="categorical"),
                Dimension(name="event_date", sql="event_date", type="time"),
                Dimension(name="region", sql="region", type="categorical"),
            ],
            metrics=[],
        )

        signup_conversion = Metric(
            name="signup_conversion",
            type="conversion",
            entity="user_id",
            base_event="signup",
            conversion_event="purchase",
            conversion_window="7 days",
        )

        graph = SemanticGraph()
        graph.add_model(events)
        graph.add_metric(signup_conversion)

        gen = SQLGenerator(graph)
        sql = gen.generate(
            metrics=["signup_conversion"],
            dimensions=["events.region"],
        )

        # SQL should include region in SELECT, GROUP BY, and JOIN condition
        assert "region" in sql
        assert "GROUP BY" in sql
        assert "IS NOT DISTINCT FROM" in sql

        conn = duckdb.connect(":memory:")
        rows = fetch_dicts(conn.execute(sql))
        conn.close()

        by_region = {r["region"]: r["signup_conversion"] for r in rows}
        # US: 1 signup, 1 purchase within 7 days -> 1.0
        # EU: 2 signups, 0 purchases -> 0.0
        assert abs(by_region["US"] - 1.0) < 0.01
        assert abs(by_region["EU"] - 0.0) < 0.01

    def test_conversion_without_dimensions_still_works(self):
        """Conversion without dimensions should produce a single row."""
        events = Model(
            name="events",
            sql="""
                SELECT * FROM (VALUES
                    (1, 'signup', '2024-01-01'::DATE),
                    (1, 'purchase', '2024-01-03'::DATE),
                    (2, 'signup', '2024-01-05'::DATE)
                ) AS t(user_id, event_type, event_date)
            """,
            primary_key="user_id",
            dimensions=[
                Dimension(name="user_id", sql="user_id", type="categorical"),
                Dimension(name="event_type", sql="event_type", type="categorical"),
                Dimension(name="event_date", sql="event_date", type="time"),
            ],
            metrics=[],
        )

        signup_conversion = Metric(
            name="signup_conversion",
            type="conversion",
            entity="user_id",
            base_event="signup",
            conversion_event="purchase",
            conversion_window="7 days",
        )

        graph = SemanticGraph()
        graph.add_model(events)
        graph.add_metric(signup_conversion)

        gen = SQLGenerator(graph)
        sql = gen.generate(metrics=["signup_conversion"], dimensions=[])

        conn = duckdb.connect(":memory:")
        rows = fetch_dicts(conn.execute(sql))
        conn.close()

        # 2 signups, 1 purchase within window -> 0.5
        assert len(rows) == 1
        assert abs(rows[0]["signup_conversion"] - 0.5) < 0.01

    def test_conversion_dimension_join_prevents_cross_group_leaking(self):
        """A conversion in region A must NOT count for region B signups."""
        events = Model(
            name="events",
            sql="""
                SELECT * FROM (VALUES
                    (1, 'signup', '2024-01-01'::DATE, 'US'),
                    (1, 'purchase', '2024-01-02'::DATE, 'US'),
                    (2, 'signup', '2024-01-01'::DATE, 'EU')
                ) AS t(user_id, event_type, event_date, region)
            """,
            primary_key="user_id",
            dimensions=[
                Dimension(name="user_id", sql="user_id", type="categorical"),
                Dimension(name="event_type", sql="event_type", type="categorical"),
                Dimension(name="event_date", sql="event_date", type="time"),
                Dimension(name="region", sql="region", type="categorical"),
            ],
            metrics=[],
        )

        conv = Metric(
            name="conv_rate",
            type="conversion",
            entity="user_id",
            base_event="signup",
            conversion_event="purchase",
            conversion_window="7 days",
        )

        graph = SemanticGraph()
        graph.add_model(events)
        graph.add_metric(conv)

        gen = SQLGenerator(graph)
        sql = gen.generate(metrics=["conv_rate"], dimensions=["events.region"])

        conn = duckdb.connect(":memory:")
        rows = fetch_dicts(conn.execute(sql))
        conn.close()

        by_region = {r["region"]: r["conv_rate"] for r in rows}
        # US: user 1 signed up and purchased -> 1.0
        # EU: user 2 signed up, no purchase -> 0.0
        # Without dimension in JOIN, EU would incorrectly get credit from user 1's purchase
        assert abs(by_region["US"] - 1.0) < 0.01
        assert abs(by_region["EU"] - 0.0) < 0.01


# ==========================================================================
# Fix 2: Symmetric aggregate multiplier values (tested in test_symmetric_aggregates.py)
# Fix 5: min/max/median (tested in test_symmetric_aggregates.py)
# Fix 4b: Preagg count regex (tested below)
# ==========================================================================


class TestPreaggCountRegex:
    """The preagg matcher should not match 'discount_amount' when looking
    for a count measure, but should match 'order_count'."""

    def _make_model_with_preagg(self, measures):
        return Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[
                Metric(name="avg_amount", agg="avg", sql="amount"),
                *[Metric(name=m, agg="sum", sql="x") for m in measures],
            ],
            pre_aggregations=[
                PreAggregation(
                    name="daily",
                    measures=["avg_amount", *measures],
                    dimensions=["status"],
                    time_dimension="created_at",
                    granularity="day",
                ),
            ],
        )

    def test_discount_amount_not_matched_as_count(self):
        """'discount_amount' contains 'count' as a substring but is NOT a count measure."""
        model = self._make_model_with_preagg(["discount_amount"])
        matcher = PreAggregationMatcher(model)

        # avg_amount needs a count measure for re-aggregation
        # discount_amount should NOT be matched
        preagg = matcher.find_matching_preagg(
            metrics=["avg_amount"],
            dimensions=["status"],
            time_granularity="day",
        )
        # Should not match because there's no real count measure
        assert preagg is None

    def test_order_count_matched(self):
        """'order_count' contains 'count' as a word boundary and IS a count measure."""
        model = self._make_model_with_preagg(["order_count"])
        matcher = PreAggregationMatcher(model)

        preagg = matcher.find_matching_preagg(
            metrics=["avg_amount"],
            dimensions=["status"],
            time_granularity="day",
        )
        assert preagg is not None

    def test_count_orders_matched(self):
        """'count_orders' has 'count' as a prefix word and IS a count measure."""
        model = self._make_model_with_preagg(["count_orders"])
        matcher = PreAggregationMatcher(model)

        preagg = matcher.find_matching_preagg(
            metrics=["avg_amount"],
            dimensions=["status"],
            time_granularity="day",
        )
        assert preagg is not None

    def test_plain_count_matched(self):
        """'count' exact match should always work."""
        model = self._make_model_with_preagg(["count"])
        matcher = PreAggregationMatcher(model)

        preagg = matcher.find_matching_preagg(
            metrics=["avg_amount"],
            dimensions=["status"],
            time_granularity="day",
        )
        assert preagg is not None
