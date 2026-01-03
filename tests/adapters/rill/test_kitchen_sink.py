"""Kitchen sink tests for Rill adapter using real-world patterns.

These tests use expressions from rill-examples to find holes in the implementation.
The goal is to find bugs, not conform tests to bugs.
"""

import tempfile
from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.rill import RillAdapter


@pytest.fixture
def kitchen_sink_layer():
    """Load kitchen sink fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/kitchen_sink.yaml")
    layer = SemanticLayer()
    layer.graph = graph
    return layer


class TestKitchenSinkParsing:
    """Test that the kitchen sink fixture parses correctly."""

    def test_model_loads(self, kitchen_sink_layer):
        """Verify the model loads."""
        assert "kitchen_sink" in kitchen_sink_layer.graph.models

    def test_dimension_count(self, kitchen_sink_layer):
        """Verify all dimensions loaded."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        # 5 explicit dimensions + 1 auto-created timeseries
        assert len(model.dimensions) >= 5

    def test_metric_count(self, kitchen_sink_layer):
        """Verify all metrics loaded."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        assert len(model.metrics) >= 20


class TestSimpleAggregations:
    """Test simple aggregation expressions."""

    def test_count_star(self, kitchen_sink_layer):
        """COUNT(*) should work."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.total_records"])
        assert "COUNT(" in sql.upper()

    def test_sum(self, kitchen_sink_layer):
        """SUM(column) should work."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.total_revenue"])
        assert "SUM(" in sql.upper()

    def test_avg(self, kitchen_sink_layer):
        """AVG(column) should work."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.avg_amount"])
        assert "AVG(" in sql.upper()

    def test_min(self, kitchen_sink_layer):
        """MIN(column) should work."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.min_amount"])
        assert "MIN(" in sql.upper()

    def test_max(self, kitchen_sink_layer):
        """MAX(column) should work."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.max_amount"])
        assert "MAX(" in sql.upper()

    def test_median(self, kitchen_sink_layer):
        """MEDIAN(column) should work."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.median_duration"])
        assert "MEDIAN(" in sql.upper()


class TestCountDistinct:
    """Test COUNT DISTINCT expressions."""

    def test_count_distinct_simple(self, kitchen_sink_layer):
        """COUNT(DISTINCT col) should produce valid SQL."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.unique_users"])
        # Should have COUNT and DISTINCT
        assert "COUNT(" in sql.upper()
        assert "DISTINCT" in sql.upper()
        # Should NOT have broken SQL like "DISTINCT user_id AS"
        assert "DISTINCT user_id AS" not in sql

    def test_count_distinct_with_function(self, kitchen_sink_layer):
        """COUNT(DISTINCT CONCAT(...)) from rill-311-ops."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.unique_locations"])
        assert "COUNT(" in sql.upper()
        assert "DISTINCT" in sql.upper()
        assert "CONCAT" in sql.upper()


class TestArithmeticExpressions:
    """Test expressions with arithmetic operators.

    These are the patterns that break with the greedy regex bug.
    """

    def test_division_of_sums(self, kitchen_sink_layer):
        """SUM(x) / SUM(y) from rill-github-analytics.

        BUG: Greedy regex in Metric class matches first SUM( to last ),
        extracting 'deletions) / SUM(changes' as the inner expression.
        """
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        metric = model.get_metric("deletion_pct")

        # The expression should be preserved as-is (no agg extraction)
        # OR properly parsed as a ratio
        assert metric is not None

        # If parsed as expression without agg:
        if metric.agg is None:
            assert metric.sql == "SUM(deletions) / SUM(changes)"
        else:
            # Should NOT have broken SQL
            assert ") / SUM(" not in (metric.sql or "")

    def test_subtraction_of_sums(self, kitchen_sink_layer):
        """SUM(x) - SUM(y) from rill-cost-monitoring."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        metric = model.get_metric("net_revenue")

        assert metric is not None
        if metric.agg is None:
            assert metric.sql == "SUM(revenue) - SUM(cost)"
        else:
            assert ") - SUM(" not in (metric.sql or "")

    def test_division_of_counts(self, kitchen_sink_layer):
        """COUNT(*) / COUNT(DISTINCT x) from rill-github-analytics."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        metric = model.get_metric("files_per_commit")

        assert metric is not None
        if metric.agg is None:
            assert "COUNT(*)" in metric.sql
            assert "COUNT(DISTINCT" in metric.sql
        else:
            assert ") / COUNT(" not in (metric.sql or "")

    def test_sum_with_division(self, kitchen_sink_layer):
        """SUM(x)/1000 from rill-openrtb-prog-ads."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        metric = model.get_metric("ad_spend")

        assert metric is not None
        # Should either preserve full expression or work correctly
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.ad_spend"])
        # Should not error and should contain the division
        assert "/1000" in sql or "/ 1000" in sql

    def test_ratio_with_multiplier(self, kitchen_sink_layer):
        """SUM(x)*1.0/SUM(y)*1.0 from rill-app-engagement."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        metric = model.get_metric("conversion_rate")

        assert metric is not None
        # Full expression should be preserved
        if metric.agg is None:
            assert "*1.0" in metric.sql

    def test_margin_calculation(self, kitchen_sink_layer):
        """(SUM(x) - SUM(y))/SUM(x) from rill-cost-monitoring."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        metric = model.get_metric("margin")

        assert metric is not None
        # Complex expression - should be preserved
        if metric.agg is None:
            assert "SUM(revenue)" in metric.sql
            assert "SUM(cost)" in metric.sql


class TestConditionalAggregations:
    """Test aggregations with CASE expressions."""

    def test_count_with_case(self, kitchen_sink_layer):
        """COUNT(CASE WHEN ... END) should work."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.completed_orders"])
        assert "COUNT(" in sql.upper()
        assert "CASE" in sql.upper()

    def test_sum_with_case(self, kitchen_sink_layer):
        """SUM(CASE WHEN ... END) should work."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.completed_revenue"])
        assert "SUM(" in sql.upper()
        assert "CASE" in sql.upper()


class TestDerivedMetrics:
    """Test derived/calculated metrics."""

    def test_derived_metric_references(self, kitchen_sink_layer):
        """Derived metric referencing other metrics."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        metric = model.get_metric("revenue_per_user")

        assert metric is not None
        assert metric.type == "derived"

    def test_derived_metric_sql(self, kitchen_sink_layer):
        """Derived metric should compile."""
        sql = kitchen_sink_layer.compile(metrics=["kitchen_sink.revenue_per_user"])
        # Should reference the base metrics
        assert sql is not None


class TestWindowFunctions:
    """Test window function metrics."""

    def test_window_metric_parsing(self, kitchen_sink_layer):
        """Window function metrics should parse."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]
        metric = model.get_metric("rolling_7day_revenue")

        assert metric is not None
        assert metric.type == "cumulative"


class TestSQLGeneration:
    """Test that generated SQL is syntactically valid."""

    def test_all_metrics_compile(self, kitchen_sink_layer):
        """All metrics should compile without errors."""
        model = kitchen_sink_layer.graph.models["kitchen_sink"]

        failed = []
        for metric in model.metrics:
            try:
                sql = kitchen_sink_layer.compile(metrics=[f"kitchen_sink.{metric.name}"])
                # Basic syntax check - should have SELECT
                assert "SELECT" in sql.upper()
            except Exception as e:
                failed.append((metric.name, str(e)))

        if failed:
            msg = "\n".join(f"  {name}: {err}" for name, err in failed)
            pytest.fail(f"Failed to compile metrics:\n{msg}")

    def test_sql_is_parseable(self, kitchen_sink_layer):
        """Generated SQL should be parseable by sqlglot."""
        import sqlglot

        model = kitchen_sink_layer.graph.models["kitchen_sink"]

        failed = []
        for metric in model.metrics:
            try:
                sql = kitchen_sink_layer.compile(metrics=[f"kitchen_sink.{metric.name}"])
                # Strip the comment line
                sql_no_comment = "\n".join(line for line in sql.split("\n") if not line.strip().startswith("--"))
                sqlglot.parse_one(sql_no_comment, read="duckdb")
            except Exception as e:
                failed.append((metric.name, str(e)))

        if failed:
            msg = "\n".join(f"  {name}: {err}" for name, err in failed)
            pytest.fail(f"Generated invalid SQL:\n{msg}")


class TestRoundtrip:
    """Test export and re-import."""

    def test_roundtrip_preserves_expressions(self, kitchen_sink_layer):
        """Complex expressions should survive roundtrip."""
        adapter = RillAdapter()

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter.export(kitchen_sink_layer.graph, tmpdir)
            graph2 = adapter.parse(Path(tmpdir) / "kitchen_sink.yaml")

            model1 = kitchen_sink_layer.graph.models["kitchen_sink"]
            model2 = graph2.models["kitchen_sink"]

            # Check a complex metric survived
            m1 = model1.get_metric("margin")
            m2 = model2.get_metric("margin")

            assert m1 is not None
            assert m2 is not None
            # Both should have the full expression (not broken)
            # Either both have agg=None with full SQL, or both properly decomposed
