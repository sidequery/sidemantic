"""Tests for Malloy adapter - edge cases."""

from pathlib import Path

from sidemantic.adapters.malloy import MalloyAdapter


class TestEdgeCases:
    """Test edge case handling in Malloy adapter."""

    def setup_method(self):
        """Parse the edge cases fixture."""
        adapter = MalloyAdapter()
        self.graph = adapter.parse(Path("tests/fixtures/malloy/edge_cases.malloy"))

    def test_minimal_source(self):
        """Test minimal source with only required fields."""
        minimal = self.graph.get_model("minimal")
        assert minimal is not None
        assert minimal.table == "data.parquet"
        assert minimal.primary_key == "id"
        assert len(minimal.dimensions) == 1
        assert len(minimal.metrics) == 1
        assert minimal.dimensions[0].name == "id"
        assert minimal.metrics[0].name == "count_all"

    def test_complex_expressions(self):
        """Test complex expression parsing."""
        complex_expr = self.graph.get_model("complex_expressions")
        assert complex_expr is not None

        # Arithmetic expression
        profit_margin = complex_expr.get_dimension("profit_margin")
        assert profit_margin is not None
        assert profit_margin.type == "numeric"
        assert "/" in profit_margin.sql

        # Nested pick/when -> CASE
        priority = complex_expr.get_dimension("priority_level")
        assert priority is not None
        sql_lower = priority.sql.lower()
        assert "case" in sql_lower
        assert "when" in sql_lower

        # Boolean from comparison
        is_high = complex_expr.get_dimension("is_high_severity")
        assert is_high is not None
        assert is_high.type == "boolean"

    def test_all_aggregation_types(self):
        """Test all standard aggregation types."""
        all_agg = self.graph.get_model("all_aggregations")
        assert all_agg is not None

        # count()
        count_all = all_agg.get_metric("count_all")
        assert count_all.agg == "count"
        assert count_all.sql is None or count_all.sql == ""

        # sum(x)
        total_value = all_agg.get_metric("total_value")
        assert total_value.agg == "sum"
        assert total_value.sql == "value"

        # avg(x)
        avg_value = all_agg.get_metric("avg_value")
        assert avg_value.agg == "avg"

        # min(x)
        min_value = all_agg.get_metric("min_value")
        assert min_value.agg == "min"

        # max(x)
        max_value = all_agg.get_metric("max_value")
        assert max_value.agg == "max"

        # count_distinct(x)
        unique_cats = all_agg.get_metric("unique_categories")
        assert unique_cats.agg == "count_distinct"

    def test_all_join_types(self):
        """Test all join type mappings."""
        all_joins = self.graph.get_model("all_join_types")
        assert all_joins is not None
        assert len(all_joins.relationships) == 3

        # Map relationships by name for easier assertion
        rels = {r.name: r for r in all_joins.relationships}

        # join_one -> many_to_one
        assert "join_target_a" in rels
        assert rels["join_target_a"].type == "many_to_one"
        assert rels["join_target_a"].foreign_key == "a_id"

        # join_many -> one_to_many
        assert "join_target_b" in rels
        assert rels["join_target_b"].type == "one_to_many"

        # join_cross -> one_to_one (our mapping)
        assert "join_target_c" in rels
        assert rels["join_target_c"].type == "one_to_one"

    def test_bare_minimum_source(self):
        """Test source with only primary_key (no dimensions/measures)."""
        bare = self.graph.get_model("bare_minimum")
        assert bare is not None
        assert bare.primary_key == "id"
        # Should have no dimensions or measures
        assert len(bare.dimensions) == 0
        assert len(bare.metrics) == 0

    def test_filtered_source_creates_segment(self):
        """Test that source-level where creates a segment."""
        filtered = self.graph.get_model("filtered_source")
        assert filtered is not None

        # Should have segments from where clause
        assert len(filtered.segments) >= 1
        # Check segment content
        segment_sqls = [s.sql for s in filtered.segments]
        # At least one segment should mention status or deleted_at
        has_filter = any("status" in sql or "deleted" in sql for sql in segment_sqls)
        assert has_filter

    def test_time_granularities(self):
        """Test all time granularity detection."""
        time_gran = self.graph.get_model("time_granularities")
        assert time_gran is not None

        granularities = {
            "event_minute": "minute",
            "event_hour": "hour",
            "event_day": "day",
            "event_week": "week",
            "event_month": "month",
            "event_quarter": "quarter",
            "event_year": "year",
        }

        for dim_name, expected_gran in granularities.items():
            dim = time_gran.get_dimension(dim_name)
            assert dim is not None, f"Dimension {dim_name} not found"
            assert dim.type == "time", f"{dim_name} should be time type"
            assert dim.granularity == expected_gran, f"{dim_name} granularity mismatch"

    def test_boolean_patterns(self):
        """Test various boolean dimension patterns."""
        bool_patterns = self.graph.get_model("boolean_patterns")
        assert bool_patterns is not None

        # Direct boolean column might not be detected as boolean
        # (depends on column type inference)

        # Comparison operators should be boolean
        comparison_dims = [
            "is_positive",
            "is_negative",
            "is_zero",
            "is_not_zero",
            "is_large",
            "is_small",
        ]
        for dim_name in comparison_dims:
            dim = bool_patterns.get_dimension(dim_name)
            assert dim is not None, f"Dimension {dim_name} not found"
            assert dim.type == "boolean", f"{dim_name} should be boolean type"


def _is_passthrough_dimension(dim, primary_key: str) -> bool:
    """Check if a dimension is a passthrough (sql == name) that won't be exported."""
    if dim.name == primary_key:
        return True
    sql = (dim.sql or dim.name).replace("{model}.", "").strip()
    return sql == dim.name


class TestExportRoundtrip:
    """Test export and roundtrip functionality."""

    def test_edge_cases_roundtrip(self):
        """Test that edge cases can be exported and re-parsed.

        Note: Passthrough dimensions (where sql == name) are not exported to Malloy
        because Malloy auto-exposes table columns. We only compare non-passthrough
        dimensions in the roundtrip check.
        """
        import tempfile

        adapter = MalloyAdapter()
        graph1 = adapter.parse(Path("tests/fixtures/malloy/edge_cases.malloy"))

        # Export
        with tempfile.NamedTemporaryFile(suffix=".malloy", delete=False) as f:
            output_path = Path(f.name)

        try:
            adapter.export(graph1, output_path)

            # Re-parse
            graph2 = adapter.parse(output_path)

            # Compare model count
            assert len(graph2.models) == len(graph1.models)

            # Verify key models exist
            for model_name in graph1.models:
                assert model_name in graph2.models

                model1 = graph1.get_model(model_name)
                model2 = graph2.get_model(model_name)

                # Compare dimension count (excluding passthrough dimensions which are skipped in export)
                # Malloy auto-exposes table columns, so we don't export `name is name` patterns
                dims1_non_passthrough = [
                    d for d in model1.dimensions if not _is_passthrough_dimension(d, model1.primary_key)
                ]
                dims2_non_passthrough = [
                    d for d in model2.dimensions if not _is_passthrough_dimension(d, model2.primary_key)
                ]
                assert len(dims2_non_passthrough) == len(dims1_non_passthrough), (
                    f"Dimension count mismatch for {model_name}"
                )

                # Compare metric count
                assert len(model2.metrics) == len(model1.metrics), f"Metric count mismatch for {model_name}"

        finally:
            output_path.unlink()
