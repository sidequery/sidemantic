"""Tests for newer Rill metrics-view features.

Covers behavior aligned with rilldata/rill runtime/parser/parse_metrics_view.go:
- name derivation for dimensions/measures lacking an explicit `name`
- `property:` deprecated shorthand alias for `column:`
- `label:` deprecated alias for `display_name:`
- `time_comparison` measure type
- `per:` measure field selector
- parent / derived metrics views (parent, parent_dimensions, parent_measures)
- graceful handling of new sections: ai_instructions, cache, watermark,
  rollups, and lookup_table dimensions
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.rill import RillAdapter

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def parent_metrics():
    """Parse parent_metrics fixture (the source metrics view)."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/parent_metrics.yaml")
    return graph.models["parent_metrics"]


@pytest.fixture
def derived_metrics():
    """Parse derived_metrics fixture (a parent/derived metrics view)."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/derived_metrics.yaml")
    return graph.models["derived_metrics"]


# =============================================================================
# PARENT METRICS VIEW (label/property aliases, time_comparison, per, lookup)
# =============================================================================


class TestParentMetricsParsing:
    """Tests for parent_metrics.yaml modern-feature coverage."""

    def test_parses_without_error(self):
        """Fixture parses despite ai_instructions/cache/watermark/rollups."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/parent_metrics.yaml")
        assert "parent_metrics" in graph.models

    def test_property_shorthand_alias(self, parent_metrics):
        """property: resolves to a column for the dimension SQL."""
        dims = {d.name: d for d in parent_metrics.dimensions}
        assert dims["region"].sql == "region_code"

    def test_dimension_label_alias(self, parent_metrics):
        """label: is read as the deprecated alias for display_name:."""
        dims = {d.name: d for d in parent_metrics.dimensions}
        assert dims["region"].label == "Region"
        assert dims["channel"].label == "Sales Channel"

    def test_lookup_table_dimension_preserved(self, parent_metrics):
        """lookup_table dimensions are kept (not dropped) with lookup metadata."""
        dims = {d.name: d for d in parent_metrics.dimensions}
        assert "country_name" in dims
        meta = dims["country_name"].meta
        assert meta["rill_lookup_table"] == "countries"
        assert meta["rill_lookup_key_column"] == "country_code"
        assert meta["rill_lookup_value_column"] == "country_name"

    def test_measure_label_alias(self, parent_metrics):
        """label: is read as the deprecated alias for display_name: on measures."""
        metrics = {m.name: m for m in parent_metrics.metrics}
        assert metrics["revenue"].label == "Total Revenue"

    def test_simple_measure(self, parent_metrics):
        """Simple SUM/COUNT measures still decompose correctly."""
        metrics = {m.name: m for m in parent_metrics.metrics}
        assert metrics["revenue"].agg == "sum"
        assert metrics["revenue"].sql == "amount"
        assert metrics["orders"].agg == "count"

    def test_per_measure(self, parent_metrics):
        """`per:` field selector is preserved in metadata and promotes to derived."""
        metrics = {m.name: m for m in parent_metrics.metrics}
        m = metrics["revenue_per_region"]
        assert m.type == "derived"
        assert m.meta["rill_per"] == "region"

    def test_time_comparison_measure(self, parent_metrics):
        """time_comparison measures parse and record the Rill type in metadata."""
        metrics = {m.name: m for m in parent_metrics.metrics}
        m = metrics["revenue_prev_period"]
        # Mapped to derived (expression over a base measure) with type recorded.
        assert m.type == "derived"
        assert m.meta["rill_type"] == "time_comparison"
        assert m.sql == "revenue"

    def test_ai_instructions_cache_watermark_ignored(self):
        """ai_instructions / cache / watermark / rollups do not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/parent_metrics.yaml")
        assert len(graph.models) == 1


# =============================================================================
# DERIVED METRICS VIEW (parent, parent_dimensions, parent_measures)
# =============================================================================


class TestDerivedMetricsParsing:
    """Tests for derived_metrics.yaml (parent/derived metrics view)."""

    def test_parses_without_error(self):
        """A derived metrics view (parent: ...) parses into a model."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/derived_metrics.yaml")
        assert "derived_metrics" in graph.models

    def test_parent_linkage_in_metadata(self, derived_metrics):
        """parent / parent_dimensions / parent_measures captured in metadata."""
        meta = derived_metrics.meta
        assert meta["rill_parent"] == "parent_metrics"
        assert meta["rill_parent_dimensions"] == ["region", "channel"]
        assert meta["rill_parent_measures"] == ["revenue", "orders"]

    def test_derived_view_has_no_own_fields(self, derived_metrics):
        """A derived view defines no dimensions/measures of its own."""
        assert len(derived_metrics.dimensions) == 0
        assert len(derived_metrics.metrics) == 0


# =============================================================================
# UNIT TESTS: name derivation, ignore flag, time_comparison promotion
# =============================================================================


def _parse_inline(rill_yaml: dict):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)
    try:
        return RillAdapter().parse(temp_path)
    finally:
        temp_path.unlink()


def test_property_alias_with_name_fallback():
    """property: provides both the SQL and (when name is absent) the name."""
    graph = _parse_inline(
        {
            "type": "metrics_view",
            "name": "test",
            "model": "m",
            "dimensions": [{"property": "publisher"}],
        }
    )
    dims = graph.models["test"].dimensions
    assert len(dims) == 1
    assert dims[0].name == "publisher"
    assert dims[0].sql == "publisher"


def test_column_and_property_both_set_prefers_column():
    """When both column and property are set, column wins (Rill behavior)."""
    graph = _parse_inline(
        {
            "type": "metrics_view",
            "name": "test",
            "model": "m",
            "dimensions": [{"name": "d", "column": "col_a", "property": "col_b"}],
        }
    )
    dims = {d.name: d for d in graph.models["test"].dimensions}
    assert dims["d"].sql == "col_a"


def test_dimension_ignore_flag_skipped():
    """Dimensions with ignore: true are skipped."""
    graph = _parse_inline(
        {
            "type": "metrics_view",
            "name": "test",
            "model": "m",
            "dimensions": [
                {"name": "kept", "column": "a"},
                {"name": "dropped", "column": "b", "ignore": True},
            ],
        }
    )
    dim_names = {d.name for d in graph.models["test"].dimensions}
    assert "kept" in dim_names
    assert "dropped" not in dim_names


def test_measure_ignore_flag_skipped():
    """Measures with ignore: true are skipped."""
    graph = _parse_inline(
        {
            "type": "metrics_view",
            "name": "test",
            "model": "m",
            "measures": [
                {"name": "kept", "expression": "COUNT(*)"},
                {"name": "dropped", "expression": "SUM(x)", "ignore": True},
            ],
        }
    )
    metric_names = {m.name for m in graph.models["test"].metrics}
    assert "kept" in metric_names
    assert "dropped" not in metric_names


def test_empty_type_with_requires_is_derived():
    """A measure with no type but a requires list is promoted to derived."""
    graph = _parse_inline(
        {
            "type": "metrics_view",
            "name": "test",
            "model": "m",
            "measures": [
                {"name": "base", "expression": "SUM(x)"},
                {"name": "calc", "expression": "base * 2", "requires": ["base"]},
            ],
        }
    )
    metrics = {m.name: m for m in graph.models["test"].metrics}
    assert metrics["calc"].type == "derived"


def test_time_comparison_type_records_metadata():
    """time_comparison measures map to derived and record the original type."""
    graph = _parse_inline(
        {
            "type": "metrics_view",
            "name": "test",
            "model": "m",
            "measures": [
                {"name": "rev", "expression": "SUM(amount)"},
                {
                    "name": "rev_yoy",
                    "type": "time_comparison",
                    "expression": "rev",
                    "requires": ["rev"],
                },
            ],
        }
    )
    metrics = {m.name: m for m in graph.models["test"].metrics}
    assert metrics["rev_yoy"].type == "derived"
    assert metrics["rev_yoy"].meta["rill_type"] == "time_comparison"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
