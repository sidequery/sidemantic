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

from sidemantic import SemanticLayer
from sidemantic.adapters.rill import RillAdapter
from sidemantic.validation import validate_model

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
        """`per:` field selector is preserved in metadata.

        A `per` measure whose expression is a plain aggregation (`SUM(amount)`)
        keeps simple aggregate parsing rather than being promoted to a derived
        formula, so it still decomposes to agg/sql and stays queryable.
        """
        metrics = {m.name: m for m in parent_metrics.metrics}
        m = metrics["revenue_per_region"]
        assert m.type is None
        assert m.agg == "sum"
        assert m.sql == "amount"
        assert m.meta["rill_per"] == "region"

    def test_time_comparison_measure(self, parent_metrics):
        """time_comparison measures map to a native period-over-period comparison."""
        metrics = {m.name: m for m in parent_metrics.metrics}
        m = metrics["revenue_prev_period"]
        # Mapped to a native time_comparison so it queries as an actual comparison
        # (a derived metric would silently resolve to the current-period value).
        assert m.type == "time_comparison"
        assert m.base_metric == "revenue"
        assert m.comparison_type == "prior_period"
        assert m.meta["rill_type"] == "time_comparison"

    def test_ai_instructions_cache_watermark_ignored(self):
        """ai_instructions / cache / watermark / rollups do not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/parent_metrics.yaml")
        assert len(graph.models) == 1


# =============================================================================
# PER-MEASURE AGGREGATE PARSING (regression: per must not break decomposition)
# =============================================================================


class TestPerMeasureAggregateParsing:
    """A `per` measure whose expression is a plain aggregation stays simple.

    Regression for promoting every `per` measure to a derived formula: when the
    expression is an ordinary aggregation (e.g. `SUM(amount)`), keeping it as a
    derived metric leaves the raw `SUM(amount)` in the outer query while the CTE
    only projects the decomposed column. That generates invalid SQL whenever a
    source column name also exists as a measure name.
    """

    def _build_view(self, tmp_path: Path) -> Path:
        view = {
            "type": "metrics_view",
            "model": "sales",
            "dimensions": [{"name": "region", "column": "region"}],
            "measures": [
                # Measure name collides with the source column name `amount`.
                {"name": "amount", "expression": "SUM(amount)"},
                # `per` measure over the same plain aggregation.
                {"name": "amount_per_region", "expression": "SUM(amount)", "per": "region"},
            ],
        }
        path = tmp_path / "sales.yaml"
        path.write_text(yaml.dump(view, sort_keys=False))
        return path

    def test_per_aggregate_measure_keeps_simple_parsing(self, tmp_path):
        """`amount_per_region` decomposes to agg=sum/sql=amount, not a derived formula."""
        graph = RillAdapter().parse(self._build_view(tmp_path))
        metrics = {m.name: m for m in graph.models["sales"].metrics}

        per_measure = metrics["amount_per_region"]
        assert per_measure.type is None
        assert per_measure.agg == "sum"
        assert per_measure.sql == "amount"
        assert per_measure.meta["rill_per"] == "region"

    def test_per_aggregate_measure_compiles_and_runs(self, tmp_path):
        """The colliding measure-name scenario compiles to valid, executable SQL.

        Before the fix, `amount_per_region` was emitted as a derived `SUM(amount)`
        in the outer query referencing the raw `amount` column, which the inner CTE
        did not project under that name -> invalid SQL (Binder error).
        """
        graph = RillAdapter().parse(self._build_view(tmp_path))

        layer = SemanticLayer()
        layer.graph = graph
        layer.adapter.execute("CREATE TABLE sales (region VARCHAR, amount INT)")
        layer.adapter.execute("INSERT INTO sales VALUES ('east', 10), ('east', 5), ('west', 7)")

        rows = layer.query(
            metrics=["sales.amount", "sales.amount_per_region"],
            dimensions=["sales.region"],
            order_by=["sales.region"],
        ).fetchall()

        # region, amount, amount_per_region
        assert rows == [("east", 15, 15), ("west", 7, 7)]


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
        """A derived view defines no dimensions/measures of its own.

        When parsed standalone (no parent file available), it keeps zero own
        fields; parent inheritance only fills these in when the parent metrics
        view is present in the same project (see TestDerivedMetricsResolution).
        """
        assert len(derived_metrics.dimensions) == 0
        assert len(derived_metrics.metrics) == 0

    def test_derived_view_inherits_parent_as_table(self, derived_metrics):
        """A standalone parent-only view falls back to the parent name as its table."""
        assert derived_metrics.table == "parent_metrics"

    def test_derived_view_passes_validation(self, derived_metrics):
        """A parent-only derived view is a valid, importable model.

        Without a table/sql/dax/source_uri fallback, validate_model rejects the
        derived view, breaking the CLI import path (`sidemantic validate`).
        """
        assert validate_model(derived_metrics) == []

    def test_derived_view_importable_via_semantic_layer(self, derived_metrics):
        """The CLI-first path (add_model -> validate_model) accepts the view."""
        layer = SemanticLayer()
        layer.add_model(derived_metrics)
        assert "derived_metrics" in layer.graph.models


class TestDerivedMetricsResolution:
    """Parent inheritance when the parent metrics view is in the same project."""

    @pytest.fixture
    def resolved_derived(self):
        """Parse the rill fixtures directory so the parent is available."""
        graph = RillAdapter().parse("tests/fixtures/rill")
        return graph.models["derived_metrics"]

    def test_inherits_parent_data_source(self, resolved_derived):
        """The derived view adopts the parent's real table, not the parent name."""
        assert resolved_derived.table == "sales_model"

    def test_inherits_selected_dimensions(self, resolved_derived):
        """The parent_dimensions selectors (plus the inherited time dim) are materialized."""
        dim_names = {d.name for d in resolved_derived.dimensions}
        # region/channel are selected; order_date comes from the parent's
        # inherited default timeseries.
        assert {"region", "channel"} <= dim_names
        # country_name is on the parent but not selected, so it is excluded.
        assert "country_name" not in dim_names

    def test_inherits_selected_measures(self, resolved_derived):
        """Only the parent_measures selectors are materialized."""
        metric_names = {m.name for m in resolved_derived.metrics}
        assert metric_names == {"revenue", "orders"}

    def test_resolved_fields_are_queryable(self, resolved_derived):
        """Inherited measures resolve as fields on the derived model."""
        revenue = next(m for m in resolved_derived.metrics if m.name == "revenue")
        assert revenue.agg == "sum"
        assert revenue.sql == "amount"

    def test_inherited_fields_are_copies(self):
        """Inherited fields are deep copies, not shared parent instances."""
        graph = RillAdapter().parse("tests/fixtures/rill")
        parent = graph.models["parent_metrics"]
        derived = graph.models["derived_metrics"]
        parent_revenue = next(m for m in parent.metrics if m.name == "revenue")
        derived_revenue = next(m for m in derived.metrics if m.name == "revenue")
        assert derived_revenue is not parent_revenue

    def test_parent_linkage_preserved_after_resolution(self, resolved_derived):
        """Resolution keeps the parent linkage metadata intact."""
        assert resolved_derived.meta["rill_parent"] == "parent_metrics"

    def test_inherits_parent_default_timeseries(self, resolved_derived):
        """The derived view inherits the parent's default time dimension/grain."""
        assert resolved_derived.default_time_dimension == "order_date"
        assert resolved_derived.default_grain == "day"
        assert any(d.name == "order_date" for d in resolved_derived.dimensions)

    def test_inherited_metric_compiles(self):
        """An inherited measure compiles to SQL against the parent's table."""
        graph = RillAdapter().parse("tests/fixtures/rill")
        layer = SemanticLayer()
        for name in ("parent_metrics", "derived_metrics"):
            layer.add_model(graph.models[name])
        sql = layer.compile(metrics=["derived_metrics.revenue"], dimensions=["derived_metrics.region"])
        assert "sales_model" in sql
        assert "SUM" in sql.upper()


def _parse_project(files: dict[str, dict]):
    """Write each {filename: yaml-dict} into a temp dir and parse the directory."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        for filename, content in files.items():
            (tmp_path / filename).write_text(yaml.dump(content))
        return RillAdapter().parse(tmp_path)


class TestDerivedMetricsSelectorForms:
    """Parent selector normalization (Rill `*`, exclude, regex, omitted)."""

    PARENT = {
        "type": "metrics_view",
        "name": "parent",
        "model": "src",
        "timeseries": "day",
        "smallest_time_grain": "day",
        "dimensions": [{"name": "a", "column": "a"}, {"name": "b", "column": "b"}],
        "measures": [
            {"name": "m1", "expression": "SUM(x)"},
            {"name": "m2", "expression": "SUM(y)"},
        ],
    }

    def test_star_selector_inherits_all(self):
        """`parent_dimensions: '*'` / `parent_measures: '*'` inherit every field."""
        graph = _parse_project(
            {
                "parent.yaml": self.PARENT,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "parent_dimensions": "*",
                    "parent_measures": "*",
                },
            }
        )
        child = graph.models["child"]
        assert {d.name for d in child.dimensions} >= {"a", "b"}
        assert {m.name for m in child.metrics} == {"m1", "m2"}

    def test_omitted_selector_inherits_all(self):
        """A parent without explicit selectors inherits all parent fields."""
        graph = _parse_project(
            {
                "parent.yaml": self.PARENT,
                "child.yaml": {"type": "metrics_view", "name": "child", "parent": "parent"},
            }
        )
        child = graph.models["child"]
        assert {m.name for m in child.metrics} == {"m1", "m2"}

    def test_exclude_selector(self):
        """A mapping with `exclude` inherits everything except the listed names."""
        graph = _parse_project(
            {
                "parent.yaml": self.PARENT,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "parent_measures": {"exclude": ["m2"]},
                },
            }
        )
        child = graph.models["child"]
        assert {m.name for m in child.metrics} == {"m1"}

    def test_regex_selector(self):
        """A mapping with `regex` inherits names matching the pattern."""
        graph = _parse_project(
            {
                "parent.yaml": self.PARENT,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "parent_measures": {"regex": "^m1$"},
                },
            }
        )
        child = graph.models["child"]
        assert {m.name for m in child.metrics} == {"m1"}

    def test_expr_exclude_selector(self):
        """An `expr: '* EXCLUDE (...)'` selector excludes the named fields."""
        graph = _parse_project(
            {
                "parent.yaml": self.PARENT,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "parent_dimensions": {"expr": "* EXCLUDE (b)"},
                },
            }
        )
        dim_names = {d.name for d in graph.models["child"].dimensions}
        assert "a" in dim_names
        assert "b" not in dim_names

    def test_string_expr_exclude_selector(self):
        """A bare string `'* EXCLUDE (...)'` selector is also honored."""
        graph = _parse_project(
            {
                "parent.yaml": self.PARENT,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "parent_measures": "* EXCLUDE (m2)",
                },
            }
        )
        assert {m.name for m in graph.models["child"].metrics} == {"m1"}

    def test_inherits_parent_default_time_settings(self):
        """A derived view omitting timeseries inherits the parent's defaults."""
        graph = _parse_project(
            {
                "parent.yaml": self.PARENT,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "parent_measures": ["m1"],
                },
            }
        )
        child = graph.models["child"]
        assert child.default_time_dimension == "day"
        assert child.default_grain == "day"
        # The referenced time dimension is materialized so the default resolves.
        assert any(d.name == "day" for d in child.dimensions)

    def test_child_grain_override_preserved(self):
        """A child-only smallest_time_grain override is kept over the parent grain."""
        parent = dict(self.PARENT, smallest_time_grain="hour")
        graph = _parse_project(
            {
                "parent.yaml": parent,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "smallest_time_grain": "day",
                    "parent_measures": ["m1"],
                },
            }
        )
        child = graph.models["child"]
        # Parent grain is hourly; the child intends daily and must keep it.
        assert child.default_grain == "day"

    def test_hidden_dependencies_copied(self):
        """Selecting a derived measure pulls in its parent deps as hidden fields."""
        parent = dict(
            self.PARENT,
            measures=[
                {"name": "revenue", "expression": "SUM(x)"},
                {"name": "orders", "expression": "COUNT(*)"},
                {
                    "name": "aov",
                    "type": "derived",
                    "expression": "revenue / orders",
                    "requires": ["revenue", "orders"],
                },
            ],
        )
        graph = _parse_project(
            {
                "parent.yaml": parent,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "parent_measures": ["aov"],
                },
            }
        )
        metrics = {m.name: m for m in graph.models["child"].metrics}
        # aov is exposed; its dependencies are inherited but hidden.
        assert metrics["aov"].public is True
        assert metrics["revenue"].public is False
        assert metrics["orders"].public is False


class TestDerivedViewRejectsChildFields:
    """A derived view (parent: ...) must not define its own dimensions/measures.

    Rill's own validation rejects such a project: a derived view may only select
    inherited parent fields via parent_dimensions/parent_measures. The importer
    must reject it too rather than producing a model that exposes non-existent
    child fields against the parent table.
    """

    PARENT = {
        "type": "metrics_view",
        "name": "parent",
        "model": "src",
        "dimensions": [{"name": "a", "column": "a"}],
        "measures": [{"name": "m1", "expression": "SUM(x)"}],
    }

    def test_child_dimensions_rejected(self):
        """A parent view defining its own dimensions errors."""
        with pytest.raises(ValueError, match="defines its own dimensions"):
            _parse_project(
                {
                    "parent.yaml": self.PARENT,
                    "child.yaml": {
                        "type": "metrics_view",
                        "name": "child",
                        "parent": "parent",
                        "dimensions": [{"name": "bogus", "column": "bogus"}],
                    },
                }
            )

    def test_child_measures_rejected(self):
        """A parent view defining its own measures errors."""
        with pytest.raises(ValueError, match="defines its own measures"):
            _parse_project(
                {
                    "parent.yaml": self.PARENT,
                    "child.yaml": {
                        "type": "metrics_view",
                        "name": "child",
                        "parent": "parent",
                        "measures": [{"name": "bogus", "expression": "SUM(z)"}],
                    },
                }
            )

    def test_child_dimensions_and_measures_rejected(self):
        """Defining both child dimensions and measures is rejected together."""
        with pytest.raises(ValueError, match="dimensions and measures"):
            _parse_project(
                {
                    "parent.yaml": self.PARENT,
                    "child.yaml": {
                        "type": "metrics_view",
                        "name": "child",
                        "parent": "parent",
                        "dimensions": [{"name": "bogus", "column": "bogus"}],
                        "measures": [{"name": "bogus_m", "expression": "SUM(z)"}],
                    },
                }
            )

    def test_selector_only_child_still_allowed(self):
        """A derived view that only selects parent fields stays valid."""
        graph = _parse_project(
            {
                "parent.yaml": self.PARENT,
                "child.yaml": {
                    "type": "metrics_view",
                    "name": "child",
                    "parent": "parent",
                    "parent_measures": ["m1"],
                },
            }
        )
        assert {m.name for m in graph.models["child"].metrics} == {"m1"}


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
    """time_comparison measures map to a native comparison over the base measure."""
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
    m = metrics["rev_yoy"]
    assert m.type == "time_comparison"
    assert m.base_metric == "rev"
    assert m.comparison_type == "prior_period"
    assert m.meta["rill_type"] == "time_comparison"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
