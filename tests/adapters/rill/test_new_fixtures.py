"""Tests for permissively licensed Rill fixtures from rilldata repos.

Sources (all Apache 2.0):
- rilldata/rill: ad_bids_advanced, ad_bids_policy, bids_explore, bids_canvas,
  metrics_geospatial, metrics_annotations, metrics_null_filling
- rilldata/rill-nyc-taxi: nyc_trips_dashboard
- rilldata/clickhouse-system-analytics: query_log_metrics

Tests are permissive: parse without errors, check counts, verify key names.
"""

import pytest

from sidemantic.adapters.rill import RillAdapter

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def ad_bids_advanced():
    """Parse ad_bids_advanced fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/ad_bids_advanced.yaml")
    return graph.models["ad_bids_advanced"]


@pytest.fixture
def ad_bids_policy():
    """Parse ad_bids_policy fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/ad_bids_policy.yaml")
    return graph.models["ad_bids_policy"]


@pytest.fixture
def metrics_geospatial():
    """Parse metrics_geospatial fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/metrics_geospatial.yaml")
    return graph.models["metrics_geospatial"]


@pytest.fixture
def metrics_annotations():
    """Parse metrics_annotations fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/metrics_annotations.yaml")
    return graph.models["metrics_annotations"]


@pytest.fixture
def metrics_null_filling():
    """Parse metrics_null_filling fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/metrics_null_filling.yaml")
    return graph.models["metrics_null_filling"]


@pytest.fixture
def query_log_metrics():
    """Parse query_log_metrics fixture."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/query_log_metrics.yaml")
    return graph.models["query_log_metrics"]


# =============================================================================
# AD BIDS ADVANCED (property shorthand, unnest, regexp, rolling window)
# =============================================================================


class TestAdBidsAdvancedParsing:
    """Tests for ad_bids_advanced.yaml from rilldata/rill testdata."""

    def test_parses_without_error(self):
        """Fixture parses successfully."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/ad_bids_advanced.yaml")
        assert "ad_bids_advanced" in graph.models

    def test_model_name_from_filename(self, ad_bids_advanced):
        """Model name derived from filename since YAML has no 'name' key."""
        assert ad_bids_advanced.table == "ad_bids"

    def test_dimension_count(self, ad_bids_advanced):
        """4 expression dims + 1 auto-created timestamp = 5 total.

        Dimensions using 'property:' shorthand are skipped (no expression/column).
        """
        assert len(ad_bids_advanced.dimensions) == 5

    def test_property_shorthand_dims_skipped(self, ad_bids_advanced):
        """property: field shorthand is not recognized; those dims are skipped."""
        dim_names = {d.name for d in ad_bids_advanced.dimensions}
        # pub, dom, nolabel_pub use property: which maps to neither column nor expression
        assert "pub" not in dim_names
        assert "dom" not in dim_names
        assert "nolabel_pub" not in dim_names

    def test_expression_dimensions_parsed(self, ad_bids_advanced):
        """Dimensions with expression: field are parsed correctly."""
        dim_names = {d.name for d in ad_bids_advanced.dimensions}
        assert "space_label" in dim_names
        assert "domain_parts" in dim_names
        assert "tld" in dim_names
        assert "null_publisher" in dim_names

    def test_regexp_expression_preserved(self, ad_bids_advanced):
        """regexp_extract expression preserved in sql."""
        dims = {d.name: d for d in ad_bids_advanced.dimensions}
        assert "regexp_extract" in dims["tld"].sql

    def test_string_split_expression_preserved(self, ad_bids_advanced):
        """string_split expression preserved (unnest dimension)."""
        dims = {d.name: d for d in ad_bids_advanced.dimensions}
        assert "string_split" in dims["domain_parts"].sql

    def test_null_handling_expression(self, ad_bids_advanced):
        """CASE WHEN publisher is null expression preserved."""
        dims = {d.name: d for d in ad_bids_advanced.dimensions}
        assert "publisher is null" in dims["null_publisher"].sql

    def test_dimension_labels(self, ad_bids_advanced):
        """display_name maps to label for expression dimensions."""
        dims = {d.name: d for d in ad_bids_advanced.dimensions}
        assert dims["space_label"].label == "Space Label"
        assert dims["domain_parts"].label == "Domain Parts"
        assert dims["tld"].label == "TLD"

    def test_timeseries_auto_created(self, ad_bids_advanced):
        """timestamp auto-created as time dimension."""
        time_dims = [d for d in ad_bids_advanced.dimensions if d.type == "time"]
        assert len(time_dims) == 1
        assert time_dims[0].name == "timestamp"

    def test_default_time_dimension(self, ad_bids_advanced):
        """timeseries: timestamp sets default_time_dimension."""
        assert ad_bids_advanced.default_time_dimension == "timestamp"

    def test_empty_smallest_time_grain(self, ad_bids_advanced):
        """Empty string smallest_time_grain defaults to day."""
        assert ad_bids_advanced.default_grain == "day"

    def test_metric_count(self, ad_bids_advanced):
        """4 measures parsed (measure without name is skipped)."""
        assert len(ad_bids_advanced.metrics) == 4

    def test_measure_without_name_skipped(self, ad_bids_advanced):
        """Measure with display_name but no name field is skipped."""
        metric_names = {m.name for m in ad_bids_advanced.metrics}
        # The "Average bid price" measure has no name field
        assert len(metric_names) == 4

    def test_count_star_metric(self, ad_bids_advanced):
        """count(*) decomposed correctly."""
        metrics = {m.name: m for m in ad_bids_advanced.metrics}
        assert metrics["bids"].agg == "count"

    def test_avg_metrics(self, ad_bids_advanced):
        """avg(bid_price) decomposed correctly."""
        metrics = {m.name: m for m in ad_bids_advanced.metrics}
        assert metrics["m1"].agg == "avg"
        assert metrics["m1"].sql == "bid_price"
        assert metrics["bid_price"].agg == "avg"
        assert metrics["bid_price"].sql == "bid_price"

    def test_rolling_window_metric(self, ad_bids_advanced):
        """Rolling window measure parsed as cumulative type."""
        metrics = {m.name: m for m in ad_bids_advanced.metrics}
        m = metrics["bids_1day_rolling_avg"]
        assert m.type == "cumulative"
        assert m.agg == "avg"
        assert m.window_order == "timestamp"
        assert "INTERVAL 1 DAY" in m.window_frame

    def test_format_presets(self, ad_bids_advanced):
        """humanize format_preset maps to decimal_0."""
        metrics = {m.name: m for m in ad_bids_advanced.metrics}
        assert metrics["bids"].value_format_name == "decimal_0"
        assert metrics["m1"].value_format_name == "decimal_0"


# =============================================================================
# AD BIDS POLICY (security, special char names, expression dims)
# =============================================================================


class TestAdBidsPolicyParsing:
    """Tests for ad_bids_policy.yaml from rilldata/rill testdata."""

    def test_parses_without_error(self):
        """Fixture parses successfully despite security section."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/ad_bids_policy.yaml")
        assert "ad_bids_policy" in graph.models

    def test_model_table(self, ad_bids_policy):
        """model: ad_bids_mini captured as table."""
        assert ad_bids_policy.table == "ad_bids_mini"

    def test_dimension_count(self, ad_bids_policy):
        """1 expression dim + 1 auto-created timestamp = 2.

        Dimension without name (Domain) is skipped.
        """
        assert len(ad_bids_policy.dimensions) == 2

    def test_dimension_without_name_skipped(self, ad_bids_policy):
        """Dimension with display_name but no name is skipped."""
        dim_names = {d.name for d in ad_bids_policy.dimensions}
        assert "Domain" not in dim_names

    def test_expression_dimension(self, ad_bids_policy):
        """upper(publisher) expression parsed."""
        dims = {d.name: d for d in ad_bids_policy.dimensions}
        assert dims["publisher_dim"].sql == "upper(publisher)"
        assert dims["publisher_dim"].label == "Publisher"

    def test_metric_count(self, ad_bids_policy):
        """4 measures with special character names parsed."""
        assert len(ad_bids_policy.metrics) == 4

    def test_special_char_metric_names(self, ad_bids_policy):
        """Measure names with special characters (apostrophes, quotes, spaces)."""
        metric_names = {m.name for m in ad_bids_policy.metrics}
        assert "bid's number" in metric_names
        assert "total volume" in metric_names
        assert "total impressions" in metric_names
        assert 'total click"s' in metric_names

    def test_special_char_metric_aggregations(self, ad_bids_policy):
        """Metrics with special names still have correct aggregations."""
        metrics = {m.name: m for m in ad_bids_policy.metrics}
        assert metrics["bid's number"].agg == "count"
        assert metrics["total volume"].agg == "sum"
        assert metrics["total volume"].sql == "volume"
        assert metrics["total impressions"].agg == "sum"
        assert metrics["total impressions"].sql == "impressions"
        assert metrics['total click"s'].agg == "sum"
        assert metrics['total click"s'].sql == "clicks"

    def test_security_section_ignored(self):
        """security with row_filter and exclude does not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/ad_bids_policy.yaml")
        assert len(graph.models) == 1

    def test_timeseries_auto_created(self, ad_bids_policy):
        """timestamp auto-created as time dimension."""
        time_dims = [d for d in ad_bids_policy.dimensions if d.type == "time"]
        assert len(time_dims) == 1
        assert time_dims[0].name == "timestamp"


# =============================================================================
# BIDS EXPLORE (type: explore, skipped by adapter)
# =============================================================================


class TestBidsExploreParsing:
    """Tests for bids_explore.yaml (type: explore) from rilldata/rill."""

    def test_explore_type_skipped(self):
        """type: explore is not metrics_view, so it is skipped."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/bids_explore.yaml")
        assert len(graph.models) == 0

    def test_explore_parses_without_error(self):
        """The file parses without raising any exceptions."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/bids_explore.yaml")
        assert graph is not None


# =============================================================================
# BIDS CANVAS (type: canvas, skipped by adapter)
# =============================================================================


class TestBidsCanvasParsing:
    """Tests for bids_canvas.yaml (type: canvas) from rilldata/rill."""

    def test_canvas_type_skipped(self):
        """type: canvas is not metrics_view, so it is skipped."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/bids_canvas.yaml")
        assert len(graph.models) == 0

    def test_canvas_parses_without_error(self):
        """The file parses without raising any exceptions."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/bids_canvas.yaml")
        assert graph is not None


# =============================================================================
# METRICS GEOSPATIAL (geo dimension type, column-only dims)
# =============================================================================


class TestMetricsGeospatialParsing:
    """Tests for metrics_geospatial.yaml from rilldata/rill resolvers testdata."""

    def test_parses_without_error(self):
        """Fixture parses successfully."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/metrics_geospatial.yaml")
        assert "metrics_geospatial" in graph.models

    def test_dimension_count(self, metrics_geospatial):
        """Only 1 dimension parsed: time auto-created from timeseries.

        Dims with column but no name are skipped (country, coordinates, area).
        """
        assert len(metrics_geospatial.dimensions) == 1

    def test_column_only_dims_skipped(self, metrics_geospatial):
        """Dimensions with column but no name field are skipped."""
        dim_names = {d.name for d in metrics_geospatial.dimensions}
        assert "country" not in dim_names
        assert "coordinates" not in dim_names
        assert "area" not in dim_names

    def test_timeseries_auto_created(self, metrics_geospatial):
        """time dimension auto-created from timeseries field."""
        time_dims = [d for d in metrics_geospatial.dimensions if d.type == "time"]
        assert len(time_dims) == 1
        assert time_dims[0].name == "time"

    def test_metric_count(self, metrics_geospatial):
        """2 measures parsed: count and sum."""
        assert len(metrics_geospatial.metrics) == 2

    def test_metrics(self, metrics_geospatial):
        """count(*) and sum(val) decomposed correctly."""
        metrics = {m.name: m for m in metrics_geospatial.metrics}
        assert metrics["count"].agg == "count"
        assert metrics["sum"].agg == "sum"
        assert metrics["sum"].sql == "val"


# =============================================================================
# METRICS ANNOTATIONS (annotations list, multiple measures)
# =============================================================================


class TestMetricsAnnotationsParsing:
    """Tests for metrics_annotations.yaml from rilldata/rill resolvers testdata."""

    def test_parses_without_error(self):
        """Fixture parses successfully despite annotations section."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/metrics_annotations.yaml")
        assert "metrics_annotations" in graph.models

    def test_dimension_count(self, metrics_annotations):
        """Only timeseries auto-dim; column-only dim (country) is skipped."""
        assert len(metrics_annotations.dimensions) == 1

    def test_metric_count(self, metrics_annotations):
        """3 measures parsed: count, sum, mes_for_grain_annotation."""
        assert len(metrics_annotations.metrics) == 3

    def test_metric_names(self, metrics_annotations):
        """All measure names present."""
        metric_names = {m.name for m in metrics_annotations.metrics}
        assert metric_names == {"count", "sum", "mes_for_grain_annotation"}

    def test_count_metrics(self, metrics_annotations):
        """Both count(*) measures decomposed correctly."""
        metrics = {m.name: m for m in metrics_annotations.metrics}
        assert metrics["count"].agg == "count"
        assert metrics["mes_for_grain_annotation"].agg == "count"

    def test_annotations_section_ignored(self):
        """annotations: list does not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/metrics_annotations.yaml")
        assert len(graph.models) == 1


# =============================================================================
# METRICS NULL FILLING (treat_nulls_as, rolling windows, recursive measures)
# =============================================================================


class TestMetricsNullFillingParsing:
    """Tests for metrics_null_filling.yaml from rilldata/rill resolvers testdata."""

    def test_parses_without_error(self):
        """Fixture parses successfully despite treat_nulls_as fields."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/metrics_null_filling.yaml")
        assert "metrics_null_filling" in graph.models

    def test_dimension_count(self, metrics_null_filling):
        """Only timeseries auto-dim; column-only dim (country) is skipped."""
        assert len(metrics_null_filling.dimensions) == 1

    def test_metric_count(self, metrics_null_filling):
        """8 measures parsed."""
        assert len(metrics_null_filling.metrics) == 8

    def test_metric_names(self, metrics_null_filling):
        """All measure names present."""
        metric_names = {m.name for m in metrics_null_filling.metrics}
        expected = {
            "count",
            "sum",
            "sum_nullable",
            "sum_nullable_2day_rolling_avg_nullable",
            "sum_nullable_2day_rolling_avg",
            "sum_2day_rolling_avg_nullable",
            "recursive_measure",
            "recursive_measure_1",
        }
        assert metric_names == expected

    def test_simple_metrics(self, metrics_null_filling):
        """count(*) and sum(val) decomposed correctly."""
        metrics = {m.name: m for m in metrics_null_filling.metrics}
        assert metrics["count"].agg == "count"
        assert metrics["sum"].agg == "sum"
        assert metrics["sum"].sql == "val"
        assert metrics["sum_nullable"].agg == "sum"
        assert metrics["sum_nullable"].sql == "val"

    def test_rolling_window_metrics(self, metrics_null_filling):
        """Rolling window measures parsed as cumulative type."""
        metrics = {m.name: m for m in metrics_null_filling.metrics}

        m = metrics["sum_nullable_2day_rolling_avg_nullable"]
        assert m.type == "cumulative"
        assert m.agg == "avg"
        assert m.window_order == "time"
        assert "INTERVAL 2 DAY" in m.window_frame

        m = metrics["sum_nullable_2day_rolling_avg"]
        assert m.type == "cumulative"
        assert m.window_order == "time"

        m = metrics["sum_2day_rolling_avg_nullable"]
        assert m.type == "cumulative"
        assert m.window_order == "time"

    def test_recursive_derived_measures(self, metrics_null_filling):
        """Recursive/derived measures parsed with requires."""
        metrics = {m.name: m for m in metrics_null_filling.metrics}

        m = metrics["recursive_measure"]
        assert m.type == "derived"
        assert "sum_nullable_2day_rolling_avg" in m.sql
        assert "sum" in m.sql

        m = metrics["recursive_measure_1"]
        assert m.type == "derived"
        assert "sum_nullable_2day_rolling_avg_nullable" in m.sql

    def test_treat_nulls_as_ignored(self):
        """treat_nulls_as field does not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/metrics_null_filling.yaml")
        assert len(graph.models) == 1


# =============================================================================
# NYC TRIPS DASHBOARD (legacy format, title:, label:, no type: field)
# =============================================================================


class TestNycTripsDashboardParsing:
    """Tests for nyc_trips_dashboard.yaml from rilldata/rill-nyc-taxi."""

    def test_legacy_format_skipped(self):
        """Legacy format without type: metrics_view is skipped.

        The NYC trips dashboard uses title: instead of display_name
        and has no type: field, so the adapter correctly skips it.
        """
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/nyc_trips_dashboard.yaml")
        assert len(graph.models) == 0

    def test_parses_without_error(self):
        """The file parses without raising any exceptions."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/nyc_trips_dashboard.yaml")
        assert graph is not None


# =============================================================================
# QUERY LOG METRICS (quantiles, large dim set, CASE WHEN, unit conversions)
# =============================================================================


class TestQueryLogMetricsParsing:
    """Tests for query_log_metrics.yaml from rilldata/clickhouse-system-analytics."""

    def test_parses_without_error(self):
        """Fixture parses successfully."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/query_log_metrics.yaml")
        assert "query_log_metrics" in graph.models

    def test_model_table(self, query_log_metrics):
        """model: rill_query_log captured as table."""
        assert query_log_metrics.table == "rill_query_log"

    def test_large_dimension_set(self, query_log_metrics):
        """14 explicit dimensions + 1 auto-created event_time = 15."""
        assert len(query_log_metrics.dimensions) == 15

    def test_dimension_names(self, query_log_metrics):
        """All 14 business dimensions present."""
        dim_names = {d.name for d in query_log_metrics.dimensions}
        expected = {
            "hostname",
            "type",
            "event_date",
            "query_start_time",
            "current_database",
            "query_kind",
            "user",
            "query_id",
            "query",
            "client_name",
            "interface",
            "exception",
            "exception_code",
            "query_table_name",
        }
        assert expected.issubset(dim_names)

    def test_dimension_labels(self, query_log_metrics):
        """display_name maps to label on dimensions."""
        dims = {d.name: d for d in query_log_metrics.dimensions}
        assert dims["hostname"].label == "Hostname"
        assert dims["type"].label == "Query Event Type"
        assert dims["query_table_name"].label == "Query Table Name"

    def test_dimension_descriptions(self, query_log_metrics):
        """Descriptions are preserved."""
        dims = {d.name: d for d in query_log_metrics.dimensions}
        assert "server executing" in dims["hostname"].description.lower()
        assert "unique identifier" in dims["query_id"].description.lower()

    def test_column_expression_mapping(self, query_log_metrics):
        """column: table_name maps to different sql than dimension name."""
        dims = {d.name: d for d in query_log_metrics.dimensions}
        assert dims["query_table_name"].sql == "table_name"

    def test_timeseries_auto_created(self, query_log_metrics):
        """event_time auto-created as time dimension."""
        time_dims = [d for d in query_log_metrics.dimensions if d.type == "time"]
        assert len(time_dims) == 1
        assert time_dims[0].name == "event_time"

    def test_default_time_dimension(self, query_log_metrics):
        """timeseries: event_time sets default_time_dimension."""
        assert query_log_metrics.default_time_dimension == "event_time"

    def test_metric_count(self, query_log_metrics):
        """14 measures parsed."""
        assert len(query_log_metrics.metrics) == 14

    def test_metric_names(self, query_log_metrics):
        """All measure names present."""
        metric_names = {m.name for m in query_log_metrics.metrics}
        expected = {
            "avg_query_duration_ms",
            "min_query_duration_ms",
            "max_query_duration_ms",
            "p99_query_duration_ms",
            "p95_query_duration_ms",
            "p90_query_duration_ms",
            "successful_queries",
            "failed_queries",
            "total_read_rows",
            "total_read_bytes_gb",
            "total_written_rows",
            "total_memory_usage_mb",
            "profile_query",
            "profile_select_query",
        }
        assert metric_names == expected

    def test_simple_aggregations(self, query_log_metrics):
        """MIN, MAX, SUM decomposed correctly."""
        metrics = {m.name: m for m in query_log_metrics.metrics}
        assert metrics["min_query_duration_ms"].agg == "min"
        assert metrics["min_query_duration_ms"].sql == "query_duration_ms"
        assert metrics["max_query_duration_ms"].agg == "max"
        assert metrics["total_read_rows"].agg == "sum"
        assert metrics["total_read_rows"].sql == "read_rows"
        assert metrics["total_written_rows"].agg == "sum"
        assert metrics["total_written_rows"].sql == "written_rows"

    def test_sum_division_ratio(self, query_log_metrics):
        """SUM(x)/COUNT() kept as full expression."""
        metrics = {m.name: m for m in query_log_metrics.metrics}
        m = metrics["avg_query_duration_ms"]
        assert m.agg is None
        assert "SUM(query_duration_ms)" in m.sql
        assert "COUNT()" in m.sql

    def test_quantiles_expression(self, query_log_metrics):
        """quantiles() ClickHouse function kept as full expression."""
        metrics = {m.name: m for m in query_log_metrics.metrics}
        for name in ["p99_query_duration_ms", "p95_query_duration_ms", "p90_query_duration_ms"]:
            m = metrics[name]
            assert m.agg is None
            assert "quantiles" in m.sql

    def test_case_when_in_sum(self, query_log_metrics):
        """SUM(CASE WHEN ...) decomposed as sum with CASE expression."""
        metrics = {m.name: m for m in query_log_metrics.metrics}
        assert metrics["successful_queries"].agg == "sum"
        assert "CASE WHEN" in metrics["successful_queries"].sql
        assert "exception_code" in metrics["successful_queries"].sql
        assert metrics["failed_queries"].agg == "sum"

    def test_unit_conversion_expressions(self, query_log_metrics):
        """SUM(x) / 1024 / 1024 / 1024 kept as full expression."""
        metrics = {m.name: m for m in query_log_metrics.metrics}
        m = metrics["total_read_bytes_gb"]
        assert m.agg is None
        assert "1024" in m.sql

        m = metrics["total_memory_usage_mb"]
        assert m.agg is None
        assert "1024" in m.sql

    def test_metric_labels(self, query_log_metrics):
        """display_name maps to label on metrics."""
        metrics = {m.name: m for m in query_log_metrics.metrics}
        assert metrics["avg_query_duration_ms"].label == "Avg Query Duration (ms)"
        assert metrics["p99_query_duration_ms"].label == "P99 Query Duration (ms)"
        assert metrics["profile_query"].label == "Query Count"

    def test_metric_descriptions(self, query_log_metrics):
        """Metric descriptions are preserved."""
        metrics = {m.name: m for m in query_log_metrics.metrics}
        assert "average duration" in metrics["avg_query_duration_ms"].description.lower()
        assert "gigabytes" in metrics["total_read_bytes_gb"].description.lower()
        assert "megabytes" in metrics["total_memory_usage_mb"].description.lower()

    def test_version_field_ignored(self):
        """version: 1 does not break parsing."""
        adapter = RillAdapter()
        graph = adapter.parse("tests/fixtures/rill/query_log_metrics.yaml")
        assert len(graph.models) == 1

    def test_sum_column_reference_metrics(self, query_log_metrics):
        """SUM(Query) and SUM(SelectQuery) use column names as-is."""
        metrics = {m.name: m for m in query_log_metrics.metrics}
        assert metrics["profile_query"].agg == "sum"
        assert metrics["profile_query"].sql == "Query"
        assert metrics["profile_select_query"].agg == "sum"
        assert metrics["profile_select_query"].sql == "SelectQuery"
