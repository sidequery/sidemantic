"""Test validation and error handling."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.validation import (
    MetricValidationError,
    ModelValidationError,
    QueryValidationError,
    validate_model_warnings,
)
from sidemantic.validation_runner import validate_directory


def test_model_has_default_primary_key(layer):
    """Test that models have a default primary key."""
    # Model without explicit primary_key should default to "id"
    model = Model(
        name="orders",
        table="orders",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[],
    )

    layer.add_model(model)
    assert model.primary_key == "id"


def test_model_validation_no_table(layer):
    """Test that models without a physical, SQL, DAX, or source URI definition are rejected."""
    invalid_model = Model(
        name="orders",
        primary_key="id",
        dimensions=[],
        metrics=[],
    )

    with pytest.raises(ModelValidationError) as exc_info:
        layer.add_model(invalid_model)

    assert "must have one of 'table', 'sql', 'dax', or 'source_uri' defined" in str(exc_info.value)


def test_source_uri_model_validates_but_python_compile_is_not_supported(layer):
    """source_uri-only models can load, but Python SQL generation cannot query them yet."""
    layer.add_model(
        Model(
            name="events",
            source_uri="s3://warehouse/events.parquet",
            primary_key="event_id",
            dimensions=[],
            metrics=[Metric(name="event_count", agg="count")],
        )
    )

    with pytest.raises(ValueError) as exc_info:
        layer.compile(metrics=["events.event_count"])

    message = str(exc_info.value)
    assert "source_uri" in message
    assert "Python SQL generation does not load source_uri data" in message


def test_metric_validation_simple_no_measure():
    """Test that Pydantic rejects invalid metric types at model creation time."""
    # Try to create metric with invalid type - should fail at Pydantic validation
    with pytest.raises(Exception) as exc_info:
        Metric(name="bad_metric", type="invalid_type")

    assert "literal_error" in str(exc_info.value).lower() or "validation" in str(exc_info.value).lower()


def test_metric_validation_measure_not_found(layer):
    """Test that metrics referencing non-existent measures are rejected."""
    # Add valid model
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    # Try to reference non-existent measure
    invalid_metric = Metric(name="bad_metric", sql="orders.nonexistent")

    with pytest.raises(MetricValidationError) as exc_info:
        layer.add_metric(invalid_metric)

    assert "measure 'nonexistent' not found" in str(exc_info.value)


def test_metric_validation_self_reference(layer):
    """Test that self-referencing metrics are detected."""
    # Add model
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    # Try to add self-referencing metric
    # Note: dependencies are auto-detected from expr now
    invalid_metric = Metric(
        name="metric_a",
        type="derived",
        sql="metric_a + 1",
    )

    with pytest.raises(MetricValidationError) as exc_info:
        layer.add_metric(invalid_metric)

    assert "cannot reference itself" in str(exc_info.value)


def test_query_validation_metric_not_found(layer):
    """Test that queries with non-existent metrics fail validation."""
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    with pytest.raises(QueryValidationError) as exc_info:
        layer.compile(metrics=["nonexistent_metric"], dimensions=["orders.status"])

    assert "Metric 'nonexistent_metric' not found" in str(exc_info.value)


def test_query_validation_accepts_multidot_graph_metric_name(layer):
    """Exact graph metric names with multiple dots should not be split as model.metric."""
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical", sql="status")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    layer.add_metric(Metric(name="company.sales.revenue", sql="orders.revenue"))

    sql = layer.compile(metrics=["company.sales.revenue"], dimensions=["orders.status"])

    assert '"company.sales.revenue"' in sql
    assert "amount AS revenue_raw" in sql


def test_graph_metric_exact_name_wins_over_model_metric_reference(layer):
    """Exact graph metric names should resolve before model.metric interpretation."""
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical", sql="status")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    layer.add_metric(Metric(name="orders.revenue", sql="SUM(orders.amount) * 2"))

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])

    assert 'AS "orders.revenue"' in sql
    assert "* 2" in sql


def test_query_validation_dimension_not_found(layer):
    """Test that queries with non-existent dimensions fail validation."""
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    with pytest.raises(QueryValidationError) as exc_info:
        layer.compile(metrics=["orders.revenue"], dimensions=["orders.nonexistent"])

    assert "Dimension 'nonexistent' not found" in str(exc_info.value)


def test_query_validation_no_join_path(layer):
    """Test that queries requiring non-existent joins fail validation."""
    # Add two disconnected models
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    layer.add_model(
        Model(
            name="products",
            table="products",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
            metrics=[],
        )
    )

    with pytest.raises(QueryValidationError) as exc_info:
        layer.compile(metrics=["orders.revenue"], dimensions=["products.category"])

    # Order of models in error message may vary
    assert "No join path found between models" in str(exc_info.value)
    assert "'orders'" in str(exc_info.value)
    assert "'products'" in str(exc_info.value)


def test_query_validation_invalid_granularity(layer):
    """Test that invalid time granularities are rejected."""
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="created_at")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    with pytest.raises(QueryValidationError) as exc_info:
        layer.compile(metrics=["orders.revenue"], dimensions=["orders.order_date__invalid_granularity"])

    assert "Invalid time granularity 'invalid_granularity'" in str(exc_info.value)


def _model_with_preagg(preagg: PreAggregation) -> Model:
    """Build a minimal valid model carrying a single pre-aggregation."""
    return Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[Metric(name="order_count", agg="count")],
        pre_aggregations=[preagg],
    )


@pytest.mark.parametrize(
    "preagg_type, needle",
    [
        ("rollup_join", "type 'rollup_join' is parsed but not executed"),
        ("lambda", "type 'lambda' is parsed but not executed"),
    ],
)
def test_inert_preagg_type_warns(preagg_type, needle):
    """Pre-aggregation types Sidemantic does not execute are flagged as warnings."""
    model = _model_with_preagg(PreAggregation(name="r", type=preagg_type, measures=["order_count"]))

    warnings = validate_model_warnings(model)

    assert any(needle in w for w in warnings), warnings


def test_lambda_union_with_source_data_does_not_warn():
    """A lambda with union_with_source_data + build_range_end IS executed (it unions
    the batch rollup with fresh source data), so neither the inert-type note nor the
    build_range note is emitted."""
    model = _model_with_preagg(
        PreAggregation(
            name="r",
            type="lambda",
            measures=["order_count"],
            dimensions=["status"],
            time_dimension="created_at",
            granularity="day",
            union_with_source_data=True,
            build_range_end="'2024-04-01'",
        )
    )

    warnings = validate_model_warnings(model)

    assert not any("type 'lambda' is parsed but not executed" in w for w in warnings), warnings
    assert not any("build_range" in w for w in warnings), warnings


def test_plain_rollup_produces_no_inert_warnings():
    """A standard additive rollup (the executed path) must not generate warning noise."""
    model = _model_with_preagg(
        PreAggregation(
            name="r",
            type="rollup",
            measures=["order_count"],
            dimensions=["status"],
            time_dimension="created_at",
            granularity="day",
        )
    )

    assert validate_model_warnings(model) == []


def test_partition_granularity_does_not_warn_now_that_it_builds():
    """partition_granularity is materialized via build_partitions(), so it no longer warns."""
    model = _model_with_preagg(
        PreAggregation(
            name="r",
            measures=["order_count"],
            time_dimension="created_at",
            granularity="day",
            partition_granularity="month",
        )
    )

    assert not any("partition_granularity" in w for w in validate_model_warnings(model))


def test_inert_build_range_warns():
    """build_range_start/end are stored for round-trip only."""
    model = _model_with_preagg(
        PreAggregation(
            name="r",
            measures=["order_count"],
            build_range_start="SELECT '2024-01-01'",
            build_range_end="SELECT CURRENT_DATE",
        )
    )

    warnings = validate_model_warnings(model)

    assert any("build_range_start/build_range_end have no runtime effect" in w for w in warnings), warnings


def test_indexes_do_not_warn_now_that_they_materialize():
    """indexes are emitted as CREATE INDEX during refresh, so they no longer warn."""
    model = _model_with_preagg(
        PreAggregation(
            name="r",
            measures=["order_count"],
            dimensions=["status"],
            indexes=[Index(name="by_status", columns=["status"])],
        )
    )

    assert not any("index" in w.lower() for w in validate_model_warnings(model))


def test_inert_refresh_key_sql_warns():
    """refresh_key.sql is Cube change-detection; Sidemantic has no scheduler to run it."""
    model = _model_with_preagg(
        PreAggregation(
            name="r",
            measures=["order_count"],
            refresh_key=RefreshKey(sql="SELECT MAX(updated_at) FROM orders"),
        )
    )

    warnings = validate_model_warnings(model)

    assert any("refresh_key.sql is parsed but not executed" in w for w in warnings), warnings


def test_refresh_key_without_sql_does_not_warn():
    """A refresh_key that only uses runtime-honored fields produces no sql warning."""
    model = _model_with_preagg(
        PreAggregation(
            name="r",
            measures=["order_count"],
            refresh_key=RefreshKey(every="1 hour", incremental=True),
        )
    )

    assert not any("refresh_key.sql" in w for w in validate_model_warnings(model))


def test_count_distinct_approx_marker_warns():
    """A metric imported from Cube's count_distinct_approx is flagged as exact/non-additive."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[
            Metric(
                name="uniq_users",
                agg="count_distinct",
                sql="user_id",
                meta={"cube_type": "count_distinct_approx"},
            )
        ],
    )

    warnings = validate_model_warnings(model)

    assert any("count_distinct_approx" in w and "non-additive" in w for w in warnings), warnings


def test_plain_count_distinct_metric_does_not_warn():
    """An ordinary exact count_distinct (no Cube approx marker) is not flagged."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="uniq_users", agg="count_distinct", sql="user_id")],
    )

    assert validate_model_warnings(model) == []


def test_validate_directory_surfaces_preagg_warnings_without_failing(tmp_path):
    """The CLI validation path reports inert-field warnings but still passes."""
    (tmp_path / "orders.yml").write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: created_at
        type: time
        granularity: day
      - name: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
    pre_aggregations:
      - name: lambda_rollup
        type: lambda
        measures:
          - order_count
        dimensions:
          - status
        time_dimension: created_at
        granularity: day
        partition_granularity: month
"""
    )

    report = validate_directory(tmp_path)

    assert report.passed  # warnings never fail validation
    joined = "\n".join(report.warnings)
    assert "type 'lambda' is parsed but not executed" in joined
    assert "partition_granularity" not in joined  # now functional via build_partitions(), no longer warned


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
