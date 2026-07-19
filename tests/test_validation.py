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


def test_model_without_primary_key_remains_explicitly_keyless(layer):
    """Models must not manufacture an ``id`` key when none was declared."""
    model = Model(
        name="orders",
        table="orders",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[],
    )

    layer.add_model(model)
    assert model.primary_key is None
    assert model.primary_key_columns == []


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


def _write_warehouse_validation_model(tmp_path, *, foreign_key: str = "customer_id"):
    (tmp_path / "models.yml").write_text(
        f"""
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
      - name: revenue
        agg: sum
        sql: amount
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: {foreign_key}
  - name: customers
    table: customers
    primary_key: customer_id
    dimensions:
      - name: region
        type: categorical
"""
    )


def test_warehouse_validation_checks_schema_types_joins_and_queries(tmp_path):
    import duckdb

    _write_warehouse_validation_model(tmp_path)
    database = tmp_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(
        "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, region VARCHAR); "
        "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, created_at TIMESTAMP, "
        "status VARCHAR, amount DECIMAL(12, 2))"
    )
    connection.close()

    report = validate_directory(tmp_path, connection=f"duckdb:///{database}")

    assert report.passed, report.all_errors
    assert report.connection_errors == []
    assert report.warehouse_errors == []
    assert any("Warehouse validation:" in item for item in report.info)


def test_warehouse_validation_reports_missing_columns_and_type_mismatches(tmp_path):
    import duckdb

    _write_warehouse_validation_model(tmp_path, foreign_key="missing_customer_id")
    database = tmp_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(
        "CREATE TABLE customers (customer_id INTEGER, region VARCHAR); "
        "CREATE TABLE orders (order_id INTEGER, created_at VARCHAR, status VARCHAR, amount VARCHAR)"
    )
    connection.close()

    report = validate_directory(
        tmp_path,
        connection=f"duckdb:///{database}",
        check_queries=False,
    )

    errors = "\n".join(report.warehouse_errors)
    assert report.errors == []
    assert "missing column 'missing_customer_id'" in errors
    assert "column 'created_at' is declared time" in errors
    assert "column 'amount' is declared numeric" in errors


def test_warehouse_validation_reports_incompatible_join_key_types(tmp_path):
    import duckdb

    _write_warehouse_validation_model(tmp_path)
    database = tmp_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(
        "CREATE TABLE customers (customer_id VARCHAR, region VARCHAR); "
        "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, created_at TIMESTAMP, "
        "status VARCHAR, amount DECIMAL(12, 2))"
    )
    connection.close()

    report = validate_directory(tmp_path, connection=f"duckdb:///{database}", check_queries=False)

    errors = "\n".join(report.warehouse_errors)
    assert "Relationship 'orders.customers' joins incompatible warehouse types" in errors
    assert "orders.customer_id is INTEGER" in errors
    assert "customers.customer_id is VARCHAR" in errors


def test_warehouse_key_checks_find_null_and_duplicate_primary_keys(tmp_path):
    import duckdb

    (tmp_path / "models.yml").write_text(
        """
models:
  - name: events
    table: events
    primary_key: event_id
    dimensions:
      - name: category
        type: categorical
"""
    )
    database = tmp_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute("CREATE TABLE events (event_id INTEGER, category VARCHAR)")
    connection.execute("INSERT INTO events VALUES (1, 'a'), (1, 'b'), (NULL, 'c')")
    connection.close()

    report = validate_directory(
        tmp_path,
        connection=f"duckdb:///{database}",
        check_keys=True,
        check_queries=False,
    )

    errors = "\n".join(report.warehouse_errors)
    assert "contains NULL values" in errors
    assert "is not unique" in errors


def test_warehouse_key_checks_verify_one_to_one_cardinality(tmp_path):
    import duckdb

    (tmp_path / "models.yml").write_text(
        """
models:
  - name: customers
    table: customers
    primary_key: customer_id
    dimensions:
      - name: name
        type: categorical
    relationships:
      - name: profiles
        type: one_to_one
        foreign_key: customer_id
  - name: profiles
    table: profiles
    primary_key: profile_id
    dimensions:
      - name: plan
        type: categorical
"""
    )
    database = tmp_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute("CREATE TABLE customers (customer_id INTEGER, name VARCHAR)")
    connection.execute("CREATE TABLE profiles (profile_id INTEGER, customer_id INTEGER, plan VARCHAR)")
    connection.execute("INSERT INTO profiles VALUES (1, 7, 'free'), (2, 7, 'paid')")
    connection.close()

    report = validate_directory(
        tmp_path,
        connection=f"duckdb:///{database}",
        check_keys=True,
        check_queries=False,
    )

    errors = "\n".join(report.warehouse_errors)
    assert "relationship 'customers.profiles' foreign_key ['customer_id'] is not unique" in errors


def test_warehouse_key_checks_allow_multiple_null_one_to_one_foreign_keys(tmp_path):
    import duckdb

    (tmp_path / "models.yml").write_text(
        """
models:
  - name: customers
    table: customers
    primary_key: customer_id
    relationships:
      - name: profiles
        type: one_to_one
        foreign_key: customer_id
  - name: profiles
    table: profiles
    primary_key: profile_id
"""
    )
    database = tmp_path / "warehouse.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute("CREATE TABLE customers (customer_id INTEGER)")
    connection.execute("CREATE TABLE profiles (profile_id INTEGER, customer_id INTEGER)")
    connection.execute("INSERT INTO profiles VALUES (1, NULL), (2, NULL), (3, 7)")
    connection.close()

    report = validate_directory(
        tmp_path,
        connection=f"duckdb:///{database}",
        check_keys=True,
        check_queries=False,
    )

    assert report.passed, report.all_errors


def test_warehouse_connection_failure_is_separate_from_structural_errors(monkeypatch, tmp_path):
    (tmp_path / "models.yml").write_text(
        """
models:
  - name: events
    table: events
"""
    )

    from sidemantic.validation_runner import SemanticLayer as RealSemanticLayer

    def fail_to_connect(*args, **kwargs):
        if kwargs.get("connection") is not None:
            raise RuntimeError("warehouse unavailable")
        return RealSemanticLayer(*args, **kwargs)

    monkeypatch.setattr("sidemantic.validation_runner.SemanticLayer", fail_to_connect)

    report = validate_directory(tmp_path, connection="duckdb:///unreachable.duckdb")

    assert report.errors == []
    assert report.warehouse_errors == []
    assert report.connection_errors == ["Could not connect to warehouse: warehouse unavailable"]


def test_snowflake_warehouse_identifiers_match_unquoted_metadata_case():
    from sidemantic.validation_runner import _split_table_reference, _warehouse_column_type, _warehouse_table_exists

    table_ref = _split_table_reference("public.orders", "snowflake")

    assert table_ref is not None
    assert _warehouse_table_exists(table_ref, {("PUBLIC", "ORDERS")}, "snowflake")
    assert _warehouse_column_type({"ORDER_ID": "NUMBER"}, "order_id", "snowflake") == "NUMBER"


def test_warehouse_table_reference_preserves_catalog_for_inspection():
    from sidemantic.validation_runner import _split_table_reference

    table_ref = _split_table_reference("analytics.public.orders", "snowflake")

    assert table_ref is not None
    assert table_ref.catalog == "analytics"
    assert table_ref.schema == "public"
    assert table_ref.name == "orders"
    assert table_ref.qualified_name == "analytics.public.orders"


def test_warehouse_validation_inspects_catalog_qualified_snowflake_table(monkeypatch, tmp_path):
    (tmp_path / "models.yml").write_text(
        """
models:
  - name: orders
    table: analytics.public.orders
    primary_key: order_id
"""
    )

    inspected = []

    class FakeSnowflakeAdapter:
        def get_tables(self):
            # Catalog-less metadata describes only the connection's current database.
            return [{"schema": "PUBLIC", "table_name": "CURRENT_DATABASE_TABLE"}]

        def get_columns(self, table_name, schema=None):
            inspected.append((table_name, schema))
            return [{"column_name": "ORDER_ID", "data_type": "NUMBER"}]

        def close(self):
            pass

    from sidemantic.validation_runner import SemanticLayer as RealSemanticLayer

    def semantic_layer(*args, **kwargs):
        layer = RealSemanticLayer(auto_register=False)
        if kwargs.get("connection") is not None:
            layer.adapter = FakeSnowflakeAdapter()
            layer.dialect = "snowflake"
        return layer

    monkeypatch.setattr("sidemantic.validation_runner.SemanticLayer", semantic_layer)

    report = validate_directory(
        tmp_path,
        connection="snowflake://unused",
        check_queries=False,
    )

    assert report.passed, report.all_errors
    assert inspected == [("analytics.public.orders", None)]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
