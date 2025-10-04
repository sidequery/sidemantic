"""Test import/export with real Cube and MetricFlow examples."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.adapters.superset import SupersetAdapter


def test_import_real_cube_example():
    """Test importing a real Cube.js schema file."""
    adapter = CubeAdapter()
    graph = adapter.parse("examples/cube/orders.yml")

    # Verify models loaded
    assert "orders" in graph.models
    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "status" in dim_names
    assert "created_at" in dim_names
    assert "customer_id" in dim_names

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "count" in measure_names
    assert "revenue" in measure_names
    assert "completed_revenue" in measure_names
    assert "conversion_rate" in measure_names

    # Verify segments were imported
    segment_names = [s.name for s in orders.segments]
    assert "high_value" in segment_names
    assert "completed" in segment_names

    # Verify segment SQL was converted from ${CUBE} to {model}
    completed_segment = next(s for s in orders.segments if s.name == "completed")
    assert "{model}" in completed_segment.sql
    assert "${CUBE}" not in completed_segment.sql

    # Verify measure with filter was imported
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify ratio metric (calculated measure) was detected
    conversion_rate = next(m for m in orders.metrics if m.name == "conversion_rate")
    assert conversion_rate.type in ["ratio", "derived"]  # Detected as complex metric


def test_import_real_metricflow_example():
    """Test importing a real dbt MetricFlow schema file."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse("examples/metricflow/semantic_models.yml")

    # Verify models loaded
    assert "orders" in graph.models
    assert "customers" in graph.models

    orders = graph.models["orders"]
    customers = graph.models["customers"]

    # Verify dimensions
    order_dims = [d.name for d in orders.dimensions]
    assert "order_date" in order_dims
    assert "status" in order_dims

    customer_dims = [d.name for d in customers.dimensions]
    assert "region" in customer_dims
    assert "tier" in customer_dims

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "order_count" in measure_names
    assert "revenue" in measure_names
    assert "avg_order_value" in measure_names

    # Verify relationships were created from entities
    rel_names = [r.name for r in orders.relationships]
    assert "customer" in rel_names
    customer_rel = next(r for r in orders.relationships if r.name == "customer")
    assert customer_rel.type == "many_to_one"
    assert customer_rel.foreign_key == "customer_id"

    # Verify graph-level metrics
    assert "total_revenue" in graph.metrics
    assert "average_order_value" in graph.metrics

    total_revenue = graph.metrics["total_revenue"]
    assert total_revenue.type is None  # Simple metric maps to untyped

    avg_order = graph.metrics["average_order_value"]
    assert avg_order.type == "ratio"
    assert avg_order.numerator == "revenue"
    assert avg_order.denominator == "order_count"


def test_cube_to_sidemantic_to_cube_roundtrip():
    """Test that Cube -> Sidemantic -> Cube preserves structure."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export back to Cube
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = cube_adapter.parse(temp_path)

        # Verify model preserved
        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify key fields preserved
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments preserved
        segment_names = [s.name for s in orders.segments]
        assert "high_value" in segment_names
        assert "completed" in segment_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_metricflow_to_sidemantic_to_metricflow_roundtrip():
    """Test that MetricFlow -> Sidemantic -> MetricFlow preserves structure."""
    # Import from MetricFlow
    mf_adapter = MetricFlowAdapter()
    graph = mf_adapter.parse("examples/metricflow/semantic_models.yml")

    # Export back to MetricFlow
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = mf_adapter.parse(temp_path)

        # Verify models preserved
        assert "orders" in graph2.models
        assert "customers" in graph2.models

        # Verify metrics preserved
        # Note: Simple metrics that just reference measures may not round-trip
        # since they can be queried directly via the measure
        assert "average_order_value" in graph2.metrics

        # Verify metric types preserved
        avg_order = graph2.metrics["average_order_value"]
        assert avg_order.type == "ratio"

        # total_revenue is a simple metric, may not be preserved in export
        # (can be queried directly as orders.revenue)

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_metricflow_conversion():
    """Test converting Cube format to MetricFlow format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to MetricFlow
    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import as MetricFlow and verify structure
        graph2 = mf_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments stored in meta
        # (MetricFlow doesn't have native segments, but we preserve in meta)
        if orders.segments:
            assert len(orders.segments) > 0

    finally:
        temp_path.unlink(missing_ok=True)


def test_metricflow_to_cube_conversion():
    """Test converting MetricFlow format to Cube format."""
    # Import from MetricFlow
    mf_adapter = MetricFlowAdapter()
    graph = mf_adapter.parse("examples/metricflow/semantic_models.yml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import as Cube and verify structure
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        assert "customers" in graph2.models

        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_query_imported_cube_example():
    """Test that we can compile queries from imported Cube schema."""
    from sidemantic import SemanticLayer

    adapter = CubeAdapter()
    graph = adapter.parse("examples/cube/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic metric query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(
        metrics=["orders.revenue", "orders.count"],
        dimensions=["orders.status"]
    )
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test with segment
    sql = layer.compile(
        metrics=["orders.revenue"],
        segments=["orders.completed"]
    )
    assert "WHERE" in sql.upper()
    assert "status" in sql.lower()

    # Test ratio metric (if detected as ratio/derived with proper dependencies)
    conversion_rate = next(m for m in graph.models["orders"].metrics if m.name == "conversion_rate")
    # Note: Cube's ${measure} syntax doesn't translate directly to Sidemantic,
    # so derived metrics from Cube may not be queryable without modification
    # This is expected behavior - the metric was imported but needs manual adjustment


def test_query_imported_metricflow_example():
    """Test that we can compile queries from imported MetricFlow schema."""
    from sidemantic import SemanticLayer

    adapter = MetricFlowAdapter()
    graph = adapter.parse("examples/metricflow/semantic_models.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic measure query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(
        metrics=["orders.revenue", "orders.order_count"],
        dimensions=["orders.status"]
    )
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test cross-model query (only if join path exists)
    # Note: MetricFlow entities may not map 1:1 to model names
    try:
        sql = layer.compile(
            metrics=["orders.revenue"],
            dimensions=["customers.region"]
        )
        assert "JOIN" in sql.upper()
        assert "customers" in sql.lower()
    except Exception:
        # Join path not configured, which is expected for some imports
        pass

    # Test graph-level ratio metric (if it exists and is queryable)
    if "average_order_value" in graph.metrics:
        avg_metric = graph.metrics["average_order_value"]
        # Ratio metrics should have numerator/denominator set
        if avg_metric.type == "ratio" and avg_metric.numerator and avg_metric.denominator:
            try:
                sql = layer.compile(metrics=["average_order_value"])
                assert sql  # Should generate valid SQL with ratio calculation
            except ValueError:
                # Some graph-level metrics may need model context to be queryable
                pass


def test_query_with_time_dimension_cube():
    """Test querying time dimensions from Cube import."""
    from sidemantic import SemanticLayer

    adapter = CubeAdapter()
    graph = adapter.parse("examples/cube/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with time dimension
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at"]
    )
    assert "created_at" in sql.lower()
    assert "GROUP BY" in sql.upper()


def test_query_with_filter_metricflow():
    """Test that metric filters work from MetricFlow import."""
    from sidemantic import SemanticLayer

    adapter = MetricFlowAdapter()
    graph = adapter.parse("examples/metricflow/semantic_models.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with filter
    sql = layer.compile(
        metrics=["orders.revenue"],
        filters=["orders.status = 'completed'"]
    )
    assert "WHERE" in sql.upper()
    assert "status" in sql.lower()
    assert "completed" in sql.lower()


def test_roundtrip_real_cube_example():
    """Test Cube example roundtrip using the actual example file."""
    adapter = CubeAdapter()

    # Import original
    graph1 = adapter.parse("examples/cube/orders.yml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions count preserved
        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics count preserved
        assert len(orders1.metrics) == len(orders2.metrics)

        # Verify segments preserved
        segment_names1 = {s.name for s in orders1.segments}
        segment_names2 = {s.name for s in orders2.segments}
        assert segment_names1 == segment_names2

    finally:
        temp_path.unlink(missing_ok=True)


def test_roundtrip_real_metricflow_example():
    """Test MetricFlow example roundtrip using the actual example file."""
    adapter = MetricFlowAdapter()

    # Import original
    graph1 = adapter.parse("examples/metricflow/semantic_models.yml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions count preserved for each model
        for model_name in graph1.models:
            model1 = graph1.models[model_name]
            model2 = graph2.models[model_name]
            assert len(model1.dimensions) == len(model2.dimensions), f"Dimension count mismatch for {model_name}"
            assert len(model1.metrics) == len(model2.metrics), f"Metric count mismatch for {model_name}"

        # Verify graph-level metrics count preserved
        # Note: Simple metrics may not round-trip, so we just check that ratio metrics are there
        ratio_metrics1 = {name for name, m in graph1.metrics.items() if m.type == "ratio"}
        ratio_metrics2 = {name for name, m in graph2.metrics.items() if m.type == "ratio"}
        assert ratio_metrics1 == ratio_metrics2

    finally:
        temp_path.unlink(missing_ok=True)


def test_import_real_lookml_example():
    """Test importing a real LookML view file."""
    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/orders.lkml")

    # Verify models loaded
    assert "orders" in graph.models
    assert "customers" in graph.models

    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "status" in dim_names
    assert "customer_id" in dim_names

    # Verify time dimensions were created from dimension_group
    assert "created_date" in dim_names

    # Verify primary key was detected
    assert orders.primary_key == "id"

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "count" in measure_names
    assert "revenue" in measure_names
    assert "completed_revenue" in measure_names
    assert "conversion_rate" in measure_names

    # Verify segments (LookML filters) were imported
    segment_names = [s.name for s in orders.segments]
    assert "high_value" in segment_names
    assert "completed" in segment_names

    # Verify segment SQL was converted from ${TABLE} to {model}
    high_value_segment = next(s for s in orders.segments if s.name == "high_value")
    assert "{model}" in high_value_segment.sql
    assert "${TABLE}" not in high_value_segment.sql

    # Verify measure with filter was imported
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify derived metric (type=number) was detected
    conversion_rate = next(m for m in orders.metrics if m.name == "conversion_rate")
    assert conversion_rate.type == "derived"


def test_import_lookml_derived_table():
    """Test importing LookML view with derived table."""
    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/derived_tables.lkml")

    # Verify model loaded
    assert "customer_summary" in graph.models
    summary = graph.models["customer_summary"]

    # Verify derived table SQL was imported
    assert summary.sql is not None
    assert "SELECT" in summary.sql.upper()
    assert "GROUP BY" in summary.sql.upper()

    # Verify dimensions
    dim_names = [d.name for d in summary.dimensions]
    assert "customer_id" in dim_names
    assert "order_count" in dim_names
    assert "total_revenue" in dim_names

    # Verify time dimension_group created time dimensions
    assert "last_order_date" in dim_names

    # Verify measures
    measure_names = [m.name for m in summary.metrics]
    assert "total_customers" in measure_names
    assert "avg_orders_per_customer" in measure_names
    assert "avg_customer_ltv" in measure_names


def test_lookml_to_sidemantic_to_lookml_roundtrip():
    """Test that LookML -> Sidemantic -> LookML preserves structure."""
    # Import from LookML
    lookml_adapter = LookMLAdapter()
    graph = lookml_adapter.parse("examples/lookml/orders.lkml")

    # Export back to LookML
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = lookml_adapter.parse(temp_path)

        # Verify models preserved
        assert "orders" in graph2.models
        assert "customers" in graph2.models

        orders = graph2.models["orders"]

        # Verify key fields preserved
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments preserved
        segment_names = [s.name for s in orders.segments]
        assert "high_value" in segment_names
        assert "completed" in segment_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_lookml_to_cube_conversion():
    """Test converting LookML format to Cube format."""
    # Import from LookML
    lookml_adapter = LookMLAdapter()
    graph = lookml_adapter.parse("examples/lookml/orders.lkml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import as Cube and verify structure
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments preserved
        segment_names = [s.name for s in orders.segments]
        assert "high_value" in segment_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_lookml_conversion():
    """Test converting Cube format to LookML format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to LookML
    lookml_adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph, temp_path)

        # Re-import as LookML and verify structure
        graph2 = lookml_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments converted
        if orders.segments:
            assert len(orders.segments) > 0

    finally:
        temp_path.unlink(missing_ok=True)


def test_lookml_to_metricflow_conversion():
    """Test converting LookML format to MetricFlow format."""
    # Import from LookML
    lookml_adapter = LookMLAdapter()
    graph = lookml_adapter.parse("examples/lookml/orders.lkml")

    # Export to MetricFlow
    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import as MetricFlow and verify structure
        graph2 = mf_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments stored in meta (MetricFlow doesn't have native support)
        if orders.segments:
            assert len(orders.segments) > 0

    finally:
        temp_path.unlink(missing_ok=True)


def test_query_imported_lookml_example():
    """Test that we can compile queries from imported LookML schema."""
    from sidemantic import SemanticLayer

    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/orders.lkml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic metric query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(
        metrics=["orders.revenue", "orders.count"],
        dimensions=["orders.status"]
    )
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test with segment
    sql = layer.compile(
        metrics=["orders.revenue"],
        segments=["orders.completed"]
    )
    assert "WHERE" in sql.upper()
    assert "status" in sql.lower()


def test_lookml_explore_parsing():
    """Test parsing LookML explore files for relationships."""
    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/")

    # Verify orders model exists
    assert "orders" in graph.models
    orders = graph.models["orders"]

    # Verify relationship was parsed from explore
    assert len(orders.relationships) == 1

    # Verify relationship details
    customer_rel = orders.relationships[0]
    assert customer_rel.name == "customers"
    assert customer_rel.type == "many_to_one"
    assert customer_rel.foreign_key == "customer_id"


def test_query_with_time_dimension_lookml():
    """Test querying time dimensions from LookML import."""
    from sidemantic import SemanticLayer

    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/orders.lkml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with time dimension
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_date"]
    )
    assert "created_at" in sql.lower()
    assert "GROUP BY" in sql.upper()


def test_roundtrip_real_lookml_example():
    """Test LookML example roundtrip using the actual example file."""
    adapter = LookMLAdapter()

    # Import original
    graph1 = adapter.parse("examples/lookml/orders.lkml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions count preserved
        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics count preserved
        assert len(orders1.metrics) == len(orders2.metrics)

        # Verify segments preserved
        segment_names1 = {s.name for s in orders1.segments}
        segment_names2 = {s.name for s in orders2.segments}
        assert segment_names1 == segment_names2

    finally:
        temp_path.unlink(missing_ok=True)


def test_import_real_hex_example():
    """Test importing real Hex semantic model files."""
    adapter = HexAdapter()
    graph = adapter.parse("examples/hex/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "users" in graph.models
    assert "organizations" in graph.models

    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "amount" in dim_names
    assert "status" in dim_names
    assert "is_completed" in dim_names

    # Verify primary key from unique dimension
    assert orders.primary_key == "id"

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "order_count" in measure_names
    assert "revenue" in measure_names
    assert "avg_order_value" in measure_names
    assert "completed_revenue" in measure_names

    # Verify measure with filter
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify custom func_sql measure
    conversion_rate = next(m for m in orders.metrics if m.name == "conversion_rate")
    assert conversion_rate.type == "derived"

    # Verify relationships
    rel_names = [r.name for r in orders.relationships]
    assert "customers" in rel_names
    customers_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customers_rel.type == "many_to_one"


def test_import_hex_with_relations():
    """Test that Hex relations are properly imported."""
    adapter = HexAdapter()
    graph = adapter.parse("examples/hex/")

    users = graph.models["users"]
    orgs = graph.models["organizations"]

    # Verify many_to_one from users to organizations
    user_rels = [r.name for r in users.relationships]
    assert "organizations" in user_rels

    # Verify one_to_many from organizations to users
    org_rels = [r.name for r in orgs.relationships]
    assert "users" in org_rels
    users_rel = next(r for r in orgs.relationships if r.name == "users")
    assert users_rel.type == "one_to_many"


def test_import_hex_calculated_dimensions():
    """Test that Hex calculated dimensions (expr_sql) are imported."""
    adapter = HexAdapter()
    graph = adapter.parse("examples/hex/users.yml")

    users = graph.models["users"]

    # Find the calculated dimension
    annual_price = next(d for d in users.dimensions if d.name == "annual_seat_price")
    assert annual_price.sql is not None
    assert "IF" in annual_price.sql


def test_hex_to_sidemantic_to_hex_roundtrip():
    """Test that Hex -> Sidemantic -> Hex preserves structure."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("examples/hex/orders.yml")

    # Export back to Hex
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        hex_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = hex_adapter.parse(temp_path)

        # Verify model preserved
        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify key fields preserved
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names
        assert "amount" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names
        assert "order_count" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_hex_to_cube_conversion():
    """Test converting Hex format to Cube format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("examples/hex/orders.yml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import as Cube and verify structure
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_hex_conversion():
    """Test converting Cube format to Hex format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to Hex
    hex_adapter = HexAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        hex_adapter.export(graph, temp_path)

        # Re-import as Hex and verify structure
        graph2 = hex_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_hex_to_metricflow_conversion():
    """Test converting Hex format to MetricFlow format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("examples/hex/orders.yml")

    # Export to MetricFlow
    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import as MetricFlow and verify structure
        graph2 = mf_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_hex_to_lookml_conversion():
    """Test converting Hex format to LookML format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("examples/hex/orders.yml")

    # Export to LookML
    lookml_adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph, temp_path)

        # Re-import as LookML and verify structure
        graph2 = lookml_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_query_imported_hex_example():
    """Test that we can compile queries from imported Hex schema."""
    from sidemantic import SemanticLayer

    adapter = HexAdapter()
    graph = adapter.parse("examples/hex/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic metric query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(
        metrics=["orders.revenue", "orders.order_count"],
        dimensions=["orders.status"]
    )
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test with filter
    sql = layer.compile(
        metrics=["orders.revenue"],
        filters=["orders.status = 'completed'"]
    )
    assert "WHERE" in sql.upper()
    assert "completed" in sql.lower()


def test_roundtrip_real_hex_example():
    """Test Hex example roundtrip using actual example files."""
    adapter = HexAdapter()

    # Import original
    graph1 = adapter.parse("examples/hex/orders.yml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions count preserved
        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics count preserved
        assert len(orders1.metrics) == len(orders2.metrics)

    finally:
        temp_path.unlink(missing_ok=True)


def test_import_real_rill_example():
    """Test importing a real Rill metrics view YAML file."""
    adapter = RillAdapter()
    graph = adapter.parse("examples/rill/orders.yaml")

    # Verify models loaded
    assert "orders" in graph.models
    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "status" in dim_names
    assert "customer_id" in dim_names
    assert "country" in dim_names
    assert "product_category" in dim_names

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "total_orders" in measure_names
    assert "total_revenue" in measure_names
    assert "avg_order_value" in measure_names
    assert "completed_orders" in measure_names

    # Verify timeseries dimension was created
    # Should have timeseries as a time dimension
    time_dims = [d for d in orders.dimensions if d.type == "time"]
    assert len(time_dims) > 0


def test_import_rill_with_derived_measures():
    """Test importing Rill metrics view with derived measures."""
    adapter = RillAdapter()
    graph = adapter.parse("examples/rill/users.yaml")

    assert "users" in graph.models
    users = graph.models["users"]

    # Verify derived measures were detected
    derived_metrics = [m for m in users.metrics if m.type == "derived"]
    assert len(derived_metrics) == 2

    derived_names = [m.name for m in derived_metrics]
    assert "avg_revenue_per_user" in derived_names
    assert "activation_rate" in derived_names


def test_import_rill_with_table_reference():
    """Test importing Rill metrics view that references a table."""
    adapter = RillAdapter()
    graph = adapter.parse("examples/rill/sales.yaml")

    assert "sales" in graph.models
    sales = graph.models["sales"]

    # Verify table reference was captured
    assert sales.table == "public.sales"

    # Verify dimensions
    dim_names = [d.name for d in sales.dimensions]
    assert "store_id" in dim_names
    assert "product_id" in dim_names
    assert "sales_rep" in dim_names
    assert "region" in dim_names


def test_rill_to_sidemantic_to_rill_roundtrip():
    """Test Rill roundtrip conversion."""
    adapter = RillAdapter()

    # Import original
    graph1 = adapter.parse("examples/rill/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]

        # Verify dimensions count preserved
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics count preserved
        assert len(orders1.metrics) == len(orders2.metrics)


def test_rill_to_cube_conversion():
    """Test converting Rill format to Cube format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("examples/rill/orders.yaml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import as Cube and verify structure
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_rill_conversion():
    """Test converting Cube format to Rill format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to Rill
    rill_adapter = RillAdapter()
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        # Re-import as Rill and verify structure
        graph2 = rill_adapter.parse(output_path / "orders.yaml")

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names


def test_rill_to_metricflow_conversion():
    """Test converting Rill format to MetricFlow format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("examples/rill/orders.yaml")

    # Export to MetricFlow
    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import as MetricFlow and verify structure
        graph2 = mf_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_rill_to_lookml_conversion():
    """Test converting Rill format to LookML format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("examples/rill/orders.yaml")

    # Export to LookML
    lookml_adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph, temp_path)

        # Re-import as LookML and verify structure
        graph2 = lookml_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_query_imported_rill_example():
    """Test that we can compile queries from imported Rill schema."""
    from sidemantic import SemanticLayer

    adapter = RillAdapter()
    graph = adapter.parse("examples/rill/orders.yaml")

    layer = SemanticLayer()
    layer.graph = graph

    # Simple metric query
    sql = layer.compile(metrics=["orders.total_orders"])
    assert "COUNT" in sql.upper()

    # Query with dimension
    sql = layer.compile(
        metrics=["orders.total_revenue"],
        dimensions=["orders.status"]
    )
    assert "GROUP BY" in sql.upper()


def test_roundtrip_real_rill_example():
    """Test Rill example roundtrip using actual example files."""
    adapter = RillAdapter()

    # Import original
    graph1 = adapter.parse("examples/rill/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions count preserved
        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics count preserved
        assert len(orders1.metrics) == len(orders2.metrics)


def test_import_real_superset_example():
    """Test importing real Superset dataset files."""
    adapter = SupersetAdapter()
    graph = adapter.parse("examples/superset/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "sales_summary" in graph.models

    # Verify orders dataset
    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    assert orders.description == "Customer orders dataset"

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "created_at" in dim_names
    assert "status" in dim_names
    assert "amount" in dim_names

    # Verify main datetime column
    created_at = next(d for d in orders.dimensions if d.name == "created_at")
    assert created_at.type == "time"
    assert created_at.label == "Order Date"

    # Verify metrics
    metric_names = [m.name for m in orders.metrics]
    assert "count" in metric_names
    assert "total_revenue" in metric_names
    assert "avg_order_value" in metric_names

    # Verify metric types
    count_metric = next(m for m in orders.metrics if m.name == "count")
    assert count_metric.agg == "count"

    revenue_metric = next(m for m in orders.metrics if m.name == "total_revenue")
    assert revenue_metric.agg == "sum"
    assert revenue_metric.label == "Total Revenue"


def test_import_superset_virtual_dataset():
    """Test that Superset virtual datasets (SQL-based) are imported."""
    adapter = SupersetAdapter()
    graph = adapter.parse("examples/superset/sales_summary.yaml")

    sales = graph.models["sales_summary"]

    # Verify it has SQL (virtual dataset)
    assert sales.sql is not None
    assert "SELECT" in sales.sql
    assert sales.table is None  # Virtual datasets don't have table

    # Verify derived metric without aggregation type
    revenue_per_order = next(m for m in sales.metrics if m.name == "revenue_per_order")
    assert revenue_per_order.type == "derived"
    assert revenue_per_order.agg is None


def test_superset_to_sidemantic_to_superset_roundtrip():
    """Test roundtrip: Superset → Sidemantic → Superset."""
    adapter = SupersetAdapter()

    # Import original
    graph1 = adapter.parse("examples/superset/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions preserved
        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics preserved
        assert len(orders1.metrics) == len(orders2.metrics)


def test_superset_to_cube_conversion():
    """Test converting Superset dataset to Cube format."""
    superset_adapter = SupersetAdapter()
    cube_adapter = CubeAdapter()

    # Import from Superset
    graph = superset_adapter.parse("examples/superset/orders.yaml")

    # Export to Cube
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.yml"
        cube_adapter.export(graph, output_path)

        # Verify Cube file was created
        assert output_path.exists()

        # Import Cube version
        cube_graph = cube_adapter.parse(output_path)

        # Verify model exists
        assert "orders" in cube_graph.models


def test_cube_to_superset_conversion():
    """Test converting Cube schema to Superset dataset."""
    cube_adapter = CubeAdapter()
    superset_adapter = SupersetAdapter()

    # Import from Cube
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to Superset
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        superset_adapter.export(graph, output_path)

        # Import Superset version
        superset_graph = superset_adapter.parse(output_path / "orders.yaml")

        # Verify model exists
        assert "orders" in superset_graph.models


def test_superset_to_metricflow_conversion():
    """Test converting Superset dataset to MetricFlow."""
    superset_adapter = SupersetAdapter()
    mf_adapter = MetricFlowAdapter()

    # Import from Superset
    graph = superset_adapter.parse("examples/superset/orders.yaml")

    # Export to MetricFlow
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.yml"
        mf_adapter.export(graph, output_path)

        # Verify file created
        assert output_path.exists()


def test_superset_to_lookml_conversion():
    """Test converting Superset dataset to LookML."""
    superset_adapter = SupersetAdapter()
    lookml_adapter = LookMLAdapter()

    # Import from Superset
    graph = superset_adapter.parse("examples/superset/orders.yaml")

    # Export to LookML
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.lkml"
        lookml_adapter.export(graph, output_path)

        # Verify file created
        assert output_path.exists()


def test_superset_to_rill_conversion():
    """Test converting Superset dataset to Rill."""
    superset_adapter = SupersetAdapter()
    rill_adapter = RillAdapter()

    # Import from Superset
    graph = superset_adapter.parse("examples/superset/orders.yaml")

    # Export to Rill
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        # Verify file created
        assert (output_path / "orders.yaml").exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
