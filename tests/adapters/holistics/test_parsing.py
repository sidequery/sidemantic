"""Tests for Holistics AML adapter - parsing and export."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.holistics import HolisticsAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

# =============================================================================
# BASIC PARSING TESTS
# =============================================================================


def test_import_real_holistics_example():
    """Test importing real Holistics AML model files."""
    adapter = HolisticsAdapter()
    graph = adapter.parse("tests/fixtures/holistics")

    assert "users" in graph.models
    assert "orders" in graph.models
    assert "accounts" in graph.models

    users = graph.models["users"]
    orders = graph.models["orders"]

    assert users.primary_key == "id"
    assert orders.primary_key == "id"

    created_at = users.get_dimension("created_at")
    assert created_at is not None
    assert created_at.type == "time"
    assert created_at.granularity == "hour"
    assert users.get_dimension("id").format == "#,##0"

    user_count = users.get_metric("user_count")
    assert user_count is not None
    assert user_count.agg == "count"
    assert users.get_metric("avg_age").format == "#,##0.00"

    aov = orders.get_metric("aov")
    assert aov is not None
    assert aov.type == "ratio"
    assert aov.numerator == "revenue"
    assert aov.denominator == "order_count"

    revenue = orders.get_metric("revenue")
    assert revenue.format == "$#,##0.00"

    stdev_metric = orders.get_metric("amount_stdev")
    assert stdev_metric.type == "derived"
    assert "STDDEV_SAMP" in stdev_metric.sql

    orders_rel = next(r for r in orders.relationships if r.name == "users")
    assert orders_rel.type == "many_to_one"
    assert orders_rel.foreign_key == "user_id"

    users_rel = next(r for r in users.relationships if r.name == "accounts")
    assert users_rel.type == "one_to_one"
    assert users_rel.foreign_key == "user_id"


# =============================================================================
# DIMENSION TYPE MAPPING TESTS
# =============================================================================


def test_holistics_dimension_type_mapping():
    """Test Holistics dimension types map to Sidemantic types."""
    aml_content = """
Model test_model {
  type: 'table'
  table_name: 'test_table'

  dimension name { type: 'text' }
  dimension amount { type: 'number' }
  dimension is_active { type: 'truefalse' }
  dimension event_date { type: 'date' }
  dimension created_at { type: 'datetime' }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test_model"]

        assert model.get_dimension("name").type == "categorical"
        assert model.get_dimension("amount").type == "numeric"
        assert model.get_dimension("is_active").type == "boolean"

        event_date = model.get_dimension("event_date")
        assert event_date.type == "time"
        assert event_date.granularity == "day"

        created_at = model.get_dimension("created_at")
        assert created_at.type == "time"
        assert created_at.granularity == "hour"
    finally:
        temp_path.unlink()


# =============================================================================
# MEASURE AGGREGATION TYPE TESTS
# =============================================================================


def test_holistics_measure_aggregation_types():
    """Test Holistics aggregation types map to Sidemantic metrics."""
    aml_content = """
Model test_model {
  type: 'table'
  table_name: 'test_table'

  measure total_count {
    type: 'number'
    definition: @sql {{ id }};;
    aggregation_type: 'count'
  }

  measure unique_users {
    type: 'number'
    definition: @sql {{ user_id }};;
    aggregation_type: 'count distinct'
  }

  measure total_sum {
    type: 'number'
    definition: @sql {{ amount }};;
    aggregation_type: 'sum'
  }

  measure total_avg {
    type: 'number'
    definition: @sql {{ amount }};;
    aggregation_type: 'avg'
  }

  measure total_min {
    type: 'number'
    definition: @sql {{ amount }};;
    aggregation_type: 'min'
  }

  measure total_max {
    type: 'number'
    definition: @sql {{ amount }};;
    aggregation_type: 'max'
  }

  measure total_median {
    type: 'number'
    definition: @sql {{ amount }};;
    aggregation_type: 'median'
  }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test_model"]
        assert model.get_metric("total_count").agg == "count"
        assert model.get_metric("unique_users").agg == "count_distinct"
        assert model.get_metric("total_sum").agg == "sum"
        assert model.get_metric("total_avg").agg == "avg"
        assert model.get_metric("total_min").agg == "min"
        assert model.get_metric("total_max").agg == "max"
        assert model.get_metric("total_median").agg == "median"
    finally:
        temp_path.unlink()


def test_holistics_custom_measure_is_derived():
    """Test custom aggregation types are parsed as derived metrics."""
    aml_content = """
Model test_model {
  type: 'table'
  table_name: 'test_table'

  measure custom_calc {
    type: 'number'
    definition: @sql {{ revenue }} - {{ cost }};;
    aggregation_type: 'custom'
  }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test_model"].get_metric("custom_calc")
        assert metric.type == "derived"
        assert "revenue" in metric.sql
        assert "cost" in metric.sql
    finally:
        temp_path.unlink()


def test_holistics_unknown_aggregation_type():
    """Test unsupported aggregation types are wrapped as derived SQL."""
    aml_content = """
Model test_model {
  type: 'table'
  table_name: 'test_table'

  measure sample_stddev {
    type: 'number'
    definition: @sql {{ amount }};;
    aggregation_type: 'stddev_pop'
  }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test_model"].get_metric("sample_stddev")
        assert metric.type == "derived"
        assert "STDDEV_POP" in metric.sql
    finally:
        temp_path.unlink()


def test_holistics_metric_count_without_definition():
    """Count metrics without definition still map to agg=count with no SQL."""
    aml_content = """
Model test_model {
  type: 'table'
  table_name: 'test_table'

  measure row_count {
    type: 'number'
    aggregation_type: 'count'
  }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test_model"].get_metric("row_count")
        assert metric.agg == "count"
        assert metric.sql is None
    finally:
        temp_path.unlink()


def test_holistics_aql_translation_and_macros():
    """AQL expressions translate to SQL, including macros."""
    aml_content = """
Model test_model {
  type: 'table'
  table_name: 'test_table'

  measure aql_calc {
    type: 'number'
    definition: @aql sum(amount) / count(order_id) + count_if(amount > 0) + @now;;
    aggregation_type: 'custom'
  }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test_model"].get_metric("aql_calc")
        assert "SUM(amount)" in metric.sql
        assert "COUNT(order_id)" in metric.sql
        assert "SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END)" in metric.sql
        assert "CURRENT_TIMESTAMP" in metric.sql
    finally:
        temp_path.unlink()


# =============================================================================
# RELATIONSHIP PARSING TESTS
# =============================================================================


def test_holistics_relationship_block_parsing():
    """Test Relationship block parsing."""
    aml_content = """
Relationship users_accounts {
  type: 'one_to_one'
  from: r(users.id)
  to: r(accounts.user_id)
}

Model users {
  type: 'table'
  table_name: 'users'
  dimension id { type: 'number' primary_key: true }
}

Model accounts {
  type: 'table'
  table_name: 'accounts'
  dimension user_id { type: 'number' primary_key: true }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        users = graph.models["users"]
        rel = next(r for r in users.relationships if r.name == "accounts")
        assert rel.type == "one_to_one"
        assert rel.foreign_key == "user_id"
    finally:
        temp_path.unlink()


def test_holistics_relationship_types_and_active_flags():
    """Test one_to_many mapping, FieldRef parsing, and inactive relationships."""
    aml_content = """
Relationship parent_children {
  type: 'one_to_many'
  from: r(parent.id)
  to: r(child.parent_id)
}

Dataset sample {
  relationships: [
    RelationshipConfig {
      active: true
      rel: Relationship {
        type: 'many_to_one'
        from: FieldRef { model: 'child' field: 'parent_id' }
        to: FieldRef { model: 'parent' field: 'id' }
      }
    },
    RelationshipConfig {
      active: false
      rel: Relationship {
        type: 'many_to_one'
        from: r(orphan.parent_id)
        to: r(parent.id)
      }
    }
  ]
}

Model parent {
  type: 'table'
  table_name: 'parent'
  dimension id { type: 'number' primary_key: true }
}

Model child {
  type: 'table'
  table_name: 'child'
  dimension parent_id { type: 'number' }
}

Model orphan {
  type: 'table'
  table_name: 'orphan'
  dimension parent_id { type: 'number' }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        parent = graph.models["parent"]
        child = graph.models["child"]
        orphan = graph.models["orphan"]

        rel_to_child = next(r for r in parent.relationships if r.name == "child")
        assert rel_to_child.type == "one_to_many"
        assert rel_to_child.foreign_key == "parent_id"
        assert rel_to_child.primary_key == "id"

        rel_to_parent = next(r for r in child.relationships if r.name == "parent")
        assert rel_to_parent.type == "many_to_one"
        assert rel_to_parent.foreign_key == "parent_id"

        assert all(r.name != "parent" for r in orphan.relationships)
    finally:
        temp_path.unlink()


# =============================================================================
# MODEL AND DIMENSION PROPERTY TESTS
# =============================================================================


def test_holistics_primary_key_default_and_definition_name():
    """Default primary_key is id; definition matching name is omitted."""
    aml_content = """
Model test_model {
  table_name: 'test_table'
  description: 'Test model'

  dimension id { type: 'number' }
  dimension name { type: 'text' definition: @sql {{ name }};; }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test_model"]
        assert model.primary_key == "id"
        assert model.description == "Test model"

        name_dim = model.get_dimension("name")
        assert name_dim.sql is None
    finally:
        temp_path.unlink()


def test_holistics_dimension_and_metric_labels():
    """Labels/descriptions/formats map onto dimensions and metrics."""
    aml_content = """
Model test_model {
  type: 'table'
  table_name: 'test_table'

  dimension status {
    type: 'text'
    label: 'Status'
    description: 'Order status'
    format: 'status_fmt'
  }

  measure total {
    type: 'number'
    label: 'Total'
    description: 'Total amount'
    format: '$#,##0.00'
    definition: @sql {{ amount }};;
    aggregation_type: 'sum'
  }
}
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write(aml_content)
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test_model"]
        status = model.get_dimension("status")
        assert status.label == "Status"
        assert status.description == "Order status"
        assert status.format == "status_fmt"

        total = model.get_metric("total")
        assert total.label == "Total"
        assert total.description == "Total amount"
        assert total.format == "$#,##0.00"
    finally:
        temp_path.unlink()


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================


def test_holistics_parse_empty_file():
    """Test parsing empty AML file returns empty graph."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        f.write("")
        temp_path = Path(f.name)

    try:
        adapter = HolisticsAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models) == 0
    finally:
        temp_path.unlink()


def test_holistics_parse_nonexistent_file():
    """Test parsing nonexistent AML file raises FileNotFoundError."""
    adapter = HolisticsAdapter()
    with pytest.raises(FileNotFoundError):
        adapter.parse("/nonexistent/path/file.aml")


# =============================================================================
# EXPORT TESTS
# =============================================================================


def test_holistics_export_simple_model(tmp_path):
    """Test exporting a simple model to AML format."""
    model = Model(
        name="test_model",
        table="public.test_table",
        primary_key="id",
        dimensions=[
            Dimension(name="id", type="numeric", sql="id", label="ID"),
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="total", agg="sum", sql="amount"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = HolisticsAdapter()
    output_file = tmp_path / "model.aml"
    adapter.export(graph, output_file)

    content = output_file.read_text()
    assert "Model test_model" in content
    assert "table_name: 'public.test_table'" in content
    assert "dimension id" in content
    assert "aggregation_type: 'count'" in content
    assert "aggregation_type: 'sum'" in content


def test_holistics_export_relationships(tmp_path):
    """Test exporting relationships to relationships.aml."""
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="id", type="numeric")],
        relationships=[Relationship(name="users", type="many_to_one", foreign_key="user_id", primary_key="id")],
    )
    users = Model(
        name="users",
        table="users",
        primary_key="id",
        dimensions=[Dimension(name="id", type="numeric")],
    )

    graph = SemanticGraph()
    graph.add_model(orders)
    graph.add_model(users)

    adapter = HolisticsAdapter()
    adapter.export(graph, tmp_path)

    relationships_file = tmp_path / "relationships.aml"
    assert relationships_file.exists()
    content = relationships_file.read_text()
    assert "Relationship orders_users" in content
    assert "from: r(orders.user_id)" in content
    assert "to: r(users.id)" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
