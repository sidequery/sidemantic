"""Tests for parameter functionality."""

import duckdb
import pytest

from sidemantic.core.model import Dimension, Metric, Model
from sidemantic.core.parameter import Parameter, ParameterSet
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator
from tests.utils import fetch_rows


def test_parameter_string_type():
    """Test string parameter formatting."""
    param = Parameter(name="status", type="string", default_value="pending")

    assert param.format_value("completed") == "'completed'"
    assert param.format_value("pending") == "'pending'"


def test_parameter_number_type():
    """Test number parameter formatting."""
    param = Parameter(name="min_amount", type="number", default_value=0)

    assert param.format_value(100) == "100"
    assert param.format_value(0) == "0"


def test_parameter_date_type():
    """Test date parameter formatting."""
    param = Parameter(name="start_date", type="date", default_value="2024-01-01")

    assert param.format_value("2024-01-15") == "'2024-01-15'"


def test_parameter_unquoted_type():
    """Test unquoted parameter formatting."""
    param = Parameter(name="table_name", type="unquoted", default_value="orders")

    assert param.format_value("customers") == "customers"
    assert param.format_value("orders") == "orders"


def test_parameter_yesno_type():
    """Test yesno parameter formatting."""
    param = Parameter(name="include_tax", type="yesno", default_value=False)

    assert param.format_value(True) == "TRUE"
    assert param.format_value(False) == "FALSE"


def test_parameter_default_value():
    """Test parameter default value handling."""
    param = Parameter(name="region", type="string", default_value="US")

    assert param.format_value(None) == "'US'"


def test_parameter_allowed_values():
    """Test parameter allowed values constraint."""
    param = Parameter(
        name="status",
        type="string",
        default_value="pending",
        allowed_values=["pending", "completed", "cancelled"],
    )

    # Just verify the param was created with allowed_values
    assert param.allowed_values == ["pending", "completed", "cancelled"]


def test_parameter_set_get():
    """Test ParameterSet get method."""
    params = {
        "status": Parameter(name="status", type="string", default_value="pending"),
        "min_amount": Parameter(name="min_amount", type="number", default_value=0),
    }

    param_set = ParameterSet(params, {"status": "completed", "min_amount": 100})

    assert param_set.get("status") == "completed"
    assert param_set.get("min_amount") == 100


def test_parameter_set_defaults():
    """Test ParameterSet defaults when values not provided."""
    params = {
        "status": Parameter(name="status", type="string", default_value="pending"),
        "min_amount": Parameter(name="min_amount", type="number", default_value=0),
    }

    param_set = ParameterSet(params, {})

    assert param_set.get("status") == "pending"
    assert param_set.get("min_amount") == 0


def test_parameter_set_format():
    """Test ParameterSet format method."""
    params = {
        "status": Parameter(name="status", type="string", default_value="pending"),
        "min_amount": Parameter(name="min_amount", type="number", default_value=0),
    }

    param_set = ParameterSet(params, {"status": "completed", "min_amount": 100})

    assert param_set.format("status") == "'completed'"
    assert param_set.format("min_amount") == "100"


def test_parameter_set_interpolate():
    """Test ParameterSet interpolate method."""
    params = {
        "status": Parameter(name="status", type="string", default_value="pending"),
        "min_amount": Parameter(name="min_amount", type="number", default_value=0),
    }

    param_set = ParameterSet(params, {"status": "completed", "min_amount": 100})

    sql = "SELECT * FROM orders WHERE status = {{ status }} AND amount >= {{ min_amount }}"
    result = param_set.interpolate(sql)

    assert result == "SELECT * FROM orders WHERE status = 'completed' AND amount >= 100"


def test_parameter_set_interpolate_with_spaces():
    """Test parameter interpolation with various spacing."""
    params = {
        "status": Parameter(name="status", type="string", default_value="pending"),
    }

    param_set = ParameterSet(params, {"status": "completed"})

    # Test various spacing patterns
    assert param_set.interpolate("{{ status }}") == "'completed'"
    assert param_set.interpolate("{{status}}") == "'completed'"
    assert param_set.interpolate("{{  status  }}") == "'completed'"


def test_parameter_set_interpolate_unknown_param():
    """Test that unknown parameters are left unchanged."""
    params = {
        "status": Parameter(name="status", type="string", default_value="pending"),
    }

    param_set = ParameterSet(params, {"status": "completed"})

    sql = "SELECT * FROM orders WHERE status = {{ status }} AND region = {{ region }}"
    result = param_set.interpolate(sql)

    # status should be interpolated, region should be left as-is
    assert result == "SELECT * FROM orders WHERE status = 'completed' AND region = {{ region }}"


def test_semantic_graph_add_parameter():
    """Test adding parameters to semantic graph."""
    graph = SemanticGraph()

    param1 = Parameter(name="status", type="string", default_value="pending")
    param2 = Parameter(name="min_amount", type="number", default_value=0)

    graph.add_parameter(param1)
    graph.add_parameter(param2)

    assert graph.get_parameter("status") == param1
    assert graph.get_parameter("min_amount") == param2


def test_semantic_graph_duplicate_parameter():
    """Test that adding duplicate parameter raises error."""
    graph = SemanticGraph()

    param = Parameter(name="status", type="string", default_value="pending")

    graph.add_parameter(param)

    with pytest.raises(ValueError, match="Parameter status already exists"):
        graph.add_parameter(param)


def test_semantic_graph_get_nonexistent_parameter():
    """Test that getting nonexistent parameter raises error."""
    graph = SemanticGraph()

    with pytest.raises(KeyError, match="Parameter status not found"):
        graph.get_parameter("status")


def test_sql_generator_with_parameters():
    """Test SQL generation with parameter interpolation."""
    # Create graph
    graph = SemanticGraph()

    # Add parameter
    status_param = Parameter(name="status", type="string", default_value="pending")
    graph.add_parameter(status_param)

    # Add model
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        relationships=[],
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date"),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    graph.add_model(orders)

    # Generate SQL with parameter
    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["orders.revenue"],
        dimensions=["orders.order_date"],
        filters=["orders.status = {{ status }}"],
        parameters={"status": "completed"},
    )

    # Verify parameter was interpolated
    assert "'completed'" in sql
    assert "{{ status }}" not in sql


def test_sql_generator_with_default_parameter():
    """Test SQL generation using parameter default value."""
    # Create graph
    graph = SemanticGraph()

    # Add parameter with default
    status_param = Parameter(name="status", type="string", default_value="pending")
    graph.add_parameter(status_param)

    # Add model
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        relationships=[],
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date"),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    graph.add_model(orders)

    # Generate SQL without providing parameter value
    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["orders.revenue"],
        dimensions=["orders.order_date"],
        filters=["orders.status = {{ status }}"],
        parameters={},  # No parameter value provided
    )

    # Verify default was used
    assert "'pending'" in sql
    assert "{{ status }}" not in sql


def test_sql_generator_with_multiple_parameters():
    """Test SQL generation with multiple parameters."""
    # Create graph
    graph = SemanticGraph()

    # Add parameters
    graph.add_parameter(Parameter(name="status", type="string", default_value="pending"))
    graph.add_parameter(Parameter(name="min_amount", type="number", default_value=0))

    # Add model
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        relationships=[],
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date"),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    graph.add_model(orders)

    # Generate SQL with multiple parameters
    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["orders.revenue"],
        dimensions=["orders.order_date"],
        filters=[
            "orders.status = {{ status }}",
            "orders.revenue >= {{ min_amount }}",
        ],
        parameters={"status": "completed", "min_amount": 100},
    )

    # Verify both parameters were interpolated
    assert "'completed'" in sql
    assert "100" in sql
    assert "{{ status }}" not in sql
    assert "{{ min_amount }}" not in sql


def test_parameters_with_actual_data():
    """Test parameters with actual DuckDB query execution."""
    # Create in-memory DuckDB
    conn = duckdb.connect(":memory:")

    # Create test data
    conn.execute("""
        CREATE TABLE raw_orders AS
        SELECT * FROM (VALUES
            (1, '2024-01-01'::DATE, 'pending', 100),
            (2, '2024-01-02'::DATE, 'completed', 200),
            (3, '2024-01-03'::DATE, 'completed', 300),
            (4, '2024-01-04'::DATE, 'cancelled', 150)
        ) AS t(id, order_date, status, amount)
    """)

    # Create graph
    graph = SemanticGraph()

    # Add parameter
    graph.add_parameter(Parameter(name="status", type="string", default_value="pending"))

    # Add model
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        relationships=[],
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date"),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    graph.add_model(orders)

    # Generate SQL with parameter
    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["orders.revenue"],
        dimensions=["orders.order_date"],
        filters=["orders.status = {{ status }}"],
        parameters={"status": "completed"},
    )

    # Execute query
    result = conn.execute(sql)
    rows = fetch_rows(result)

    # Should only get completed orders (2 rows: 200 and 300)
    assert len(rows) == 2
    assert sum(row[1] for row in rows) == 500  # Total revenue

    conn.close()


def test_numeric_parameter_sql_injection_protection():
    """Test that numeric parameters reject SQL injection attempts."""
    param = Parameter(name="amount", type="number", default_value=0)

    # Valid numeric values should work
    assert param.format_value(100) == "100"
    assert param.format_value(0) == "0"
    assert param.format_value(-50.5) == "-50.5"
    assert param.format_value("42") == "42.0"

    # SQL injection attempts should be rejected
    with pytest.raises(ValueError, match="Invalid numeric parameter value"):
        param.format_value("0); DROP TABLE users; --")

    with pytest.raises(ValueError, match="Invalid numeric parameter value"):
        param.format_value("1 OR 1=1")

    with pytest.raises(ValueError, match="Invalid numeric parameter value"):
        param.format_value("'; DELETE FROM orders; --")

    with pytest.raises(ValueError):
        param.format_value("not_a_number")

    # Non-string, non-numeric types should be rejected
    with pytest.raises(ValueError, match="Numeric parameter must be"):
        param.format_value([1, 2, 3])

    with pytest.raises(ValueError, match="Numeric parameter must be"):
        param.format_value({"value": 100})


def test_unquoted_parameter_sql_injection_protection():
    """Test that unquoted parameters reject dangerous values."""
    param = Parameter(name="table_name", type="unquoted", default_value="orders")

    # Valid identifiers should work
    assert param.format_value("customers") == "customers"
    assert param.format_value("user_orders") == "user_orders"
    assert param.format_value("schema.table") == "schema.table"
    assert param.format_value("table123") == "table123"

    # SQL injection attempts should be rejected
    with pytest.raises(ValueError, match="must be alphanumeric"):
        param.format_value("orders; DROP TABLE users; --")

    with pytest.raises(ValueError, match="must be alphanumeric"):
        param.format_value("orders' OR '1'='1")

    with pytest.raises(ValueError, match="must be alphanumeric"):
        param.format_value("table (SELECT * FROM passwords)")


def test_string_parameter_escaping():
    """Test that string parameters properly escape quotes."""
    param = Parameter(name="description", type="string", default_value="")

    # Normal strings
    assert param.format_value("test") == "'test'"

    # Strings with single quotes should be escaped
    assert param.format_value("O'Reilly") == "'O''Reilly'"
    assert param.format_value("it's") == "'it''s'"

    # SQL injection attempts should be escaped (not rejected, but rendered harmless)
    result = param.format_value("'; DROP TABLE users; --")
    assert result == "'''; DROP TABLE users; --'"
    # The single quote at the start is escaped, so it becomes a literal string value


def test_parameter_interpolation_with_sql_injection():
    """Test that parameter interpolation prevents SQL injection in actual queries."""
    params = {
        "amount": Parameter(name="amount", type="number", default_value=0),
        "table": Parameter(name="table", type="unquoted", default_value="orders"),
    }

    param_set = ParameterSet(params)

    # Attempt SQL injection via numeric parameter should raise error
    with pytest.raises(ValueError, match="Invalid numeric parameter value"):
        param_set.format("amount")  # Will try to use default
        # Now try with malicious value
        param_set_bad = ParameterSet(params, {"amount": "0); DROP TABLE users; --"})
        param_set_bad.format("amount")

    # Attempt SQL injection via unquoted parameter should raise error
    with pytest.raises(ValueError, match="must be alphanumeric"):
        param_set_bad = ParameterSet(params, {"table": "orders; DROP TABLE users; --"})
        param_set_bad.format("table")


def test_query_method_accepts_parameters():
    """Test that .query() method accepts parameters argument.

    Bug: Documentation showed parameters argument but method didn't accept it.
    Fix: Add parameters argument and forward to compile().
    """
    from sidemantic import Dimension, Metric, Model, SemanticLayer

    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE orders_table (
            order_id INTEGER,
            region VARCHAR,
            amount DECIMAL(10, 2)
        )
    """)
    conn.execute("INSERT INTO orders_table VALUES (1, 'US', 100)")

    layer.conn = conn
    layer.add_model(orders)

    # Should accept parameters argument without error
    result = layer.query(metrics=["orders.revenue"], dimensions=["orders.region"], parameters={"test_param": "value"})

    # Just verify it doesn't crash - parameters may not be used in this query
    assert result is not None
