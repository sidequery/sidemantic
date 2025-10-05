"""Test Jinja template integration with parameter system."""

from sidemantic import Dimension, Metric, Model, Parameter, SemanticLayer


def test_simple_parameter_substitution():
    """Test simple parameter substitution still works."""
    layer = SemanticLayer()
    layer.graph.add_parameter(Parameter(name="min_amount", type="number", default_value=100))

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Use parameter in filter
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        filters=["orders_cte.amount >= {{ min_amount }}"],
        parameters={"min_amount": 500},
    )

    # Should have substituted the value
    assert "500" in sql
    assert "{{ min_amount }}" not in sql


def test_jinja_conditional_with_parameters():
    """Test Jinja conditional template with parameters."""
    layer = SemanticLayer()
    layer.graph.add_parameter(Parameter(name="include_pending", type="yesno", default_value=False))

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Filter with Jinja conditional
    template_filter = "{% if include_pending %}orders_cte.status IN ('completed', 'pending'){% else %}orders_cte.status = 'completed'{% endif %}"

    # With include_pending = False
    sql = layer.compile(
        metrics=["orders.revenue"], filters=[template_filter], parameters={"include_pending": False}
    )

    assert "status = 'completed'" in sql
    assert "pending" not in sql.lower()

    # With include_pending = True
    sql = layer.compile(
        metrics=["orders.revenue"], filters=[template_filter], parameters={"include_pending": True}
    )

    assert (
        "status IN ('completed', 'pending')" in sql
        or "IN('completed', 'pending')" in sql.replace(" ", "")
    )


def test_jinja_loop_with_parameters():
    """Test Jinja loop template with parameters."""
    from sidemantic.core.parameter import Parameter, ParameterSet

    # Create parameter set
    params = {"status_list": Parameter(name="status_list", type="unquoted")}
    param_set = ParameterSet(params, {"status_list": ["completed", "shipped", "delivered"]})

    # Template with loop
    template = "status IN ({% for s in status_list %}'{{ s }}'{% if not loop.last %}, {% endif %}{% endfor %})"

    result = param_set.interpolate(template)

    # Just check it has the values in order
    assert "'completed'" in result
    assert "'shipped'" in result
    assert "'delivered'" in result
    assert "status IN" in result


def test_mixed_simple_and_complex_templates():
    """Test mix of simple and complex templates."""
    from sidemantic.core.parameter import Parameter, ParameterSet

    params = {
        "min_val": Parameter(name="min_val", type="number"),
        "use_filter": Parameter(name="use_filter", type="yesno"),
    }

    param_set = ParameterSet(params, {"min_val": 100, "use_filter": True})

    # Simple substitution
    simple = "amount >= {{ min_val }}"
    assert param_set.interpolate(simple) == "amount >= 100"

    # Complex template
    complex_template = "{% if use_filter %}amount >= {{ min_val }}{% else %}1=1{% endif %}"
    result = param_set.interpolate(complex_template)
    assert "amount >= 100" in result


def test_template_with_date_parameter():
    """Test template with date parameter."""
    from sidemantic.core.parameter import Parameter, ParameterSet

    params = {
        "start_date": Parameter(name="start_date", type="date"),
        "use_date_filter": Parameter(name="use_date_filter", type="yesno"),
    }

    param_set = ParameterSet(params, {"start_date": "2024-01-01", "use_date_filter": True})

    template = "{% if use_date_filter %}created_at >= '{{ start_date }}'{% else %}1=1{% endif %}"
    result = param_set.interpolate(template)

    assert "created_at >= '2024-01-01'" in result
