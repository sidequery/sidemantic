"""Test Jinja template rendering in SQL fields."""

from sidemantic.core.template import SQLTemplateRenderer, render_sql_template, is_sql_template
from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_basic_template_rendering():
    """Test basic Jinja template rendering."""
    renderer = SQLTemplateRenderer()

    result = renderer.render("SELECT * FROM {{ table }}", {"table": "orders"})
    assert result == "SELECT * FROM orders"


def test_conditional_template():
    """Test conditional template rendering."""
    renderer = SQLTemplateRenderer()

    # With condition true
    result = renderer.render(
        "{% if active %}status = 'active'{% endif %}",
        {"active": True}
    )
    assert result == "status = 'active'"

    # With condition false
    result = renderer.render(
        "{% if active %}status = 'active'{% else %}1=1{% endif %}",
        {"active": False}
    )
    assert result == "1=1"


def test_template_with_loop():
    """Test template with for loop."""
    renderer = SQLTemplateRenderer()

    result = renderer.render(
        "id IN ({% for id in ids %}{{ id }}{% if not loop.last %}, {% endif %}{% endfor %})",
        {"ids": [1, 2, 3]}
    )
    assert result == "id IN (1, 2, 3)"


def test_is_template():
    """Test checking if string is a template."""
    assert is_sql_template("SELECT * FROM {{ table }}") is True
    assert is_sql_template("{% if x %}foo{% endif %}") is True
    assert is_sql_template("{# comment #}") is True
    assert is_sql_template("SELECT * FROM orders") is False


def test_render_if_template():
    """Test conditional rendering."""
    # Template should be rendered
    result = render_sql_template("{{ col }}", {"col": "amount"})
    assert result == "amount"

    # Non-template should pass through
    result = render_sql_template("amount", {"col": "amount"})
    assert result == "amount"


def test_template_with_filters():
    """Test template with Jinja filters."""
    renderer = SQLTemplateRenderer()

    result = renderer.render(
        "SELECT {{ col | upper }}",
        {"col": "name"}
    )
    assert result == "SELECT NAME"


def test_template_in_metric_sql():
    """Test using templates in metric SQL."""
    layer = SemanticLayer()

    # Note: This test just validates the template would work
    # Actual integration would need parameter passing
    renderer = SQLTemplateRenderer()

    sql_template = "CASE WHEN {{ condition }} THEN amount ELSE 0 END"
    rendered = renderer.render(sql_template, {"condition": "status = 'active'"})

    assert rendered == "CASE WHEN status = 'active' THEN amount ELSE 0 END"


def test_template_with_multiple_variables():
    """Test template with multiple variables."""
    renderer = SQLTemplateRenderer()

    result = renderer.render(
        "{{ col1 }} + {{ col2 }}",
        {"col1": "revenue", "col2": "costs"}
    )
    assert result == "revenue + costs"


def test_template_error_handling():
    """Test template syntax error handling."""
    renderer = SQLTemplateRenderer()

    try:
        renderer.render("{{ unclosed", {})
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Template syntax error" in str(e)


def test_complex_template_example():
    """Test complex real-world template example."""
    renderer = SQLTemplateRenderer()

    template = """
    CASE
        {% for status, value in status_map.items() %}
        WHEN status = '{{ status }}' THEN {{ value }}
        {% endfor %}
        ELSE 0
    END
    """.strip()

    result = renderer.render(template, {
        "status_map": {
            "completed": 1,
            "pending": 0.5,
            "cancelled": 0
        }
    })

    assert "WHEN status = 'completed' THEN 1" in result
    assert "WHEN status = 'pending' THEN 0.5" in result
    assert "ELSE 0" in result


def test_template_with_date_logic():
    """Test template with date-based logic."""
    renderer = SQLTemplateRenderer()

    template = """
    {% if use_recent %}
    created_at >= CURRENT_DATE - {{ days }}
    {% else %}
    1=1
    {% endif %}
    """.strip()

    result = renderer.render(template, {"use_recent": True, "days": 30})
    assert "created_at >= CURRENT_DATE - 30" in result

    result = renderer.render(template, {"use_recent": False, "days": 30})
    assert "1=1" in result
