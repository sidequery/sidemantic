"""Tests for the LSP server."""

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.lsp.server import (
    DEF_TYPE_TO_MODEL,
    get_all_properties,
    get_completion_context,
    get_field_docs,
    get_word_at_position,
)


def test_get_completion_context_top_level():
    """Test context detection at top level."""
    text = """
MODEL (
    name orders
);

"""
    # After the MODEL definition, we're at top level
    assert get_completion_context(text, 5, 0) == "top_level"


def test_get_completion_context_inside_model():
    """Test context detection inside MODEL block."""
    text = """MODEL (
    name orders,

);"""
    # Line 2, inside the MODEL parens
    assert get_completion_context(text, 2, 4) == "inside_model"


def test_get_completion_context_inside_metric():
    """Test context detection inside METRIC block."""
    text = """MODEL (name orders);

METRIC (
    name revenue,

);"""
    # Line 4, inside the METRIC parens
    assert get_completion_context(text, 4, 4) == "inside_metric"


def test_get_word_at_position():
    """Test word extraction at cursor position."""
    text = "MODEL (\n    name orders,\n);"

    # At "MODEL"
    assert get_word_at_position(text, 0, 2) == "MODEL"

    # At "name"
    assert get_word_at_position(text, 1, 6) == "name"

    # At "orders"
    assert get_word_at_position(text, 1, 12) == "orders"


def test_get_word_at_position_empty():
    """Test word extraction returns None for whitespace."""
    text = "MODEL (  )"
    # At whitespace
    assert get_word_at_position(text, 0, 8) is None


def test_get_all_properties():
    """Test getting all properties from a model class."""
    props = get_all_properties(Model)
    prop_names = [name for name, _ in props]

    assert "name" in prop_names
    assert "table" in prop_names
    assert "primary_key" in prop_names
    assert "dimensions" in prop_names
    assert "metrics" in prop_names


def test_get_field_docs():
    """Test getting field documentation."""
    # Model.name should have a description
    doc = get_field_docs(Model, "name")
    assert doc is not None
    assert "name" in doc.lower() or "model" in doc.lower() or "unique" in doc.lower()

    # Metric.agg should have a description
    doc = get_field_docs(Metric, "agg")
    assert doc is not None


def test_def_type_to_model_mapping():
    """Test that all definition types map to valid models."""
    assert "MODEL" in DEF_TYPE_TO_MODEL
    assert "DIMENSION" in DEF_TYPE_TO_MODEL
    assert "METRIC" in DEF_TYPE_TO_MODEL
    assert "RELATIONSHIP" in DEF_TYPE_TO_MODEL
    assert "SEGMENT" in DEF_TYPE_TO_MODEL

    # All values should be pydantic model classes
    for model_class in DEF_TYPE_TO_MODEL.values():
        assert hasattr(model_class, "model_fields")
