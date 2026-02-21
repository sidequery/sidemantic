"""Tests for the LSP server."""

from lsprotocol import types as lsp

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.lsp.server import (
    DEF_TYPE_TO_MODEL,
    DEF_TYPE_TO_SYMBOL_KIND,
    KEYWORDS,
    build_code_actions,
    build_document_symbols,
    build_python_document_symbols,
    build_python_signature_help,
    build_reference_locations,
    build_rename_workspace_edit,
    build_signature_help,
    extract_definitions,
    extract_python_definitions,
    find_definition_by_name,
    find_python_definition_by_name,
    format_sidemantic_document,
    get_all_properties,
    get_completion_context,
    get_field_docs,
    get_python_constructor_context,
    get_word_at_position,
    range_equals,
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


def test_extract_definitions():
    """Test extracting named top-level definitions from SQL text."""
    text = """MODEL (
    name orders,
    table order_items
);

METRIC (
    name revenue,
    model orders,
    sql amount,
    agg sum
);
"""
    definitions = extract_definitions(text)
    assert [definition.def_type for definition in definitions] == ["MODEL", "METRIC"]
    assert [definition.name for definition in definitions] == ["orders", "revenue"]
    assert definitions[0].name_range.start.line == 1
    assert definitions[1].name_range.start.line == 6


def test_find_definition_by_name_case_insensitive():
    """Test finding definitions by name regardless of case."""
    text = """MODEL (
    name orders,
    table order_items
);
"""
    definition = find_definition_by_name(text, "ORDERS")
    assert definition is not None
    assert definition.name == "orders"


def test_build_reference_locations_include_declaration():
    """Test references include declaration when requested."""
    uri = "file:///workspace/orders.sidemantic.sql"
    text = """MODEL (
    name orders,
    table order_items
);

METRIC (
    name revenue,
    model orders,
    sql amount,
    agg sum
);
"""
    references = build_reference_locations(uri=uri, text=text, word="orders", include_declaration=True)
    assert len(references) == 2
    assert all(reference.uri == uri for reference in references)


def test_build_reference_locations_exclude_declaration():
    """Test references exclude declaration when requested."""
    uri = "file:///workspace/orders.sidemantic.sql"
    text = """MODEL (
    name orders,
    table order_items
);

METRIC (
    name revenue,
    model orders,
    sql amount,
    agg sum
);
"""
    references = build_reference_locations(uri=uri, text=text, word="orders", include_declaration=False)
    assert len(references) == 1
    assert references[0].range.start.line == 7


def test_build_document_symbols():
    """Test document symbol generation for definition blocks."""
    text = """MODEL (
    name orders,
    table order_items
);

METRIC (
    name revenue,
    model orders,
    sql amount,
    agg sum
);
"""
    symbols = build_document_symbols(text)
    assert [symbol.name for symbol in symbols] == ["orders", "revenue"]
    assert [symbol.kind for symbol in symbols] == [
        DEF_TYPE_TO_SYMBOL_KIND["MODEL"],
        DEF_TYPE_TO_SYMBOL_KIND["METRIC"],
    ]


def test_build_rename_workspace_edit():
    """Test workspace edits rename all identifier matches."""
    uri = "file:///workspace/orders.sidemantic.sql"
    text = """MODEL (
    name orders,
    table order_items
);

METRIC (
    name revenue,
    model orders,
    sql amount,
    agg sum
);
"""
    workspace_edit = build_rename_workspace_edit(
        uri=uri,
        text=text,
        old_name="orders",
        new_name="sales_orders",
    )
    assert workspace_edit is not None
    assert workspace_edit.changes is not None
    edits = workspace_edit.changes[uri]
    assert len(edits) == 2
    assert all(edit.new_text == "sales_orders" for edit in edits)


def test_build_rename_workspace_edit_skips_keywords():
    """Test renaming keywords is blocked."""
    uri = "file:///workspace/orders.sidemantic.sql"
    text = "MODEL (name orders);"
    workspace_edit = build_rename_workspace_edit(
        uri=uri,
        text=text,
        old_name="MODEL",
        new_name="SOMETHING",
    )
    assert workspace_edit is None


def test_range_equals():
    """Test range equality helper."""
    first = lsp.Range(start=lsp.Position(line=1, character=2), end=lsp.Position(line=1, character=8))
    second = lsp.Range(start=lsp.Position(line=1, character=2), end=lsp.Position(line=1, character=8))
    third = lsp.Range(start=lsp.Position(line=1, character=3), end=lsp.Position(line=1, character=8))
    assert range_equals(first, second)
    assert not range_equals(first, third)


def test_format_sidemantic_document_roundtrip():
    """Test formatting Sidemantic definitions into canonical multiline form."""
    unformatted = """MODEL (name orders,table order_items);\n\nMETRIC (name revenue,expression amount,agg sum);\n"""
    formatted = format_sidemantic_document(unformatted)
    assert formatted is not None
    assert formatted.startswith("MODEL (")
    assert "    name orders," in formatted
    assert "    table order_items" in formatted
    assert "METRIC (" in formatted
    assert formatted.endswith("\n")


def test_format_sidemantic_document_invalid_returns_none():
    """Test invalid Sidemantic SQL returns no formatted text."""
    invalid = "asdf"
    assert format_sidemantic_document(invalid) is None


def test_build_signature_help_inside_definition():
    """Test signature help when cursor is inside a definition block."""
    text = """MODEL (
    name orders,
    table order_items
);
"""
    signature_help = build_signature_help(text, 1, 8)
    assert signature_help is not None
    assert len(signature_help.signatures) == 1
    assert signature_help.signatures[0].label.startswith("MODEL(")


def test_build_signature_help_top_level_keyword():
    """Test signature help when cursor is on a top-level keyword."""
    text = "MODEL (\n    name orders\n);"
    signature_help = build_signature_help(text, 0, 2)
    assert signature_help is not None
    assert signature_help.signatures[0].label.startswith("MODEL(")


def test_build_signature_help_none_for_non_keyword_top_level():
    """Test signature help returns None when cursor context is unrelated."""
    text = "SELECT 1"
    assert build_signature_help(text, 0, 1) is None


def test_get_python_constructor_context_inside_model():
    """Test Python constructor context detection."""
    text = """from sidemantic import Model

orders = Model(
    name="orders",
    table="orders",
)
"""
    assert get_python_constructor_context(text, 3, 8) == "Model"


def test_extract_python_definitions():
    """Test extracting Sidemantic definitions from Python constructor calls."""
    text = """from sidemantic import Model, Metric

orders = Model(name="orders", table="orders")
revenue = Metric(name="revenue", agg="sum", sql="amount")
"""
    definitions = extract_python_definitions(text)
    assert [definition.def_type for definition in definitions] == ["MODEL", "METRIC"]
    assert [definition.name for definition in definitions] == ["orders", "revenue"]


def test_find_python_definition_by_name_case_insensitive():
    """Test case-insensitive definition lookup in Python files."""
    text = """from sidemantic import Model
orders = Model(name="orders", table="orders")
"""
    definition = find_python_definition_by_name(text, "ORDERS")
    assert definition is not None
    assert definition.name == "orders"


def test_build_python_document_symbols():
    """Test document symbols for Python constructor definitions."""
    text = """from sidemantic import Model, Metric

orders = Model(name="orders", table="orders")
revenue = Metric(name="revenue", agg="sum", sql="amount")
"""
    symbols = build_python_document_symbols(text)
    assert [symbol.name for symbol in symbols] == ["orders", "revenue"]
    assert [symbol.kind for symbol in symbols] == [
        DEF_TYPE_TO_SYMBOL_KIND["MODEL"],
        DEF_TYPE_TO_SYMBOL_KIND["METRIC"],
    ]


def test_build_python_signature_help_inside_constructor():
    """Test signature help for Python constructor calls."""
    text = """from sidemantic import Model

orders = Model(
    name="orders",
)
"""
    signature_help = build_python_signature_help(text, 3, 8)
    assert signature_help is not None
    assert signature_help.signatures[0].label.startswith("Model(")


def test_build_code_actions_missing_name_quick_fix():
    """Test code action suggests missing name property fix."""
    uri = "file:///workspace/orders.sidemantic.sql"
    text = """MODEL (
    table order_items
);
"""
    diagnostics = [
        lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=10),
            ),
            message="1 validation error for Model\nname\n  Field required",
            severity=lsp.DiagnosticSeverity.Error,
            source="sidemantic",
        )
    ]
    actions = build_code_actions(uri=uri, text=text, diagnostics=diagnostics)
    assert len(actions) == 1
    action = actions[0]
    assert action.title == "Add missing name property"
    assert action.edit is not None
    assert action.edit.changes is not None
    edits = action.edit.changes[uri]
    assert len(edits) == 1
    assert "name model_name," in edits[0].new_text


def test_build_code_actions_no_known_fix():
    """Test no code action is returned for unrelated diagnostics."""
    uri = "file:///workspace/orders.sidemantic.sql"
    text = """MODEL (
    name orders,
    table order_items
);
"""
    diagnostics = [
        lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=1, character=0),
                end=lsp.Position(line=1, character=12),
            ),
            message="Some unrelated error",
            severity=lsp.DiagnosticSeverity.Error,
            source="sidemantic",
        )
    ]
    actions = build_code_actions(uri=uri, text=text, diagnostics=diagnostics)
    assert actions == []


def test_keywords_include_core_definitions():
    """Test keyword list stays aligned with core top-level definition types."""
    assert sorted(KEYWORDS) == sorted(DEF_TYPE_TO_MODEL.keys())
