"""Tests for TMDL parser."""

import textwrap

import pytest

from sidemantic.adapters.tmdl_parser import TmdlExpression, TmdlParseError, TmdlParser, merge_documents


def test_parser_description_and_meta():
    """Descriptions and meta blocks are parsed."""
    tmdl = textwrap.dedent(
        """
        /// Model description line
        model 'Sales Model'
            expression Server = "localhost" meta [IsParameterQuery=true, Type="Text"]
        """
    )

    parser = TmdlParser()
    doc = parser.parse(tmdl)
    model = doc.nodes[0]
    assert model.description == "Model description line"

    expr = next(child for child in model.children if child.type == "expression")
    assert isinstance(expr.default_property, TmdlExpression)
    assert expr.default_property.text == '"localhost"'
    assert expr.default_property.meta["IsParameterQuery"] is True
    assert expr.default_property.meta["Type"] == "Text"


def test_parser_backtick_block():
    """Backtick expressions preserve text blocks."""
    tmdl = textwrap.dedent(
        """
        table test
            measure revenue = ```
                SUM('test'[amount])
                ```
        """
    )

    parser = TmdlParser()
    doc = parser.parse(tmdl)
    table = doc.nodes[0]
    measure = next(child for child in table.children if child.type == "measure")
    assert isinstance(measure.default_property, TmdlExpression)
    assert measure.default_property.text == "SUM('test'[amount])"
    assert measure.default_property.block_delimiter == "```"


def test_parser_preserves_leading_comments_on_object():
    tmdl = textwrap.dedent(
        """
        # file comment
        table test
            column id
                dataType: int64
        """
    )

    parser = TmdlParser()
    doc = parser.parse(tmdl)
    table = doc.nodes[0]
    assert table.leading_comments == ["# file comment"]


def test_parser_preserves_leading_comments_on_dedented_sibling_object():
    tmdl = textwrap.dedent(
        """
        table test
            # first child comment
            column id
                dataType: int64
            // second child comment
            measure revenue = SUM('test'[amount])
        """
    )

    parser = TmdlParser()
    doc = parser.parse(tmdl)
    table = doc.nodes[0]
    column = next(child for child in table.children if child.type == "column")
    measure = next(child for child in table.children if child.type == "measure")
    assert column.leading_comments == ["# first child comment"]
    assert measure.leading_comments == ["// second child comment"]


def test_parser_preserves_leading_comments_on_root_sibling_object():
    tmdl = textwrap.dedent(
        """
        table sales
            column id
                dataType: int64
        // relationship comment
        relationship sales_products
            fromColumn: sales[id]
            toColumn: products[id]
            fromCardinality: many
            toCardinality: one
        """
    )

    parser = TmdlParser()
    doc = parser.parse(tmdl)
    relationship = next(node for node in doc.nodes if node.type == "relationship")
    assert relationship.leading_comments == ["// relationship comment"]


def test_parser_allows_unindented_blank_lines_between_child_objects():
    tmdl = textwrap.dedent(
        """
        table test
            column id
                dataType: int64

            measure revenue = SUM('test'[amount])
        """
    )

    parser = TmdlParser()
    doc = parser.parse(tmdl)
    table = doc.nodes[0]
    assert any(child.type == "column" for child in table.children)
    assert any(child.type == "measure" for child in table.children)


def test_parser_backtick_block_unterminated_raises_parse_error():
    """Unterminated backtick expressions raise a typed parser error with location."""
    tmdl = textwrap.dedent(
        """
        table test
            measure revenue = ```
                SUM('test'[amount])
        """
    )

    parser = TmdlParser()
    with pytest.raises(TmdlParseError, match="Unterminated backtick expression block") as exc_info:
        parser.parse(tmdl, file="bad.tmdl")

    assert exc_info.value.location is not None
    assert exc_info.value.location.file == "bad.tmdl"
    assert exc_info.value.location.line == 3
    assert exc_info.value.location.column == 5


def test_parser_create_or_replace():
    """createOrReplace scripts parse into a root node."""
    tmdl = textwrap.dedent(
        """
        createOrReplace
            table test
                column id
                    dataType: int64
        """
    )

    parser = TmdlParser()
    doc = parser.parse(tmdl)
    root = doc.nodes[0]
    assert root.type.lower() == "createorreplace"
    table = root.children[0]
    assert table.type == "table"


def test_parser_merge_partial_declarations():
    """Partial declarations merge without losing properties."""
    part1 = textwrap.dedent(
        """
        table test
            column id
                dataType: int64
        """
    )
    part2 = textwrap.dedent(
        """
        table test
            measure count = COUNTROWS('test')
        """
    )

    parser = TmdlParser()
    doc1 = parser.parse(part1, file="part1.tmdl")
    doc2 = parser.parse(part2, file="part2.tmdl")

    merged = merge_documents([doc1, doc2])
    table = merged[0]
    assert any(child.type == "column" for child in table.children)
    assert any(child.type == "measure" for child in table.children)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
