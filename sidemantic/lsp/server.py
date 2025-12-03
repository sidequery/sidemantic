"""LSP server for the Sidemantic SQL dialect.

Provides editor support for .sql files using Sidemantic syntax:
- MODEL, DIMENSION, METRIC, RELATIONSHIP, SEGMENT statements
- Property completions and validation
- Hover documentation

This is NOT a general SQL language server.
"""

from lsprotocol import types as lsp
from pygls.lsp.server import LanguageServer

from sidemantic.core.dialect import DimensionDef, MetricDef, ModelDef, PropertyEQ, RelationshipDef, SegmentDef, parse
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment

# Keywords for top-level statements
KEYWORDS = ["MODEL", "DIMENSION", "METRIC", "RELATIONSHIP", "SEGMENT"]

# Map definition types to their pydantic models for property lookup
DEF_TYPE_TO_MODEL = {
    "MODEL": Model,
    "DIMENSION": Dimension,
    "METRIC": Metric,
    "RELATIONSHIP": Relationship,
    "SEGMENT": Segment,
}


def get_field_docs(model_class, field_name: str) -> str | None:
    """Get field description from pydantic model."""
    field_info = model_class.model_fields.get(field_name)
    if field_info:
        return field_info.description
    return None


def get_all_properties(model_class) -> list[tuple[str, str | None]]:
    """Get all property names and descriptions for a model."""
    return [(name, field.description) for name, field in model_class.model_fields.items()]


def create_server() -> LanguageServer:
    """Create and configure the LSP server."""
    server = LanguageServer("sidemantic", "0.1.0")

    @server.feature(lsp.TEXT_DOCUMENT_DID_OPEN)
    def did_open(params: lsp.DidOpenTextDocumentParams):
        """Handle document open - run initial diagnostics."""
        validate_document(server, params.text_document.uri)

    @server.feature(lsp.TEXT_DOCUMENT_DID_CHANGE)
    def did_change(params: lsp.DidChangeTextDocumentParams):
        """Handle document change - revalidate."""
        validate_document(server, params.text_document.uri)

    @server.feature(lsp.TEXT_DOCUMENT_DID_SAVE)
    def did_save(params: lsp.DidSaveTextDocumentParams):
        """Handle document save - revalidate."""
        validate_document(server, params.text_document.uri)

    @server.feature(lsp.TEXT_DOCUMENT_COMPLETION)
    def completions(params: lsp.CompletionParams) -> lsp.CompletionList:
        """Provide completions based on context."""
        document = server.workspace.get_text_document(params.text_document.uri)
        text = document.source
        line = params.position.line
        character = params.position.character

        # Get current line up to cursor
        lines = text.split("\n")
        if line >= len(lines):
            return lsp.CompletionList(is_incomplete=False, items=[])

        # Determine context
        context = get_completion_context(text, line, character)

        items = []

        if context == "top_level":
            # Suggest keywords
            for kw in KEYWORDS:
                items.append(
                    lsp.CompletionItem(
                        label=kw,
                        kind=lsp.CompletionItemKind.Keyword,
                        detail="Sidemantic definition",
                        insert_text=f"{kw} (\n    name $1,\n    $0\n);",
                        insert_text_format=lsp.InsertTextFormat.Snippet,
                    )
                )
        elif context.startswith("inside_"):
            # Inside a definition block - suggest properties
            def_type = context.replace("inside_", "").upper()
            model_class = DEF_TYPE_TO_MODEL.get(def_type)
            if model_class:
                for prop_name, description in get_all_properties(model_class):
                    items.append(
                        lsp.CompletionItem(
                            label=prop_name,
                            kind=lsp.CompletionItemKind.Property,
                            detail=description or f"{def_type} property",
                            insert_text=f"{prop_name} $0,",
                            insert_text_format=lsp.InsertTextFormat.Snippet,
                        )
                    )

        return lsp.CompletionList(is_incomplete=False, items=items)

    @server.feature(lsp.TEXT_DOCUMENT_HOVER)
    def hover(params: lsp.HoverParams) -> lsp.Hover | None:
        """Provide hover information for keywords and properties."""
        document = server.workspace.get_text_document(params.text_document.uri)
        text = document.source
        line = params.position.line
        character = params.position.character

        # Get word at position
        word = get_word_at_position(text, line, character)
        if not word:
            return None

        word_upper = word.upper()

        # Check if it's a keyword
        if word_upper in KEYWORDS:
            model_class = DEF_TYPE_TO_MODEL.get(word_upper)
            if model_class:
                doc = model_class.__doc__ or f"{word_upper} definition"
                return lsp.Hover(
                    contents=lsp.MarkupContent(kind=lsp.MarkupKind.Markdown, value=f"**{word_upper}**\n\n{doc}")
                )

        # Check if it's a property - need to find context
        context = get_completion_context(text, line, character)
        if context.startswith("inside_"):
            def_type = context.replace("inside_", "").upper()
            model_class = DEF_TYPE_TO_MODEL.get(def_type)
            if model_class:
                description = get_field_docs(model_class, word.lower())
                if description:
                    return lsp.Hover(
                        contents=lsp.MarkupContent(kind=lsp.MarkupKind.Markdown, value=f"**{word}**\n\n{description}")
                    )

        return None

    return server


def validate_document(server: LanguageServer, uri: str):
    """Validate document and publish diagnostics."""
    document = server.workspace.get_text_document(uri)
    text = document.source
    diagnostics = []

    try:
        statements = parse(text)

        # Validate each statement
        for stmt in statements:
            if isinstance(stmt, (ModelDef, DimensionDef, MetricDef, RelationshipDef, SegmentDef)):
                # Extract properties
                props = {}
                for expr in stmt.expressions:
                    if isinstance(expr, PropertyEQ):
                        key = expr.this.this
                        value = expr.expression.this
                        props[key] = value

                # Try to create pydantic model for validation
                def_type = type(stmt).__name__.replace("Def", "").upper()
                model_class = DEF_TYPE_TO_MODEL.get(def_type)

                if model_class and "name" in props:
                    try:
                        # Attempt to create instance for validation
                        model_class(**props)
                    except Exception as e:
                        # Find line number for this definition (approximate)
                        name = props.get("name", "unknown")
                        line_num = find_definition_line(text, def_type, name)
                        diagnostics.append(
                            lsp.Diagnostic(
                                range=lsp.Range(
                                    start=lsp.Position(line=line_num, character=0),
                                    end=lsp.Position(line=line_num, character=100),
                                ),
                                message=str(e),
                                severity=lsp.DiagnosticSeverity.Error,
                                source="sidemantic",
                            )
                        )

    except Exception as e:
        # Parse error
        diagnostics.append(
            lsp.Diagnostic(
                range=lsp.Range(
                    start=lsp.Position(line=0, character=0),
                    end=lsp.Position(line=0, character=100),
                ),
                message=f"Parse error: {e}",
                severity=lsp.DiagnosticSeverity.Error,
                source="sidemantic",
            )
        )

    server.publish_diagnostics(uri, diagnostics)


def get_completion_context(text: str, line: int, character: int) -> str:
    """Determine completion context based on cursor position."""
    lines = text.split("\n")

    # Look backwards to find what block we're in
    paren_depth = 0
    current_def = None

    for i in range(line, -1, -1):
        check_line = lines[i] if i < line else lines[i][:character]

        # Count parens (backwards)
        for char in reversed(check_line):
            if char == ")":
                paren_depth += 1
            elif char == "(":
                paren_depth -= 1

        # Check for definition start
        for kw in KEYWORDS:
            if kw in lines[i].upper() and "(" in lines[i]:
                if paren_depth < 0:
                    return f"inside_{kw.lower()}"
                current_def = kw

    if paren_depth < 0 and current_def:
        return f"inside_{current_def.lower()}"

    return "top_level"


def get_word_at_position(text: str, line: int, character: int) -> str | None:
    """Get the word at the given position."""
    lines = text.split("\n")
    if line >= len(lines):
        return None

    current_line = lines[line]
    if character >= len(current_line):
        character = len(current_line) - 1
    if character < 0:
        return None

    # Find word boundaries
    start = character
    end = character

    while start > 0 and (current_line[start - 1].isalnum() or current_line[start - 1] == "_"):
        start -= 1

    while end < len(current_line) and (current_line[end].isalnum() or current_line[end] == "_"):
        end += 1

    if start == end:
        return None

    return current_line[start:end]


def find_definition_line(text: str, def_type: str, name: str) -> int:
    """Find the line number of a definition by name."""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if def_type in line.upper() and name in line:
            return i
    return 0


def main():
    """Run the LSP server."""
    server = create_server()
    server.start_io()


if __name__ == "__main__":
    main()
