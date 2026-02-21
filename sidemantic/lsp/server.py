"""LSP server for the Sidemantic SQL dialect.

Provides editor support for .sql files using Sidemantic syntax:
- MODEL, DIMENSION, METRIC, RELATIONSHIP, SEGMENT statements
- Property completions and validation
- Hover documentation

This is NOT a general SQL language server.
"""

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

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
IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*"

# Map definition types to their pydantic models for property lookup
DEF_TYPE_TO_MODEL = {
    "MODEL": Model,
    "DIMENSION": Dimension,
    "METRIC": Metric,
    "RELATIONSHIP": Relationship,
    "SEGMENT": Segment,
}

DEF_TYPE_TO_SYMBOL_KIND = {
    "MODEL": lsp.SymbolKind.Class,
    "DIMENSION": lsp.SymbolKind.Property,
    "METRIC": lsp.SymbolKind.Function,
    "RELATIONSHIP": lsp.SymbolKind.Interface,
    "SEGMENT": lsp.SymbolKind.Namespace,
}

PYTHON_CALL_TO_DEF_TYPE = {
    "Model": "MODEL",
    "Dimension": "DIMENSION",
    "Metric": "METRIC",
    "Relationship": "RELATIONSHIP",
    "Segment": "SEGMENT",
}

PYTHON_DEF_FILE_SUFFIX = ".sidemantic.py"


@dataclass(frozen=True)
class DefinitionInfo:
    """Represents a named top-level Sidemantic definition in a document."""

    def_type: str
    name: str
    keyword_range: lsp.Range
    name_range: lsp.Range
    block_range: lsp.Range


def _uri_path(uri: str) -> str:
    """Resolve a file URI to a local path string."""
    parsed = urlparse(uri)
    return unquote(parsed.path or "")


def is_python_definition_document(uri: str) -> bool:
    """Return True for Python files that contain Sidemantic definitions."""
    path = _uri_path(uri).lower()
    if not path.endswith(".py"):
        return False

    name = Path(path).name
    return name == "sidemantic.py" or name.endswith(PYTHON_DEF_FILE_SUFFIX)


def _to_lsp_position(line: int, character: int) -> lsp.Position:
    """Convert 1-based ast coordinates to LSP 0-based coordinates."""
    return lsp.Position(line=max(0, line - 1), character=max(0, character))


def _python_call_name(node: ast.Call) -> str | None:
    """Get a constructor name from an AST call node."""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _python_literal_or_none(node: ast.AST) -> object | None:
    """Best-effort conversion of AST node to a Python literal value."""
    try:
        return ast.literal_eval(node)
    except Exception:
        return None


def extract_python_definitions(text: str) -> list[DefinitionInfo]:
    """Extract named Sidemantic definitions from Python constructor calls."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []

    definitions: list[DefinitionInfo] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        constructor_name = _python_call_name(node)
        if constructor_name not in PYTHON_CALL_TO_DEF_TYPE:
            continue

        name_keyword = next((keyword for keyword in node.keywords if keyword.arg == "name"), None)
        if not name_keyword or not isinstance(name_keyword.value, ast.Constant):
            continue
        if not isinstance(name_keyword.value.value, str):
            continue

        name_value = name_keyword.value.value
        def_type = PYTHON_CALL_TO_DEF_TYPE[constructor_name]

        func_node = node.func
        func_start = _to_lsp_position(getattr(func_node, "lineno", node.lineno), getattr(func_node, "col_offset", 0))
        func_end = _to_lsp_position(
            getattr(func_node, "end_lineno", node.lineno),
            getattr(func_node, "end_col_offset", getattr(func_node, "col_offset", 0) + len(constructor_name)),
        )

        value_node = name_keyword.value
        name_start_col = getattr(value_node, "col_offset", 0) + 1
        name_end_col = name_start_col + len(name_value)
        name_range = lsp.Range(
            start=_to_lsp_position(getattr(value_node, "lineno", node.lineno), name_start_col),
            end=_to_lsp_position(getattr(value_node, "lineno", node.lineno), name_end_col),
        )

        block_range = lsp.Range(
            start=_to_lsp_position(getattr(node, "lineno", 1), getattr(node, "col_offset", 0)),
            end=_to_lsp_position(
                getattr(node, "end_lineno", getattr(node, "lineno", 1)),
                getattr(node, "end_col_offset", getattr(node, "col_offset", 0)),
            ),
        )

        definitions.append(
            DefinitionInfo(
                def_type=def_type,
                name=name_value,
                keyword_range=lsp.Range(start=func_start, end=func_end),
                name_range=name_range,
                block_range=block_range,
            )
        )

    definitions.sort(key=lambda definition: (definition.block_range.start.line, definition.block_range.start.character))
    return definitions


def find_python_definition_by_name(text: str, name: str) -> DefinitionInfo | None:
    """Find Python constructor-based definition by model object name."""
    target_name = name.lower()
    for definition in extract_python_definitions(text):
        if definition.name.lower() == target_name:
            return definition
    return None


def _line_character_to_offset(text: str, line: int, character: int) -> int:
    """Convert LSP line/character to absolute text offset."""
    lines = text.splitlines(keepends=True)
    if line < 0 or line >= len(lines):
        return -1

    safe_character = max(0, min(character, len(lines[line])))
    return sum(len(entry) for entry in lines[:line]) + safe_character


def get_python_constructor_context(text: str, line: int, character: int) -> str | None:
    """Return enclosing Sidemantic constructor name at a cursor position."""
    offset = _line_character_to_offset(text, line, character)
    if offset < 0:
        return None

    depth = 0
    index = min(offset - 1, len(text) - 1)
    while index >= 0:
        char = text[index]
        if char == ")":
            depth += 1
        elif char == "(":
            if depth == 0:
                probe = index - 1
                while probe >= 0 and text[probe].isspace():
                    probe -= 1
                end = probe + 1
                while probe >= 0 and (text[probe].isalnum() or text[probe] == "_"):
                    probe -= 1
                call_name = text[probe + 1 : end]
                if call_name in PYTHON_CALL_TO_DEF_TYPE:
                    return call_name
                return None
            depth -= 1
        index -= 1

    return None


def _definition_type_from_statement(stmt: object) -> str | None:
    """Get top-level Sidemantic definition type from a parsed statement."""
    statement_type_name = type(stmt).__name__
    if not statement_type_name.endswith("Def"):
        return None
    return statement_type_name.replace("Def", "").upper()


def _extract_property_pairs(stmt: object) -> list[tuple[str, str]]:
    """Extract property key/value pairs from a parsed Sidemantic definition."""
    properties: list[tuple[str, str]] = []

    for expr in getattr(stmt, "expressions", []):
        if not isinstance(expr, PropertyEQ):
            continue

        key = str(expr.this.this)
        value = expr.expression.this
        if isinstance(value, bool):
            value_text = "true" if value else "false"
        elif value is None:
            value_text = "null"
        else:
            value_text = str(value)

        properties.append((key, value_text))

    return properties


def format_sidemantic_document(text: str) -> str | None:
    """Format a Sidemantic SQL document using parsed statements."""
    try:
        statements = parse(text)
    except Exception:
        return None

    formatted_statements: list[str] = []
    for stmt in statements:
        def_type = _definition_type_from_statement(stmt)
        if not def_type:
            continue

        properties = _extract_property_pairs(stmt)
        lines = [f"{def_type} ("]
        for index, (key, value) in enumerate(properties):
            suffix = "," if index < len(properties) - 1 else ""
            lines.append(f"    {key} {value}{suffix}")
        lines.append(");")
        formatted_statements.append("\n".join(lines))

    if not formatted_statements:
        return None if text.strip() else ""

    return "\n\n".join(formatted_statements).rstrip() + "\n"


def _full_document_range(text: str) -> lsp.Range:
    """Get range covering the full document text."""
    lines = text.split("\n")
    last_line = len(lines) - 1
    last_character = len(lines[last_line]) if last_line >= 0 else 0
    return lsp.Range(
        start=lsp.Position(line=0, character=0),
        end=lsp.Position(line=last_line, character=last_character),
    )


def _find_enclosing_definition_type(text: str, line: int) -> str | None:
    """Find definition type around a line by scanning nearby lines."""
    lines = text.split("\n")
    if not lines:
        return None

    start_line = max(0, line - 20)
    end_line = min(len(lines) - 1, line + 20)
    pattern = re.compile(r"\b(MODEL|DIMENSION|METRIC|RELATIONSHIP|SEGMENT)\b", re.IGNORECASE)

    for check_line in range(line, start_line - 1, -1):
        match = pattern.search(lines[check_line])
        if match and "(" in lines[check_line]:
            return match.group(1).upper()

    for check_line in range(line + 1, end_line + 1):
        match = pattern.search(lines[check_line])
        if match and "(" in lines[check_line]:
            return match.group(1).upper()

    return None


def _build_missing_name_code_action(
    *,
    uri: str,
    text: str,
    diagnostic: lsp.Diagnostic,
) -> lsp.CodeAction | None:
    """Build quick-fix for diagnostics indicating a missing required name property."""
    message = diagnostic.message.lower()
    if "name" not in message or ("required" not in message and "field required" not in message):
        return None

    line = diagnostic.range.start.line
    def_type = _find_enclosing_definition_type(text, line)
    if not def_type:
        return None

    placeholder = f"{def_type.lower()}_name"
    insertion = f"    name {placeholder},\n"
    insert_at_line = line + 1

    edit = lsp.TextEdit(
        range=lsp.Range(
            start=lsp.Position(line=insert_at_line, character=0),
            end=lsp.Position(line=insert_at_line, character=0),
        ),
        new_text=insertion,
    )

    return lsp.CodeAction(
        title="Add missing name property",
        kind=lsp.CodeActionKind.QuickFix,
        diagnostics=[diagnostic],
        edit=lsp.WorkspaceEdit(changes={uri: [edit]}),
    )


def build_code_actions(uri: str, text: str, diagnostics: list[lsp.Diagnostic]) -> list[lsp.CodeAction]:
    """Build quick-fix code actions from diagnostics."""
    actions: list[lsp.CodeAction] = []
    for diagnostic in diagnostics:
        action = _build_missing_name_code_action(uri=uri, text=text, diagnostic=diagnostic)
        if action:
            actions.append(action)
    return actions


def _build_signature_information(def_type: str, display_name: str | None = None) -> lsp.SignatureInformation | None:
    """Build signature information for a Sidemantic definition type."""
    model_class = DEF_TYPE_TO_MODEL.get(def_type)
    if not model_class:
        return None

    properties = list(model_class.model_fields.keys())
    preview = properties[:8]
    label_tail = ", ..." if len(properties) > len(preview) else ""
    label_name = display_name or def_type
    label = f"{label_name}({', '.join(preview)}{label_tail})"
    parameters = [lsp.ParameterInformation(label=property_name) for property_name in preview]
    return lsp.SignatureInformation(label=label, parameters=parameters)


def build_signature_help(text: str, line: int, character: int) -> lsp.SignatureHelp | None:
    """Build signature help based on cursor context."""
    context = get_completion_context(text, line, character)
    def_type: str | None = None

    if context.startswith("inside_"):
        def_type = context.replace("inside_", "").upper()
    elif context == "top_level":
        word = get_word_at_position(text, line, character)
        if word and word.upper() in KEYWORDS:
            def_type = word.upper()

    if not def_type:
        return None

    signature = _build_signature_information(def_type)
    if not signature:
        return None

    return lsp.SignatureHelp(signatures=[signature], active_signature=0, active_parameter=0)


def build_python_signature_help(text: str, line: int, character: int) -> lsp.SignatureHelp | None:
    """Build signature help for Python Sidemantic constructors."""
    constructor = get_python_constructor_context(text, line, character)
    if not constructor:
        return None

    def_type = PYTHON_CALL_TO_DEF_TYPE.get(constructor)
    if not def_type:
        return None

    signature = _build_signature_information(def_type, display_name=constructor)
    if not signature:
        return None

    return lsp.SignatureHelp(signatures=[signature], active_signature=0, active_parameter=0)


def get_field_docs(model_class, field_name: str) -> str | None:
    """Get field description from pydantic model."""
    field_info = model_class.model_fields.get(field_name)
    if field_info:
        return field_info.description
    return None


def get_all_properties(model_class) -> list[tuple[str, str | None]]:
    """Get all property names and descriptions for a model."""
    return [(name, field.description) for name, field in model_class.model_fields.items()]


def build_python_property_completions(constructor_name: str) -> list[lsp.CompletionItem]:
    """Build keyword argument completions for a Python constructor call."""
    def_type = PYTHON_CALL_TO_DEF_TYPE.get(constructor_name)
    model_class = DEF_TYPE_TO_MODEL.get(def_type or "")
    if not model_class:
        return []

    items: list[lsp.CompletionItem] = []
    for prop_name, description in get_all_properties(model_class):
        items.append(
            lsp.CompletionItem(
                label=prop_name,
                kind=lsp.CompletionItemKind.Property,
                detail=description or f"{constructor_name} keyword argument",
                insert_text=f"{prop_name}=$0,",
                insert_text_format=lsp.InsertTextFormat.Snippet,
            )
        )

    return items


def _find_definition_end_line(lines: list[str], start_line: int) -> int:
    """Find end line of a definition block by matching parentheses."""
    depth = 0
    opened = False

    for line_idx in range(start_line, len(lines)):
        for char in lines[line_idx]:
            if char == "(":
                depth += 1
                opened = True
            elif char == ")" and opened:
                depth -= 1

        if opened and depth <= 0:
            return line_idx

    return start_line


def extract_definitions(text: str) -> list[DefinitionInfo]:
    """Extract top-level Sidemantic definition blocks and their name locations."""
    lines = text.split("\n")
    def_pattern = re.compile(r"\b(MODEL|DIMENSION|METRIC|RELATIONSHIP|SEGMENT)\b", re.IGNORECASE)
    name_pattern = re.compile(rf"\bname\s+({IDENTIFIER_PATTERN})\b", re.IGNORECASE)

    definitions: list[DefinitionInfo] = []
    line_idx = 0

    while line_idx < len(lines):
        line = lines[line_idx]
        def_match = def_pattern.search(line)

        if not def_match:
            line_idx += 1
            continue

        def_type = def_match.group(1).upper()
        end_line = _find_definition_end_line(lines, line_idx)

        name = None
        name_range = None
        for search_line_idx in range(line_idx, end_line + 1):
            name_match = name_pattern.search(lines[search_line_idx])
            if name_match:
                name = name_match.group(1)
                name_range = lsp.Range(
                    start=lsp.Position(line=search_line_idx, character=name_match.start(1)),
                    end=lsp.Position(line=search_line_idx, character=name_match.end(1)),
                )
                break

        if name and name_range:
            keyword_range = lsp.Range(
                start=lsp.Position(line=line_idx, character=def_match.start(1)),
                end=lsp.Position(line=line_idx, character=def_match.end(1)),
            )
            block_range = lsp.Range(
                start=lsp.Position(line=line_idx, character=0),
                end=lsp.Position(line=end_line, character=len(lines[end_line])),
            )
            definitions.append(
                DefinitionInfo(
                    def_type=def_type,
                    name=name,
                    keyword_range=keyword_range,
                    name_range=name_range,
                    block_range=block_range,
                )
            )

        line_idx = end_line + 1

    return definitions


def find_definition_by_name(text: str, name: str) -> DefinitionInfo | None:
    """Find a definition by name (case-insensitive)."""
    target_name = name.lower()
    for definition in extract_definitions(text):
        if definition.name.lower() == target_name:
            return definition
    return None


def find_word_ranges(text: str, word: str) -> list[lsp.Range]:
    """Find all exact identifier matches for a word in text."""
    if not word:
        return []

    lines = text.split("\n")
    pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(word)}(?![A-Za-z0-9_])")
    ranges: list[lsp.Range] = []

    for line_idx, line in enumerate(lines):
        for match in pattern.finditer(line):
            ranges.append(
                lsp.Range(
                    start=lsp.Position(line=line_idx, character=match.start()),
                    end=lsp.Position(line=line_idx, character=match.end()),
                )
            )

    return ranges


def range_equals(left: lsp.Range, right: lsp.Range) -> bool:
    """Check if two LSP ranges are equal."""
    return left.start == right.start and left.end == right.end


def build_reference_locations(uri: str, text: str, word: str, include_declaration: bool) -> list[lsp.Location]:
    """Build reference locations for a word in a single document."""
    all_ranges = find_word_ranges(text, word)

    if not include_declaration:
        definition = find_definition_by_name(text, word)
        if definition:
            all_ranges = [
                search_range for search_range in all_ranges if not range_equals(search_range, definition.name_range)
            ]

    return [lsp.Location(uri=uri, range=search_range) for search_range in all_ranges]


def build_document_symbols(text: str) -> list[lsp.DocumentSymbol]:
    """Build document symbols from definition blocks."""
    symbols: list[lsp.DocumentSymbol] = []

    for definition in extract_definitions(text):
        symbols.append(
            lsp.DocumentSymbol(
                name=definition.name,
                detail=definition.def_type,
                kind=DEF_TYPE_TO_SYMBOL_KIND.get(definition.def_type, lsp.SymbolKind.Object),
                range=definition.block_range,
                selection_range=definition.name_range,
            )
        )

    return symbols


def build_python_document_symbols(text: str) -> list[lsp.DocumentSymbol]:
    """Build document symbols from Python constructor definitions."""
    symbols: list[lsp.DocumentSymbol] = []

    for definition in extract_python_definitions(text):
        symbols.append(
            lsp.DocumentSymbol(
                name=definition.name,
                detail=definition.def_type,
                kind=DEF_TYPE_TO_SYMBOL_KIND.get(definition.def_type, lsp.SymbolKind.Object),
                range=definition.block_range,
                selection_range=definition.name_range,
            )
        )

    return symbols


def build_rename_workspace_edit(uri: str, text: str, old_name: str, new_name: str) -> lsp.WorkspaceEdit | None:
    """Build workspace edit for renaming all references to an identifier."""
    if not old_name or old_name.upper() in KEYWORDS:
        return None

    ranges = find_word_ranges(text, old_name)
    if not ranges:
        return None

    edits = [lsp.TextEdit(range=search_range, new_text=new_name) for search_range in ranges]
    return lsp.WorkspaceEdit(changes={uri: edits})


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
        is_python_doc = is_python_definition_document(params.text_document.uri)

        # Get current line up to cursor
        lines = text.split("\n")
        if line >= len(lines):
            return lsp.CompletionList(is_incomplete=False, items=[])

        if is_python_doc:
            constructor_context = get_python_constructor_context(text, line, character)
            if not constructor_context:
                return lsp.CompletionList(is_incomplete=False, items=[])
            return lsp.CompletionList(
                is_incomplete=False,
                items=build_python_property_completions(constructor_context),
            )

        # Determine SQL context
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
        is_python_doc = is_python_definition_document(params.text_document.uri)

        # Get word at position
        word = get_word_at_position(text, line, character)
        if not word:
            return None

        if is_python_doc:
            constructor_doc_type = PYTHON_CALL_TO_DEF_TYPE.get(word)
            if constructor_doc_type:
                model_class = DEF_TYPE_TO_MODEL.get(constructor_doc_type)
                if model_class:
                    doc = model_class.__doc__ or f"{word} constructor"
                    return lsp.Hover(
                        contents=lsp.MarkupContent(kind=lsp.MarkupKind.Markdown, value=f"**{word}**\n\n{doc}")
                    )

            constructor_context = get_python_constructor_context(text, line, character)
            if constructor_context:
                def_type = PYTHON_CALL_TO_DEF_TYPE.get(constructor_context)
                model_class = DEF_TYPE_TO_MODEL.get(def_type or "")
                if model_class:
                    description = get_field_docs(model_class, word)
                    if description:
                        return lsp.Hover(
                            contents=lsp.MarkupContent(
                                kind=lsp.MarkupKind.Markdown, value=f"**{word}**\n\n{description}"
                            )
                        )
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

    @server.feature(lsp.TEXT_DOCUMENT_FORMATTING)
    def formatting(params: lsp.DocumentFormattingParams) -> list[lsp.TextEdit]:
        """Format entire Sidemantic document."""
        document = server.workspace.get_text_document(params.text_document.uri)
        if is_python_definition_document(params.text_document.uri):
            return []

        text = document.source
        formatted = format_sidemantic_document(text)
        if formatted is None or formatted == text:
            return []

        return [lsp.TextEdit(range=_full_document_range(text), new_text=formatted)]

    @server.feature(lsp.TEXT_DOCUMENT_CODE_ACTION)
    def code_actions(params: lsp.CodeActionParams) -> list[lsp.CodeAction]:
        """Return code actions for known diagnostics."""
        document = server.workspace.get_text_document(params.text_document.uri)
        text = document.source
        if is_python_definition_document(params.text_document.uri):
            return []

        diagnostics = list(params.context.diagnostics)
        return build_code_actions(params.text_document.uri, text, diagnostics)

    @server.feature(lsp.TEXT_DOCUMENT_SIGNATURE_HELP)
    def signature_help(params: lsp.SignatureHelpParams) -> lsp.SignatureHelp | None:
        """Provide signature help for top-level definitions and in-block properties."""
        document = server.workspace.get_text_document(params.text_document.uri)
        text = document.source
        if is_python_definition_document(params.text_document.uri):
            return build_python_signature_help(text, params.position.line, params.position.character)

        return build_signature_help(text, params.position.line, params.position.character)

    @server.feature(lsp.TEXT_DOCUMENT_DEFINITION)
    def definition(params: lsp.DefinitionParams) -> lsp.Location | None:
        """Jump to definition for known model, metric, dimension, relationship, and segment names."""
        document = server.workspace.get_text_document(params.text_document.uri)
        text = document.source
        line = params.position.line
        character = params.position.character
        is_python_doc = is_python_definition_document(params.text_document.uri)

        word = get_word_at_position(text, line, character)
        if not word:
            return None

        if is_python_doc:
            definition_info = find_python_definition_by_name(text, word)
        else:
            if word.upper() in KEYWORDS:
                return None
            definition_info = find_definition_by_name(text, word)
        if not definition_info:
            return None

        return lsp.Location(uri=params.text_document.uri, range=definition_info.name_range)

    @server.feature(lsp.TEXT_DOCUMENT_REFERENCES)
    def references(params: lsp.ReferenceParams) -> list[lsp.Location]:
        """Find all references to an identifier in the current document."""
        document = server.workspace.get_text_document(params.text_document.uri)
        text = document.source
        line = params.position.line
        character = params.position.character
        is_python_doc = is_python_definition_document(params.text_document.uri)

        word = get_word_at_position(text, line, character)
        if not word:
            return []
        if not is_python_doc and word.upper() in KEYWORDS:
            return []

        return build_reference_locations(
            uri=params.text_document.uri,
            text=text,
            word=word,
            include_declaration=params.context.include_declaration,
        )

    @server.feature(lsp.TEXT_DOCUMENT_DOCUMENT_SYMBOL)
    def document_symbol(params: lsp.DocumentSymbolParams) -> list[lsp.DocumentSymbol]:
        """Return document symbols for top-level Sidemantic definitions."""
        document = server.workspace.get_text_document(params.text_document.uri)
        if is_python_definition_document(params.text_document.uri):
            return build_python_document_symbols(document.source)

        return build_document_symbols(document.source)

    @server.feature(lsp.TEXT_DOCUMENT_RENAME)
    def rename(params: lsp.RenameParams) -> lsp.WorkspaceEdit | None:
        """Rename a definition name and all matching references in the current document."""
        document = server.workspace.get_text_document(params.text_document.uri)
        text = document.source
        line = params.position.line
        character = params.position.character

        word = get_word_at_position(text, line, character)
        if not word:
            return None

        return build_rename_workspace_edit(
            uri=params.text_document.uri,
            text=text,
            old_name=word,
            new_name=params.new_name,
        )

    return server


def validate_document(server: LanguageServer, uri: str):
    """Validate document and publish diagnostics."""
    document = server.workspace.get_text_document(uri)
    text = document.source
    diagnostics = []

    if is_python_definition_document(uri):
        try:
            tree = ast.parse(text)
        except SyntaxError as e:
            line = max(0, (e.lineno or 1) - 1)
            start_char = max(0, (e.offset or 1) - 1)
            diagnostics.append(
                lsp.Diagnostic(
                    range=lsp.Range(
                        start=lsp.Position(line=line, character=start_char),
                        end=lsp.Position(line=line, character=start_char + 1),
                    ),
                    message=f"Parse error: {e.msg}",
                    severity=lsp.DiagnosticSeverity.Error,
                    source="sidemantic",
                )
            )
            server.publish_diagnostics(uri, diagnostics)
            return

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            constructor_name = _python_call_name(node)
            def_type = PYTHON_CALL_TO_DEF_TYPE.get(constructor_name or "")
            if not def_type:
                continue

            model_class = DEF_TYPE_TO_MODEL.get(def_type)
            if not model_class:
                continue

            props = {
                keyword.arg: _python_literal_or_none(keyword.value)
                for keyword in node.keywords
                if keyword.arg is not None
            }
            if "name" not in props or props["name"] is None:
                continue

            try:
                model_class(**props)
            except Exception as e:
                line_num = max(0, getattr(node, "lineno", 1) - 1)
                start_col = max(0, getattr(node, "col_offset", 0))
                end_col = max(start_col + 1, getattr(node, "end_col_offset", start_col + 1))
                diagnostics.append(
                    lsp.Diagnostic(
                        range=lsp.Range(
                            start=lsp.Position(line=line_num, character=start_col),
                            end=lsp.Position(line=line_num, character=end_col),
                        ),
                        message=str(e),
                        severity=lsp.DiagnosticSeverity.Error,
                        source="sidemantic",
                    )
                )

        server.publish_diagnostics(uri, diagnostics)
        return

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
