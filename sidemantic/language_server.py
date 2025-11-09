"""
SQL Language Server for Sidemantic.

Provides IDE features for SQL queries that use the semantic layer:
- Autocomplete for models, dimensions, metrics
- Hover information for semantic elements
- Diagnostics and validation
- Go to definition
"""

import logging
from pathlib import Path

from lsprotocol.types import (
    TEXT_DOCUMENT_COMPLETION,
    TEXT_DOCUMENT_DID_CHANGE,
    TEXT_DOCUMENT_DID_OPEN,
    TEXT_DOCUMENT_HOVER,
    CompletionItem,
    CompletionItemKind,
    CompletionList,
    CompletionParams,
    Diagnostic,
    DiagnosticSeverity,
    DidChangeTextDocumentParams,
    DidOpenTextDocumentParams,
    Hover,
    HoverParams,
    MarkupContent,
    MarkupKind,
    Position,
    Range,
)
from pygls.protocol import LanguageServerProtocol, default_converter
from pygls.server import JsonRPCServer

from sidemantic import SemanticLayer, load_from_directory

logger = logging.getLogger(__name__)


class TableMetadata:
    """Metadata about a database table."""

    def __init__(self, name: str, schema: str | None = None, columns: dict[str, str] | None = None):
        self.name = name
        self.schema = schema
        self.columns = columns or {}  # column_name -> data_type


class SidemanticLanguageServer(JsonRPCServer):
    """Language server for Sidemantic SQL queries."""

    def __init__(self, protocol_cls, converter_factory, max_workers=None):
        self.name = "sidemantic-sql"
        self.version = "v0.1"
        super().__init__(protocol_cls, converter_factory, max_workers)
        self.semantic_layer: SemanticLayer | None = None
        self.config_path: Path | None = None
        self.table_metadata: dict[str, TableMetadata] = {}  # table_name -> TableMetadata

    def load_semantic_layer(self, config_path: str | Path) -> None:
        """Load semantic layer from configuration."""
        try:
            self.config_path = Path(config_path) if isinstance(config_path, str) else config_path
            self.semantic_layer = SemanticLayer()
            load_from_directory(self.semantic_layer, str(self.config_path))
            logger.info(f"Loaded semantic layer from {self.config_path}")

            # Load table metadata from database
            self._load_table_metadata()
        except Exception as e:
            logger.error(f"Failed to load semantic layer: {e}")
            self.semantic_layer = None

    def _load_table_metadata(self) -> None:
        """Introspect database to load table and column metadata."""
        if not self.semantic_layer:
            return

        try:
            conn = self.semantic_layer.conn
            dialect = self.semantic_layer.dialect

            if dialect == "duckdb":
                # DuckDB: Get all tables and their columns
                tables_query = """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                """
                tables_result = conn.execute(tables_query).fetchall()

                for schema, table_name in tables_result:
                    # Get columns for this table
                    columns_query = f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """
                    columns_result = conn.execute(columns_query).fetchall()
                    columns = {col_name: data_type for col_name, data_type in columns_result}

                    # Store metadata
                    full_name = f"{schema}.{table_name}" if schema != "main" else table_name
                    self.table_metadata[table_name] = TableMetadata(table_name, schema, columns)
                    if schema != "main":
                        self.table_metadata[full_name] = TableMetadata(table_name, schema, columns)

                logger.info(f"Loaded metadata for {len(self.table_metadata)} tables")

            elif dialect in ["postgres", "postgresql"]:
                # PostgreSQL
                tables_query = """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                """
                tables_result = conn.execute(tables_query).fetchall()

                for schema, table_name in tables_result:
                    columns_query = f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """
                    columns_result = conn.execute(columns_query).fetchall()
                    columns = {col_name: data_type for col_name, data_type in columns_result}

                    full_name = f"{schema}.{table_name}"
                    self.table_metadata[table_name] = TableMetadata(table_name, schema, columns)
                    self.table_metadata[full_name] = TableMetadata(table_name, schema, columns)

                logger.info(f"Loaded metadata for {len(self.table_metadata)} tables")

            else:
                logger.warning(f"Table metadata introspection not yet implemented for {dialect}")

        except Exception as e:
            logger.error(f"Failed to load table metadata: {e}")


# Create the language server instance
server = SidemanticLanguageServer(protocol_cls=LanguageServerProtocol, converter_factory=default_converter)


@server.feature(TEXT_DOCUMENT_DID_OPEN)
async def did_open(ls: SidemanticLanguageServer, params: DidOpenTextDocumentParams):
    """Handle document open events."""
    logger.info(f"Document opened: {params.text_document.uri}")
    # Validate the document
    await validate_document(ls, params.text_document.uri, params.text_document.text)


@server.feature(TEXT_DOCUMENT_DID_CHANGE)
async def did_change(ls: SidemanticLanguageServer, params: DidChangeTextDocumentParams):
    """Handle document change events."""
    logger.info(f"Document changed: {params.text_document.uri}")
    # Get the latest content
    text = params.content_changes[0].text if params.content_changes else ""
    await validate_document(ls, params.text_document.uri, text)


async def validate_document(ls: SidemanticLanguageServer, uri: str, text: str):
    """Validate a SQL document against the semantic layer."""
    diagnostics = []

    if not ls.semantic_layer:
        # If no semantic layer loaded, add a warning
        diagnostic = Diagnostic(
            range=Range(
                start=Position(line=0, character=0),
                end=Position(line=0, character=0),
            ),
            message="No semantic layer configuration loaded. Set via initialization options.",
            severity=DiagnosticSeverity.Warning,
            source="sidemantic",
        )
        diagnostics.append(diagnostic)
    else:
        # Try to validate the query
        try:
            # Parse and validate the SQL
            from sidemantic.sql.query_rewriter import QueryRewriter

            rewriter = QueryRewriter(ls.semantic_layer, strict=True)
            rewriter.rewrite(text)
            logger.info("Query validated successfully")
        except Exception as e:
            # Add diagnostic for validation error
            diagnostic = Diagnostic(
                range=Range(
                    start=Position(line=0, character=0),
                    end=Position(line=0, character=len(text.split("\n")[0]) if text else 0),
                ),
                message=f"Query validation error: {str(e)}",
                severity=DiagnosticSeverity.Error,
                source="sidemantic",
            )
            diagnostics.append(diagnostic)

    ls.publish_diagnostics(uri, diagnostics)


@server.feature(TEXT_DOCUMENT_COMPLETION)
async def completions(ls: SidemanticLanguageServer, params: CompletionParams) -> CompletionList:
    """Provide completion items for SQL queries."""
    items = []

    if not ls.semantic_layer:
        return CompletionList(is_incomplete=False, items=items)

    # Get the current line text to determine context
    document = ls.workspace.get_text_document(params.text_document.uri)
    line = document.lines[params.position.line]
    before_cursor = line[: params.position.character]

    # Determine if we're in FROM/JOIN context
    in_from_context = any(keyword in before_cursor.upper() for keyword in ["FROM", "JOIN"])

    # Add model completions
    for model_name, model in ls.semantic_layer.graph.models.items():
        description = model.description or f"Model: {model_name}"

        if in_from_context:
            # In FROM/JOIN context, suggest model names
            items.append(
                CompletionItem(
                    label=model_name,
                    kind=CompletionItemKind.Class,
                    detail="Model",
                    documentation=MarkupContent(kind=MarkupKind.Markdown, value=description),
                )
            )

    # Add dimension completions
    for model_name, model in ls.semantic_layer.graph.models.items():
        for dim in model.dimensions:
            full_name = f"{model_name}.{dim.name}"
            description_parts = [f"**Dimension**: `{full_name}`"]
            if dim.description:
                description_parts.append(f"\n\n{dim.description}")
            if dim.sql:
                description_parts.append(f"\n\n```sql\n{dim.sql}\n```")

            items.append(
                CompletionItem(
                    label=dim.name,
                    kind=CompletionItemKind.Field,
                    detail=f"Dimension from {model_name}",
                    documentation=MarkupContent(kind=MarkupKind.Markdown, value="".join(description_parts)),
                )
            )

    # Add metric completions
    for model_name, model in ls.semantic_layer.graph.models.items():
        for metric in model.metrics:
            full_name = f"{model_name}.{metric.name}"
            description_parts = [f"**Metric**: `{full_name}`", f"\n\n**Type**: {metric.type}"]
            if metric.description:
                description_parts.append(f"\n\n{metric.description}")
            if metric.sql:
                description_parts.append(f"\n\n```sql\n{metric.sql}\n```")

            items.append(
                CompletionItem(
                    label=metric.name,
                    kind=CompletionItemKind.Function,
                    detail=f"Metric from {model_name} ({metric.type})",
                    documentation=MarkupContent(kind=MarkupKind.Markdown, value="".join(description_parts)),
                )
            )

    # Check if we're completing columns after a table reference (e.g., "table_name.")
    table_prefix = None
    if "." in before_cursor:
        parts = before_cursor.split()
        if parts:
            last_token = parts[-1]
            if "." in last_token:
                table_prefix = last_token.rsplit(".", 1)[0]

    # Add table metadata completions
    if ls.table_metadata:
        # Add table completions in FROM/JOIN context
        if in_from_context:
            for table_name, table_meta in ls.table_metadata.items():
                if "." not in table_name:  # Only show simple names, not schema-qualified duplicates
                    schema_info = f" (schema: {table_meta.schema})" if table_meta.schema else ""
                    description = f"**Table**: `{table_name}`{schema_info}\n\n**Columns**: {len(table_meta.columns)}"

                    items.append(
                        CompletionItem(
                            label=table_name,
                            kind=CompletionItemKind.Class,
                            detail=f"Database Table{schema_info}",
                            documentation=MarkupContent(kind=MarkupKind.Markdown, value=description),
                        )
                    )

        # Add column completions
        if table_prefix and table_prefix in ls.table_metadata:
            # User is typing "table_name." - suggest columns from that specific table
            table_meta = ls.table_metadata[table_prefix]
            for col_name, col_type in table_meta.columns.items():
                description = f"**Column**: `{table_prefix}.{col_name}`\n\n**Type**: `{col_type}`"
                items.append(
                    CompletionItem(
                        label=col_name,
                        kind=CompletionItemKind.Field,
                        detail=f"{col_type} (from {table_prefix})",
                        documentation=MarkupContent(kind=MarkupKind.Markdown, value=description),
                    )
                )
        else:
            # General column suggestions (show all columns with their tables)
            for table_name, table_meta in ls.table_metadata.items():
                if "." not in table_name:  # Avoid duplicates from schema-qualified names
                    for col_name, col_type in table_meta.columns.items():
                        description = (
                            f"**Column**: `{table_name}.{col_name}`\n\n"
                            f"**Table**: {table_name}\n\n"
                            f"**Type**: `{col_type}`"
                        )
                        items.append(
                            CompletionItem(
                                label=col_name,
                                kind=CompletionItemKind.Field,
                                detail=f"{col_type} (from {table_name})",
                                documentation=MarkupContent(kind=MarkupKind.Markdown, value=description),
                            )
                        )

    # Add SQL keywords
    sql_keywords = [
        "SELECT",
        "FROM",
        "WHERE",
        "GROUP BY",
        "ORDER BY",
        "HAVING",
        "LIMIT",
        "JOIN",
        "LEFT JOIN",
        "INNER JOIN",
        "AS",
        "AND",
        "OR",
        "NOT",
        "IN",
        "LIKE",
        "BETWEEN",
        "IS NULL",
        "IS NOT NULL",
    ]

    for keyword in sql_keywords:
        items.append(
            CompletionItem(
                label=keyword,
                kind=CompletionItemKind.Keyword,
                detail="SQL Keyword",
            )
        )

    return CompletionList(is_incomplete=False, items=items)


@server.feature(TEXT_DOCUMENT_HOVER)
async def hover(ls: SidemanticLanguageServer, params: HoverParams) -> Hover | None:
    """Provide hover information for SQL elements."""
    if not ls.semantic_layer:
        return None

    # Get the word under cursor
    document = ls.workspace.get_text_document(params.text_document.uri)
    line = document.lines[params.position.line]

    # Extract word at cursor position
    start = params.position.character
    end = params.position.character

    # Move start backwards to find word boundary
    while start > 0 and (line[start - 1].isalnum() or line[start - 1] in "_"):
        start -= 1

    # Move end forward to find word boundary
    while end < len(line) and (line[end].isalnum() or line[end] in "_"):
        end += 1

    word = line[start:end]

    if not word:
        return None

    # Check if it's a model name
    if word in ls.semantic_layer.graph.models:
        model = ls.semantic_layer.graph.models[word]
        content_parts = [f"# Model: {word}"]
        if model.description:
            content_parts.append(f"\n\n{model.description}")
        content_parts.append(f"\n\n**SQL**: `{model.sql}`")
        content_parts.append(f"\n\n**Dimensions**: {len(model.dimensions)}")
        content_parts.append(f"\n**Metrics**: {len(model.metrics)}")

        return Hover(
            contents=MarkupContent(kind=MarkupKind.Markdown, value="".join(content_parts)),
            range=Range(
                start=Position(line=params.position.line, character=start),
                end=Position(line=params.position.line, character=end),
            ),
        )

    # Check if it's a dimension or metric
    for model_name, model in ls.semantic_layer.graph.models.items():
        # Check dimensions
        for dim in model.dimensions:
            if dim.name == word:
                content_parts = [f"# Dimension: {word}", f"\n\n**Model**: {model_name}"]
                if dim.description:
                    content_parts.append(f"\n\n{dim.description}")
                if dim.sql:
                    content_parts.append(f"\n\n```sql\n{dim.sql}\n```")
                if dim.type:
                    content_parts.append(f"\n\n**Type**: {dim.type}")

                return Hover(
                    contents=MarkupContent(kind=MarkupKind.Markdown, value="".join(content_parts)),
                    range=Range(
                        start=Position(line=params.position.line, character=start),
                        end=Position(line=params.position.line, character=end),
                    ),
                )

        # Check metrics
        for metric in model.metrics:
            if metric.name == word:
                content_parts = [f"# Metric: {word}", f"\n\n**Model**: {model_name}", f"\n\n**Type**: {metric.type}"]
                if metric.description:
                    content_parts.append(f"\n\n{metric.description}")
                if metric.sql:
                    content_parts.append(f"\n\n```sql\n{metric.sql}\n```")

                return Hover(
                    contents=MarkupContent(kind=MarkupKind.Markdown, value="".join(content_parts)),
                    range=Range(
                        start=Position(line=params.position.line, character=start),
                        end=Position(line=params.position.line, character=end),
                    ),
                )

    # Check if it's a table name
    if word in ls.table_metadata:
        table_meta = ls.table_metadata[word]
        schema_info = f" (schema: {table_meta.schema})" if table_meta.schema else ""
        content_parts = [f"# Table: {word}{schema_info}"]
        content_parts.append(f"\n\n**Columns**: {len(table_meta.columns)}")

        if table_meta.columns:
            content_parts.append("\n\n**Column List**:")
            for col_name, col_type in list(table_meta.columns.items())[:10]:  # Show first 10 columns
                content_parts.append(f"\n- `{col_name}`: {col_type}")
            if len(table_meta.columns) > 10:
                content_parts.append(f"\n- ... and {len(table_meta.columns) - 10} more")

        return Hover(
            contents=MarkupContent(kind=MarkupKind.Markdown, value="".join(content_parts)),
            range=Range(
                start=Position(line=params.position.line, character=start),
                end=Position(line=params.position.line, character=end),
            ),
        )

    # Check if it's a column name (search all tables)
    for table_name, table_meta in ls.table_metadata.items():
        if "." not in table_name and word in table_meta.columns:  # Avoid schema-qualified duplicates
            col_type = table_meta.columns[word]
            content_parts = [
                f"# Column: {word}",
                f"\n\n**Table**: {table_name}",
                f"\n\n**Type**: `{col_type}`",
            ]
            if table_meta.schema:
                content_parts.append(f"\n\n**Schema**: {table_meta.schema}")

            return Hover(
                contents=MarkupContent(kind=MarkupKind.Markdown, value="".join(content_parts)),
                range=Range(
                    start=Position(line=params.position.line, character=start),
                    end=Position(line=params.position.line, character=end),
                ),
            )

    return None


def start_language_server(config_path: str | None = None, stdio: bool = True):
    """
    Start the Sidemantic SQL language server.

    Args:
        config_path: Path to semantic layer configuration folder
        stdio: Whether to use stdio for communication (default: True)
    """
    from pygls.server import run

    # Load semantic layer if config path provided
    if config_path:
        server.load_semantic_layer(config_path)

    # Start the server
    if stdio:
        logger.info("Starting Sidemantic SQL Language Server via stdio...")
        run(server, transport="stdio")
    else:
        # TCP mode for testing/debugging
        logger.info("Starting Sidemantic SQL Language Server via TCP on localhost:5007...")
        run(server, transport="tcp", host="localhost", port=5007)
