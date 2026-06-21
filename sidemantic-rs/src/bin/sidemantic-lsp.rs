//! Rust-native LSP server for Sidemantic SQL definitions.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value as JsonValue;
use sidemantic::runtime::parse_sql_statement_blocks_payload;
use tokio::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::{
    CodeAction, CodeActionKind, CodeActionOrCommand, CodeActionParams,
    CodeActionProviderCapability, CodeActionResponse, CompletionItem, CompletionItemKind,
    CompletionOptions, CompletionParams, CompletionResponse, Diagnostic, DiagnosticSeverity,
    DidChangeTextDocumentParams, DidOpenTextDocumentParams, DidSaveTextDocumentParams,
    DocumentFormattingParams, DocumentSymbol, DocumentSymbolParams, DocumentSymbolResponse,
    GotoDefinitionParams, GotoDefinitionResponse, Hover, HoverContents, HoverParams,
    InitializeParams, InitializeResult, InitializedParams, Location, MarkupContent, MarkupKind,
    MessageType, OneOf, ParameterInformation, ParameterLabel, Position, Range, ReferenceParams,
    RenameParams, ServerCapabilities, SignatureHelp, SignatureHelpOptions, SignatureHelpParams,
    SignatureInformation, SymbolKind, TextDocumentSyncCapability, TextDocumentSyncKind, TextEdit,
    Url, WorkspaceEdit,
};
use tower_lsp::{Client, LanguageServer, LspService, Server};

const KEYWORDS: &[&str] = &[
    "MODEL",
    "DIMENSION",
    "METRIC",
    "RELATIONSHIP",
    "SEGMENT",
    "PARAMETER",
    "PRE_AGGREGATION",
];

fn model_properties(def_type: &str) -> &'static [(&'static str, &'static str)] {
    match def_type {
        "MODEL" => &[
            ("name", "Unique model name."),
            ("table", "Physical table for the model."),
            ("sql", "Optional model SQL."),
            ("primary_key", "Primary key column."),
            ("default_time_dimension", "Default time dimension."),
        ],
        "DIMENSION" => &[
            ("name", "Unique dimension name."),
            (
                "type",
                "Dimension type (categorical, time, number, boolean).",
            ),
            ("sql", "Dimension SQL expression."),
            ("description", "Human description."),
            ("granularity", "Time granularity for time dimensions."),
        ],
        "METRIC" => &[
            ("name", "Unique metric name."),
            ("agg", "Aggregation type."),
            ("sql", "Metric SQL expression."),
            ("type", "Metric semantic type."),
            ("description", "Human description."),
        ],
        "RELATIONSHIP" => &[
            ("name", "Target model name."),
            ("type", "Relationship type."),
            ("foreign_key", "Foreign key column."),
            ("primary_key", "Primary key column."),
            ("through", "Junction model for many_to_many."),
        ],
        "SEGMENT" => &[
            ("name", "Unique segment name."),
            ("sql", "Segment filter SQL."),
            ("description", "Human description."),
            ("public", "Visibility flag."),
        ],
        "PARAMETER" => &[
            ("name", "Unique parameter name."),
            ("type", "Parameter type."),
            ("default", "Default parameter value."),
            ("description", "Human description."),
        ],
        "PRE_AGGREGATION" => &[
            ("name", "Unique pre-aggregation name."),
            ("type", "Pre-aggregation type."),
            ("measures", "Metric references included in the rollup."),
            ("dimensions", "Dimension references included in the rollup."),
            ("time_dimension", "Time dimension for granular rollups."),
            ("granularity", "Time granularity."),
        ],
        _ => &[],
    }
}

fn keyword_doc(keyword: &str) -> Option<&'static str> {
    match keyword {
        "MODEL" => Some("Top-level model definition."),
        "DIMENSION" => Some("Dimension definition inside or alongside a model."),
        "METRIC" => Some("Metric definition."),
        "RELATIONSHIP" => Some("Relationship definition between models."),
        "SEGMENT" => Some("Reusable filter segment."),
        "PARAMETER" => Some("Runtime query parameter definition."),
        "PRE_AGGREGATION" => Some("Pre-aggregation definition for query routing."),
        _ => None,
    }
}

fn symbol_kind_for_def(def_type: &str) -> SymbolKind {
    match def_type {
        "MODEL" => SymbolKind::CLASS,
        "DIMENSION" => SymbolKind::FIELD,
        "METRIC" => SymbolKind::FUNCTION,
        "RELATIONSHIP" => SymbolKind::INTERFACE,
        "SEGMENT" => SymbolKind::BOOLEAN,
        "PARAMETER" => SymbolKind::VARIABLE,
        "PRE_AGGREGATION" => SymbolKind::STRUCT,
        _ => SymbolKind::OBJECT,
    }
}

fn get_completion_context(text: &str, line: u32, character: u32) -> String {
    let lines: Vec<&str> = text.split('\n').collect();
    let mut paren_depth: i32 = 0;
    let mut current_def: Option<&str> = None;
    let max_line = line as usize;

    let mut i = max_line as i32;
    while i >= 0 {
        let idx = i as usize;
        let check_line = if idx == max_line {
            let ch = character as usize;
            lines
                .get(idx)
                .map(|line_text| {
                    if ch <= line_text.len() {
                        &line_text[..ch]
                    } else {
                        *line_text
                    }
                })
                .unwrap_or("")
        } else {
            lines.get(idx).copied().unwrap_or("")
        };

        for ch in check_line.chars().rev() {
            if ch == ')' {
                paren_depth += 1;
            } else if ch == '(' {
                paren_depth -= 1;
            }
        }

        let full_line_upper = lines
            .get(idx)
            .map(|line_text| line_text.to_ascii_uppercase())
            .unwrap_or_default();
        for keyword in KEYWORDS {
            if full_line_upper.contains(keyword) && full_line_upper.contains('(') {
                if paren_depth < 0 {
                    return format!("inside_{}", keyword.to_ascii_lowercase());
                }
                current_def = Some(keyword);
            }
        }

        i -= 1;
    }

    if paren_depth < 0 {
        if let Some(keyword) = current_def {
            return format!("inside_{}", keyword.to_ascii_lowercase());
        }
    }

    "top_level".to_string()
}

fn get_word_at_position(text: &str, line: u32, character: u32) -> Option<String> {
    let lines: Vec<&str> = text.split('\n').collect();
    let line_text = lines.get(line as usize)?;
    if line_text.is_empty() {
        return None;
    }
    let mut ch = character as usize;
    if ch >= line_text.len() {
        ch = line_text.len().saturating_sub(1);
    }

    let bytes = line_text.as_bytes();
    if !(bytes[ch].is_ascii_alphanumeric() || bytes[ch] == b'_') {
        return None;
    }

    let mut start = ch;
    let mut end = ch;

    while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        start -= 1;
    }
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }

    Some(line_text[start..end].to_string())
}

fn diagnostics_for_text(text: &str) -> Vec<Diagnostic> {
    match parse_sql_statement_blocks_payload(text) {
        Ok(_) => Vec::new(),
        Err(e) => vec![Diagnostic {
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: 100,
                },
            },
            severity: Some(DiagnosticSeverity::ERROR),
            code: None,
            code_description: None,
            source: Some("sidemantic-rs".to_string()),
            message: format!("Parse error: {e}"),
            related_information: None,
            tags: None,
            data: None,
        }],
    }
}

#[derive(Debug, Clone)]
struct DefinitionInfo {
    def_type: String,
    name: String,
    range: Range,
    selection_range: Range,
}

fn offset_to_position(text: &str, offset: usize) -> Position {
    let mut line = 0u32;
    let mut character = 0u32;
    for (idx, ch) in text.char_indices() {
        if idx >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            character = 0;
        } else {
            character += 1;
        }
    }
    Position { line, character }
}

fn range_for_offsets(text: &str, start: usize, end: usize) -> Range {
    Range {
        start: offset_to_position(text, start),
        end: offset_to_position(text, end),
    }
}

fn find_matching_definition_end(text: &str, open_offset: usize) -> usize {
    let mut depth = 0i32;
    for (offset, ch) in text[open_offset..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let after_close = open_offset + offset + ch.len_utf8();
                    let rest = &text[after_close..];
                    let semicolon_len = rest.chars().take_while(|ch| ch.is_whitespace()).count()
                        + usize::from(rest.trim_start().starts_with(';'));
                    return after_close + semicolon_len;
                }
            }
            _ => {}
        }
    }
    text.len()
}

fn find_definition_name(block: &str, base_offset: usize) -> Option<(String, usize, usize)> {
    let bytes = block.as_bytes();
    let mut idx = 0usize;
    while idx + 4 <= bytes.len() {
        if !block[idx..].starts_with("name") {
            idx += 1;
            continue;
        }
        let before_ok = idx == 0 || !bytes[idx - 1].is_ascii_alphanumeric();
        let after = idx + 4;
        let after_ok = after < bytes.len() && bytes[after].is_ascii_whitespace();
        if !before_ok || !after_ok {
            idx += 1;
            continue;
        }
        let mut start = after;
        while start < bytes.len() && bytes[start].is_ascii_whitespace() {
            start += 1;
        }
        if start >= bytes.len() {
            return None;
        }

        let (name_start, name_end, name) = if bytes[start] == b'"' || bytes[start] == b'\'' {
            let quote = bytes[start];
            let name_start = start + 1;
            let mut name_end = name_start;
            while name_end < bytes.len() && bytes[name_end] != quote {
                name_end += 1;
            }
            (
                name_start,
                name_end,
                block[name_start..name_end].to_string(),
            )
        } else {
            let name_start = start;
            let mut name_end = name_start;
            while name_end < bytes.len()
                && (bytes[name_end].is_ascii_alphanumeric() || bytes[name_end] == b'_')
            {
                name_end += 1;
            }
            (
                name_start,
                name_end,
                block[name_start..name_end].to_string(),
            )
        };
        if !name.is_empty() {
            return Some((name, base_offset + name_start, base_offset + name_end));
        }
        idx += 1;
    }
    None
}

fn extract_definitions(text: &str) -> Vec<DefinitionInfo> {
    let mut definitions = Vec::new();
    let upper = text.to_ascii_uppercase();
    let mut cursor = 0usize;
    while cursor < text.len() {
        let mut next_match: Option<(&str, usize)> = None;
        for keyword in KEYWORDS {
            if let Some(relative) = upper[cursor..].find(keyword) {
                let offset = cursor + relative;
                let before_ok = offset == 0
                    || !upper.as_bytes()[offset.saturating_sub(1)].is_ascii_alphanumeric();
                let after_keyword = offset + keyword.len();
                let after_ok = upper[after_keyword..].trim_start().starts_with('(');
                if before_ok
                    && after_ok
                    && next_match
                        .as_ref()
                        .is_none_or(|(_, current_offset)| offset < *current_offset)
                {
                    next_match = Some((keyword, offset));
                }
            }
        }

        let Some((keyword, start_offset)) = next_match else {
            break;
        };
        let open_offset = text[start_offset..]
            .find('(')
            .map(|relative| start_offset + relative)
            .unwrap_or(start_offset + keyword.len());
        let end_offset = find_matching_definition_end(text, open_offset);
        let block = &text[start_offset..end_offset.min(text.len())];
        if let Some((name, name_start, name_end)) = find_definition_name(block, start_offset) {
            definitions.push(DefinitionInfo {
                def_type: keyword.to_string(),
                name,
                range: range_for_offsets(text, start_offset, end_offset),
                selection_range: range_for_offsets(text, name_start, name_end),
            });
        }
        cursor = end_offset.max(start_offset + keyword.len());
    }
    definitions
}

fn format_sidemantic_document(text: &str) -> Option<String> {
    let payload = parse_sql_statement_blocks_payload(text).ok()?;
    let blocks: JsonValue = serde_json::from_str(&payload).ok()?;
    let blocks = blocks.as_array()?;
    let mut formatted_blocks = Vec::new();
    for block in blocks {
        let kind = block.get("kind")?.as_str()?;
        let properties = block.get("properties")?.as_object()?;
        let keyword = kind.to_ascii_uppercase();
        let keyword = if keyword == "PRE_AGGREGATION" {
            "PRE_AGGREGATION".to_string()
        } else {
            keyword
        };
        let mut lines = Vec::new();
        for (idx, (key, value)) in properties.iter().enumerate() {
            let value_text = match value {
                JsonValue::String(value) => value.clone(),
                other => other.to_string(),
            };
            let comma = if idx + 1 == properties.len() { "" } else { "," };
            lines.push(format!("    {key} {value_text}{comma}"));
        }
        formatted_blocks.push(format!("{keyword} (\n{}\n);", lines.join("\n")));
    }
    Some(format!("{}\n", formatted_blocks.join("\n\n")))
}

fn signature_help_for_context(context: &str, word: Option<&str>) -> Option<SignatureHelp> {
    let keyword = context
        .strip_prefix("inside_")
        .map(|def_type| def_type.to_ascii_uppercase())
        .or_else(|| word.map(|word| word.to_ascii_uppercase()))?;
    if !KEYWORDS.contains(&keyword.as_str()) {
        return None;
    }
    let props = model_properties(&keyword)
        .iter()
        .map(|(name, _)| format!("{name}: value"))
        .collect::<Vec<_>>();
    let label = format!("{}({})", keyword, props.join(", "));
    let parameters = props
        .iter()
        .map(|label| ParameterInformation {
            label: ParameterLabel::Simple(label.clone()),
            documentation: None,
        })
        .collect::<Vec<_>>();
    Some(SignatureHelp {
        signatures: vec![SignatureInformation {
            label,
            documentation: None,
            parameters: Some(parameters),
            active_parameter: None,
        }],
        active_signature: Some(0),
        active_parameter: Some(0),
    })
}

fn reference_locations(
    uri: &Url,
    text: &str,
    word: &str,
    include_declaration: bool,
) -> Vec<Location> {
    if KEYWORDS.contains(&word.to_ascii_uppercase().as_str()) {
        return Vec::new();
    }
    let definitions = extract_definitions(text);
    let declaration_ranges = definitions
        .iter()
        .filter(|definition| definition.name.eq_ignore_ascii_case(word))
        .map(|definition| definition.selection_range)
        .collect::<Vec<_>>();
    let mut locations = Vec::new();
    let mut cursor = 0usize;
    while let Some(relative) = text[cursor..]
        .to_ascii_lowercase()
        .find(&word.to_ascii_lowercase())
    {
        let start = cursor + relative;
        let end = start + word.len();
        let before_ok = start == 0 || !text.as_bytes()[start - 1].is_ascii_alphanumeric();
        let after_ok = end == text.len() || !text.as_bytes()[end].is_ascii_alphanumeric();
        if before_ok && after_ok {
            let range = range_for_offsets(text, start, end);
            let is_declaration = declaration_ranges.contains(&range);
            if include_declaration || !is_declaration {
                locations.push(Location {
                    uri: uri.clone(),
                    range,
                });
            }
        }
        cursor = end;
    }
    locations
}

struct Backend {
    client: Client,
    documents: Arc<RwLock<HashMap<Url, String>>>,
}

impl Backend {
    async fn publish_diagnostics_for_uri(&self, uri: &Url) {
        let docs = self.documents.read().await;
        if let Some(text) = docs.get(uri) {
            let diagnostics = diagnostics_for_text(text);
            self.client
                .publish_diagnostics(uri.clone(), diagnostics, None)
                .await;
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            server_info: None,
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                completion_provider: Some(CompletionOptions::default()),
                hover_provider: Some(tower_lsp::lsp_types::HoverProviderCapability::Simple(true)),
                document_formatting_provider: Some(OneOf::Left(true)),
                document_symbol_provider: Some(OneOf::Left(true)),
                definition_provider: Some(OneOf::Left(true)),
                references_provider: Some(OneOf::Left(true)),
                rename_provider: Some(OneOf::Left(true)),
                signature_help_provider: Some(SignatureHelpOptions::default()),
                code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
                ..ServerCapabilities::default()
            },
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "sidemantic-rs LSP initialized")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        {
            let mut docs = self.documents.write().await;
            docs.insert(
                params.text_document.uri.clone(),
                params.text_document.text.clone(),
            );
        }
        self.publish_diagnostics_for_uri(&params.text_document.uri)
            .await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        if let Some(change) = params.content_changes.first() {
            let mut docs = self.documents.write().await;
            docs.insert(params.text_document.uri.clone(), change.text.clone());
        }
        self.publish_diagnostics_for_uri(&params.text_document.uri)
            .await;
    }

    async fn did_save(&self, params: DidSaveTextDocumentParams) {
        self.publish_diagnostics_for_uri(&params.text_document.uri)
            .await;
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let docs = self.documents.read().await;
        let Some(text) = docs.get(&params.text_document_position.text_document.uri) else {
            return Ok(Some(CompletionResponse::Array(Vec::new())));
        };

        let context = get_completion_context(
            text,
            params.text_document_position.position.line,
            params.text_document_position.position.character,
        );

        let mut items = Vec::new();
        if context == "top_level" {
            for keyword in KEYWORDS {
                items.push(CompletionItem {
                    label: (*keyword).to_string(),
                    kind: Some(CompletionItemKind::KEYWORD),
                    detail: Some("Sidemantic definition".to_string()),
                    insert_text: Some(format!("{keyword} (\n    name $1,\n    $0\n);")),
                    insert_text_format: Some(tower_lsp::lsp_types::InsertTextFormat::SNIPPET),
                    ..CompletionItem::default()
                });
            }
        } else if let Some(def_type) = context.strip_prefix("inside_") {
            let upper = def_type.to_ascii_uppercase();
            for (prop, description) in model_properties(&upper) {
                items.push(CompletionItem {
                    label: (*prop).to_string(),
                    kind: Some(CompletionItemKind::PROPERTY),
                    detail: Some((*description).to_string()),
                    insert_text: Some(format!("{prop} $0,")),
                    insert_text_format: Some(tower_lsp::lsp_types::InsertTextFormat::SNIPPET),
                    ..CompletionItem::default()
                });
            }
        }

        Ok(Some(CompletionResponse::Array(items)))
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let docs = self.documents.read().await;
        let Some(text) = docs.get(&params.text_document_position_params.text_document.uri) else {
            return Ok(None);
        };
        let pos = params.text_document_position_params.position;
        let Some(word) = get_word_at_position(text, pos.line, pos.character) else {
            return Ok(None);
        };

        let word_upper = word.to_ascii_uppercase();
        if let Some(doc) = keyword_doc(&word_upper) {
            return Ok(Some(Hover {
                contents: HoverContents::Markup(MarkupContent {
                    kind: MarkupKind::Markdown,
                    value: format!("**{word_upper}**\n\n{doc}"),
                }),
                range: None,
            }));
        }

        let context = get_completion_context(text, pos.line, pos.character);
        if let Some(def_type) = context.strip_prefix("inside_") {
            let upper = def_type.to_ascii_uppercase();
            for (prop, description) in model_properties(&upper) {
                if prop.eq_ignore_ascii_case(&word) {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**{prop}**\n\n{description}"),
                        }),
                        range: None,
                    }));
                }
            }
        }

        Ok(None)
    }

    async fn formatting(&self, params: DocumentFormattingParams) -> Result<Option<Vec<TextEdit>>> {
        let docs = self.documents.read().await;
        let Some(text) = docs.get(&params.text_document.uri) else {
            return Ok(None);
        };
        let Some(formatted) = format_sidemantic_document(text) else {
            return Ok(None);
        };
        let end = offset_to_position(text, text.len());
        Ok(Some(vec![TextEdit {
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end,
            },
            new_text: formatted,
        }]))
    }

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        let docs = self.documents.read().await;
        let Some(text) = docs.get(&params.text_document.uri) else {
            return Ok(Some(DocumentSymbolResponse::Nested(Vec::new())));
        };
        let symbols = extract_definitions(text)
            .into_iter()
            .map(|definition| {
                #[allow(deprecated)]
                DocumentSymbol {
                    name: definition.name,
                    detail: Some(definition.def_type.clone()),
                    kind: symbol_kind_for_def(&definition.def_type),
                    tags: None,
                    deprecated: None,
                    range: definition.range,
                    selection_range: definition.selection_range,
                    children: None,
                }
            })
            .collect::<Vec<_>>();
        Ok(Some(DocumentSymbolResponse::Nested(symbols)))
    }

    async fn signature_help(&self, params: SignatureHelpParams) -> Result<Option<SignatureHelp>> {
        let docs = self.documents.read().await;
        let Some(text) = docs.get(&params.text_document_position_params.text_document.uri) else {
            return Ok(None);
        };
        let pos = params.text_document_position_params.position;
        let context = get_completion_context(text, pos.line, pos.character);
        let word = get_word_at_position(text, pos.line, pos.character);
        Ok(signature_help_for_context(&context, word.as_deref()))
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let docs = self.documents.read().await;
        let uri = params.text_document_position_params.text_document.uri;
        let Some(text) = docs.get(&uri) else {
            return Ok(None);
        };
        let pos = params.text_document_position_params.position;
        let Some(word) = get_word_at_position(text, pos.line, pos.character) else {
            return Ok(None);
        };
        let Some(definition) = extract_definitions(text)
            .into_iter()
            .find(|definition| definition.name.eq_ignore_ascii_case(&word))
        else {
            return Ok(None);
        };
        Ok(Some(GotoDefinitionResponse::Scalar(Location {
            uri,
            range: definition.selection_range,
        })))
    }

    async fn references(&self, params: ReferenceParams) -> Result<Option<Vec<Location>>> {
        let docs = self.documents.read().await;
        let uri = params.text_document_position.text_document.uri;
        let Some(text) = docs.get(&uri) else {
            return Ok(Some(Vec::new()));
        };
        let pos = params.text_document_position.position;
        let Some(word) = get_word_at_position(text, pos.line, pos.character) else {
            return Ok(Some(Vec::new()));
        };
        Ok(Some(reference_locations(
            &uri,
            text,
            &word,
            params.context.include_declaration,
        )))
    }

    async fn rename(&self, params: RenameParams) -> Result<Option<WorkspaceEdit>> {
        let docs = self.documents.read().await;
        let uri = params.text_document_position.text_document.uri;
        let Some(text) = docs.get(&uri) else {
            return Ok(None);
        };
        let pos = params.text_document_position.position;
        let Some(word) = get_word_at_position(text, pos.line, pos.character) else {
            return Ok(None);
        };
        if KEYWORDS.contains(&word.to_ascii_uppercase().as_str()) {
            return Ok(None);
        }
        let edits = reference_locations(&uri, text, &word, true)
            .into_iter()
            .map(|location| TextEdit {
                range: location.range,
                new_text: params.new_name.clone(),
            })
            .collect::<Vec<_>>();
        if edits.is_empty() {
            return Ok(None);
        }
        let mut changes = HashMap::new();
        changes.insert(uri, edits);
        Ok(Some(WorkspaceEdit {
            changes: Some(changes),
            document_changes: None,
            change_annotations: None,
        }))
    }

    async fn code_action(&self, params: CodeActionParams) -> Result<Option<CodeActionResponse>> {
        if !params.context.diagnostics.iter().any(|diagnostic| {
            diagnostic.message.contains("name") || diagnostic.message.contains("Parse error")
        }) {
            return Ok(Some(Vec::new()));
        }
        let docs = self.documents.read().await;
        let uri = params.text_document.uri;
        let Some(text) = docs.get(&uri) else {
            return Ok(Some(Vec::new()));
        };
        let insertion_line = text
            .lines()
            .position(|line| line.contains('('))
            .map(|line| line as u32 + 1)
            .unwrap_or(1);
        let edit = TextEdit {
            range: Range {
                start: Position {
                    line: insertion_line,
                    character: 0,
                },
                end: Position {
                    line: insertion_line,
                    character: 0,
                },
            },
            new_text: "    name model_name,\n".to_string(),
        };
        let mut changes = HashMap::new();
        changes.insert(uri, vec![edit]);
        Ok(Some(vec![CodeActionOrCommand::CodeAction(CodeAction {
            title: "Add missing name property".to_string(),
            kind: Some(CodeActionKind::QUICKFIX),
            diagnostics: Some(params.context.diagnostics),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                document_changes: None,
                change_annotations: None,
            }),
            command: None,
            is_preferred: Some(true),
            disabled: None,
            data: None,
        })]))
    }
}

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| Backend {
        client,
        documents: Arc::new(RwLock::new(HashMap::new())),
    });

    Server::new(stdin, stdout, socket).serve(service).await;
}

#[cfg(test)]
mod tests {
    use super::{get_completion_context, get_word_at_position};

    #[test]
    fn completion_context_top_level() {
        let text = "\nMODEL (\n    name orders\n);\n\n";
        assert_eq!(get_completion_context(text, 5, 0), "top_level");
    }

    #[test]
    fn completion_context_inside_model() {
        let text = "MODEL (\n    name orders,\n\n);";
        assert_eq!(get_completion_context(text, 2, 4), "inside_model");
    }

    #[test]
    fn completion_context_inside_metric() {
        let text = "MODEL (name orders);\n\nMETRIC (\n    name revenue,\n\n);";
        assert_eq!(get_completion_context(text, 4, 4), "inside_metric");
    }

    #[test]
    fn word_at_position() {
        let text = "MODEL (\n    name orders,\n);";
        assert_eq!(get_word_at_position(text, 0, 2).as_deref(), Some("MODEL"));
        assert_eq!(get_word_at_position(text, 1, 6).as_deref(), Some("name"));
        assert_eq!(get_word_at_position(text, 1, 12).as_deref(), Some("orders"));
    }

    #[test]
    fn word_at_position_none_for_whitespace() {
        let text = "MODEL (  )";
        assert_eq!(get_word_at_position(text, 0, 8), None);
    }
}
