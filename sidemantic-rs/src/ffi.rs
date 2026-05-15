//! C FFI bindings for sidemantic-rs
//!
//! Exposes the query rewriter to C/C++ consumers like the DuckDB extension.
//!
//! Safety: These functions are `extern "C"` and expect valid C strings.
//! Callers must ensure pointers are valid. Documented in header.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::raw::c_char;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Mutex;

use once_cell::sync::Lazy;

use crate::config::{load_from_directory, load_from_file, load_from_string, parse_sql_model};
use crate::core::SemanticGraph;
use crate::sql::QueryRewriter;

const DEFAULT_CONTEXT_KEY: &str = "__sidemantic_default_context__";

#[derive(Default)]
struct FfiState {
    graph: SemanticGraph,
    active_model: Option<String>,
}

/// Semantic graph state keyed by DuckDB database/session context.
static FFI_STATES: Lazy<Mutex<HashMap<String, FfiState>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Result from rewrite operation
#[repr(C)]
pub struct SidemanticRewriteResult {
    /// Rewritten SQL (null if error)
    pub sql: *mut c_char,
    /// Error message (null if success)
    pub error: *mut c_char,
    /// Whether the query was rewritten (false = passthrough)
    pub was_rewritten: bool,
}

fn c_string_arg(ptr: *const c_char, name: &str) -> std::result::Result<String, *mut c_char> {
    if ptr.is_null() {
        return Err(to_c_string(&format!("Error: null {name} pointer")));
    }

    unsafe {
        CStr::from_ptr(ptr)
            .to_str()
            .map(str::to_string)
            .map_err(|e| to_c_string(&format!("Error: invalid UTF-8: {e}")))
    }
}

fn context_key(context: *const c_char) -> std::result::Result<String, *mut c_char> {
    if context.is_null() {
        return Ok(DEFAULT_CONTEXT_KEY.to_string());
    }

    let raw = unsafe {
        CStr::from_ptr(context)
            .to_str()
            .map_err(|e| to_c_string(&format!("Error: invalid UTF-8: {e}")))?
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        Ok(DEFAULT_CONTEXT_KEY.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

/// Load semantic models from YAML string
///
/// Returns null on success, error message on failure.
/// Caller must free the returned string with `sidemantic_free`.
#[no_mangle]
pub extern "C" fn sidemantic_load_yaml(yaml: *const c_char) -> *mut c_char {
    sidemantic_load_yaml_for_context(ptr::null(), yaml)
}

/// Load semantic models from YAML string into a context-keyed graph.
#[no_mangle]
pub extern "C" fn sidemantic_load_yaml_for_context(
    context: *const c_char,
    yaml: *const c_char,
) -> *mut c_char {
    let key = match context_key(context) {
        Ok(key) => key,
        Err(error) => return error,
    };
    let yaml_str = match c_string_arg(yaml, "yaml") {
        Ok(value) => value,
        Err(error) => return error,
    };

    match load_from_string(&yaml_str) {
        Ok(new_graph) => {
            let mut states = FFI_STATES.lock().unwrap();
            let state = states.entry(key).or_default();
            // Merge new models into existing graph, replacing same-name definitions.
            for model in new_graph.models() {
                if let Err(e) = state.graph.replace_model(model.clone()) {
                    return to_c_string(&format!("Error adding model: {e}"));
                }
            }
            ptr::null_mut() // Success
        }
        Err(e) => to_c_string(&format!("Error: {e}")),
    }
}

/// Load semantic models from a file or directory path
///
/// Returns null on success, error message on failure.
/// Caller must free the returned string with `sidemantic_free`.
#[no_mangle]
pub extern "C" fn sidemantic_load_file(path: *const c_char) -> *mut c_char {
    sidemantic_load_file_for_context(ptr::null(), path)
}

/// Load semantic models from a file or directory into a context-keyed graph.
#[no_mangle]
pub extern "C" fn sidemantic_load_file_for_context(
    context: *const c_char,
    path: *const c_char,
) -> *mut c_char {
    let key = match context_key(context) {
        Ok(key) => key,
        Err(error) => return error,
    };
    let path_str = match c_string_arg(path, "path") {
        Ok(value) => value,
        Err(error) => return error,
    };

    let path = Path::new(&path_str);

    // Check if path exists
    if !path.exists() {
        return to_c_string(&format!("Error: path does not exist: {path_str}"));
    }

    let result = if path.is_dir() {
        load_from_directory(path)
    } else {
        load_from_file(path)
    };

    match result {
        Ok(new_graph) => {
            let mut states = FFI_STATES.lock().unwrap();
            let state = states.entry(key).or_default();
            // Merge new models into existing graph, replacing same-name definitions.
            for model in new_graph.models() {
                if let Err(e) = state.graph.replace_model(model.clone()) {
                    return to_c_string(&format!("Error adding model: {e}"));
                }
            }
            ptr::null_mut() // Success
        }
        Err(e) => to_c_string(&format!("Error: {e}")),
    }
}

/// Clear all loaded semantic models
#[no_mangle]
pub extern "C" fn sidemantic_clear() {
    sidemantic_clear_for_context(ptr::null());
}

/// Clear all loaded semantic models for one context.
#[no_mangle]
pub extern "C" fn sidemantic_clear_for_context(context: *const c_char) {
    let Ok(key) = context_key(context) else {
        return;
    };
    let mut states = FFI_STATES.lock().unwrap();
    states.insert(key, FfiState::default());
}

/// Define a semantic model from SQL definition format
///
/// Parses the definition, saves to file, and loads into current session.
/// If `replace` is true, removes any existing model with the same name from the file.
///
/// Returns null on success, error message on failure.
/// Caller must free the returned string with `sidemantic_free`.
#[no_mangle]
pub extern "C" fn sidemantic_define(
    definition_sql: *const c_char,
    db_path: *const c_char,
    replace: bool,
) -> *mut c_char {
    sidemantic_define_for_context(ptr::null(), definition_sql, db_path, replace)
}

/// Define a semantic model in a context-keyed graph.
#[no_mangle]
pub extern "C" fn sidemantic_define_for_context(
    context: *const c_char,
    definition_sql: *const c_char,
    db_path: *const c_char,
    replace: bool,
) -> *mut c_char {
    let key = match context_key(context) {
        Ok(key) => key,
        Err(error) => return error,
    };
    let sql_str = match c_string_arg(definition_sql, "definition_sql") {
        Ok(value) => value,
        Err(error) => return error,
    };

    // Parse the definition to validate and get model name
    let model = match parse_sql_model(&sql_str) {
        Ok(m) => m,
        Err(e) => return to_c_string(&format!("Error parsing definition: {e}")),
    };

    let model_name = model.name.clone();

    // Determine the definitions file path
    let definitions_path = get_definitions_path(db_path);

    // Handle OR REPLACE: read existing file, remove model if exists
    if replace {
        if let Some(definitions_path) = definitions_path.as_ref() {
            if let Err(e) = remove_model_from_file(definitions_path, &model_name) {
                return to_c_string(&format!("Error removing existing model: {e}"));
            }
        }
    }

    // Append definition to file
    if let Some(definitions_path) = definitions_path.as_ref() {
        if let Err(e) = append_definition_to_file(definitions_path, &sql_str) {
            return to_c_string(&format!("Error writing to definitions file: {e}"));
        }
    }

    // Load model into current session
    let mut states = FFI_STATES.lock().unwrap();
    let state = states.entry(key).or_default();
    let result = if replace {
        state.graph.replace_model(model)
    } else {
        state.graph.add_model(model)
    };
    if let Err(e) = result {
        return to_c_string(&format!("Error adding model to session: {e}"));
    }

    // Set this model as the active model for subsequent METRIC/DIMENSION additions
    state.active_model = Some(model_name);

    ptr::null_mut() // Success
}

/// Get the definitions file path based on database path
fn get_definitions_path(db_path: *const c_char) -> Option<PathBuf> {
    if db_path.is_null() {
        return None;
    }

    let path_str = unsafe {
        match CStr::from_ptr(db_path).to_str() {
            Ok(s) => s.trim(),
            Err(_) => return None,
        }
    };

    if path_str.is_empty() || path_str == ":memory:" {
        return None;
    }

    // Replace .duckdb extension with .sidemantic.sql
    let db_path = Path::new(path_str);
    let stem = db_path.file_stem().unwrap_or_default();
    let parent = db_path.parent().unwrap_or(Path::new("."));
    Some(parent.join(format!("{}.sidemantic.sql", stem.to_string_lossy())))
}

/// Remove a model definition from the file by name
fn remove_model_from_file(path: &Path, model_name: &str) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(()); // Nothing to remove
    }

    let content = fs::read_to_string(path)?;
    let mut result = String::new();

    let mut cursor = 0;
    for (start, end) in model_definition_ranges(&content) {
        result.push_str(&content[cursor..start]);

        let block = &content[start..end];
        let should_remove = parse_sql_model(block)
            .map(|model| model.name == model_name)
            .unwrap_or(false);

        if !should_remove {
            result.push_str(block);
        }

        cursor = end;
    }
    result.push_str(&content[cursor..]);

    fs::write(path, result.trim_end())?;
    Ok(())
}

fn model_definition_ranges(content: &str) -> Vec<(usize, usize)> {
    let mut starts = Vec::new();
    let content_upper = content.to_uppercase();
    let mut search_start = 0;

    while let Some(pos) = content_upper[search_start..].find("MODEL") {
        let actual_pos = search_start + pos;
        let is_start =
            actual_pos == 0 || !content.as_bytes()[actual_pos - 1].is_ascii_alphanumeric();
        let is_followed_by_boundary = actual_pos + 5 >= content.len()
            || matches!(
                content.as_bytes()[actual_pos + 5],
                b' ' | b'(' | b'\t' | b'\n'
            );

        if is_start && is_followed_by_boundary {
            starts.push(actual_pos);
        }

        search_start = actual_pos + 1;
    }

    starts
        .iter()
        .enumerate()
        .map(|(index, start)| {
            let end = starts.get(index + 1).copied().unwrap_or(content.len());
            (*start, end)
        })
        .collect()
}

/// Append a definition to the file
fn append_definition_to_file(path: &Path, definition: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    // Add newlines for separation if file is not empty
    if path.exists() && fs::metadata(path)?.len() > 0 {
        writeln!(file)?;
        writeln!(file)?;
    }

    writeln!(file, "{}", definition.trim())?;
    Ok(())
}

/// Load definitions from file if it exists (for auto-load on extension start)
///
/// Returns null on success (including when file doesn't exist), error message on failure.
/// Caller must free the returned string with `sidemantic_free`.
#[no_mangle]
pub extern "C" fn sidemantic_autoload(db_path: *const c_char) -> *mut c_char {
    sidemantic_autoload_for_context(ptr::null(), db_path)
}

/// Load persisted definitions into a context-keyed graph if they exist.
#[no_mangle]
pub extern "C" fn sidemantic_autoload_for_context(
    context: *const c_char,
    db_path: *const c_char,
) -> *mut c_char {
    let key = match context_key(context) {
        Ok(key) => key,
        Err(error) => return error,
    };
    let Some(definitions_path) = get_definitions_path(db_path) else {
        return ptr::null_mut();
    };

    if !definitions_path.exists() {
        return ptr::null_mut(); // No file to load, success
    }

    // Read and parse the definitions file
    let content = match fs::read_to_string(&definitions_path) {
        Ok(c) => c,
        Err(e) => return to_c_string(&format!("Error reading definitions file: {e}")),
    };

    if content.trim().is_empty() {
        return ptr::null_mut(); // Empty file, success
    }

    // Parse each model definition in the file
    // Split on MODEL keyword to handle multiple definitions
    let mut states = FFI_STATES.lock().unwrap();
    let state = states.entry(key).or_default();

    let mut last_model_name = None;
    for block in split_definitions(&content) {
        if block.trim().is_empty() {
            continue;
        }
        match parse_sql_model(block) {
            Ok(model) => {
                last_model_name = Some(model.name.clone());
                if let Err(e) = state.graph.replace_model(model) {
                    return to_c_string(&format!("Error loading model: {e}"));
                }
            }
            Err(e) => {
                // Log but don't fail on parse errors for individual models
                eprintln!("Warning: failed to parse model definition: {e}");
            }
        }
    }

    state.active_model = last_model_name;

    ptr::null_mut() // Success
}

/// Split content into individual model definitions
fn split_definitions(content: &str) -> Vec<&str> {
    let mut definitions = Vec::new();
    let mut start = 0;

    // Find each MODEL keyword and split there
    let content_upper = content.to_uppercase();
    let mut search_start = 0;

    while let Some(pos) = content_upper[search_start..].find("MODEL") {
        let actual_pos = search_start + pos;

        // Check this is actually the start of a MODEL statement (not inside a word)
        let is_start =
            actual_pos == 0 || !content.as_bytes()[actual_pos - 1].is_ascii_alphanumeric();
        let is_followed_by_space = actual_pos + 5 < content.len()
            && (content.as_bytes()[actual_pos + 5] == b' '
                || content.as_bytes()[actual_pos + 5] == b'('
                || content.as_bytes()[actual_pos + 5] == b'\t'
                || content.as_bytes()[actual_pos + 5] == b'\n');

        if is_start && is_followed_by_space {
            if start < actual_pos && start > 0 {
                definitions.push(&content[start..actual_pos]);
            }
            start = actual_pos;
        }

        search_start = actual_pos + 1;
    }

    // Don't forget the last definition
    if start < content.len() {
        definitions.push(&content[start..]);
    }

    definitions
}

/// Add a metric/dimension/segment to the most recently created model
///
/// definition_sql: The definition in nom format (e.g., "METRIC (name revenue, agg sum, sql amount)")
/// db_path: Path to database file for persistence (null for in-memory)
/// is_replace: If true, replace existing metric/dimension/segment with the same name
///
/// Supports syntaxes:
/// - `METRIC (name foo, ...)` - adds to active model
/// - `METRIC model.foo (...)` - adds to specified model
/// - `METRIC foo AS SUM(x)` - adds to active model
/// - `METRIC model.foo AS SUM(x)` - adds to specified model
///
/// Returns null on success, error message on failure.
#[no_mangle]
pub extern "C" fn sidemantic_add_definition(
    definition_sql: *const c_char,
    db_path: *const c_char,
    is_replace: bool,
) -> *mut c_char {
    sidemantic_add_definition_for_context(ptr::null(), definition_sql, db_path, is_replace)
}

/// Add a metric/dimension/segment in a context-keyed graph.
#[no_mangle]
pub extern "C" fn sidemantic_add_definition_for_context(
    context: *const c_char,
    definition_sql: *const c_char,
    db_path: *const c_char,
    is_replace: bool,
) -> *mut c_char {
    use crate::config::parse_sql_model;

    let key = match context_key(context) {
        Ok(key) => key,
        Err(error) => return error,
    };
    let sql_str = match c_string_arg(definition_sql, "definition_sql") {
        Ok(value) => value,
        Err(error) => return error,
    };

    // Parse to determine what type it is and extract properties
    let sql_trimmed = sql_str.trim();
    let sql_upper = sql_trimmed.to_uppercase();

    let mut states = FFI_STATES.lock().unwrap();
    let state = states.entry(key).or_default();

    // Check for model.name syntax: "METRIC model.name (...)" or "DIMENSION model.name (...)"
    // Extract model name if present, otherwise use ACTIVE_MODEL
    let (target_model_name, adjusted_sql) = extract_model_prefix(sql_trimmed);

    let model_name = if let Some(explicit_model) = target_model_name {
        // Verify the model exists
        if state.graph.get_model(&explicit_model).is_none() {
            return to_c_string(&format!("Error: model '{explicit_model}' not found"));
        }
        explicit_model
    } else {
        // Use ACTIVE_MODEL or fall back to last model
        if let Some(ref name) = state.active_model {
            name.clone()
        } else {
            // Fall back to last model
            let model_names: Vec<String> = state.graph.models().map(|m| m.name.clone()).collect();
            if model_names.is_empty() {
                return to_c_string("Error: no model defined yet. Create a model first with SEMANTIC CREATE MODEL, or use SEMANTIC USE <model>.");
            }
            model_names.last().unwrap().clone()
        }
    };

    // Get the model to modify
    let model = match state.graph.get_model(&model_name) {
        Some(m) => m.clone(),
        None => return to_c_string(&format!("Error: could not find model '{model_name}'")),
    };

    // Parse the definition using a dummy model wrapper
    let dummy_sql = format!("MODEL (name {model_name}, table dummy);\n{adjusted_sql}");
    let parsed = match parse_sql_model(&dummy_sql) {
        Ok(m) => m,
        Err(e) => return to_c_string(&format!("Error parsing definition: {e}")),
    };

    // Extract what was added and update the model
    let mut updated_model = model.clone();

    if sql_upper.starts_with("METRIC") {
        for metric in parsed.metrics {
            if is_replace {
                // Remove existing metric with same name
                updated_model.metrics.retain(|m| m.name != metric.name);
            }
            updated_model.metrics.push(metric);
        }
    } else if sql_upper.starts_with("DIMENSION") {
        for dim in parsed.dimensions {
            if is_replace {
                // Remove existing dimension with same name
                updated_model.dimensions.retain(|d| d.name != dim.name);
            }
            updated_model.dimensions.push(dim);
        }
    } else if sql_upper.starts_with("SEGMENT") {
        for seg in parsed.segments {
            if is_replace {
                // Remove existing segment with same name
                updated_model.segments.retain(|s| s.name != seg.name);
            }
            updated_model.segments.push(seg);
        }
    }

    if let Err(e) = state.graph.replace_model(updated_model) {
        return to_c_string(&format!("Error updating model: {e}"));
    }

    // Append to definitions file
    if let Some(definitions_path) = get_definitions_path(db_path) {
        if let Err(e) = append_definition_to_file(&definitions_path, &sql_str) {
            return to_c_string(&format!("Error writing to definitions file: {e}"));
        }
    }

    ptr::null_mut() // Success
}

/// Extract model prefix from "METRIC model.name (...)" or "METRIC model.name AS expr" syntax
/// Returns (Some(model), adjusted_sql) if prefix found, (None, original_sql) otherwise
fn extract_model_prefix(sql: &str) -> (Option<String>, String) {
    let sql_upper = sql.to_uppercase();

    // Find the keyword (METRIC, DIMENSION, SEGMENT)
    let keyword = if sql_upper.starts_with("METRIC") {
        "METRIC"
    } else if sql_upper.starts_with("DIMENSION") {
        "DIMENSION"
    } else if sql_upper.starts_with("SEGMENT") {
        "SEGMENT"
    } else {
        return (None, sql.to_string());
    };

    // Get everything after the keyword
    let rest = sql[keyword.len()..].trim_start();

    // Check for model.name pattern - could be followed by ( or AS
    // Look for a dot in the first identifier
    let first_space_or_paren = rest
        .find(|c: char| c.is_whitespace() || c == '(')
        .unwrap_or(rest.len());
    let first_token = &rest[..first_space_or_paren];

    if let Some(dot_pos) = first_token.find('.') {
        let model_name = first_token[..dot_pos].trim();
        let field_name = first_token[dot_pos + 1..].trim();
        let after_token = rest[first_space_or_paren..].trim_start();

        // Check what follows: ( for paren syntax, or AS for simple syntax
        if after_token.starts_with('(') {
            // Paren syntax: "model.name (props...)"
            let paren_content = after_token;
            let adjusted = if let Some(stripped) = paren_content.strip_prefix('(') {
                let inner = stripped.trim_start();
                format!("{keyword} (name {field_name}, {inner}")
            } else {
                format!("{keyword} {rest}")
            };
            return (Some(model_name.to_string()), adjusted);
        } else if after_token.to_uppercase().starts_with("AS ") {
            // AS syntax: "model.name AS expr"
            let after_as = &after_token[2..].trim_start();
            let adjusted = format!("{keyword} {field_name} AS {after_as}");
            return (Some(model_name.to_string()), adjusted);
        }
    }

    (None, sql.to_string())
}

/// Set the active model for subsequent METRIC/DIMENSION/SEGMENT additions
///
/// Returns null on success, error message on failure.
#[no_mangle]
pub extern "C" fn sidemantic_use(model_name: *const c_char) -> *mut c_char {
    sidemantic_use_for_context(ptr::null(), model_name)
}

/// Set the active model for one context.
#[no_mangle]
pub extern "C" fn sidemantic_use_for_context(
    context: *const c_char,
    model_name: *const c_char,
) -> *mut c_char {
    let key = match context_key(context) {
        Ok(key) => key,
        Err(error) => return error,
    };
    let name_str = match c_string_arg(model_name, "model_name") {
        Ok(value) => value,
        Err(error) => return error,
    };

    let name = name_str.trim();
    if name.is_empty() {
        return to_c_string("Error: model name cannot be empty");
    }

    // Verify the model exists
    let mut states = FFI_STATES.lock().unwrap();
    let state = states.entry(key).or_default();
    if state.graph.get_model(name).is_none() {
        let available: Vec<&str> = state.graph.models().map(|m| m.name.as_str()).collect();
        return to_c_string(&format!(
            "Error: model '{}' not found. Available models: {}",
            name,
            if available.is_empty() {
                "(none)".to_string()
            } else {
                available.join(", ")
            }
        ));
    }

    // Set active model
    state.active_model = Some(name.to_string());

    ptr::null_mut() // Success
}

/// Check if a table name is a registered semantic model
#[no_mangle]
pub extern "C" fn sidemantic_is_model(table_name: *const c_char) -> bool {
    sidemantic_is_model_for_context(ptr::null(), table_name)
}

/// Check if a table name is a registered semantic model in one context.
#[no_mangle]
pub extern "C" fn sidemantic_is_model_for_context(
    context: *const c_char,
    table_name: *const c_char,
) -> bool {
    let Ok(key) = context_key(context) else {
        return false;
    };
    let Ok(name) = c_string_arg(table_name, "table_name") else {
        return false;
    };

    let states = FFI_STATES.lock().unwrap();
    states
        .get(&key)
        .map(|state| state.graph.get_model(&name).is_some())
        .unwrap_or(false)
}

/// Get list of registered model names (comma-separated)
///
/// Caller must free the returned string with `sidemantic_free`.
#[no_mangle]
pub extern "C" fn sidemantic_list_models() -> *mut c_char {
    sidemantic_list_models_for_context(ptr::null())
}

/// Get list of registered model names for one context.
#[no_mangle]
pub extern "C" fn sidemantic_list_models_for_context(context: *const c_char) -> *mut c_char {
    let key = match context_key(context) {
        Ok(key) => key,
        Err(error) => return error,
    };
    let states = FFI_STATES.lock().unwrap();
    let names: Vec<&str> = states
        .get(&key)
        .map(|state| state.graph.models().map(|m| m.name.as_str()).collect())
        .unwrap_or_default();
    to_c_string(&names.join(","))
}

/// Rewrite a SQL query using semantic definitions
///
/// Returns a SidemanticRewriteResult struct. Caller must free with `sidemantic_free_result`.
#[no_mangle]
pub extern "C" fn sidemantic_rewrite(sql: *const c_char) -> SidemanticRewriteResult {
    sidemantic_rewrite_for_context(ptr::null(), sql)
}

/// Rewrite a SQL query using semantic definitions from one context.
#[no_mangle]
pub extern "C" fn sidemantic_rewrite_for_context(
    context: *const c_char,
    sql: *const c_char,
) -> SidemanticRewriteResult {
    let key = match context_key(context) {
        Ok(key) => key,
        Err(error) => {
            return SidemanticRewriteResult {
                sql: ptr::null_mut(),
                error,
                was_rewritten: false,
            }
        }
    };

    let sql_str = match c_string_arg(sql, "sql") {
        Ok(value) => value,
        Err(error) => {
            return SidemanticRewriteResult {
                sql: ptr::null_mut(),
                error,
                was_rewritten: false,
            }
        }
    };

    let states = FFI_STATES.lock().unwrap();
    let Some(state) = states.get(&key) else {
        return SidemanticRewriteResult {
            sql: to_c_string(&sql_str),
            error: ptr::null_mut(),
            was_rewritten: false,
        };
    };

    // Check if query references any semantic models
    if !query_references_models(&sql_str, &state.graph) {
        // Passthrough - not a semantic query
        return SidemanticRewriteResult {
            sql: to_c_string(&sql_str),
            error: ptr::null_mut(),
            was_rewritten: false,
        };
    }

    // Rewrite the query
    let rewriter = QueryRewriter::new(&state.graph);
    match rewriter.rewrite(&sql_str) {
        Ok(rewritten) => SidemanticRewriteResult {
            sql: to_c_string(&rewritten),
            error: ptr::null_mut(),
            was_rewritten: true,
        },
        Err(e) => SidemanticRewriteResult {
            sql: ptr::null_mut(),
            error: to_c_string(&format!("Error: {e}")),
            was_rewritten: false,
        },
    }
}

/// Free a string returned by sidemantic functions
#[no_mangle]
pub extern "C" fn sidemantic_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// Free a SidemanticRewriteResult
#[no_mangle]
pub extern "C" fn sidemantic_free_result(result: SidemanticRewriteResult) {
    sidemantic_free(result.sql);
    sidemantic_free(result.error);
}

// Helper: convert Rust string to C string
fn to_c_string(s: &str) -> *mut c_char {
    match CString::new(s) {
        Ok(cs) => cs.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

// Helper: check if SQL references any registered models
fn query_references_models(sql: &str, graph: &SemanticGraph) -> bool {
    let sql_lower = sql.to_lowercase();

    for model in graph.models() {
        let model_lower = model.name.to_lowercase();

        // Check for FROM model or JOIN model patterns
        if sql_lower.contains(&format!("from {model_lower}"))
            || sql_lower.contains(&format!("from {model_lower} "))
            || sql_lower.contains(&format!("join {model_lower}"))
            || sql_lower.contains(&format!("join {model_lower} "))
            // Also check for model.column references
            || sql_lower.contains(&format!("{model_lower}."))
        {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::sync::{Mutex, MutexGuard};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn test_lock() -> MutexGuard<'static, ()> {
        TEST_MUTEX.lock().unwrap()
    }

    fn assert_success(result: *mut c_char) {
        if result.is_null() {
            return;
        }

        let message = unsafe { CStr::from_ptr(result).to_string_lossy().into_owned() };
        sidemantic_free(result);
        panic!("{message}");
    }

    fn take_error(result: *mut c_char) -> String {
        assert!(!result.is_null());
        let message = unsafe { CStr::from_ptr(result).to_string_lossy().into_owned() };
        sidemantic_free(result);
        message
    }

    fn take_rewrite_sql(result: SidemanticRewriteResult) -> String {
        assert!(result.error.is_null());
        let sql = unsafe { CStr::from_ptr(result.sql).to_string_lossy().into_owned() };
        sidemantic_free_result(result);
        sql
    }

    fn unique_db_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("sidemantic_{name}_{nanos}.duckdb"))
    }

    fn remove_definitions_file(db_path: &CString) {
        if let Some(definitions_path) = get_definitions_path(db_path.as_ptr()) {
            let _ = fs::remove_file(definitions_path);
        }
    }

    #[test]
    fn test_load_and_rewrite() {
        let _guard = test_lock();
        // Clear any existing state
        sidemantic_clear();

        // Load a model
        let yaml = CString::new(
            r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#,
        )
        .unwrap();

        let result = sidemantic_load_yaml(yaml.as_ptr());
        assert!(result.is_null()); // Success

        // Check model is registered
        let name = CString::new("orders").unwrap();
        assert!(sidemantic_is_model(name.as_ptr()));

        // Rewrite a query
        let sql = CString::new("SELECT orders.revenue, orders.status FROM orders").unwrap();
        let result = sidemantic_rewrite(sql.as_ptr());

        assert!(result.error.is_null());
        assert!(result.was_rewritten);

        let rewritten = unsafe { CStr::from_ptr(result.sql).to_str().unwrap() };
        assert!(rewritten.contains("SUM"));

        sidemantic_free_result(result);
    }

    #[test]
    fn test_passthrough() {
        let _guard = test_lock();
        sidemantic_clear();

        // Query without semantic models should pass through
        let sql = CString::new("SELECT * FROM some_table").unwrap();
        let result = sidemantic_rewrite(sql.as_ptr());

        assert!(result.error.is_null());
        assert!(!result.was_rewritten);

        sidemantic_free_result(result);
    }

    #[test]
    fn test_define_replace_updates_in_memory_graph() {
        let _guard = test_lock();
        sidemantic_clear();

        let db_path = unique_db_path("define_replace");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);

        let first =
            CString::new("MODEL (name orders, table orders, primary_key order_id);").unwrap();
        assert_success(sidemantic_define(first.as_ptr(), db_path.as_ptr(), false));

        let replacement = CString::new(
            "MODEL (name orders, table orders_v2, primary_key order_id);\nMETRIC (name order_count, agg count);",
        )
        .unwrap();
        assert_success(sidemantic_define(
            replacement.as_ptr(),
            db_path.as_ptr(),
            true,
        ));

        let result = sidemantic_rewrite(
            CString::new("SELECT orders.order_count FROM orders")
                .unwrap()
                .as_ptr(),
        );
        assert!(result.error.is_null());
        assert!(result.was_rewritten);

        let rewritten = unsafe { CStr::from_ptr(result.sql).to_string_lossy().into_owned() };
        assert!(rewritten.contains("orders_v2"), "{rewritten}");
        sidemantic_free_result(result);

        remove_definitions_file(&db_path);
    }

    #[test]
    fn test_add_definition_updates_existing_model() {
        let _guard = test_lock();
        sidemantic_clear();

        let db_path = unique_db_path("add_definition");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);

        let model =
            CString::new("MODEL (name orders, table orders, primary_key order_id);").unwrap();
        assert_success(sidemantic_define(model.as_ptr(), db_path.as_ptr(), false));

        let metric = CString::new("METRIC (name revenue, agg sum, sql amount);").unwrap();
        assert_success(sidemantic_add_definition(
            metric.as_ptr(),
            db_path.as_ptr(),
            false,
        ));

        let result = sidemantic_rewrite(
            CString::new("SELECT orders.revenue FROM orders")
                .unwrap()
                .as_ptr(),
        );
        assert!(result.error.is_null());
        assert!(result.was_rewritten);

        let rewritten = unsafe { CStr::from_ptr(result.sql).to_string_lossy().into_owned() };
        assert!(rewritten.contains("SUM"), "{rewritten}");
        sidemantic_free_result(result);

        remove_definitions_file(&db_path);
    }

    #[test]
    fn test_clear_resets_active_model() {
        let _guard = test_lock();
        sidemantic_clear();

        let db_path = unique_db_path("clear_active");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);

        let model =
            CString::new("MODEL (name orders, table orders, primary_key order_id);").unwrap();
        assert_success(sidemantic_define(model.as_ptr(), db_path.as_ptr(), false));

        sidemantic_clear();

        let metric = CString::new("METRIC (name revenue, agg sum, sql amount);").unwrap();
        let error = take_error(sidemantic_add_definition(
            metric.as_ptr(),
            db_path.as_ptr(),
            false,
        ));

        assert!(error.contains("no model defined yet"), "{error}");

        remove_definitions_file(&db_path);
    }

    #[test]
    fn test_context_keyed_state_isolates_models_and_active_model() {
        let _guard = test_lock();

        let context_a = CString::new("duckdb:a").unwrap();
        let context_b = CString::new("duckdb:b").unwrap();
        sidemantic_clear_for_context(context_a.as_ptr());
        sidemantic_clear_for_context(context_b.as_ptr());

        let db_path_a = unique_db_path("context_a");
        let db_path_a = CString::new(db_path_a.to_string_lossy().to_string()).unwrap();
        let db_path_b = unique_db_path("context_b");
        let db_path_b = CString::new(db_path_b.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path_a);
        remove_definitions_file(&db_path_b);

        let model_a =
            CString::new("MODEL (name orders, table orders_a, primary_key order_id);").unwrap();
        let model_b =
            CString::new("MODEL (name orders, table orders_b, primary_key order_id);").unwrap();

        assert_success(sidemantic_define_for_context(
            context_a.as_ptr(),
            model_a.as_ptr(),
            db_path_a.as_ptr(),
            false,
        ));
        assert_success(sidemantic_define_for_context(
            context_b.as_ptr(),
            model_b.as_ptr(),
            db_path_b.as_ptr(),
            false,
        ));

        let metric_a = CString::new("METRIC (name revenue, agg sum, sql amount);").unwrap();
        let metric_b = CString::new("METRIC (name order_count, agg count);").unwrap();
        assert_success(sidemantic_add_definition_for_context(
            context_a.as_ptr(),
            metric_a.as_ptr(),
            db_path_a.as_ptr(),
            false,
        ));
        assert_success(sidemantic_add_definition_for_context(
            context_b.as_ptr(),
            metric_b.as_ptr(),
            db_path_b.as_ptr(),
            false,
        ));

        let sql_a = CString::new("SELECT orders.revenue FROM orders").unwrap();
        let rewritten_a = take_rewrite_sql(sidemantic_rewrite_for_context(
            context_a.as_ptr(),
            sql_a.as_ptr(),
        ));
        assert!(rewritten_a.contains("orders_a"), "{rewritten_a}");
        assert!(rewritten_a.contains("SUM"), "{rewritten_a}");

        let sql_b = CString::new("SELECT orders.order_count FROM orders").unwrap();
        let rewritten_b = take_rewrite_sql(sidemantic_rewrite_for_context(
            context_b.as_ptr(),
            sql_b.as_ptr(),
        ));
        assert!(rewritten_b.contains("orders_b"), "{rewritten_b}");
        assert!(rewritten_b.contains("COUNT"), "{rewritten_b}");

        sidemantic_clear_for_context(context_a.as_ptr());

        let passthrough = sidemantic_rewrite_for_context(context_a.as_ptr(), sql_a.as_ptr());
        assert!(passthrough.error.is_null());
        assert!(!passthrough.was_rewritten);
        sidemantic_free_result(passthrough);

        let still_rewritten = sidemantic_rewrite_for_context(context_b.as_ptr(), sql_b.as_ptr());
        assert!(still_rewritten.error.is_null());
        assert!(still_rewritten.was_rewritten);
        sidemantic_free_result(still_rewritten);

        remove_definitions_file(&db_path_a);
        remove_definitions_file(&db_path_b);
    }

    #[test]
    fn test_load_yaml_replaces_same_name_model_in_context() {
        let _guard = test_lock();

        let context = CString::new("duckdb:replace-load").unwrap();
        sidemantic_clear_for_context(context.as_ptr());

        let first = CString::new(
            r#"
models:
  - name: orders
    table: orders_v1
    primary_key: order_id
    metrics:
      - name: order_count
        agg: count
"#,
        )
        .unwrap();
        assert_success(sidemantic_load_yaml_for_context(
            context.as_ptr(),
            first.as_ptr(),
        ));

        let second = CString::new(
            r#"
models:
  - name: orders
    table: orders_v2
    primary_key: order_id
    metrics:
      - name: order_count
        agg: count
"#,
        )
        .unwrap();
        assert_success(sidemantic_load_yaml_for_context(
            context.as_ptr(),
            second.as_ptr(),
        ));

        let sql = CString::new("SELECT orders.order_count FROM orders").unwrap();
        let rewritten = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            sql.as_ptr(),
        ));
        assert!(rewritten.contains("orders_v2"), "{rewritten}");
        assert!(rewritten.contains("COUNT"), "{rewritten}");

        sidemantic_clear_for_context(context.as_ptr());
    }

    #[test]
    fn test_autoload_sets_active_model_to_last_loaded_model() {
        let _guard = test_lock();

        let context = CString::new("duckdb:autoload-active").unwrap();
        sidemantic_clear_for_context(context.as_ptr());

        let db_path = unique_db_path("autoload_active");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();
        fs::write(
            &definitions_path,
            "MODEL (name orders, table orders, primary_key order_id);",
        )
        .unwrap();

        assert_success(sidemantic_autoload_for_context(
            context.as_ptr(),
            db_path.as_ptr(),
        ));

        let metric = CString::new("METRIC (name revenue, agg sum, sql amount);").unwrap();
        assert_success(sidemantic_add_definition_for_context(
            context.as_ptr(),
            metric.as_ptr(),
            db_path.as_ptr(),
            false,
        ));

        let sql = CString::new("SELECT orders.revenue FROM orders").unwrap();
        let rewritten = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            sql.as_ptr(),
        ));
        assert!(rewritten.contains("SUM"), "{rewritten}");

        sidemantic_clear_for_context(context.as_ptr());
        let _ = fs::remove_file(definitions_path);
    }

    #[test]
    fn test_remove_model_from_file_handles_multiline_models() {
        let _guard = test_lock();
        let db_path = unique_db_path("remove_multiline");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();
        let content = r#"
MODEL (
  name orders,
  table orders,
  primary_key order_id
);
METRIC (name revenue, agg sum, sql amount);

MODEL (name customers, table customers, primary_key customer_id);
"#;
        fs::write(&definitions_path, content).unwrap();

        remove_model_from_file(&definitions_path, "orders").unwrap();

        let updated = fs::read_to_string(&definitions_path).unwrap();
        assert!(!updated.contains("name orders"), "{updated}");
        assert!(!updated.contains("name revenue"), "{updated}");
        assert!(updated.contains("name customers"), "{updated}");

        let _ = fs::remove_file(definitions_path);
    }
}
