//! C FFI bindings for sidemantic-rs
//!
//! Exposes the query rewriter to C/C++ consumers like the DuckDB extension.
//!
//! Safety: These functions are `extern "C"` and expect valid C strings.
//! Callers must ensure pointers are valid. Documented in header.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::raw::c_char;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use once_cell::sync::Lazy;

use crate::config::{
    load_from_directory_with_metadata, load_from_file_with_metadata,
    load_from_sql_string_with_metadata, load_from_string_with_metadata, parse_sql_model,
};
use crate::core::SemanticGraph;
use crate::sql::QueryRewriter;

const DEFAULT_CONTEXT_KEY: &str = "__sidemantic_default_context__";
const DEFINITIONS_LOCK_TIMEOUT: Duration = Duration::from_secs(10);
const DEFINITIONS_STALE_LOCK_AFTER: Duration = Duration::from_secs(300);

#[derive(Default, Clone)]
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

fn active_model_for_loaded_models(model_order: &[String]) -> Option<String> {
    if model_order.len() == 1 {
        model_order.first().cloned()
    } else {
        None
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

    match load_from_string_with_metadata(&yaml_str) {
        Ok(metadata) => {
            let active_model = active_model_for_loaded_models(&metadata.model_order);
            let mut states = FFI_STATES.lock().unwrap();
            let state = states.entry(key).or_default();
            // Merge new models into existing graph, replacing same-name definitions.
            for model in metadata.graph.models() {
                if let Err(e) = state.graph.replace_model(model.clone()) {
                    return to_c_string(&format!("Error adding model: {e}"));
                }
            }
            state.active_model = active_model;
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
        load_from_directory_with_metadata(path)
    } else {
        load_from_file_with_metadata(path)
    };

    match result {
        Ok(metadata) => {
            let active_model = active_model_for_loaded_models(&metadata.model_order);
            let mut states = FFI_STATES.lock().unwrap();
            let state = states.entry(key).or_default();
            // Merge new models into existing graph, replacing same-name definitions.
            for model in metadata.graph.models() {
                if let Err(e) = state.graph.replace_model(model.clone()) {
                    return to_c_string(&format!("Error adding model: {e}"));
                }
            }
            state.active_model = active_model;
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

    let definitions_path = get_definitions_path(db_path);

    // Stage all in-memory work first so duplicate/invalid definitions never touch disk.
    let mut states = FFI_STATES.lock().unwrap();
    let state = states.entry(key).or_default();
    let mut candidate_state = state.clone();
    let result = if replace {
        candidate_state.graph.replace_model(model)
    } else {
        candidate_state.graph.add_model(model)
    };
    if let Err(e) = result {
        return to_c_string(&format!("Error adding model to session: {e}"));
    }
    candidate_state.active_model = Some(model_name.clone());

    if let Some(definitions_path) = definitions_path.as_ref() {
        let _definitions_lock = match lock_definitions_file(definitions_path) {
            Ok(lock) => lock,
            Err(e) => return to_c_string(&format!("Error locking definitions file: {e}")),
        };
        let content = match read_definitions_file(definitions_path) {
            Ok(content) => content,
            Err(e) => return to_c_string(&format!("Error reading definitions file: {e}")),
        };
        let content = if replace {
            remove_model_from_content(&content, &model_name)
        } else {
            content
        };
        let candidate_content = append_definition_to_content(&content, &sql_str);
        if let Err(e) = validate_definitions_content(&candidate_content) {
            return to_c_string(&format!("Error validating definitions file: {e}"));
        }
        if let Err(e) = write_definitions_file_atomic(definitions_path, &candidate_content) {
            return to_c_string(&format!("Error writing to definitions file: {e}"));
        }
    }

    *state = candidate_state;

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
#[cfg(test)]
fn remove_model_from_file(path: &Path, model_name: &str) -> std::io::Result<()> {
    let _definitions_lock = lock_definitions_file(path)?;
    let content = read_definitions_file(path)?;
    let result = remove_model_from_content(&content, model_name);
    write_definitions_file_atomic(path, &result)
}

fn remove_model_from_content(content: &str, model_name: &str) -> String {
    let mut result = String::new();

    let mut cursor = 0;
    for (start, end) in model_definition_ranges(content) {
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

    result.trim_end().to_string()
}

fn content_has_model_block(content: &str, model_name: &str) -> bool {
    model_definition_ranges(content)
        .into_iter()
        .filter_map(|(start, end)| parse_sql_model(&content[start..end]).ok())
        .any(|model| model.name == model_name)
}

fn model_definition_ranges(content: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut current_start = None;
    let mut current_end = None;

    for (start, end) in statement_ranges(content) {
        let statement = &content[start..end];
        if starts_with_definition_keyword(statement, "MODEL") {
            if let (Some(block_start), Some(block_end)) = (current_start, current_end) {
                ranges.push((block_start, block_end));
            }
            current_start = Some(start);
        }
        if current_start.is_some() {
            current_end = Some(end);
        }
    }

    if let (Some(block_start), Some(block_end)) = (current_start, current_end) {
        ranges.push((block_start, block_end));
    }

    ranges
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DefinitionKind {
    Metric,
    Dimension,
    Segment,
}

fn definition_kind(sql: &str) -> Option<DefinitionKind> {
    if starts_with_definition_keyword(sql, "METRIC") {
        Some(DefinitionKind::Metric)
    } else if starts_with_definition_keyword(sql, "DIMENSION") {
        Some(DefinitionKind::Dimension)
    } else if starts_with_definition_keyword(sql, "SEGMENT") {
        Some(DefinitionKind::Segment)
    } else {
        None
    }
}

fn starts_with_definition_keyword(sql: &str, keyword: &str) -> bool {
    let trimmed = sql.trim_start();
    if trimmed.len() < keyword.len() {
        return false;
    }

    let prefix = &trimmed[..keyword.len()];
    if !prefix.eq_ignore_ascii_case(keyword) {
        return false;
    }

    trimmed[keyword.len()..]
        .chars()
        .next()
        .map(|ch| ch.is_whitespace())
        .unwrap_or(true)
}

fn statement_ranges(block: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = None;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let bytes = block.as_bytes();
    let mut idx = 0;

    while idx < bytes.len() {
        let byte = bytes[idx];

        if in_line_comment {
            if byte == b'\n' {
                in_line_comment = false;
            }
            idx += 1;
            continue;
        }
        if in_block_comment {
            if byte == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                in_block_comment = false;
                idx += 2;
            } else {
                idx += 1;
            }
            continue;
        }
        if in_single_quote {
            if byte == b'\'' && bytes.get(idx + 1) == Some(&b'\'') {
                idx += 2;
                continue;
            }
            if byte == b'\'' {
                in_single_quote = false;
            }
            idx += 1;
            continue;
        }
        if in_double_quote {
            if byte == b'"' && bytes.get(idx + 1) == Some(&b'"') {
                idx += 2;
                continue;
            }
            if byte == b'"' {
                in_double_quote = false;
            }
            idx += 1;
            continue;
        }

        if start.is_none() {
            if byte.is_ascii_whitespace() {
                idx += 1;
                continue;
            }
            if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
                in_line_comment = true;
                idx += 2;
                continue;
            }
            if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
                in_block_comment = true;
                idx += 2;
                continue;
            }
            start = Some(idx);
        }

        if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
            in_line_comment = true;
            idx += 2;
            continue;
        }
        if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
            in_block_comment = true;
            idx += 2;
            continue;
        }

        match byte {
            b'\'' => in_single_quote = true,
            b'"' => in_double_quote = true,
            b';' => {
                if let Some(statement_start) = start.take() {
                    ranges.push((statement_start, idx + 1));
                }
            }
            _ => {}
        }
        idx += 1;
    }

    if let Some(statement_start) = start {
        if !block[statement_start..].trim().is_empty() {
            ranges.push((statement_start, block.len()));
        }
    }

    ranges
}

fn persist_model_item_definition_to_content(
    content: &str,
    model_name: &str,
    kind: DefinitionKind,
    item_names: &[String],
    definition: &str,
    is_replace: bool,
) -> String {
    if content.trim().is_empty() {
        return append_definition_to_content(content, definition);
    }

    let item_names: HashSet<&str> = item_names.iter().map(String::as_str).collect();
    let (_, adjusted_definition) = extract_model_prefix(definition.trim());
    let mut result = String::new();
    let mut cursor = 0;
    let mut inserted = false;

    for (start, end) in model_definition_ranges(content) {
        result.push_str(&content[cursor..start]);

        let block = &content[start..end];
        let block_model_name = parse_sql_model(block).ok().map(|model| model.name);
        let cleaned = if is_replace {
            remove_item_definitions_from_block(
                block,
                block_model_name.as_deref(),
                model_name,
                kind,
                &item_names,
            )
        } else {
            block.to_string()
        };

        if block_model_name.as_deref() == Some(model_name) {
            result.push_str(&insert_definition_at_block_end(
                &cleaned,
                &adjusted_definition,
            ));
            inserted = true;
        } else {
            result.push_str(&cleaned);
        }

        cursor = end;
    }
    result.push_str(&content[cursor..]);

    if !inserted {
        result = append_definition_to_content(&result, definition);
    }

    result
}

fn remove_item_definitions_from_block(
    block: &str,
    block_model_name: Option<&str>,
    target_model_name: &str,
    kind: DefinitionKind,
    item_names: &HashSet<&str>,
) -> String {
    if item_names.is_empty() {
        return block.to_string();
    }

    let mut result = String::new();
    let mut cursor = 0;

    for (start, end) in statement_ranges(block) {
        result.push_str(&block[cursor..start]);
        let statement = &block[start..end];
        if !should_remove_item_statement(
            statement,
            block_model_name,
            target_model_name,
            kind,
            item_names,
        ) {
            result.push_str(statement);
        }
        cursor = end;
    }
    result.push_str(&block[cursor..]);

    result
}

fn should_remove_item_statement(
    statement: &str,
    block_model_name: Option<&str>,
    target_model_name: &str,
    kind: DefinitionKind,
    item_names: &HashSet<&str>,
) -> bool {
    if definition_kind(statement) != Some(kind) {
        return false;
    }

    let (explicit_model, adjusted_statement) = extract_model_prefix(statement.trim());
    let belongs_to_target = explicit_model
        .as_deref()
        .map(|model| model == target_model_name)
        .unwrap_or(block_model_name == Some(target_model_name));
    if !belongs_to_target {
        return false;
    }

    let dummy_sql = format!("MODEL (name {target_model_name}, table dummy);\n{adjusted_statement}");
    let Ok(parsed) = parse_sql_model(&dummy_sql) else {
        return false;
    };

    match kind {
        DefinitionKind::Metric => parsed
            .metrics
            .iter()
            .any(|metric| item_names.contains(metric.name.as_str())),
        DefinitionKind::Dimension => parsed
            .dimensions
            .iter()
            .any(|dimension| item_names.contains(dimension.name.as_str())),
        DefinitionKind::Segment => parsed
            .segments
            .iter()
            .any(|segment| item_names.contains(segment.name.as_str())),
    }
}

fn insert_definition_at_block_end(block: &str, definition: &str) -> String {
    let trimmed_len = block.trim_end().len();
    let (body, trailing) = block.split_at(trimmed_len);
    let trimmed_definition = definition.trim();

    if body.is_empty() {
        return format!("{trimmed_definition}{trailing}");
    }

    format!("{body}\n\n{trimmed_definition}{trailing}")
}

fn append_definition_to_content(content: &str, definition: &str) -> String {
    let trimmed_definition = definition.trim();
    if trimmed_definition.is_empty() {
        return content.trim_end().to_string();
    }

    let mut result = content.trim_end().to_string();
    if !result.is_empty() {
        result.push_str("\n\n");
    }
    result.push_str(trimmed_definition);
    result.push('\n');
    result
}

fn read_definitions_file(path: &Path) -> io::Result<String> {
    match fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error),
    }
}

fn validate_definitions_content(content: &str) -> Result<(), String> {
    if content.trim().is_empty() {
        return Ok(());
    }
    load_from_sql_string_with_metadata(content)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

struct DefinitionsFileLock {
    path: PathBuf,
    file: Option<fs::File>,
}

impl Drop for DefinitionsFileLock {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}

fn definitions_lock_path(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("definitions.sql");
    parent.join(format!(".{file_name}.lock"))
}

fn lock_definitions_file(path: &Path) -> io::Result<DefinitionsFileLock> {
    let lock_path = definitions_lock_path(path);
    let deadline = Instant::now() + DEFINITIONS_LOCK_TIMEOUT;

    loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(mut file) => {
                writeln!(file, "pid={}", std::process::id())?;
                file.sync_all()?;
                return Ok(DefinitionsFileLock {
                    path: lock_path,
                    file: Some(file),
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if is_stale_definitions_lock(&lock_path) {
                    let _ = fs::remove_file(&lock_path);
                    continue;
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("timed out waiting for {}", lock_path.display()),
                    ));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_stale_definitions_lock(lock_path: &Path) -> bool {
    fs::metadata(lock_path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age > DEFINITIONS_STALE_LOCK_AFTER)
}

#[cfg(not(windows))]
fn replace_file_atomic(temp_path: &Path, path: &Path) -> io::Result<()> {
    fs::rename(temp_path, path)
}

#[cfg(windows)]
fn replace_file_atomic(temp_path: &Path, path: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    fn wide_path(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    let temp_wide = wide_path(temp_path);
    let path_wide = wide_path(path);
    let result = unsafe {
        MoveFileExW(
            temp_wide.as_ptr(),
            path_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn write_definitions_file_atomic(path: &Path, content: &str) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("definitions.sql");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let temp_path = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        unique
    ));

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
    }

    if let Err(error) = replace_file_atomic(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }

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
        let mut states = FFI_STATES.lock().unwrap();
        states.insert(key, FfiState::default());
        return ptr::null_mut();
    };

    if !definitions_path.exists() {
        let mut states = FFI_STATES.lock().unwrap();
        states.insert(key, FfiState::default());
        return ptr::null_mut(); // No file to load, success
    }

    // Read and parse the definitions file
    let content = match fs::read_to_string(&definitions_path) {
        Ok(c) => c,
        Err(e) => return to_c_string(&format!("Error reading definitions file: {e}")),
    };

    if content.trim().is_empty() {
        let mut states = FFI_STATES.lock().unwrap();
        states.insert(key, FfiState::default());
        return ptr::null_mut(); // Empty file, success
    }

    let metadata = match load_from_sql_string_with_metadata(&content) {
        Ok(metadata) => metadata,
        Err(e) => {
            let mut states = FFI_STATES.lock().unwrap();
            states.insert(key, FfiState::default());
            return to_c_string(&format!("Error loading definitions file: {e}"));
        }
    };

    let mut states = FFI_STATES.lock().unwrap();
    states.insert(
        key,
        FfiState {
            active_model: active_model_for_loaded_models(&metadata.model_order),
            graph: metadata.graph,
        },
    );

    ptr::null_mut() // Success
}

/// Split content into individual model definitions
#[cfg(test)]
fn split_definitions(content: &str) -> Vec<&str> {
    model_definition_ranges(content)
        .into_iter()
        .map(|(start, end)| &content[start..end])
        .collect()
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
        // Use ACTIVE_MODEL. Multi-model loads intentionally require an explicit target.
        if let Some(ref name) = state.active_model {
            name.clone()
        } else {
            return to_c_string("Error: no active model. Create a model first with SEMANTIC CREATE MODEL, select one with SEMANTIC MODEL <model>, or use METRIC/DIMENSION/SEGMENT model.name syntax.");
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

    let mut persisted_kind = None;
    let mut persisted_item_names = Vec::new();

    if sql_upper.starts_with("METRIC") {
        persisted_kind = Some(DefinitionKind::Metric);
        for metric in parsed.metrics {
            if is_replace {
                // Remove existing metric with same name
                updated_model.metrics.retain(|m| m.name != metric.name);
            }
            persisted_item_names.push(metric.name.clone());
            updated_model.metrics.push(metric);
        }
    } else if sql_upper.starts_with("DIMENSION") {
        persisted_kind = Some(DefinitionKind::Dimension);
        for dim in parsed.dimensions {
            if is_replace {
                // Remove existing dimension with same name
                updated_model.dimensions.retain(|d| d.name != dim.name);
            }
            persisted_item_names.push(dim.name.clone());
            updated_model.dimensions.push(dim);
        }
    } else if sql_upper.starts_with("SEGMENT") {
        persisted_kind = Some(DefinitionKind::Segment);
        for seg in parsed.segments {
            if is_replace {
                // Remove existing segment with same name
                updated_model.segments.retain(|s| s.name != seg.name);
            }
            persisted_item_names.push(seg.name.clone());
            updated_model.segments.push(seg);
        }
    }

    let mut candidate_state = state.clone();
    if let Err(e) = candidate_state.graph.replace_model(updated_model) {
        return to_c_string(&format!("Error updating model: {e}"));
    }

    // Persist the definition with the owning model so autoload sees the same graph.
    if let Some(definitions_path) = get_definitions_path(db_path) {
        let _definitions_lock = match lock_definitions_file(&definitions_path) {
            Ok(lock) => lock,
            Err(e) => return to_c_string(&format!("Error locking definitions file: {e}")),
        };
        let content = match read_definitions_file(&definitions_path) {
            Ok(content) => content,
            Err(e) => return to_c_string(&format!("Error reading definitions file: {e}")),
        };
        if !content_has_model_block(&content, &model_name) {
            return to_c_string(&format!(
                "Error: model '{model_name}' is not present in the persisted definitions file"
            ));
        }
        let candidate_content = if let Some(kind) = persisted_kind {
            persist_model_item_definition_to_content(
                &content,
                &model_name,
                kind,
                &persisted_item_names,
                &sql_str,
                is_replace,
            )
        } else {
            append_definition_to_content(&content, &sql_str)
        };

        if let Err(e) = validate_definitions_content(&candidate_content) {
            return to_c_string(&format!("Error validating definitions file: {e}"));
        }
        if let Err(e) = write_definitions_file_atomic(&definitions_path, &candidate_content) {
            return to_c_string(&format!("Error writing to definitions file: {e}"));
        }
    }

    *state = candidate_state;

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

    fn take_rewrite_error(result: SidemanticRewriteResult) -> String {
        assert!(!result.error.is_null());
        let message = unsafe { CStr::from_ptr(result.error).to_string_lossy().into_owned() };
        sidemantic_free_result(result);
        message
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
    fn test_define_duplicate_does_not_append_sidecar() {
        let _guard = test_lock();
        sidemantic_clear();

        let db_path = unique_db_path("define_duplicate_atomic");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();

        let model =
            CString::new("MODEL (name orders, table orders, primary_key order_id);").unwrap();
        assert_success(sidemantic_define(model.as_ptr(), db_path.as_ptr(), false));

        let error = take_error(sidemantic_define(model.as_ptr(), db_path.as_ptr(), false));
        assert!(error.contains("already exists"), "{error}");

        let content = fs::read_to_string(&definitions_path).unwrap();
        assert_eq!(split_definitions(&content).len(), 1, "{content}");
        assert_eq!(
            content.matches("MODEL (name orders").count(),
            1,
            "{content}"
        );

        remove_definitions_file(&db_path);
    }

    #[test]
    fn test_define_replace_invalid_rolls_back_sidecar_and_memory() {
        let _guard = test_lock();
        sidemantic_clear();

        let db_path = unique_db_path("define_replace_invalid_atomic");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();

        let first = CString::new(
            "MODEL (name orders, table orders, primary_key order_id);\nMETRIC revenue AS SUM(amount);",
        )
        .unwrap();
        assert_success(sidemantic_define(first.as_ptr(), db_path.as_ptr(), false));
        let before = fs::read_to_string(&definitions_path).unwrap();

        let invalid = CString::new(
            "MODEL (name orders, table orders_v2, primary_key order_id);\nMETRIC revenue AS SUM(amount);\nMETRIC revenue AS SUM(net_amount);",
        )
        .unwrap();
        let error = take_error(sidemantic_define(invalid.as_ptr(), db_path.as_ptr(), true));
        assert!(error.contains("duplicate metric"), "{error}");

        let after = fs::read_to_string(&definitions_path).unwrap();
        assert_eq!(after, before);

        let rewritten = take_rewrite_sql(sidemantic_rewrite(
            CString::new("SELECT orders.revenue FROM orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(rewritten.contains("amount"), "{rewritten}");
        assert!(!rewritten.contains("net_amount"), "{rewritten}");
        assert!(!rewritten.contains("orders_v2"), "{rewritten}");

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
    fn test_add_definition_accepts_simple_segment_syntax() {
        let _guard = test_lock();
        sidemantic_clear();

        let db_path = unique_db_path("add_segment_simple");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();

        let model =
            CString::new("MODEL (name orders, table orders, primary_key order_id);").unwrap();
        assert_success(sidemantic_define(model.as_ptr(), db_path.as_ptr(), false));

        let segment = CString::new("SEGMENT completed AS status = 'completed';").unwrap();
        assert_success(sidemantic_add_definition(
            segment.as_ptr(),
            db_path.as_ptr(),
            false,
        ));

        let content = fs::read_to_string(&definitions_path).unwrap();
        let model = parse_sql_model(&content).unwrap();
        let completed = model.get_segment("completed").unwrap();
        assert_eq!(completed.sql, "status = 'completed'");

        remove_definitions_file(&db_path);
    }

    #[test]
    fn test_add_definition_persistence_failure_rolls_back_memory() {
        let _guard = test_lock();
        sidemantic_clear();

        let model =
            CString::new("MODEL (name orders, table orders, primary_key order_id);").unwrap();
        assert_success(sidemantic_define(model.as_ptr(), ptr::null(), false));

        let db_path = unique_db_path("add_definition_missing_sidecar");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);

        let metric = CString::new("METRIC (name revenue, agg sum, sql amount);").unwrap();
        let error = take_error(sidemantic_add_definition(
            metric.as_ptr(),
            db_path.as_ptr(),
            false,
        ));
        assert!(
            error.contains("not present in the persisted definitions file"),
            "{error}"
        );

        let rewrite_error = take_rewrite_error(sidemantic_rewrite(
            CString::new("SELECT orders.revenue FROM orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(rewrite_error.contains("revenue"), "{rewrite_error}");

        remove_definitions_file(&db_path);
    }

    #[test]
    fn test_replace_metric_dimension_and_segment_updates_persisted_definitions() {
        let _guard = test_lock();
        sidemantic_clear();

        let db_path = unique_db_path("replace_item_persistence");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();

        let model =
            CString::new("MODEL (name orders, table orders, primary_key order_id);").unwrap();
        assert_success(sidemantic_define(model.as_ptr(), db_path.as_ptr(), false));

        let old_metric = CString::new("METRIC revenue AS SUM(gross_amount);").unwrap();
        assert_success(sidemantic_add_definition(
            old_metric.as_ptr(),
            db_path.as_ptr(),
            false,
        ));
        let new_metric = CString::new("METRIC revenue AS SUM(net_amount);").unwrap();
        assert_success(sidemantic_add_definition(
            new_metric.as_ptr(),
            db_path.as_ptr(),
            true,
        ));

        let old_dimension = CString::new("DIMENSION status AS raw_status;").unwrap();
        assert_success(sidemantic_add_definition(
            old_dimension.as_ptr(),
            db_path.as_ptr(),
            false,
        ));
        let new_dimension = CString::new("DIMENSION status AS clean_status;").unwrap();
        assert_success(sidemantic_add_definition(
            new_dimension.as_ptr(),
            db_path.as_ptr(),
            true,
        ));

        let old_segment =
            CString::new("SEGMENT (name target_segment, sql old_flag = true);").unwrap();
        assert_success(sidemantic_add_definition(
            old_segment.as_ptr(),
            db_path.as_ptr(),
            false,
        ));
        let new_segment =
            CString::new("SEGMENT (name target_segment, sql new_flag = true);").unwrap();
        assert_success(sidemantic_add_definition(
            new_segment.as_ptr(),
            db_path.as_ptr(),
            true,
        ));

        let content = fs::read_to_string(&definitions_path).unwrap();
        assert!(!content.contains("gross_amount"), "{content}");
        assert!(!content.contains("raw_status"), "{content}");
        assert!(!content.contains("old_flag"), "{content}");
        assert!(content.contains("net_amount"), "{content}");
        assert!(content.contains("clean_status"), "{content}");
        assert!(content.contains("new_flag"), "{content}");
        assert_eq!(content.matches("METRIC revenue").count(), 1, "{content}");
        assert_eq!(content.matches("DIMENSION status").count(), 1, "{content}");
        assert_eq!(
            content.matches("SEGMENT (name target_segment").count(),
            1,
            "{content}"
        );

        let persisted_model = parse_sql_model(&content).unwrap();
        assert_eq!(
            persisted_model.metrics[0].sql.as_deref(),
            Some("net_amount")
        );
        assert_eq!(
            persisted_model.dimensions[0].sql.as_deref(),
            Some("clean_status")
        );
        assert_eq!(persisted_model.segments[0].sql, "new_flag = true");

        sidemantic_clear();
        assert_success(sidemantic_autoload(db_path.as_ptr()));

        let metric_sql = take_rewrite_sql(sidemantic_rewrite(
            CString::new("SELECT orders.revenue FROM orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(metric_sql.contains("net_amount"), "{metric_sql}");
        assert!(!metric_sql.contains("gross_amount"), "{metric_sql}");

        let dimension_sql = take_rewrite_sql(sidemantic_rewrite(
            CString::new("SELECT orders.status FROM orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(dimension_sql.contains("clean_status"), "{dimension_sql}");
        assert!(!dimension_sql.contains("raw_status"), "{dimension_sql}");

        remove_definitions_file(&db_path);
    }

    #[test]
    fn test_prefixed_definition_persists_under_target_model_block() {
        let _guard = test_lock();
        sidemantic_clear();

        let db_path = unique_db_path("prefixed_item_persistence");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        remove_definitions_file(&db_path);
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();

        let orders =
            CString::new("MODEL (name orders, table orders, primary_key order_id);").unwrap();
        let customers =
            CString::new("MODEL (name customers, table customers, primary_key customer_id);")
                .unwrap();
        assert_success(sidemantic_define(orders.as_ptr(), db_path.as_ptr(), false));
        assert_success(sidemantic_define(
            customers.as_ptr(),
            db_path.as_ptr(),
            false,
        ));

        let metric = CString::new("METRIC orders.revenue AS SUM(amount);").unwrap();
        assert_success(sidemantic_add_definition(
            metric.as_ptr(),
            db_path.as_ptr(),
            false,
        ));

        let content = fs::read_to_string(&definitions_path).unwrap();
        let blocks = split_definitions(&content);
        let orders_model = blocks
            .iter()
            .find_map(|block| {
                let model = parse_sql_model(block).ok()?;
                (model.name == "orders").then_some(model)
            })
            .unwrap();
        let customers_model = blocks
            .iter()
            .find_map(|block| {
                let model = parse_sql_model(block).ok()?;
                (model.name == "customers").then_some(model)
            })
            .unwrap();

        assert_eq!(orders_model.metrics.len(), 1);
        assert_eq!(orders_model.metrics[0].name, "revenue");
        assert!(customers_model.metrics.is_empty());
        assert!(!content.contains("orders.revenue"), "{content}");

        sidemantic_clear();
        assert_success(sidemantic_autoload(db_path.as_ptr()));

        let rewritten = take_rewrite_sql(sidemantic_rewrite(
            CString::new("SELECT orders.revenue FROM orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(rewritten.contains("SUM"), "{rewritten}");
        assert!(rewritten.contains("amount"), "{rewritten}");

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

        assert!(error.contains("no active model"), "{error}");

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
    fn test_load_single_model_yaml_sets_active_model() {
        let _guard = test_lock();

        let context = CString::new("duckdb:single-load-active").unwrap();
        sidemantic_clear_for_context(context.as_ptr());

        let yaml = CString::new(
            r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
"#,
        )
        .unwrap();
        assert_success(sidemantic_load_yaml_for_context(
            context.as_ptr(),
            yaml.as_ptr(),
        ));

        let metric = CString::new("METRIC revenue AS SUM(amount);").unwrap();
        assert_success(sidemantic_add_definition_for_context(
            context.as_ptr(),
            metric.as_ptr(),
            ptr::null(),
            false,
        ));

        let rewritten = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            CString::new("SELECT orders.revenue FROM orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(rewritten.contains("SUM"), "{rewritten}");
        assert!(rewritten.contains("amount"), "{rewritten}");

        sidemantic_clear_for_context(context.as_ptr());
    }

    #[test]
    fn test_load_multi_model_yaml_requires_explicit_active_model() {
        let _guard = test_lock();

        let context = CString::new("duckdb:multi-load-active").unwrap();
        sidemantic_clear_for_context(context.as_ptr());

        let yaml = CString::new(
            r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
  - name: customers
    table: customers
    primary_key: customer_id
"#,
        )
        .unwrap();
        assert_success(sidemantic_load_yaml_for_context(
            context.as_ptr(),
            yaml.as_ptr(),
        ));

        let metric = CString::new("METRIC revenue AS SUM(amount);").unwrap();
        let error = take_error(sidemantic_add_definition_for_context(
            context.as_ptr(),
            metric.as_ptr(),
            ptr::null(),
            false,
        ));
        assert!(error.contains("no active model"), "{error}");

        let explicit_metric = CString::new("METRIC orders.revenue AS SUM(amount);").unwrap();
        assert_success(sidemantic_add_definition_for_context(
            context.as_ptr(),
            explicit_metric.as_ptr(),
            ptr::null(),
            false,
        ));

        let rewritten = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            CString::new("SELECT orders.revenue FROM orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(rewritten.contains("SUM"), "{rewritten}");

        sidemantic_clear_for_context(context.as_ptr());
    }

    #[test]
    fn test_autoload_sets_active_model_for_single_loaded_model() {
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
    fn test_autoload_loads_all_persisted_models() {
        let _guard = test_lock();

        let context = CString::new("duckdb:autoload-all-models").unwrap();
        sidemantic_clear_for_context(context.as_ptr());

        let db_path = unique_db_path("autoload_all_models");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();
        fs::write(
            &definitions_path,
            "MODEL (name orders, table orders, primary_key order_id);\nMETRIC revenue AS SUM(amount);\n\nMODEL (name customers, table customers, primary_key customer_id);\nMETRIC customer_count AS COUNT(*);",
        )
        .unwrap();

        assert_success(sidemantic_autoload_for_context(
            context.as_ptr(),
            db_path.as_ptr(),
        ));

        let orders_sql = CString::new("SELECT orders.revenue FROM orders").unwrap();
        let orders_rewrite = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            orders_sql.as_ptr(),
        ));
        assert!(orders_rewrite.contains("amount"), "{orders_rewrite}");

        let customers_sql = CString::new("SELECT customers.customer_count FROM customers").unwrap();
        let customers_rewrite = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            customers_sql.as_ptr(),
        ));
        assert!(customers_rewrite.contains("COUNT"), "{customers_rewrite}");

        sidemantic_clear_for_context(context.as_ptr());
        let _ = fs::remove_file(definitions_path);
    }

    #[test]
    fn test_autoload_invalid_definition_clears_context_and_returns_error() {
        let _guard = test_lock();

        let context = CString::new("duckdb:autoload-invalid-clears").unwrap();
        sidemantic_clear_for_context(context.as_ptr());

        let db_path = unique_db_path("autoload_invalid_clears");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();

        fs::write(
            &definitions_path,
            "MODEL (name events, table events, primary_key event_id);\nMETRIC event_count AS COUNT(*);",
        )
        .unwrap();

        assert_success(sidemantic_autoload_for_context(
            context.as_ptr(),
            db_path.as_ptr(),
        ));

        let loaded = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            CString::new("SELECT events.event_count FROM events")
                .unwrap()
                .as_ptr(),
        ));
        assert!(loaded.contains("COUNT"), "{loaded}");

        fs::write(&definitions_path, "MODEL (").unwrap();
        let error = take_error(sidemantic_autoload_for_context(
            context.as_ptr(),
            db_path.as_ptr(),
        ));
        assert!(error.contains("Error loading definitions file"), "{error}");

        let passthrough = sidemantic_rewrite_for_context(
            context.as_ptr(),
            CString::new("SELECT events.event_count FROM events")
                .unwrap()
                .as_ptr(),
        );
        assert!(passthrough.error.is_null());
        assert!(!passthrough.was_rewritten);
        sidemantic_free_result(passthrough);

        sidemantic_clear_for_context(context.as_ptr());
        let _ = fs::remove_file(definitions_path);
    }

    #[test]
    fn test_autoload_missing_sidecar_clears_existing_context() {
        let _guard = test_lock();

        let context = CString::new("duckdb:autoload-missing-clears").unwrap();
        sidemantic_clear_for_context(context.as_ptr());

        let db_path = unique_db_path("autoload_missing_clears");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();
        fs::write(
            &definitions_path,
            "MODEL (name orders, table orders, primary_key order_id);\nMETRIC order_count AS COUNT(*);",
        )
        .unwrap();

        assert_success(sidemantic_autoload_for_context(
            context.as_ptr(),
            db_path.as_ptr(),
        ));

        let loaded = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            CString::new("SELECT orders.order_count FROM orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(loaded.contains("COUNT"), "{loaded}");

        fs::remove_file(&definitions_path).unwrap();
        assert_success(sidemantic_autoload_for_context(
            context.as_ptr(),
            db_path.as_ptr(),
        ));

        let passthrough = sidemantic_rewrite_for_context(
            context.as_ptr(),
            CString::new("SELECT orders.order_count FROM orders")
                .unwrap()
                .as_ptr(),
        );
        assert!(passthrough.error.is_null());
        assert!(!passthrough.was_rewritten);
        sidemantic_free_result(passthrough);

        sidemantic_clear_for_context(context.as_ptr());
    }

    #[test]
    fn test_autoload_memory_context_clears_existing_context() {
        let _guard = test_lock();

        let context = CString::new("duckdb:autoload-memory-clears").unwrap();
        sidemantic_clear_for_context(context.as_ptr());

        let yaml = CString::new(
            r#"
models:
  - name: temp_orders
    table: temp_orders
    primary_key: order_id
    metrics:
      - name: order_count
        agg: count
"#,
        )
        .unwrap();

        assert_success(sidemantic_load_yaml_for_context(
            context.as_ptr(),
            yaml.as_ptr(),
        ));
        let loaded = take_rewrite_sql(sidemantic_rewrite_for_context(
            context.as_ptr(),
            CString::new("SELECT temp_orders.order_count FROM temp_orders")
                .unwrap()
                .as_ptr(),
        ));
        assert!(loaded.contains("COUNT"), "{loaded}");

        assert_success(sidemantic_autoload_for_context(
            context.as_ptr(),
            ptr::null(),
        ));

        let passthrough = sidemantic_rewrite_for_context(
            context.as_ptr(),
            CString::new("SELECT temp_orders.order_count FROM temp_orders")
                .unwrap()
                .as_ptr(),
        );
        assert!(passthrough.error.is_null());
        assert!(!passthrough.was_rewritten);
        sidemantic_free_result(passthrough);

        sidemantic_clear_for_context(context.as_ptr());
    }

    #[test]
    fn test_atomic_write_replaces_existing_definitions_file_and_cleans_lock() {
        let _guard = test_lock();

        let db_path = unique_db_path("atomic_replace");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();
        let lock_path = definitions_lock_path(&definitions_path);

        {
            let _lock = lock_definitions_file(&definitions_path).unwrap();
            assert!(lock_path.exists());
        }
        assert!(!lock_path.exists());

        write_definitions_file_atomic(&definitions_path, "first\n").unwrap();
        write_definitions_file_atomic(&definitions_path, "second\n").unwrap();

        assert_eq!(fs::read_to_string(&definitions_path).unwrap(), "second\n");
        assert!(!lock_path.exists());

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

    #[test]
    fn test_model_block_splitting_ignores_model_keyword_in_sql_string_and_comments() {
        let _guard = test_lock();
        let db_path = unique_db_path("remove_model_keyword_string");
        let db_path = CString::new(db_path.to_string_lossy().to_string()).unwrap();
        let definitions_path = get_definitions_path(db_path.as_ptr()).unwrap();
        let content = r#"
-- MODEL (name ignored, table ignored);
MODEL (name orders, table orders, primary_key order_id);
METRIC suspicious AS SUM(CASE WHEN note = 'MODEL (' THEN amount ELSE 0 END);

MODEL (name customers, table customers, primary_key customer_id);
METRIC customer_count AS COUNT(*);
"#;
        fs::write(&definitions_path, content).unwrap();

        let original = fs::read_to_string(&definitions_path).unwrap();
        assert_eq!(split_definitions(&original).len(), 2, "{original}");

        remove_model_from_file(&definitions_path, "customers").unwrap();

        let updated = fs::read_to_string(&definitions_path).unwrap();
        assert!(updated.contains("name orders"), "{updated}");
        assert!(updated.contains("'MODEL ('"), "{updated}");
        assert!(updated.contains("suspicious"), "{updated}");
        assert!(!updated.contains("name customers"), "{updated}");
        assert!(!updated.contains("customer_count"), "{updated}");

        let loaded = load_from_sql_string_with_metadata(&updated).unwrap();
        assert_eq!(loaded.model_order, vec!["orders"]);

        let _ = fs::remove_file(definitions_path);
    }
}
