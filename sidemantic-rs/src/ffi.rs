//! C FFI bindings for sidemantic-rs
//!
//! Exposes the query rewriter to C/C++ consumers like the DuckDB extension.
//!
//! Safety: These functions are `extern "C"` and expect valid C strings.
//! Callers must ensure pointers are valid. Documented in header.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;
use std::ptr;
use std::sync::Mutex;

use once_cell::sync::Lazy;

use crate::config::{load_from_directory, load_from_file, load_from_string};
use crate::core::SemanticGraph;
use crate::sql::QueryRewriter;

/// Global semantic graph state (thread-safe)
static SEMANTIC_GRAPH: Lazy<Mutex<SemanticGraph>> = Lazy::new(|| Mutex::new(SemanticGraph::new()));

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

/// Load semantic models from YAML string
///
/// Returns null on success, error message on failure.
/// Caller must free the returned string with `sidemantic_free`.
#[no_mangle]
pub extern "C" fn sidemantic_load_yaml(yaml: *const c_char) -> *mut c_char {
    if yaml.is_null() {
        return to_c_string("Error: null yaml pointer");
    }

    let yaml_str = unsafe {
        match CStr::from_ptr(yaml).to_str() {
            Ok(s) => s,
            Err(e) => return to_c_string(&format!("Error: invalid UTF-8: {e}")),
        }
    };

    match load_from_string(yaml_str) {
        Ok(new_graph) => {
            let mut graph = SEMANTIC_GRAPH.lock().unwrap();
            // Merge new models into existing graph
            for model in new_graph.models() {
                if let Err(e) = graph.add_model(model.clone()) {
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
    if path.is_null() {
        return to_c_string("Error: null path pointer");
    }

    let path_str = unsafe {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(e) => return to_c_string(&format!("Error: invalid UTF-8: {e}")),
        }
    };

    let path = Path::new(path_str);

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
            let mut graph = SEMANTIC_GRAPH.lock().unwrap();
            // Merge new models into existing graph
            for model in new_graph.models() {
                if let Err(e) = graph.add_model(model.clone()) {
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
    let mut graph = SEMANTIC_GRAPH.lock().unwrap();
    *graph = SemanticGraph::new();
}

/// Check if a table name is a registered semantic model
#[no_mangle]
pub extern "C" fn sidemantic_is_model(table_name: *const c_char) -> bool {
    if table_name.is_null() {
        return false;
    }

    let name = unsafe {
        match CStr::from_ptr(table_name).to_str() {
            Ok(s) => s,
            Err(_) => return false,
        }
    };

    let graph = SEMANTIC_GRAPH.lock().unwrap();
    graph.get_model(name).is_some()
}

/// Get list of registered model names (comma-separated)
///
/// Caller must free the returned string with `sidemantic_free`.
#[no_mangle]
pub extern "C" fn sidemantic_list_models() -> *mut c_char {
    let graph = SEMANTIC_GRAPH.lock().unwrap();
    let names: Vec<&str> = graph.models().map(|m| m.name.as_str()).collect();
    to_c_string(&names.join(","))
}

/// Rewrite a SQL query using semantic definitions
///
/// Returns a SidemanticRewriteResult struct. Caller must free with `sidemantic_free_result`.
#[no_mangle]
pub extern "C" fn sidemantic_rewrite(sql: *const c_char) -> SidemanticRewriteResult {
    if sql.is_null() {
        return SidemanticRewriteResult {
            sql: ptr::null_mut(),
            error: to_c_string("Error: null sql pointer"),
            was_rewritten: false,
        };
    }

    let sql_str = unsafe {
        match CStr::from_ptr(sql).to_str() {
            Ok(s) => s,
            Err(e) => {
                return SidemanticRewriteResult {
                    sql: ptr::null_mut(),
                    error: to_c_string(&format!("Error: invalid UTF-8: {e}")),
                    was_rewritten: false,
                }
            }
        }
    };

    let graph = SEMANTIC_GRAPH.lock().unwrap();

    // Check if query references any semantic models
    if !query_references_models(sql_str, &graph) {
        // Passthrough - not a semantic query
        return SidemanticRewriteResult {
            sql: to_c_string(sql_str),
            error: ptr::null_mut(),
            was_rewritten: false,
        };
    }

    // Rewrite the query
    let rewriter = QueryRewriter::new(&graph);
    match rewriter.rewrite(sql_str) {
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

    #[test]
    fn test_load_and_rewrite() {
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
        sidemantic_clear();

        // Query without semantic models should pass through
        let sql = CString::new("SELECT * FROM some_table").unwrap();
        let result = sidemantic_rewrite(sql.as_ptr());

        assert!(result.error.is_null());
        assert!(!result.was_rewritten);

        sidemantic_free_result(result);
    }
}
