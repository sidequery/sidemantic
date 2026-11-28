//! Host-owned immutable graph handles. Never cache physical source bindings.

use super::*;

pub struct SqlctxCompiler {
    pub(super) graph: SemanticGraph,
    pub(super) views: views::PreparedViews,
}

impl SqlctxCompiler {
    fn new(snapshot: &str) -> CatalogResult<Self> {
        let graph = Snapshot::parse(snapshot)?.into_graph()?;
        let views = views::PreparedViews::new(&graph)?;
        Ok(Self { graph, views })
    }
}

#[no_mangle]
pub extern "C" fn sqlctx_compiler_create(
    snapshot: *const c_char,
    error: *mut *mut c_char,
) -> *mut SqlctxCompiler {
    if error.is_null() {
        return ptr::null_mut();
    }
    unsafe { *error = ptr::null_mut() };
    let result = guard(|| {
        let source = optional_arg(snapshot, "snapshot")?;
        crate::semantic_input::with_semantic_stack(|| Ok(SqlctxCompiler::new(&source)))
            .map_err(|error| error.to_string())?
    });
    match result {
        Ok(compiler) => Box::into_raw(Box::new(compiler)),
        Err(message) => {
            unsafe { *error = semantic_error(message) };
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn sqlctx_compiler_rewrite(
    compiler: *const SqlctxCompiler,
    sql: *const c_char,
) -> SqlctxRewriteResult {
    semantic_result(
        guard(|| {
            let compiler = unsafe { compiler.as_ref() }.ok_or("null compiler pointer")?;
            let sql = required_arg(sql, "sql")?;
            rewrite_prepared_graph_diagnostics(&compiler.graph, &compiler.views, &sql)
                .map(|(sql, _)| sql)
        })
        .map_err(semantic_error),
    )
}

#[no_mangle]
pub extern "C" fn sqlctx_compiler_free(compiler: *mut SqlctxCompiler) {
    if !compiler.is_null() {
        drop(unsafe { Box::from_raw(compiler) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiler_is_snapshot_owned_and_matches_stateless_rewrite() {
        let (source, _) = apply(
            "",
            "",
            "model",
            "MODEL orders FROM raw_orders (sum(amount) AS revenue)",
            false,
        )
        .unwrap();
        let snapshot = CString::new(source.clone()).unwrap();
        let mut error = ptr::null_mut();
        let compiler = sqlctx_compiler_create(snapshot.as_ptr(), &mut error);
        assert!(!compiler.is_null());
        assert!(error.is_null());
        let handle = unsafe { &*compiler };
        assert!(std::ptr::eq(
            &handle.graph,
            handle.views.graph(&handle.graph)
        ));
        drop(snapshot);
        let query = CString::new("SELECT orders.revenue FROM orders").unwrap();
        let expected = rewrite_graph(
            &Snapshot::parse(&source).unwrap().into_graph().unwrap(),
            query.to_str().unwrap(),
        )
        .unwrap();
        for _ in 0..3 {
            let result = sqlctx_compiler_rewrite(compiler, query.as_ptr());
            assert!(result.error.is_null());
            assert_eq!(
                unsafe { CStr::from_ptr(result.sql) }.to_str().unwrap(),
                expected
            );
            super::super::super::sqlctx_free_result(result);
        }
        sqlctx_compiler_free(compiler);
    }

    #[test]
    fn named_view_handle_matches_stateless_rewrite_and_explain_at_each_grain() {
        fn result_text(result: SqlctxRewriteResult) -> String {
            assert!(
                result.error.is_null(),
                "{}",
                if result.error.is_null() {
                    String::new()
                } else {
                    unsafe { CStr::from_ptr(result.error) }
                        .to_string_lossy()
                        .into_owned()
                }
            );
            let text = unsafe { CStr::from_ptr(result.sql) }
                .to_str()
                .unwrap()
                .to_owned();
            super::super::super::sqlctx_free_result(result);
            text
        }
        let (source, _) = apply(
            "",
            "",
            "yaml",
            r#"
models:
  - name: orders
    table: raw_orders
    primary_key: id
    dimensions:
      - {name: status, type: categorical}
    metrics:
      - {name: paid, agg: sum, sql: amount, filters: ["status = 'paid'"]}
      - {name: rows, agg: count}
      - {name: average, type: ratio, numerator: paid, denominator: rows}
"#,
            false,
        )
        .unwrap();
        let (source, _) = apply(
            &source,
            "orders",
            "view",
            "sales AS SELECT status AS state, paid, average AS aov FROM orders",
            false,
        )
        .unwrap();
        let (source, _) = apply(
            &source,
            "orders",
            "view",
            "report AS SELECT state, paid AS total, aov FROM sales",
            false,
        )
        .unwrap();
        let snapshot = CString::new(source.clone()).unwrap();
        let mut error = ptr::null_mut();
        let compiler = sqlctx_compiler_create(snapshot.as_ptr(), &mut error);
        assert!(!compiler.is_null());
        assert!(error.is_null());
        drop(snapshot);
        let graph = Snapshot::parse(&source).unwrap().into_graph().unwrap();
        for sql in [
            "SELECT total, aov FROM report",
            "SELECT state, total, aov FROM report",
            "SELECT aov FROM sales WHERE state = 'paid'",
            "SELECT total, aov FROM report",
        ] {
            let query = CString::new(sql).unwrap();
            let expected = rewrite_graph(&graph, sql).unwrap();
            assert_eq!(
                result_text(sqlctx_compiler_rewrite(compiler, query.as_ptr())),
                expected
            );
            let report: serde_json::Value = serde_json::from_str(&result_text(
                explain::sqlctx_compiler_explain(compiler, query.as_ptr()),
            ))
            .unwrap();
            let expected_report: serde_json::Value =
                serde_json::from_str(&explain::explain_graph(&graph, sql).unwrap()).unwrap();
            assert_eq!(report, expected_report);
            assert_eq!(report["sql"], expected);
        }
        let (replacement, _) = apply(
            &source,
            "orders",
            "item",
            "METRIC paid AS SUM(replacement_amount)",
            true,
        )
        .unwrap();
        let query = CString::new("SELECT total FROM report").unwrap();
        let original_sql = result_text(sqlctx_compiler_rewrite(compiler, query.as_ptr()));
        assert!(!original_sql.contains("replacement_amount"));
        let updated_graph = Snapshot::parse(&replacement).unwrap().into_graph().unwrap();
        assert!(rewrite_graph(&updated_graph, query.to_str().unwrap())
            .unwrap()
            .contains("replacement_amount"));
        sqlctx_compiler_free(compiler);
    }

    #[test]
    fn compiler_reports_invalid_snapshots_and_null_handles() {
        let snapshot = CString::new("{\"version\":99}").unwrap();
        let mut error = ptr::null_mut();
        assert!(sqlctx_compiler_create(snapshot.as_ptr(), &mut error).is_null());
        assert!(!error.is_null());
        sqlctx_free(error);
        let result = sqlctx_compiler_rewrite(ptr::null(), ptr::null());
        assert!(!result.error.is_null());
        super::super::super::sqlctx_free_result(result);
        sqlctx_compiler_free(ptr::null_mut());
    }
}
