//! Public native compilation must work on an ordinary host worker stack.
use sidemantic::{load_from_string, SemanticQuery, SqlGenerator};

#[test]
fn direct_generator_compiles_on_a_two_mebibyte_host_stack() {
    let worker = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let graph = load_from_string(
                r#"
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: id
        type: numeric
        sql: tenant * 1000 + id
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: CASE WHEN amount > 0 THEN amount ELSE 0 END
"#,
            )
            .unwrap();
            let query = SemanticQuery::new()
                .with_metrics(vec!["orders.revenue".into()])
                .with_dimensions(vec!["orders.status".into()])
                .with_filters(vec!["orders.id > 1000".into()]);
            let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
            assert!(sql.contains("SUM("), "{sql}");
            assert!(sql.contains("CASE"), "{sql}");
            assert!(sql.contains("1000"), "{sql}");
        })
        .unwrap();
    worker
        .join()
        .expect("native host compilation should not abort");
}
