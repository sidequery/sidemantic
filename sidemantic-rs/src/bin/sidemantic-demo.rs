//! Example usage of sidemantic-rs

use sidemantic::sql::{QueryRewriter, SemanticQuery, SqlGenerator};
use sidemantic::{
    load_from_string, Dimension, Metric, Model, Relationship, Segment, SemanticGraph,
};

fn main() {
    println!("=== Sidemantic-rs Demo ===\n");

    // Demo 1: Programmatic API
    demo_programmatic_api();

    // Demo 2: YAML Loading (native format)
    demo_yaml_loading();

    // Demo 3: Cube.js Format
    demo_cube_format();

    // Demo 4: Segments
    demo_segments();

    // Demo 5: Query Rewriter
    demo_query_rewriter();
}

fn demo_programmatic_api() {
    println!("--- 1. Programmatic API ---\n");

    let mut graph = SemanticGraph::new();

    let orders = Model::new("orders", "order_id")
        .with_table("orders")
        .with_dimension(Dimension::categorical("status"))
        .with_dimension(Dimension::time("order_date").with_sql("created_at"))
        .with_metric(Metric::sum("revenue", "amount"))
        .with_metric(Metric::count("order_count"))
        .with_metric(Metric::avg("avg_order_value", "amount"))
        .with_relationship(Relationship::many_to_one("customers"));

    let customers = Model::new("customers", "id")
        .with_table("customers")
        .with_dimension(Dimension::categorical("name"))
        .with_dimension(Dimension::categorical("country"));

    graph.add_model(orders).unwrap();
    graph.add_model(customers).unwrap();

    let generator = SqlGenerator::new(&graph);

    let query = SemanticQuery::new()
        .with_metrics(vec!["orders.revenue".into(), "orders.order_count".into()])
        .with_dimensions(vec!["orders.status".into()]);

    println!("Query: revenue and order_count by status");
    println!("{}\n", generator.generate(&query).unwrap());
}

fn demo_yaml_loading() {
    println!("--- 2. YAML Loading (Native Format) ---\n");

    let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
      - name: order_date
        type: time
        sql: created_at
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
"#;

    let graph = load_from_string(yaml).unwrap();
    let generator = SqlGenerator::new(&graph);

    let query = SemanticQuery::new()
        .with_metrics(vec!["orders.revenue".into()])
        .with_dimensions(vec!["orders.status".into()]);

    println!("Loaded from YAML:");
    println!("{}\n", generator.generate(&query).unwrap());
}

fn demo_cube_format() {
    println!("--- 3. Cube.js Format ---\n");

    let yaml = r#"
cubes:
  - name: orders
    sql_table: public.orders

    dimensions:
      - name: status
        sql: "${CUBE}.status"
        type: string
      - name: created_at
        sql: "${CUBE}.created_at"
        type: time

    measures:
      - name: revenue
        sql: "${CUBE}.amount"
        type: sum
      - name: order_count
        type: count

    segments:
      - name: completed
        sql: "${CUBE}.status = 'completed'"
"#;

    let graph = load_from_string(yaml).unwrap();
    let model = graph.get_model("orders").unwrap();

    println!("Converted from Cube.js format:");
    println!("  Table: {:?}", model.table);
    println!(
        "  Dimensions: {:?}",
        model.dimensions.iter().map(|d| &d.name).collect::<Vec<_>>()
    );
    println!(
        "  Metrics: {:?}",
        model.metrics.iter().map(|m| &m.name).collect::<Vec<_>>()
    );
    println!(
        "  Segments: {:?}\n",
        model.segments.iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    let generator = SqlGenerator::new(&graph);
    let query = SemanticQuery::new()
        .with_metrics(vec!["orders.revenue".into()])
        .with_dimensions(vec!["orders.status".into()]);

    println!("Generated SQL:");
    println!("{}\n", generator.generate(&query).unwrap());
}

fn demo_segments() {
    println!("--- 4. Segments (Reusable Filters) ---\n");

    let mut graph = SemanticGraph::new();

    let orders = Model::new("orders", "order_id")
        .with_table("orders")
        .with_dimension(Dimension::categorical("status"))
        .with_metric(Metric::sum("revenue", "amount"))
        .with_segment(Segment::new("completed", "{model}.status = 'completed'"))
        .with_segment(Segment::new("high_value", "{model}.amount > 100"));

    graph.add_model(orders).unwrap();

    let generator = SqlGenerator::new(&graph);

    // Query with segment
    let query = SemanticQuery::new()
        .with_metrics(vec!["orders.revenue".into()])
        .with_segments(vec!["orders.completed".into()]);

    println!("Query with 'completed' segment:");
    println!("{}\n", generator.generate(&query).unwrap());

    // Query with multiple segments
    let query = SemanticQuery::new()
        .with_metrics(vec!["orders.revenue".into()])
        .with_segments(vec!["orders.completed".into(), "orders.high_value".into()]);

    println!("Query with multiple segments:");
    println!("{}\n", generator.generate(&query).unwrap());
}

fn demo_query_rewriter() {
    println!("--- 5. Query Rewriter ---\n");

    let mut graph = SemanticGraph::new();

    let orders = Model::new("orders", "order_id")
        .with_table("public.orders")
        .with_dimension(Dimension::categorical("status"))
        .with_metric(Metric::sum("revenue", "amount"))
        .with_metric(Metric::count("order_count"));

    graph.add_model(orders).unwrap();

    let rewriter = QueryRewriter::new(&graph);

    let sql = "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'pending'";
    println!("Original SQL:");
    println!("  {sql}\n");
    println!("Rewritten SQL:");
    println!("{}\n", rewriter.rewrite(sql).unwrap());
}
