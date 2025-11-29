//! Sidemantic: A SQL-first semantic layer in Rust
//!
//! This library provides a way to define semantic models (dimensions and metrics)
//! on top of database tables, and automatically generates SQL queries that respect
//! those definitions.
//!
//! # Example
//!
//! ```
//! use sidemantic::{SemanticGraph, Model, Dimension, Metric, Relationship};
//! use sidemantic::sql::{SqlGenerator, SemanticQuery};
//!
//! // Create a semantic graph
//! let mut graph = SemanticGraph::new();
//!
//! // Define an orders model
//! let orders = Model::new("orders", "order_id")
//!     .with_table("orders")
//!     .with_dimension(Dimension::categorical("status"))
//!     .with_dimension(Dimension::time("order_date"))
//!     .with_metric(Metric::sum("revenue", "amount"))
//!     .with_metric(Metric::count("order_count"))
//!     .with_relationship(Relationship::many_to_one("customers"));
//!
//! graph.add_model(orders).unwrap();
//!
//! // Generate SQL from a semantic query
//! let generator = SqlGenerator::new(&graph);
//! let query = SemanticQuery::new()
//!     .with_metrics(vec!["orders.revenue".into()])
//!     .with_dimensions(vec!["orders.status".into()]);
//!
//! let sql = generator.generate(&query).unwrap();
//! println!("{}", sql);
//! ```

pub mod config;
pub mod core;
pub mod error;
pub mod ffi;
pub mod sql;

// Re-export commonly used types
pub use config::{load_from_directory, load_from_file, load_from_string};
pub use core::{
    build_symmetric_aggregate_sql, merge_model, resolve_model_inheritance, Aggregation, Dimension,
    DimensionType, JoinPath, JoinStep, Metric, MetricType, Model, Relationship, RelationshipType,
    RelativeDate, Segment, SemanticGraph, SqlDialect, SymmetricAggType, TableCalcType,
    TableCalculation,
};
pub use error::{Result, SidemanticError};
pub use sql::{QueryRewriter, SemanticQuery, SqlGenerator};
