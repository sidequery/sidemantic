//! Symmetric aggregates for handling fan-out in joins
//!
//! Symmetric aggregates prevent double-counting when joins create multiple rows
//! for a single entity (fan-out). Uses the formula:
//!
//! ```sql
//! SUM(DISTINCT HASH(pk) * multiplier + value) - SUM(DISTINCT HASH(pk) * multiplier)
//! ```
//!
//! This ensures each row from the left side is counted exactly once.

use serde::{Deserialize, Serialize};

/// SQL dialect for symmetric aggregate generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqlDialect {
    #[default]
    DuckDB,
    BigQuery,
    Postgres,
    Snowflake,
    ClickHouse,
    Databricks,
    Spark,
}

impl SqlDialect {
    /// Parse dialect from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "duckdb" => Some(SqlDialect::DuckDB),
            "bigquery" => Some(SqlDialect::BigQuery),
            "postgres" | "postgresql" => Some(SqlDialect::Postgres),
            "snowflake" => Some(SqlDialect::Snowflake),
            "clickhouse" => Some(SqlDialect::ClickHouse),
            "databricks" => Some(SqlDialect::Databricks),
            "spark" => Some(SqlDialect::Spark),
            _ => None,
        }
    }
}

/// Aggregation types that support symmetric aggregates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymmetricAggType {
    Sum,
    Avg,
    Count,
    CountDistinct,
}

/// Build SQL for symmetric aggregate to prevent double-counting in fan-out joins.
///
/// # Arguments
/// * `measure_expr` - The measure expression to aggregate (e.g., "amount")
/// * `primary_key` - The primary key field for deduplication
/// * `agg_type` - Type of aggregation
/// * `model_alias` - Optional table/CTE alias
/// * `dialect` - SQL dialect
///
/// # Examples
/// ```
/// use sidemantic::core::symmetric_agg::{build_symmetric_aggregate_sql, SymmetricAggType, SqlDialect};
///
/// let sql = build_symmetric_aggregate_sql("amount", "order_id", SymmetricAggType::Sum, None, SqlDialect::DuckDB);
/// assert!(sql.contains("SUM(DISTINCT"));
/// ```
pub fn build_symmetric_aggregate_sql(
    measure_expr: &str,
    primary_key: &str,
    agg_type: SymmetricAggType,
    model_alias: Option<&str>,
    dialect: SqlDialect,
) -> String {
    // Add table prefix if provided
    let pk_col = match model_alias {
        Some(alias) => format!("{alias}.{primary_key}"),
        None => primary_key.to_string(),
    };
    let measure_col = match model_alias {
        Some(alias) => format!("{alias}.{measure_expr}"),
        None => measure_expr.to_string(),
    };

    // Dialect-specific hash function and multiplier
    let (hash_expr, multiplier) = match dialect {
        SqlDialect::BigQuery => (
            format!("FARM_FINGERPRINT(CAST({pk_col} AS STRING))"),
            "1048576".to_string(), // 2^20
        ),
        SqlDialect::Postgres => (
            format!("hashtext({pk_col}::text)::bigint"),
            "1024".to_string(), // 2^10 (smaller to avoid overflow)
        ),
        SqlDialect::Snowflake => (
            format!("(HASH({pk_col}) % 1000000000)"), // Modulo to constrain range
            "100".to_string(),                        // Small multiplier
        ),
        SqlDialect::ClickHouse => (
            format!("halfMD5(CAST({pk_col} AS String))"),
            "1048576".to_string(),
        ),
        SqlDialect::Databricks | SqlDialect::Spark => (
            format!("xxhash64(CAST({pk_col} AS STRING))"),
            "1048576".to_string(),
        ),
        SqlDialect::DuckDB => (
            format!("HASH({pk_col})::HUGEINT"),
            "(1::HUGEINT << 20)".to_string(),
        ),
    };

    match agg_type {
        SymmetricAggType::Sum => {
            // SUM(DISTINCT HASH(pk) * multiplier + value) - SUM(DISTINCT HASH(pk) * multiplier)
            format!(
                "(SUM(DISTINCT ({hash_expr} * {multiplier}) + {measure_col}) - \
                 SUM(DISTINCT ({hash_expr} * {multiplier})))"
            )
        }
        SymmetricAggType::Avg => {
            // Sum divided by distinct count
            let sum_expr = format!(
                "(SUM(DISTINCT ({hash_expr} * {multiplier}) + {measure_col}) - \
                 SUM(DISTINCT ({hash_expr} * {multiplier})))"
            );
            format!("{sum_expr} / NULLIF(COUNT(DISTINCT {pk_col}), 0)")
        }
        SymmetricAggType::Count => {
            // Count distinct primary keys
            format!("COUNT(DISTINCT {pk_col})")
        }
        SymmetricAggType::CountDistinct => {
            // Count distinct on the measure itself - no symmetric aggregate needed
            format!("COUNT(DISTINCT {measure_col})")
        }
    }
}

/// Check if symmetric aggregates are needed based on relationship type.
///
/// Symmetric aggregates are needed when the join creates a one-to-many
/// relationship from the base model's perspective.
pub fn needs_symmetric_aggregate(relationship_type: &str, is_base_model: bool) -> bool {
    // If this model is on the "one" side of a one-to-many relationship
    // and we're querying from the "many" side, we need symmetric aggregates
    relationship_type == "one_to_many" && is_base_model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symmetric_sum_duckdb() {
        let sql = build_symmetric_aggregate_sql(
            "amount",
            "order_id",
            SymmetricAggType::Sum,
            None,
            SqlDialect::DuckDB,
        );
        assert!(sql.contains("SUM(DISTINCT"));
        assert!(sql.contains("HASH(order_id)::HUGEINT"));
        assert!(sql.contains("+ amount"));
    }

    #[test]
    fn test_symmetric_avg_with_alias() {
        let sql = build_symmetric_aggregate_sql(
            "amount",
            "order_id",
            SymmetricAggType::Avg,
            Some("o"),
            SqlDialect::DuckDB,
        );
        assert!(sql.contains("o.order_id"));
        assert!(sql.contains("o.amount"));
        assert!(sql.contains("NULLIF(COUNT(DISTINCT"));
    }

    #[test]
    fn test_symmetric_count() {
        let sql = build_symmetric_aggregate_sql(
            "amount",
            "order_id",
            SymmetricAggType::Count,
            None,
            SqlDialect::DuckDB,
        );
        assert_eq!(sql, "COUNT(DISTINCT order_id)");
    }

    #[test]
    fn test_bigquery_dialect() {
        let sql = build_symmetric_aggregate_sql(
            "amount",
            "order_id",
            SymmetricAggType::Sum,
            None,
            SqlDialect::BigQuery,
        );
        assert!(sql.contains("FARM_FINGERPRINT"));
    }

    #[test]
    fn test_postgres_dialect() {
        let sql = build_symmetric_aggregate_sql(
            "amount",
            "order_id",
            SymmetricAggType::Sum,
            None,
            SqlDialect::Postgres,
        );
        assert!(sql.contains("hashtext"));
    }
}
