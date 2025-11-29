//! Table calculations - window functions applied to query results
//!
//! Unlike Python sidemantic which post-processes results, we generate SQL
//! window functions for efficiency in DuckDB.

use serde::{Deserialize, Serialize};

/// Type of table calculation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TableCalcType {
    /// Custom formula referencing result columns
    Formula,
    /// Percentage of total for a field
    PercentOfTotal,
    /// Percentage change from previous row
    PercentOfPrevious,
    /// Running cumulative sum
    RunningTotal,
    /// Rank by field value (descending)
    Rank,
    /// Dense rank (no gaps)
    DenseRank,
    /// Sequential row number
    RowNumber,
    /// Moving/rolling average
    MovingAverage,
    /// Difference from previous row
    Difference,
    /// Lead (next row value)
    Lead,
    /// Lag (previous row value)
    Lag,
}

/// Table calculation definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCalculation {
    /// Unique name for this calculation
    pub name: String,
    /// Type of calculation
    #[serde(rename = "type")]
    pub calc_type: TableCalcType,
    /// Description
    pub description: Option<String>,
    /// Formula expression (for Formula type)
    /// Uses ${field_name} syntax to reference columns
    pub expression: Option<String>,
    /// Field to calculate on (for most types)
    pub field: Option<String>,
    /// Fields to partition by (for window functions)
    pub partition_by: Option<Vec<String>>,
    /// Fields to order by (for sequential calculations)
    pub order_by: Option<Vec<String>>,
    /// Window size for moving average
    pub window_size: Option<i32>,
    /// Offset for lead/lag
    pub offset: Option<i32>,
}

impl TableCalculation {
    /// Create a new table calculation
    pub fn new(name: impl Into<String>, calc_type: TableCalcType) -> Self {
        Self {
            name: name.into(),
            calc_type,
            description: None,
            expression: None,
            field: None,
            partition_by: None,
            order_by: None,
            window_size: None,
            offset: None,
        }
    }

    /// Set the field to operate on
    pub fn with_field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    /// Set partition by fields
    pub fn with_partition_by(mut self, fields: Vec<String>) -> Self {
        self.partition_by = Some(fields);
        self
    }

    /// Set order by fields
    pub fn with_order_by(mut self, fields: Vec<String>) -> Self {
        self.order_by = Some(fields);
        self
    }

    /// Set window size for moving average
    pub fn with_window_size(mut self, size: i32) -> Self {
        self.window_size = Some(size);
        self
    }

    /// Set formula expression
    pub fn with_expression(mut self, expr: impl Into<String>) -> Self {
        self.expression = Some(expr.into());
        self
    }

    /// Generate SQL for this table calculation
    pub fn to_sql(&self) -> Result<String, String> {
        match self.calc_type {
            TableCalcType::Formula => self.formula_sql(),
            TableCalcType::PercentOfTotal => self.percent_of_total_sql(),
            TableCalcType::PercentOfPrevious => self.percent_of_previous_sql(),
            TableCalcType::RunningTotal => self.running_total_sql(),
            TableCalcType::Rank => self.rank_sql(),
            TableCalcType::DenseRank => self.dense_rank_sql(),
            TableCalcType::RowNumber => self.row_number_sql(),
            TableCalcType::MovingAverage => self.moving_average_sql(),
            TableCalcType::Difference => self.difference_sql(),
            TableCalcType::Lead => self.lead_sql(),
            TableCalcType::Lag => self.lag_sql(),
        }
    }

    fn formula_sql(&self) -> Result<String, String> {
        // For formula, we just return the expression
        // The caller should have already substituted ${field} references
        self.expression
            .clone()
            .ok_or_else(|| format!("Formula calculation '{}' missing expression", self.name))
    }

    fn percent_of_total_sql(&self) -> Result<String, String> {
        let field = self.require_field()?;
        let partition = self.partition_clause();
        Ok(format!(
            "{field} * 100.0 / NULLIF(SUM({field}) OVER ({partition}), 0)"
        ))
    }

    fn percent_of_previous_sql(&self) -> Result<String, String> {
        let field = self.require_field()?;
        let order = self.order_clause()?;
        let partition = self.partition_clause();
        let over = self.combine_partition_order(&partition, &order);
        Ok(format!(
            "({field} - LAG({field}) OVER ({over})) * 100.0 / NULLIF(LAG({field}) OVER ({over}), 0)"
        ))
    }

    fn running_total_sql(&self) -> Result<String, String> {
        let field = self.require_field()?;
        let order = self.order_clause()?;
        let partition = self.partition_clause();
        let over = self.combine_partition_order(&partition, &order);
        Ok(format!(
            "SUM({field}) OVER ({over} ROWS UNBOUNDED PRECEDING)"
        ))
    }

    fn rank_sql(&self) -> Result<String, String> {
        let order = self.order_clause()?;
        let partition = self.partition_clause();
        let over = self.combine_partition_order(&partition, &order);
        Ok(format!("RANK() OVER ({over})"))
    }

    fn dense_rank_sql(&self) -> Result<String, String> {
        let order = self.order_clause()?;
        let partition = self.partition_clause();
        let over = self.combine_partition_order(&partition, &order);
        Ok(format!("DENSE_RANK() OVER ({over})"))
    }

    fn row_number_sql(&self) -> Result<String, String> {
        let partition = self.partition_clause();
        let order = self
            .order_by
            .as_ref()
            .map(|o| format!("ORDER BY {}", o.join(", ")));
        let over = match (partition.is_empty(), &order) {
            (true, None) => String::new(),
            (true, Some(o)) => o.clone(),
            (false, None) => partition,
            (false, Some(o)) => format!("{partition} {o}"),
        };
        Ok(format!("ROW_NUMBER() OVER ({over})"))
    }

    fn moving_average_sql(&self) -> Result<String, String> {
        let field = self.require_field()?;
        let window = self
            .window_size
            .ok_or_else(|| format!("Moving average '{}' requires window_size", self.name))?;
        let order = self.order_clause()?;
        let partition = self.partition_clause();
        let over = self.combine_partition_order(&partition, &order);
        Ok(format!(
            "AVG({field}) OVER ({over} ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW)",
            preceding = window - 1
        ))
    }

    fn difference_sql(&self) -> Result<String, String> {
        let field = self.require_field()?;
        let order = self.order_clause()?;
        let partition = self.partition_clause();
        let over = self.combine_partition_order(&partition, &order);
        Ok(format!("{field} - LAG({field}) OVER ({over})"))
    }

    fn lead_sql(&self) -> Result<String, String> {
        let field = self.require_field()?;
        let offset = self.offset.unwrap_or(1);
        let order = self.order_clause()?;
        let partition = self.partition_clause();
        let over = self.combine_partition_order(&partition, &order);
        Ok(format!("LEAD({field}, {offset}) OVER ({over})"))
    }

    fn lag_sql(&self) -> Result<String, String> {
        let field = self.require_field()?;
        let offset = self.offset.unwrap_or(1);
        let order = self.order_clause()?;
        let partition = self.partition_clause();
        let over = self.combine_partition_order(&partition, &order);
        Ok(format!("LAG({field}, {offset}) OVER ({over})"))
    }

    fn require_field(&self) -> Result<&str, String> {
        self.field
            .as_deref()
            .ok_or_else(|| format!("Table calculation '{}' requires a field", self.name))
    }

    fn partition_clause(&self) -> String {
        self.partition_by
            .as_ref()
            .map(|p| format!("PARTITION BY {}", p.join(", ")))
            .unwrap_or_default()
    }

    fn order_clause(&self) -> Result<String, String> {
        // If order_by specified, use it; otherwise use field if available
        if let Some(ref order) = self.order_by {
            Ok(format!("ORDER BY {}", order.join(", ")))
        } else if let Some(ref field) = self.field {
            Ok(format!("ORDER BY {field}"))
        } else {
            Err(format!(
                "Table calculation '{}' requires order_by or field",
                self.name
            ))
        }
    }

    fn combine_partition_order(&self, partition: &str, order: &str) -> String {
        match (partition.is_empty(), order.is_empty()) {
            (true, true) => String::new(),
            (true, false) => order.to_string(),
            (false, true) => partition.to_string(),
            (false, false) => format!("{partition} {order}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_running_total() {
        let calc = TableCalculation::new("cumulative_revenue", TableCalcType::RunningTotal)
            .with_field("revenue")
            .with_order_by(vec!["order_date".into()]);

        let sql = calc.to_sql().unwrap();
        assert!(sql.contains("SUM(revenue) OVER"));
        assert!(sql.contains("ORDER BY order_date"));
        assert!(sql.contains("ROWS UNBOUNDED PRECEDING"));
    }

    #[test]
    fn test_percent_of_total() {
        let calc = TableCalculation::new("pct_revenue", TableCalcType::PercentOfTotal)
            .with_field("revenue");

        let sql = calc.to_sql().unwrap();
        assert!(sql.contains("revenue * 100.0"));
        assert!(sql.contains("SUM(revenue) OVER"));
    }

    #[test]
    fn test_moving_average() {
        let calc = TableCalculation::new("ma_7", TableCalcType::MovingAverage)
            .with_field("revenue")
            .with_order_by(vec!["order_date".into()])
            .with_window_size(7);

        let sql = calc.to_sql().unwrap();
        assert!(sql.contains("AVG(revenue) OVER"));
        assert!(sql.contains("ROWS BETWEEN 6 PRECEDING AND CURRENT ROW"));
    }

    #[test]
    fn test_rank_with_partition() {
        let calc = TableCalculation::new("rank_in_category", TableCalcType::Rank)
            .with_field("revenue")
            .with_partition_by(vec!["category".into()])
            .with_order_by(vec!["revenue DESC".into()]);

        let sql = calc.to_sql().unwrap();
        assert!(sql.contains("RANK() OVER"));
        assert!(sql.contains("PARTITION BY category"));
        assert!(sql.contains("ORDER BY revenue DESC"));
    }
}
