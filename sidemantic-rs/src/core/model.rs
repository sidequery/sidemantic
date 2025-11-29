//! Core semantic layer types: Model, Dimension, Metric, Relationship

use serde::{Deserialize, Serialize};

use super::segment::Segment;

/// Dimension type classification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DimensionType {
    #[default]
    Categorical,
    Time,
    Boolean,
    Numeric,
}

/// A dimension represents a grouping attribute
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dimension {
    pub name: String,
    #[serde(default)]
    pub r#type: DimensionType,
    /// SQL expression (defaults to name if not provided)
    pub sql: Option<String>,
    /// Time granularity (for time dimensions)
    pub granularity: Option<String>,
    /// Human-readable label
    pub label: Option<String>,
    /// Description
    pub description: Option<String>,
}

impl Dimension {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            r#type: DimensionType::Categorical,
            sql: None,
            granularity: None,
            label: None,
            description: None,
        }
    }

    pub fn categorical(name: impl Into<String>) -> Self {
        Self::new(name)
    }

    pub fn time(name: impl Into<String>) -> Self {
        Self {
            r#type: DimensionType::Time,
            ..Self::new(name)
        }
    }

    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }

    pub fn with_granularity(mut self, granularity: impl Into<String>) -> Self {
        self.granularity = Some(granularity.into());
        self
    }

    /// Returns the SQL expression for this dimension
    pub fn sql_expr(&self) -> &str {
        self.sql.as_deref().unwrap_or(&self.name)
    }

    /// Returns SQL with time granularity applied (DATE_TRUNC)
    pub fn sql_with_granularity(&self, granularity: Option<&str>) -> String {
        let base_sql = self.sql_expr();
        match granularity.or(self.granularity.as_deref()) {
            Some(g) => format!("DATE_TRUNC('{g}', {base_sql})"),
            None => base_sql.to_string(),
        }
    }
}

/// Aggregation function type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    #[default]
    Sum,
    Count,
    CountDistinct,
    Avg,
    Min,
    Max,
    Median,
    /// Raw expression that already contains aggregation (e.g., SUM(amount) * 2)
    Expression,
}

impl Aggregation {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Aggregation::Sum => "SUM",
            Aggregation::Count => "COUNT",
            Aggregation::CountDistinct => "COUNT(DISTINCT",
            Aggregation::Avg => "AVG",
            Aggregation::Min => "MIN",
            Aggregation::Max => "MAX",
            Aggregation::Median => "MEDIAN",
            Aggregation::Expression => "", // Not used - expression stored in sql field
        }
    }
}

/// Metric type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    #[default]
    Simple,
    Derived,
    Ratio,
    Cumulative,
    TimeComparison,
}

/// Time comparison type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ComparisonType {
    Yoy, // Year over year
    Mom, // Month over month
    Wow, // Week over week
    Dod, // Day over day
    Qoq, // Quarter over quarter
    PriorPeriod,
}

/// Time comparison calculation method
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonCalculation {
    Difference,
    #[default]
    PercentChange,
    Ratio,
}

/// Time grain for period-to-date calculations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeGrain {
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

/// A metric represents a business measure (aggregation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    #[serde(default)]
    pub r#type: MetricType,
    /// Aggregation function (for simple metrics)
    pub agg: Option<Aggregation>,
    /// SQL expression
    pub sql: Option<String>,
    /// Numerator metric (for ratio metrics)
    pub numerator: Option<String>,
    /// Denominator metric (for ratio metrics)
    pub denominator: Option<String>,
    /// Filters to apply
    #[serde(default)]
    pub filters: Vec<String>,
    /// Human-readable label
    pub label: Option<String>,
    /// Description
    pub description: Option<String>,

    // Cumulative metric fields
    /// Time window for cumulative (e.g., "7 days")
    #[serde(default)]
    pub window: Option<String>,
    /// Grain for period-to-date (e.g., month for MTD)
    #[serde(default)]
    pub grain_to_date: Option<TimeGrain>,

    // Time comparison fields
    /// Base metric for time comparison
    #[serde(default)]
    pub base_metric: Option<String>,
    /// Type of time comparison
    #[serde(default)]
    pub comparison_type: Option<ComparisonType>,
    /// Custom time offset (e.g., "1 month")
    #[serde(default)]
    pub time_offset: Option<String>,
    /// Comparison calculation method
    #[serde(default)]
    pub calculation: Option<ComparisonCalculation>,

    // Display formatting
    /// Default value when result is NULL
    #[serde(default)]
    pub fill_nulls_with: Option<serde_json::Value>,
    /// Display format string (e.g., "$#,##0.00", "0.00%")
    #[serde(default)]
    pub format: Option<String>,
}

impl Metric {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            r#type: MetricType::Simple,
            agg: Some(Aggregation::Sum),
            sql: None,
            numerator: None,
            denominator: None,
            filters: Vec::new(),
            label: None,
            description: None,
            window: None,
            grain_to_date: None,
            base_metric: None,
            comparison_type: None,
            time_offset: None,
            calculation: None,
            fill_nulls_with: None,
            format: None,
        }
    }

    pub fn sum(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            agg: Some(Aggregation::Sum),
            sql: Some(sql.into()),
            ..Self::new(name)
        }
    }

    pub fn count(name: impl Into<String>) -> Self {
        Self {
            agg: Some(Aggregation::Count),
            sql: Some("*".into()),
            ..Self::new(name)
        }
    }

    pub fn count_distinct(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            agg: Some(Aggregation::CountDistinct),
            sql: Some(sql.into()),
            ..Self::new(name)
        }
    }

    pub fn avg(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            agg: Some(Aggregation::Avg),
            sql: Some(sql.into()),
            ..Self::new(name)
        }
    }

    pub fn derived(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            r#type: MetricType::Derived,
            agg: None,
            sql: Some(sql.into()),
            ..Self::new(name)
        }
    }

    pub fn ratio(
        name: impl Into<String>,
        numerator: impl Into<String>,
        denominator: impl Into<String>,
    ) -> Self {
        Self {
            r#type: MetricType::Ratio,
            agg: None,
            numerator: Some(numerator.into()),
            denominator: Some(denominator.into()),
            ..Self::new(name)
        }
    }

    /// Create a cumulative (running total) metric
    pub fn cumulative(name: impl Into<String>, base_metric: impl Into<String>) -> Self {
        Self {
            r#type: MetricType::Cumulative,
            agg: None,
            sql: Some(base_metric.into()),
            ..Self::new(name)
        }
    }

    /// Create a period-to-date metric (MTD, YTD, etc.)
    pub fn period_to_date(
        name: impl Into<String>,
        base_metric: impl Into<String>,
        grain: TimeGrain,
    ) -> Self {
        Self {
            r#type: MetricType::Cumulative,
            agg: None,
            sql: Some(base_metric.into()),
            grain_to_date: Some(grain),
            ..Self::new(name)
        }
    }

    /// Create a time comparison metric (YoY, MoM, etc.)
    pub fn time_comparison(
        name: impl Into<String>,
        base_metric: impl Into<String>,
        comparison: ComparisonType,
    ) -> Self {
        Self {
            r#type: MetricType::TimeComparison,
            agg: None,
            base_metric: Some(base_metric.into()),
            comparison_type: Some(comparison),
            ..Self::new(name)
        }
    }

    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }

    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    pub fn with_fill_nulls(mut self, value: serde_json::Value) -> Self {
        self.fill_nulls_with = Some(value);
        self
    }

    pub fn with_calculation(mut self, calc: ComparisonCalculation) -> Self {
        self.calculation = Some(calc);
        self
    }

    /// Returns the SQL expression for this metric
    pub fn sql_expr(&self) -> &str {
        self.sql.as_deref().unwrap_or(&self.name)
    }

    /// Converts metric to SQL aggregation expression
    pub fn to_sql(&self, alias: Option<&str>) -> String {
        let prefix = alias.map(|a| format!("{a}.")).unwrap_or_default();

        match self.r#type {
            MetricType::Simple => {
                let agg = self.agg.as_ref().unwrap_or(&Aggregation::Sum);
                // COUNT without explicit sql defaults to COUNT(*)
                let full_expr = if *agg == Aggregation::Count && self.sql.is_none() {
                    "*".to_string()
                } else {
                    let sql_expr = self.sql_expr();
                    if sql_expr == "*" {
                        "*".to_string()
                    } else {
                        format!("{prefix}{sql_expr}")
                    }
                };

                match agg {
                    Aggregation::CountDistinct => format!("COUNT(DISTINCT {full_expr})"),
                    _ => format!("{}({})", agg.as_sql(), full_expr),
                }
            }
            MetricType::Derived => self.sql_expr().to_string(),
            MetricType::Ratio => {
                // Ratio is computed from other metrics
                format!(
                    "({}) / NULLIF({}, 0)",
                    self.numerator.as_deref().unwrap_or("1"),
                    self.denominator.as_deref().unwrap_or("1")
                )
            }
            MetricType::Cumulative => {
                // Cumulative metrics need window functions - this is a placeholder
                // The actual implementation depends on time dimension context
                let base = self.sql_expr();
                if self.grain_to_date.is_some() {
                    // Period-to-date: SUM() with window reset at period boundary
                    format!("SUM({base}) /* cumulative, grain_to_date */")
                } else if let Some(window) = &self.window {
                    // Rolling window: SUM() OVER (ORDER BY time ROWS BETWEEN window AND CURRENT ROW)
                    format!("SUM({base}) /* cumulative, window: {window} */")
                } else {
                    // Simple running total
                    format!("SUM({base}) /* cumulative */")
                }
            }
            MetricType::TimeComparison => {
                // Time comparison metrics compare current vs prior period
                let base = self.base_metric.as_deref().unwrap_or(&self.name);
                let comparison = self
                    .comparison_type
                    .as_ref()
                    .map(|c| format!("{c:?}").to_lowercase())
                    .unwrap_or_else(|| "prior_period".to_string());
                let calc = self
                    .calculation
                    .as_ref()
                    .unwrap_or(&ComparisonCalculation::PercentChange);

                match calc {
                    ComparisonCalculation::Difference => {
                        format!("({base} - LAG({base}) OVER ()) /* {comparison} */")
                    }
                    ComparisonCalculation::PercentChange => {
                        format!(
                            "(({base} - LAG({base}) OVER ()) / NULLIF(LAG({base}) OVER (), 0)) /* {comparison} */"
                        )
                    }
                    ComparisonCalculation::Ratio => {
                        format!("({base} / NULLIF(LAG({base}) OVER (), 0)) /* {comparison} */")
                    }
                }
            }
        }
    }

    /// Check if this is a simple aggregation (not a complex metric)
    pub fn is_simple_aggregation(&self) -> bool {
        self.r#type == MetricType::Simple && self.agg.is_some()
    }
}

/// Relationship type between models
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipType {
    #[default]
    ManyToOne,
    OneToOne,
    OneToMany,
    ManyToMany,
}

/// A relationship defines how models join together
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    /// Target model name
    pub name: String,
    #[serde(default)]
    pub r#type: RelationshipType,
    /// Foreign key column (defaults to {name}_id)
    pub foreign_key: Option<String>,
    /// Primary key in related model (defaults to "id")
    pub primary_key: Option<String>,
    /// Custom SQL join condition (overrides FK/PK)
    /// Use {from} and {to} placeholders for table aliases
    #[serde(default)]
    pub sql: Option<String>,
}

impl Relationship {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            name: target.into(),
            r#type: RelationshipType::ManyToOne,
            foreign_key: None,
            primary_key: None,
            sql: None,
        }
    }

    pub fn many_to_one(target: impl Into<String>) -> Self {
        Self::new(target)
    }

    pub fn one_to_many(target: impl Into<String>) -> Self {
        Self {
            r#type: RelationshipType::OneToMany,
            ..Self::new(target)
        }
    }

    pub fn with_keys(
        mut self,
        foreign_key: impl Into<String>,
        primary_key: impl Into<String>,
    ) -> Self {
        self.foreign_key = Some(foreign_key.into());
        self.primary_key = Some(primary_key.into());
        self
    }

    /// Set a custom SQL join condition (overrides FK/PK)
    /// Use {from} and {to} placeholders for table aliases
    /// Example: "{from}.date BETWEEN {to}.start_date AND {to}.end_date"
    pub fn with_condition(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }

    /// Returns the foreign key column name
    pub fn fk(&self) -> String {
        self.foreign_key
            .clone()
            .unwrap_or_else(|| format!("{}_id", self.name))
    }

    /// Returns the primary key column name in the related model
    pub fn pk(&self) -> String {
        self.primary_key.clone().unwrap_or_else(|| "id".to_string())
    }

    /// Returns the custom SQL condition if set
    pub fn custom_condition(&self) -> Option<&str> {
        self.sql.as_deref()
    }
}

/// A model represents a table or view with semantic definitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub name: String,
    /// Physical table name
    pub table: Option<String>,
    /// SQL expression for derived tables
    pub sql: Option<String>,
    /// Primary key column
    pub primary_key: String,
    /// Dimensions (grouping attributes)
    #[serde(default)]
    pub dimensions: Vec<Dimension>,
    /// Metrics (aggregations)
    #[serde(default)]
    pub metrics: Vec<Metric>,
    /// Relationships to other models
    #[serde(default)]
    pub relationships: Vec<Relationship>,
    /// Segments (reusable filters)
    #[serde(default)]
    pub segments: Vec<Segment>,
    /// Human-readable label
    pub label: Option<String>,
    /// Description
    pub description: Option<String>,
}

impl Model {
    pub fn new(name: impl Into<String>, primary_key: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: None,
            sql: None,
            primary_key: primary_key.into(),
            dimensions: Vec::new(),
            metrics: Vec::new(),
            relationships: Vec::new(),
            segments: Vec::new(),
            label: None,
            description: None,
        }
    }

    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }

    pub fn with_dimension(mut self, dimension: Dimension) -> Self {
        self.dimensions.push(dimension);
        self
    }

    pub fn with_metric(mut self, metric: Metric) -> Self {
        self.metrics.push(metric);
        self
    }

    pub fn with_relationship(mut self, relationship: Relationship) -> Self {
        self.relationships.push(relationship);
        self
    }

    pub fn with_segment(mut self, segment: Segment) -> Self {
        self.segments.push(segment);
        self
    }

    /// Returns the table name or model name as fallback
    pub fn table_name(&self) -> &str {
        self.table.as_deref().unwrap_or(&self.name)
    }

    /// Returns the table source (table name or SQL subquery)
    pub fn table_source(&self) -> String {
        if let Some(sql) = &self.sql {
            format!("({sql})")
        } else {
            self.table_name().to_string()
        }
    }

    /// Find a dimension by name
    pub fn get_dimension(&self, name: &str) -> Option<&Dimension> {
        self.dimensions.iter().find(|d| d.name == name)
    }

    /// Find a metric by name
    pub fn get_metric(&self, name: &str) -> Option<&Metric> {
        self.metrics.iter().find(|m| m.name == name)
    }

    /// Find a relationship by target model name
    pub fn get_relationship(&self, target: &str) -> Option<&Relationship> {
        self.relationships.iter().find(|r| r.name == target)
    }

    /// Find a segment by name
    pub fn get_segment(&self, name: &str) -> Option<&Segment> {
        self.segments.iter().find(|s| s.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dimension_sql_expr() {
        let dim = Dimension::new("status");
        assert_eq!(dim.sql_expr(), "status");

        let dim = Dimension::new("order_status").with_sql("status");
        assert_eq!(dim.sql_expr(), "status");
    }

    #[test]
    fn test_metric_to_sql() {
        let metric = Metric::sum("revenue", "amount");
        assert_eq!(metric.to_sql(None), "SUM(amount)");
        assert_eq!(metric.to_sql(Some("o")), "SUM(o.amount)");

        let metric = Metric::count("order_count");
        assert_eq!(metric.to_sql(None), "COUNT(*)");

        // COUNT without explicit sql (simulates parsed definition)
        let metric = Metric {
            name: "order_count".to_string(),
            agg: Some(Aggregation::Count),
            sql: None,
            ..Metric::new("order_count")
        };
        assert_eq!(metric.to_sql(None), "COUNT(*)");

        let metric = Metric::count_distinct("unique_customers", "customer_id");
        assert_eq!(metric.to_sql(Some("o")), "COUNT(DISTINCT o.customer_id)");
    }

    #[test]
    fn test_model_builder() {
        let model = Model::new("orders", "order_id")
            .with_table("public.orders")
            .with_dimension(Dimension::categorical("status"))
            .with_metric(Metric::sum("revenue", "amount"))
            .with_relationship(Relationship::many_to_one("customers"));

        assert_eq!(model.table_name(), "public.orders");
        assert!(model.get_dimension("status").is_some());
        assert!(model.get_metric("revenue").is_some());
        assert!(model.get_relationship("customers").is_some());
    }
}
