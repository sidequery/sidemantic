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
            Some(g) => format!("DATE_TRUNC('{}', {})", g, base_sql),
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

    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }

    /// Returns the SQL expression for this metric
    pub fn sql_expr(&self) -> &str {
        self.sql.as_deref().unwrap_or(&self.name)
    }

    /// Converts metric to SQL aggregation expression
    pub fn to_sql(&self, alias: Option<&str>) -> String {
        let prefix = alias.map(|a| format!("{}.", a)).unwrap_or_default();

        match self.r#type {
            MetricType::Simple => {
                let agg = self.agg.as_ref().unwrap_or(&Aggregation::Sum);
                let sql_expr = self.sql_expr();
                let full_expr = if sql_expr == "*" {
                    "*".to_string()
                } else {
                    format!("{}{}", prefix, sql_expr)
                };

                match agg {
                    Aggregation::CountDistinct => format!("COUNT(DISTINCT {})", full_expr),
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
        }
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
}

impl Relationship {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            name: target.into(),
            r#type: RelationshipType::ManyToOne,
            foreign_key: None,
            primary_key: None,
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
            format!("({})", sql)
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
