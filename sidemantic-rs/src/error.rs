//! Error types for sidemantic

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SidemanticError {
    // Model errors
    #[error("Model not found: '{0}'. Available models: {1}")]
    ModelNotFound(String, String),

    #[error(
        "Dimension not found: '{dimension}' in model '{model}'. Available dimensions: {available}"
    )]
    DimensionNotFound {
        model: String,
        dimension: String,
        available: String,
    },

    #[error("Metric not found: '{metric}' in model '{model}'. Available metrics: {available}")]
    MetricNotFound {
        model: String,
        metric: String,
        available: String,
    },

    #[error("Segment not found: '{segment}' in model '{model}'. Available segments: {available}")]
    SegmentNotFound {
        model: String,
        segment: String,
        available: String,
    },

    // Join/relationship errors
    #[error(
        "No join path found between '{from}' and '{to}'. Check that a relationship is defined."
    )]
    NoJoinPath { from: String, to: String },

    #[error("Relationship not found: '{from}' -> '{to}'")]
    RelationshipNotFound { from: String, to: String },

    #[error("Ambiguous join path between '{from}' and '{to}': multiple paths exist")]
    AmbiguousJoinPath { from: String, to: String },

    // SQL errors
    #[error("SQL parse error: {0}")]
    SqlParse(String),

    #[error("SQL generation error: {0}")]
    SqlGeneration(String),

    // Reference errors
    #[error("Invalid reference: '{reference}'. Expected format: model.field or model.field__granularity")]
    InvalidReference { reference: String },

    #[error("Ambiguous reference: '{field}' exists in multiple models: {models}. Use model.field syntax.")]
    AmbiguousReference { field: String, models: String },

    // Configuration errors
    #[error("YAML parse error: {0}")]
    YamlParse(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("IO error: {0}")]
    Io(String),

    // Validation errors
    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),

    #[error("Missing required field: '{field}' in {context}")]
    MissingField { field: String, context: String },

    // Metric-specific errors
    #[error("Invalid metric type: '{metric}' is a {metric_type} metric but was used as a simple aggregation")]
    InvalidMetricUsage { metric: String, metric_type: String },

    #[error(
        "Metric dependency not found: '{metric}' references '{dependency}' which does not exist"
    )]
    MetricDependencyNotFound { metric: String, dependency: String },
}

impl SidemanticError {
    /// Create a ModelNotFound error with available models
    pub fn model_not_found(name: &str, available: &[&str]) -> Self {
        SidemanticError::ModelNotFound(name.to_string(), available.join(", "))
    }

    /// Create a DimensionNotFound error with available dimensions
    pub fn dimension_not_found(model: &str, dimension: &str, available: &[&str]) -> Self {
        SidemanticError::DimensionNotFound {
            model: model.to_string(),
            dimension: dimension.to_string(),
            available: available.join(", "),
        }
    }

    /// Create a MetricNotFound error with available metrics
    pub fn metric_not_found(model: &str, metric: &str, available: &[&str]) -> Self {
        SidemanticError::MetricNotFound {
            model: model.to_string(),
            metric: metric.to_string(),
            available: available.join(", "),
        }
    }

    /// Create a SegmentNotFound error with available segments
    pub fn segment_not_found(model: &str, segment: &str, available: &[&str]) -> Self {
        SidemanticError::SegmentNotFound {
            model: model.to_string(),
            segment: segment.to_string(),
            available: available.join(", "),
        }
    }
}

impl From<std::io::Error> for SidemanticError {
    fn from(err: std::io::Error) -> Self {
        SidemanticError::Io(err.to_string())
    }
}

impl From<serde_yaml::Error> for SidemanticError {
    fn from(err: serde_yaml::Error) -> Self {
        SidemanticError::YamlParse(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SidemanticError>;
