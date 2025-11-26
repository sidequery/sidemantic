//! Error types for sidemantic

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SidemanticError {
    #[error("Model not found: {0}")]
    ModelNotFound(String),

    #[error("Dimension not found: {model}.{dimension}")]
    DimensionNotFound { model: String, dimension: String },

    #[error("Metric not found: {model}.{metric}")]
    MetricNotFound { model: String, metric: String },

    #[error("No join path found between {from} and {to}")]
    NoJoinPath { from: String, to: String },

    #[error("SQL parse error: {0}")]
    SqlParse(String),

    #[error("Invalid reference: {0}")]
    InvalidReference(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

pub type Result<T> = std::result::Result<T, SidemanticError>;
