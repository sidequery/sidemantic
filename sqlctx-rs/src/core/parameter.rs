//! Parameter definitions for dynamic query input.

use serde::{Deserialize, Serialize};

/// Parameter type for query-time substitution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParameterType {
    String,
    Number,
    Date,
    Unquoted,
    Yesno,
}

/// Parameter metadata defined at graph level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Parameter {
    pub name: String,
    #[serde(rename = "type")]
    pub parameter_type: ParameterType,
    pub description: Option<String>,
    pub label: Option<String>,
    #[serde(default)]
    pub default_value: Option<serde_json::Value>,
    #[serde(default)]
    pub allowed_values: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub default_to_today: bool,
}
