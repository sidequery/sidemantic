//! Segment: reusable named filter

use serde::{Deserialize, Serialize};

/// A segment is a predefined reusable filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub name: String,
    /// SQL WHERE clause expression with {model} placeholder
    pub sql: String,
    /// Human-readable description
    pub description: Option<String>,
    /// Whether segment is visible in API/UI
    #[serde(default = "default_public")]
    pub public: bool,
}

fn default_public() -> bool {
    true
}

impl Segment {
    pub fn new(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql: sql.into(),
            description: None,
            public: true,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Get SQL expression with {model} placeholder replaced
    ///
    /// Also handles ${CUBE} placeholder for Cube.js compatibility
    pub fn get_sql(&self, model_alias: &str) -> String {
        self.sql
            .replace("{model}", model_alias)
            .replace("${CUBE}", model_alias)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_get_sql() {
        let segment = Segment::new("completed", "{model}.status = 'completed'");
        assert_eq!(segment.get_sql("o"), "o.status = 'completed'");
    }

    #[test]
    fn test_segment_cube_placeholder() {
        let segment = Segment::new("completed", "${CUBE}.status = 'completed'");
        assert_eq!(segment.get_sql("orders"), "orders.status = 'completed'");
    }
}
