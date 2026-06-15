//! Reusable adapters for importing and exporting external semantic-layer formats.
//!
//! Each adapter implements the [`Adapter`] trait, turning a format-specific
//! document into models + graph-level extras ([`ParsedDocument`]) and,
//! optionally, serializing a [`SemanticGraph`] back out.
//!
//! The config loader dispatches to these adapters by detected format. Native
//! Sidemantic YAML/SQL remains built into the loader; everything else
//! (Cube, OSI, and future importers) lives here.

use crate::core::{Metric, Model, Parameter, SemanticGraph};
use crate::error::{Result, SidemanticError};

pub mod cube;
pub mod osi;

pub use cube::CubeAdapter;
pub use osi::OsiAdapter;

/// Result of parsing a single external-format document.
#[derive(Debug, Default)]
pub struct ParsedDocument {
    /// Models declared in the document, in declaration order.
    pub models: Vec<Model>,
    /// Graph-level metrics added directly to the graph (not assigned to an
    /// owning model the way native top-level metrics are).
    pub graph_metrics: Vec<Metric>,
    /// Graph-level parameters.
    pub parameters: Vec<Parameter>,
    /// Graph-level metadata payload (format-specific import state). Stored
    /// verbatim on [`SemanticGraph::set_metadata`].
    pub metadata: Option<serde_json::Value>,
    /// When true, the format specifies relationships explicitly and the loader
    /// must not infer foreign-key relationships for these models.
    pub explicit_relationships: bool,
}

/// A reusable importer/exporter for an external semantic-layer format.
pub trait Adapter {
    /// Parse a single document's text into models and graph-level extras.
    fn parse_document(&self, content: &str) -> Result<ParsedDocument>;

    /// Export a semantic graph to this format's text representation.
    ///
    /// Adapters that only support import inherit the default, which errors.
    fn export_string(&self, _graph: &SemanticGraph) -> Result<String> {
        Err(SidemanticError::Validation(
            "this adapter does not support export".to_string(),
        ))
    }
}
