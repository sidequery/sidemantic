//! Configuration loading for semantic layer definitions
//!
//! Supports loading from YAML files in native Sidemantic, Cube.js, and OSI
//! formats, as well as SQL-based definitions.

mod loader;
pub(crate) mod schema;
mod sql_parser;

pub use crate::adapters::cube::CubeConfig;
pub use loader::{
    load_from_directory, load_from_directory_with_metadata, load_from_file,
    load_from_file_with_metadata, load_from_sql_string_with_metadata, load_from_string,
    load_from_string_with_metadata, ConfigFormat, LoadedGraphMetadata, LoadedModelSource,
};
pub use schema::{ModelConfig, SidemanticConfig};
pub use sql_parser::{
    parse_sql_definitions, parse_sql_graph_definitions, parse_sql_graph_definitions_extended,
    parse_sql_model, parse_sql_models, parse_sql_statement_blocks,
};
