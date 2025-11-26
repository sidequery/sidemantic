//! Configuration loading for semantic layer definitions
//!
//! Supports loading from YAML files in both native Sidemantic format
//! and Cube.js format.

mod loader;
mod schema;

pub use loader::{load_from_directory, load_from_file, load_from_string, ConfigFormat};
pub use schema::{CubeConfig, SidemanticConfig};
