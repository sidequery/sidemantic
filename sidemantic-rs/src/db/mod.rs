#[cfg(feature = "adbc-exec")]
mod adbc;

#[cfg(feature = "adbc-exec")]
pub use adbc::{execute_with_adbc, AdbcExecutionRequest, AdbcExecutionResult, AdbcValue};
