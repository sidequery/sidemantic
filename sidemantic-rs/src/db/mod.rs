#[cfg(feature = "adbc-exec")]
mod adbc;

#[cfg(feature = "adbc-exec")]
pub use adbc::{
    execute_with_adbc, execute_with_adbc_arrow_ipc, write_adbc_arrow_ipc, AdbcArrowIpcResult,
    AdbcExecutionRequest, AdbcExecutionResult, AdbcValue,
};
