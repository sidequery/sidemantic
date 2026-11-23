#[cfg(feature = "adbc-exec")]
mod adbc;
#[cfg(feature = "adbc-exec")]
mod result;
#[cfg(feature = "adbc-exec")]
mod url;

#[cfg(feature = "adbc-exec")]
pub use adbc::{
    execute_with_adbc, execute_with_adbc_arrow_ipc, write_adbc_arrow_ipc, AdbcArrowIpcResult,
    AdbcExecutionRequest, AdbcExecutionResult, AdbcExecutor, AdbcValue,
};
#[cfg(feature = "adbc-exec")]
pub use result::ExecutionResult;
#[cfg(feature = "adbc-exec")]
pub use url::{ConnectionSpec, DBC_DRIVER_HINTS};
