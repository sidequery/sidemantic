//! Dedicated entrypoint for the Rust workbench.

use std::env;
use std::process;

type CliResult<T> = std::result::Result<T, String>;

#[allow(dead_code)]
#[path = "../main.rs"]
mod cli;

#[cfg(feature = "adbc-exec")]
pub(crate) use cli::parse_connection_url_to_adbc;

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if let Err(err) = cli::workbench_command(&args) {
        eprintln!("error: {err}");
        process::exit(1);
    }
}
