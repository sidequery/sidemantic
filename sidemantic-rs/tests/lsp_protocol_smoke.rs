#![cfg(feature = "runtime-lsp")]

use std::path::PathBuf;
use std::process::Command;

#[test]
fn lsp_stdio_protocol_exercises_diagnostics_completion_and_shutdown() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = manifest_dir.join("tests/lsp_protocol_smoke.py");
    let output = Command::new("uv")
        .arg("run")
        .arg("--no-project")
        .arg(&script)
        .arg(env!("CARGO_BIN_EXE_sidemantic-lsp"))
        .output()
        .expect("uv should run the LSP protocol smoke harness");

    assert!(
        output.status.success(),
        "LSP protocol smoke failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
