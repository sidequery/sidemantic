#![cfg(feature = "workbench-tui")]

use std::path::PathBuf;
use std::process::Command;

#[test]
fn dedicated_workbench_binary_handles_help_directly() {
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic-workbench"))
        .arg("--help")
        .output()
        .expect("sidemantic-workbench should run directly");

    assert!(
        output.status.success(),
        "sidemantic-workbench --help failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Usage: sidemantic workbench"),
        "unexpected stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn workbench_pty_exercises_launch_keys_quit_and_execution_states() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = manifest_dir.join("tests/workbench_pty_smoke.py");
    let mut command = Command::new("uv");
    command
        .arg("run")
        .arg("--no-project")
        .arg(&script)
        .arg(env!("CARGO_BIN_EXE_sidemantic"));

    if cfg!(feature = "workbench-adbc") {
        command.env("SIDEMANTIC_WORKBENCH_PTY_EXPECT_ADBC", "1");
    } else {
        command.env_remove("SIDEMANTIC_WORKBENCH_PTY_EXPECT_ADBC");
    }

    let output = command
        .output()
        .expect("uv should run the workbench PTY smoke harness");

    assert!(
        output.status.success(),
        "workbench PTY smoke failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
