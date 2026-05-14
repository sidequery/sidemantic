fn toml_string_value(contents: &str, key: &str) -> Option<String> {
    let prefix = format!("{key} = ");
    contents.lines().find_map(|line| {
        let value = line.trim().strip_prefix(&prefix)?;
        Some(value.trim().trim_matches('"').to_string())
    })
}

#[test]
fn rust_crate_and_python_extension_versions_match() {
    let pyproject = include_str!("../pyproject.toml");
    let pyproject_version =
        toml_string_value(pyproject, "version").expect("pyproject.toml project.version");

    assert_eq!(env!("CARGO_PKG_VERSION"), pyproject_version);
}

#[test]
fn python_extension_metadata_targets_the_expected_module_and_feature() {
    let pyproject = include_str!("../pyproject.toml");

    assert!(pyproject.contains("name = \"sidemantic-rs\""));
    assert!(pyproject.contains("module-name = \"sidemantic_rs\""));
    assert!(pyproject.contains("features = [\"python-adbc\"]"));
    assert!(pyproject.contains("license = \"AGPL-3.0-only\""));
    assert!(pyproject.contains("readme = \"README.md\""));
}

#[test]
fn library_crate_types_cover_rust_c_abi_and_python_wasm_artifacts() {
    let cargo_toml = include_str!("../Cargo.toml");

    assert!(cargo_toml.contains("crate-type = [\"rlib\", \"staticlib\", \"cdylib\"]"));
}

#[test]
fn cargo_metadata_and_feature_split_are_explicit() {
    let cargo_toml = include_str!("../Cargo.toml");

    for expected in [
        "license = \"AGPL-3.0-only\"",
        "repository = \"https://github.com/sidequery/sidemantic\"",
        "homepage = \"https://sidemantic.com\"",
        "readme = \"README.md\"",
        "python-adbc = [\"python\", \"adbc-exec\"]",
        "mcp-adbc = [\"mcp-server\", \"adbc-exec\"]",
        "runtime-server-adbc = [\"runtime-server\", \"adbc-exec\"]",
        "workbench-adbc = [\"workbench-tui\", \"adbc-exec\"]",
    ] {
        assert!(cargo_toml.contains(expected), "missing {expected}");
    }
}
