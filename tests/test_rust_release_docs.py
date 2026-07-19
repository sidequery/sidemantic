from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def assert_file_contains(path: str, expected: list[str]) -> None:
    contents = (ROOT / path).read_text()

    for needle in expected:
        assert needle in contents, f"{path} is missing {needle!r}"


def test_rust_native_runtime_release_workflows_are_documented() -> None:
    assert_file_contains(
        ".github/workflows/rust-runtime-release.yml",
        [
            "name: Rust Runtime Release",
            "cargo package --locked --no-verify",
            "cargo publish --locked --dry-run",
            "CARGO_REGISTRY_TOKEN",
        ],
    )
    assert_file_contains(
        ".github/workflows/sidemantic-rs-wheels.yml",
        [
            "name: sidemantic-rs Python Wheels",
            "uvx maturin sdist --out dist",
            "uv run --no-project --with dist/*.whl tests/python_wheel_smoke.py",
            "MATURIN_PYPI_TOKEN",
        ],
    )
    assert_file_contains(
        ".github/workflows/duckdb-extension-release.yml",
        [
            "name: DuckDB Extension Release",
            "duckdb_version",
            "make test",
            "sidemantic.duckdb_extension",
        ],
    )


def test_rust_native_runtime_release_docs_cover_install_and_compatibility() -> None:
    assert_file_contains(
        "docs/rust-native-runtime-packaging.md",
        [
            "# Rust Native Runtime Packaging",
            "Rust CLI Install",
            "Python Extension Wheels",
            "DuckDB Extension Package",
            "Version Compatibility",
        ],
    )
    assert_file_contains(
        "docs/duckdb-extension.md",
        [
            "# DuckDB Extension",
            "Build From Source",
            "sidemantic_load_file",
            "DuckDB extension artifacts are ABI-sensitive",
        ],
    )


def test_rust_runtime_conversion_example_validates_a_directory() -> None:
    assert_file_contains(
        "docs/rust-runtime.md",
        [
            "--output ./native-models/sidemantic.yml",
            "sidemantic validate ./native-models --engine rust",
        ],
    )
