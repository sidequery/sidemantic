# Rust Native Runtime Packaging

The Rust native runtime is packaged separately from the main `sidemantic` Python package. It supports native Sidemantic YAML and SQL projects only. Python remains the importer and migration layer for external formats.

## Artifacts

| Artifact | Package name | Current version | Release path |
|---|---|---:|---|
| Main Python package | `sidemantic` | `0.9.6` | `.github/workflows/publish.yml` |
| Rust runtime crate and CLI | `sidemantic` crate, `sidemantic` binary | `0.1.0` | `.github/workflows/rust-runtime-release.yml` |
| Python extension wheel | `sidemantic-rs`, module `sidemantic_rs` | `0.1.0` | `.github/workflows/sidemantic-rs-wheels.yml` |
| DuckDB extension | `sidemantic.duckdb_extension` | `0.1.0` source package | `.github/workflows/duckdb-extension-release.yml` |
| Native format | YAML and SQL contract | `1` | `docs/native-format.md` |

Keep the Rust crate version and `sidemantic-rs` Python extension version in lockstep. The package metadata test enforces this.

## Rust CLI Install

Until the crate is published, install from source:

```bash
cargo install --path sidemantic-rs --locked
```

For a checked-out repository without installation:

```bash
cargo run --manifest-path sidemantic-rs/Cargo.toml --bin sidemantic -- validate ./models
```

After the crate is published, the intended install path is:

```bash
cargo install sidemantic --version 0.1.0 --locked
```

GitHub release CLI binaries are produced by the `Rust Runtime Release` workflow for Linux, macOS, and Windows. Treat those binaries as release artifacts for the Rust native runtime, not as replacements for the main Python CLI.

## Rust Runtime Release

Use `.github/workflows/rust-runtime-release.yml`.

The workflow is safe to run as a check:

- `cargo test --locked --test package_metadata`
- `cargo package --locked --no-verify`
- `cargo publish --locked --dry-run`
- release-mode CLI builds for Linux, macOS, and Windows

Publishing to crates.io is opt-in. Run the workflow manually with `publish_crate: true`. If `CARGO_REGISTRY_TOKEN` is absent, the publish step exits successfully after printing a skip message.

Tag pushes matching `sidemantic-rs-v*` or `rust-runtime-v*` build and attach CLI artifacts to a GitHub release. Manual runs can also set `create_github_release: true`.

## Python Extension Wheels

The Python extension package is named `sidemantic-rs` and imports as `sidemantic_rs`.

Install from a published wheel:

```bash
uv add sidemantic-rs
```

Build and smoke-test locally:

```bash
cd sidemantic-rs
uvx maturin build --release --out dist
uv run --no-project --with dist/*.whl tests/python_wheel_smoke.py
```

Build the lightweight Python-only feature wheel:

```bash
cd sidemantic-rs
uvx maturin build --no-default-features --features python --out dist-python
uv run --no-project --with dist-python/*.whl tests/python_wheel_python_smoke.py
```

Build the ADBC-enabled wheel:

```bash
cd sidemantic-rs
uvx maturin build --no-default-features --features python-adbc --out dist-adbc
uv run --no-project --with dist-adbc/*.whl tests/python_wheel_adbc_smoke.py
```

Use `.github/workflows/sidemantic-rs-wheels.yml` for release wheels. It builds Linux, macOS, and Windows wheels plus an sdist, then smokes a host wheel. PyPI upload is opt-in with `publish_pypi: true`; if `MATURIN_PYPI_TOKEN` is absent, upload is skipped without failing the workflow.

## DuckDB Extension Package

The DuckDB extension is currently documented as a source-build path. Do not document `INSTALL sidemantic FROM community` as the primary path until community extension publication is complete.

See `docs/duckdb-extension.md` for build and load commands.

Use `.github/workflows/duckdb-extension-release.yml` to build a Linux extension artifact against a selected DuckDB tag and run the sqllogictests. The default DuckDB tag is `v1.4.2`, matching current CI. GitHub release upload is optional and controlled by `create_github_release`.

Community extension publication remains a separate release step until repository signing, platform matrix, and DuckDB community registry metadata are finalized.

## Version Compatibility

| Python package | Rust runtime crate | `sidemantic-rs` wheel | Native format | DuckDB extension | DuckDB build target |
|---|---:|---:|---:|---:|---:|
| `0.9.6` | `0.1.0` | `0.1.0` | `1` | `0.1.0` source package | `1.4.2` |

Compatibility rules:

- Native format `1` is the contract shared by Python, Rust, WASM, and DuckDB extension paths.
- Rust parses native YAML and native SQL definitions directly. It does not parse external adapter formats.
- The Python extension wheel version must match the Rust crate version.
- DuckDB extension artifacts are tied to the DuckDB version used at build time.
- The main Python package should not depend on `sidemantic-rs` until Rust installation is reliable on supported platforms.
