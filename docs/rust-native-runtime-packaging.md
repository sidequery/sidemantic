# Rust runtime packaging

Runtime artifacts have separate release paths. The presence of a workflow or an
installation command below does not establish that a version is published.
Python remains the default engine; installing the Rust bindings does not promote it.

| Artifact | Metadata | Release workflow | Qualification |
|---|---|---|---|
| Python `sqlctx` | Root `pyproject.toml` | `publish.yml` | Full suite, built wheel and DAX smoke; separate Pyodide CI |
| Python `sqlctx-rs` (`sqlctx_rs` import) | `sqlctx-rs/pyproject.toml` | `sqlctx-rs-wheels.yml` | Install and smoke each actual native wheel, plus sdist creation |
| Rust `sqlctx` crate and base CLI | `sqlctx-rs/Cargo.toml` | `rust-runtime-release.yml` | Crate package/dry-run, packaged CLI compilation smoke |
| Rust CLI and companion binaries | `sqlctx-rs/Cargo.toml` | `release-rust-binaries.yml` | Explicit existing release tag, matching crate version, binary smoke |
| Browser `sqlctx-wasm` | `sqlctx-wasm/package.json` | `wasm-package-release.yml` | WASM and consumer smoke tests |
| DuckDB extension | `sqlctx-duckdb/Makefile` | `duckdb-extension-release.yml` | Pinned DuckDB build and sqllogictests |

Workflow files live in `.github/workflows/`. Rust crate and wheel versions must
match; package metadata tests enforce this. Root Python and WASM versions are
separate. The DuckDB extension is tied to the DuckDB build version, currently
`v2.0.1` or `v1.5.6`. Select the artifact matching the host version and platform.

## Install from a checkout or downloaded artifact

The Python CLI remains the normal entry point:

```bash
uv tool install .
sqlctx validate ./models
```

For the optional Rust binding, install a compatible downloaded wheel into the
same environment as sqlctx. Source installation requires a Rust toolchain:

```bash
uv add ./sqlctx-rs
sqlctx validate ./models --engine rust --no-fallback
```

The release wheel enables `python-adbc`. ADBC execution also requires an actual
database driver; a successful compiler smoke does not establish driver availability.
A compile-only binding can be built with
`uvx maturin build --manifest-path sqlctx-rs/Cargo.toml --no-default-features --features python`.
Core Python imports and Pyodide do not require either Rust wheel.

For the standalone base Rust CLI:

```bash
cargo install --path sqlctx-rs --locked
sqlctx compile --models ./models --metric orders.revenue
```

The Rust and Python CLIs both use the executable name `sqlctx`; choose an
isolated installation or invoke the desired binary by its full path. The base
Rust release uses default features and is a compiler/rewriter. It does not include
ADBC execution or the optional server, MCP, LSP, and workbench binaries. The
`release-rust-binaries.yml` workflow attaches a separate companion-binary bundle
to an explicitly selected existing release. Its service features still do not
enable ADBC. Neither artifact silently substitutes the Python CLI.

Rust directly accepts native YAML/SQL, Cube YAML, and OSI YAML. Other source
formats continue through the Python import and migration path. See
[rust-engine-mode.md](rust-engine-mode.md) for the graph handoff and engine contract.

For DuckDB build/load instructions, use [duckdb-extension.md](duckdb-extension.md).
The release workflow currently produces Linux amd64 artifacts only. Community
registry publication, signing, and additional platforms remain separate work;
`INSTALL sqlctx FROM community` is not an established installation path here.

## Release checks and publication boundaries

The Rust crate/CLI, wheel, and DuckDB workflows support manual qualification with
publication inputs left false. Changes to those workflows also run qualification
on pull requests. Wheel tests install the uploaded matrix outputs on matching
Linux x86_64/arm64, macOS x86_64/arm64, and Windows x86_64 hosts; they do not rebuild
a different wheel for acceptance. The CLI smoke compiles a model from a temporary
directory using the packaged executable. Production release jobs do not restore
or publish compiler caches.

Publishing remains an explicit operational action. Existing tag triggers can
attach GitHub assets or publish the WASM package; manual publish/release flags
must not be enabled for qualification. Some workflows skip publishing when their
registry token is absent, so a green build alone does not prove publication.
Verify registry and release assets separately after any authorized release.
