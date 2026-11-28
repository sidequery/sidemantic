# Community registry submission

The descriptor is prepared here; signed installation becomes available only after
DuckDB's community registry merges the submission and deploys its builds. Local
release artifacts remain unsigned. Do not describe a prepared descriptor or a
green build as a published community extension.

After the complete SQLCtx change has been committed, tested on both released hosts
and pushed to `sidequery/sqlctx`, render its descriptor from that exact commit:

```bash
uv run scripts/duckdb_community_descriptor.py --ref <tested-commit> > /path/to/community-extensions/extensions/sqlctx/description.yml
```

The destination directory must already exist. The generator reads the template,
version and required build files from the selected commit, rather than mixing a
new worktree with an old source pin. Submit only this descriptor to
[`duckdb/community-extensions`](https://github.com/duckdb/community-extensions).
Do not commit a generated descriptor back into SQLCtx: its source pin would then
necessarily precede the commit containing it.

## Build contract

- `repo.ref` targets DuckDB 2.0.1; `repo.variegata` targets DuckDB 1.5.6. Both pins
  use the same SQLCtx commit, whose CMake probes select the host API.
- `repo.build_directory: sqlctx-duckdb` selects the extension Makefile without
  extracting that directory from the repository. The registry checks out the full
  repository. On Linux, its container mounts the repository root and changes its
  working directory to `sqlctx-duckdb`; CMake can therefore read `../sqlctx-rs`,
  its locked dependencies and public FFI header.
- `requires_toolchains: rust` installs the compiler used by the CMake static-library
  build. Rust is linked into the extension; users do not install Rust or Python.
- The descriptor requests the four native release targets: Linux AMD64/ARM64 and
  macOS AMD64/ARM64. macOS AMD64 is explicitly opted into reduced CI. Windows,
  musl and WASM stay excluded until their artifacts are tested.
- Community CI checks out its own `extension-ci-tools` under the build directory.
  SQLCtx's vendored copy is for local builds; changes there do not override the
  community workflow.

The owning upstream contracts are
[`scripts/build.py`](https://github.com/duckdb/community-extensions/blob/main/scripts/build.py),
[`build.yml`](https://github.com/duckdb/community-extensions/blob/main/.github/workflows/build.yml),
and [`ci_phase.py`](https://github.com/duckdb/extension-ci-tools/blob/v2.0-cyanoptera/scripts/ci_phase.py)
(`PhaseRunner.__init__`, `docker_workspace`, `docker_arguments`).

Verify both registry build lines, including extension tests, before requesting
merge. After deployment, test `INSTALL sqlctx FROM community; LOAD sqlctx;` in a
clean signed-only DuckDB client for each release and execute the descriptor's
example. That final check establishes availability; the source descriptor alone
does not.
