# Rust Native Runtime

The Rust runtime supports native sqlctx YAML and SQL projects. It is not a Rust port of every Python adapter.

Use Python to import external semantic formats, then export or normalize them into native YAML/SQL. Use Rust to validate, compile, rewrite, and eventually execute those native projects.

Export adapter output to the native contract with:

```bash
mkdir -p ./native-models
sqlctx convert ./adapter-project --to sqlctx --output ./native-models/sqlctx.yml
sqlctx validate ./native-models --engine rust
```

## Supported Inputs

Rust runtime input is limited to:

- Native YAML projects documented in `docs/native-format.md`.
- Native SQL definition files.
- Mixed directories containing native YAML and native SQL files.

The current native format version is `1`. Missing `version` is treated as version `1` for compatibility, but new exports and docs should include `version: 1`.

Native models may declare one of `table`, `sql`, or `source_uri`. `source_uri` models are accepted by Rust loading and validation as native metadata, but Rust SQL generation and rewrite reject `source_uri`-only models until they also provide a concrete `table` or `sql` source. Do not claim query/result parity for URI-backed sources until a concrete runtime source adapter exists.

## Unsupported Inputs

Rust does not parse these source formats directly:

- LookML
- MetricFlow
- Cube
- Hex
- Rill
- Malloy
- Omni
- Superset
- GoodData
- Snowflake Cortex
- ThoughtSpot
- Holistics
- Tableau
- AtScale SML
- BSL
- Yardstick
- Python semantic definition files

Those remain Python-owned import paths.

## Default runtime and installation

A normal `uv tool install git+https://github.com/sidequery/sqlctx` uses the Python
engine. From a source checkout, `uv sync --extra rust` builds and installs the
matching `sqlctx-rs` package. The `full` extra also includes it. Python remains
the default for validation, compilation, and semantic SQL rewriting.
Select `--engine rust` to use the native runtime and report an
error if it is unavailable or a capability is unsupported, or `--engine auto` to
allow fallback for known unsupported capabilities. Invalid input and unexpected
failures still propagate. `SQLCTX_ENGINE=rust` sets a process-wide default;
explicit engine selection and project runtime configuration take precedence.
The process-wide choice overrides legacy `SQLCTX_RS_*` flags.

Pyodide excludes the native dependency and defaults to Python. The distributed
Python extension uses the lightweight `python` feature; database execution
continues through Python adapters. Rust ADBC execution requires a separate
source build with `python-adbc`.

## Python API Engine Selection

Python users can select the native runtime explicitly:

```python
from sqlctx import SemanticLayer

layer = SemanticLayer(engine="rust")
```

`engine="rust"` enables Rust-backed native query validation and SQL compilation and fails if the Rust extension is unavailable.

To permit fallback:

```python
layer = SemanticLayer(engine="rust", fallback=True)
```

`engine="python"` forces Python behavior even if legacy Rust environment flags are set. `engine="auto"` attempts Rust and falls back by default.

## CLI Engine Selection

The Python CLI exposes engine selection on native validation and semantic SQL query paths:

```bash
sqlctx validate ./models --engine rust
sqlctx query "select orders.total_revenue from metrics" --models ./models --engine rust --dry-run
sqlctx rewrite "select orders.total_revenue from metrics" --models ./models --engine rust
```

Use `--fallback` when trying Rust but allowing Python fallback:

```bash
sqlctx query "select orders.total_revenue from metrics" --models ./models --engine rust --fallback
```

Config files can set the same behavior:

```yaml
runtime:
  engine: rust
  fallback: false
```

Environment variables such as `SQLCTX_RS_SQL_GENERATOR` and `SQLCTX_RS_REWRITER` remain internal migration and CI controls. New user-facing docs should prefer `engine`.

## Compatibility Checks

Native compatibility is enforced by:

```bash
uv run pytest tests/native_compat -v
cd sqlctx-rs && cargo test --test native_fixtures
```

CI runs these in the `Native Compatibility` job.

The shared fixture suite currently includes executable coverage for basic models, joins, fanout-safe symmetric aggregation, many-to-many joins, parameters in filters, embedded SQL definitions, SQL frontmatter definitions, default time dimensions, segments, derived/ratio metrics, table calculations, and pre-aggregation routing. `source_uri` is covered as a validation-only load fixture and query compilation rejects it until a concrete table or SQL source is provided.
