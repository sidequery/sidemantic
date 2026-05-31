# Rust Native Runtime

The Rust runtime supports native Sidemantic YAML and SQL projects. It is not a Rust port of every Python adapter.

Use Python to import external semantic formats, then export or normalize them into native YAML/SQL. Use Rust to validate, compile, rewrite, and eventually execute those native projects.

Export adapter output to the native contract with:

```bash
sidemantic export-native ./adapter-project --output sidemantic.yml --validate-rust
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

When `sidemantic export-native` receives a file path, it loads that file's parent directory before writing native YAML. This preserves adjacent directory context used by inherited models, sibling SQL definitions, and source metadata. Use a dedicated directory for single-file exports when you need a narrow output.
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

## Python API Engine Selection

Python users can select the native runtime explicitly:

```python
from sidemantic import SemanticLayer

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
sidemantic validate ./models --engine rust
sidemantic query "select orders.total_revenue from metrics" --models ./models --engine rust --dry-run
sidemantic rewrite "select orders.total_revenue from metrics" --models ./models --engine rust
```

Use `--fallback` when trying Rust but allowing Python fallback:

```bash
sidemantic query "select orders.total_revenue from metrics" --models ./models --engine rust --fallback
```

Config files can set the same behavior:

```yaml
runtime:
  engine: rust
  fallback: false
```

Environment variables such as `SIDEMANTIC_RS_SQL_GENERATOR` and `SIDEMANTIC_RS_REWRITER` remain internal migration and CI controls. New user-facing docs should prefer `engine`.

## Compatibility Checks

Native compatibility is enforced by:

```bash
uv run pytest tests/native_compat -v
cd sidemantic-rs && cargo test --test native_fixtures
```

CI runs these in the `Native Compatibility` job.

The shared fixture suite currently includes executable coverage for basic models, joins, fanout-safe symmetric aggregation, many-to-many joins, parameters in filters, embedded SQL definitions, SQL frontmatter definitions, default time dimensions, segments, derived/ratio metrics, table calculations, and pre-aggregation routing. `source_uri` is covered as a validation-only load fixture and query compilation rejects it until a concrete table or SQL source is provided.
