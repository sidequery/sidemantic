# Rust Engine Mode

Rust engine mode is the explicit product surface for opting into the native Rust runtime from Python-facing workflows.

## Modes

| Mode | Behavior |
|---|---|
| `python` | Use Python validation, compilation, and rewrite paths. |
| `rust` | Use the versioned Rust semantic-input path; reject unavailable or unsupported requirements unless fallback is enabled. |
| `auto` | Select Rust when supported; permit Python fallback for typed unavailable or unsupported requirements. |

The current supported boundary and conformance evidence are documented in
[Semantic input contract](semantic-input.md). Invalid definitions and unexpected
compiler failures remain errors in every mode. Automatic fallback does not hide
those failures. Fallback diagnostics are written to stderr; SQL and data remain
on stdout.

## CLI

Build the optional Rust backend in the repository environment:

```bash
git clone https://github.com/sidequery/sqlctx.git
cd sqlctx
uv sync --extra rust
```

Run the examples below with `uv run` from that checkout. The default engine is
Python. Select Rust explicitly for each command.

Validation:

```bash
sqlctx validate ./models --engine rust
```

Semantic SQL dry run:

```bash
sqlctx query "select orders.total_revenue from metrics" --models ./models --engine rust --dry-run
```

Semantic SQL rewrite without execution:

```bash
sqlctx rewrite "select orders.total_revenue from metrics" --models ./models --engine rust
```

Fallback:

```bash
sqlctx query "select orders.total_revenue from metrics" --models ./models --engine rust --fallback
```

## Config

```yaml
runtime:
  engine: rust
  fallback: false
```

CLI `--engine` and `--fallback/--no-fallback` override config values for the command invocation.

## Python API

```python
layer = SemanticLayer(engine="rust")
```

This enables:

- Rust-backed query reference validation.
- Rust-backed structured query compilation.
- No Python SQL string verification, because Rust and Python SQL do not need byte-for-byte parity.

```python
layer = SemanticLayer(engine="rust", fallback=True)
```

This permits Python fallback when the Rust extension's versioned entrypoints are
unavailable or a required capability is unsupported. Other compiler errors
propagate. `layer.last_engine_selection` reports the selected engine and reason.

## Legacy Env Vars

The older environment variables still work for CI and migration tests:

- `SQLCTX_RS_SQL_GENERATOR`
- `SQLCTX_RS_QUERY_VALIDATION`
- `SQLCTX_RS_REWRITER`
- `SQLCTX_RS_SQL_GENERATOR_VERIFY`
- `SQLCTX_RS_NO_FALLBACK`

They should not be the primary documented user interface.
