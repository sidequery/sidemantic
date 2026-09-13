# Rust Engine Mode

Rust engine mode is the explicit product surface for opting into the native Rust runtime from Python-facing workflows.

## Modes

| Mode | Behavior |
|---|---|
| `python` | Use Python structured validation and compilation. |
| `rust` | Use versioned Rust structured validation and compilation; reject unavailable or unsupported requirements unless fallback is enabled. |
| `auto` | Select Rust when supported; permit Python fallback for typed unavailable or unsupported requirements. |

The current supported boundary and conformance evidence are documented in
[Semantic input contract](semantic-input.md). Invalid definitions and unexpected
structured compiler failures remain errors in every mode. Automatic fallback
does not hide those failures. These guarantees describe structured compilation;
the CLI rewrite route is migrated separately.

## CLI

The existing CLI options remain available. In this layer, CLI validation and
semantic-SQL rewrite still use their existing integrations; these commands do
not yet establish versioned semantic-input or exact-projection conformance.
The structured compiler guarantees below apply to the Python API.

Validation:

```bash
sidemantic validate ./models --engine rust
```

Semantic SQL dry run:

```bash
sidemantic query "select orders.total_revenue from metrics" --models ./models --engine rust --dry-run
```

Semantic SQL rewrite without execution:

```bash
sidemantic rewrite "select orders.total_revenue from metrics" --models ./models --engine rust
```

Fallback:

```bash
sidemantic query "select orders.total_revenue from metrics" --models ./models --engine rust --fallback
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

- `SIDEMANTIC_RS_SQL_GENERATOR`
- `SIDEMANTIC_RS_QUERY_VALIDATION`
- `SIDEMANTIC_RS_REWRITER`
- `SIDEMANTIC_RS_SQL_GENERATOR_VERIFY`
- `SIDEMANTIC_RS_NO_FALLBACK`

They should not be the primary documented user interface.
