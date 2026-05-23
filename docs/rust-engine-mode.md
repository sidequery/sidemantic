# Rust Engine Mode

Rust engine mode is the explicit product surface for opting into the native Rust runtime from Python-facing workflows.

## Modes

| Mode | Behavior |
|---|---|
| `python` | Use Python validation, compilation, and rewrite paths. |
| `rust` | Use Rust native runtime paths and fail if Rust is unavailable unless fallback is enabled. |
| `auto` | Try Rust first and fall back to Python by default. |

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

This attempts Rust and falls back to Python if the Rust extension is unavailable or the Rust compile path rejects the query.

## CLI

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

## Legacy Env Vars

The older environment variables still work for CI and migration tests:

- `SIDEMANTIC_RS_SQL_GENERATOR`
- `SIDEMANTIC_RS_QUERY_VALIDATION`
- `SIDEMANTIC_RS_REWRITER`
- `SIDEMANTIC_RS_SQL_GENERATOR_VERIFY`
- `SIDEMANTIC_RS_NO_FALLBACK`

They should not be the primary documented user interface.
