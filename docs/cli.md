# CLI contract

Sidemantic is CLI-first. The command line follows one contract across command
families so it can be used interactively and safely composed in scripts.
Online documentation: <https://sidemantic.com/sidemantic/cli>. Support and bug
reports: <https://github.com/sidequery/sidemantic/issues>.

## Help

Full help is available through every conventional entry point:

```bash
sidemantic -h
sidemantic --help
sidemantic help
sidemantic help migrate generate
sidemantic migrate generate -h
```

`sidemantic help` accepts a nested command path and is equivalent to the
corresponding `--help` invocation.

## Output and exit codes

Primary results and machine data go to stdout. Errors, warnings, deprecations,
and status messages go to stderr. This keeps pipelines such as
`sidemantic info --json | jq .models` valid.

The stable process exit codes are:

| Code | Meaning |
| --- | --- |
| `0` | Success, including an empty but valid result |
| `1` | An operation or validation failed |
| `2` | The invocation or configuration is invalid |

Expected errors are concise. Put the global `--debug` option before the command
to let unexpected exceptions propagate with a traceback:

```bash
sidemantic --debug info ./models
```

Long project, config, output, and diagnostic options can be placed before or
after a subcommand. For example, these are equivalent:

```bash
sidemantic --project ./analytics info --format json
sidemantic info --project ./analytics --format json
```

Use the long `--quiet` spelling after `preagg recommend` and `preagg apply`,
because those commands retain the existing `-q` shorthand for `--queries`.

## JSON

Structured inspection and reporting commands accept `--json`, including:

```bash
sidemantic info --json
sidemantic validate --json
sidemantic dashboard validate dashboard.yml --json
sidemantic migrate check queries/ --json
sidemantic migrate generate queries/ --output generated/ --json
sidemantic preagg recommend --queries query-log.sql --json
sidemantic preagg apply --queries query-log.sql --dry-run --json
sidemantic preagg refresh --json
sidemantic explain "SELECT orders.revenue FROM orders"
```

JSON is the only content written to stdout in these modes. Diagnostics remain
on stderr.

## Model validation

Offline structural validation remains the default and needs no warehouse access:

```bash
sidemantic validate ./models
sidemantic validate ./models --json
```

It checks model structure, relationship targets, explicit join keys, composite-key
arity, and cardinality declarations. Unknown model identity is allowed for isolated
models, but a relationship that needs an unknown key fails with an actionable
structural error instead of compiling an inferred `id` join.

Add `--warehouse` to inspect the configured project connection, or provide a
connection explicitly:

```bash
sidemantic validate ./models --warehouse
sidemantic validate ./models --db warehouse.duckdb
sidemantic validate ./models --connection postgres://localhost/analytics
sidemantic validate ./models --warehouse --check-keys
```

Warehouse validation checks physical table and column existence, basic semantic
type compatibility, join columns, and prepares representative compiled queries.
`--no-check-queries` skips the representative query checks. `--check-keys` also
queries primary, unique, and relationship-cardinality keys for nulls or duplicates;
it is opt-in because it can scan warehouse data.

Human output separates `Structural Errors`, `Warehouse Errors`, and `Connection
Errors`. JSON reports the same categories as `structural_errors`,
`warehouse_errors`, and `connection_errors`, with their union retained in `errors`
for compatibility. Structural failures skip warehouse checks, and connection
failures do not get mislabeled as invalid model definitions.

## Standard output formats

Structured inspection, reporting, and query commands share one format option:

```bash
sidemantic query "SELECT status, order_count FROM orders" --format table
sidemantic query "SELECT status, order_count FROM orders" --format csv
sidemantic info --format json
sidemantic preagg recommend --queries queries.sql --format jsonl
```

`--format` accepts `table`, `csv`, `json`, or `jsonl`. CSV emits a header,
JSON emits one complete value, and JSON Lines emits one object per record. The
existing `--json` spelling remains an alias for `--format json` on commands
that already offered it. Use `--plain` for stable, undecorated, tab-separated,
one-record-per-line output. Scripts should select `--plain` or an explicit
machine format instead of parsing the default human presentation.

`--quiet`/`-q` suppresses non-essential status and progress without suppressing
the requested result or errors. `--verbose`/`-v` enables detailed diagnostics;
`--debug` implies verbose diagnostics and includes unexpected tracebacks.
`--version` remains the unambiguous version option.

Color is enabled only for an interactive stream. `--no-color`, a non-empty
`NO_COLOR`, `SIDEMANTIC_NO_COLOR`, or `TERM=dumb` disables it. A non-empty
`FORCE_COLOR` overrides automatic terminal detection, but not explicit disable
controls. Progress is animated only on an interactive stderr stream and is
disabled for redirects, machine formats, plain/quiet mode, and CI.

## Environment and precedence

Public Sidemantic environment variables use the `SIDEMANTIC_` prefix:

| Variable | Meaning |
| --- | --- |
| `SIDEMANTIC_PROJECT` | Project root used for discovery |
| `SIDEMANTIC_CONFIG` | Explicit YAML or JSON config path |
| `SIDEMANTIC_FORMAT` | Default `table`, `csv`, `json`, or `jsonl` output |
| `SIDEMANTIC_PLAIN` | Enable plain output when truthy |
| `SIDEMANTIC_QUIET` | Suppress non-essential diagnostics when truthy |
| `SIDEMANTIC_VERBOSE` | Enable verbose diagnostics when truthy |
| `SIDEMANTIC_DEBUG` | Enable debug tracebacks when truthy |
| `SIDEMANTIC_NO_COLOR` | Disable Sidemantic color when truthy |
| `SIDEMANTIC_PAGER` | Pager command for long interactive reports |
| `SIDEMANTIC_PG_PASSWORD_FILE` | PostgreSQL password credential file |
| `SIDEMANTIC_API_AUTH_TOKEN_FILE` | HTTP API bearer-token credential file |

Ordinary settings use flag, environment variable, project config, then
built-in default precedence. Project config may define the same presentation
defaults under `cli`. Relative environment paths resolve from the working
directory; relative config paths resolve from the config file's directory.

Secrets use credential-file options, credential-file environment variables, or
`-` for stdin. Explicit credential-file options take precedence over
credential-file environment variables, then config credential paths. Do not
put secret values directly in environment variables.

## Standard input and output

Use `-` where a command accepts streamable SQL or a single generated file:

```bash
printf 'SELECT orders.revenue FROM orders\n' | sidemantic rewrite -
printf 'SELECT orders.revenue FROM orders\n' | sidemantic query - --output -
printf 'SELECT * FROM orders;\n' | sidemantic migrate check - --json
sidemantic generate client --output -
sidemantic convert models.yml --to sidemantic --output -
cat model.yml | sidemantic convert - --from sidemantic --to sidemantic --output -
cat model.sql | sidemantic convert - --from sidemantic --source-extension .sql --to sidemantic --output -
```

`rewrite`, `query`, and `explain` read SQL from stdin when their SQL argument is
`-`. Migration and pre-aggregation query sources do the same. Query and
single-file code generation write to stdout when output is `-`.
For formats with multiple file syntaxes, stdin conversion infers JSON and native
Sidemantic SQL when possible; use `--source-extension` to select explicitly.

Some exporters produce directories or multiple files, so they cannot stream to
stdout. `migrate generate --output -`, directory-only conversion inputs, and
multi-file/shape-dependent conversion targets fail with exit code 2 and a
specific explanation.

## Credentials

Never put a PostgreSQL server password or API bearer token directly in a command
argument. Read it from a credential file:

```bash
sidemantic server postgres \
  --username analytics \
  --password-file .secrets/pg-password

sidemantic server api \
  --auth-token-file .secrets/api-token
```

Both credential options accept `-` to read from stdin, which is useful with a
secret manager:

```bash
secret-tool lookup service sidemantic-api |
  sidemantic server api --auth-token-file -
```

Project configuration can point to the same files. Relative paths are resolved
relative to the configuration file:

```yaml
pg_server:
  username: analytics
  password_file: .secrets/pg-password
api_server:
  auth_token_file: .secrets/api-token
```

Inline `pg_server.password` and `api_server.auth_token` configuration remains
supported for deployment systems that inject configuration securely. Prefer
credential files when the project configuration is shared or checked into
source control.

The old `--password` and `--auth-token` command-line options remain accepted for
compatibility but are hidden and deprecated because command arguments may be
visible to other processes. Their values are registered for output redaction,
and deprecation messages never contain the value. The flags will remain through
the 0.x line and are eligible for removal in 1.0 after at least one documented
release cycle. New command invocations should use credential files or stdin.

## Terminal documentation and completion

Use `sidemantic help [COMMAND]...` for nested offline help, or install the
generated `sidemantic(1)` manual page from the wheel. Long human-readable
reports support TTY-aware paging with `--pager`, `--no-pager`, and
`SIDEMANTIC_PAGER`. Shell completion includes commands, semantic formats,
project model names, dashboard specs, and relevant paths.

See [terminal help, paging, and completion](terminal.md) for the complete
contract. The [CLI deprecation policy](cli-deprecations.md) documents warning
behavior, compatibility guarantees, replacements, and target removal releases.
