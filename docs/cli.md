# CLI contract

Sidemantic is CLI-first. The command line follows one contract across command
families so it can be used interactively and safely composed in scripts.

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

Click dispatches root options before it selects a subcommand, so this release
does not reliably accept `--debug` after a subcommand.

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

## Standard input and output

Use `-` where a command accepts streamable SQL or a single generated file:

```bash
printf 'SELECT orders.revenue FROM orders\n' | sidemantic rewrite -
printf 'SELECT orders.revenue FROM orders\n' | sidemantic query - --output -
printf 'SELECT * FROM orders;\n' | sidemantic migrate check - --json
sidemantic generate client --output -
sidemantic convert models.yml --to sidemantic --output -
cat model.yml | sidemantic convert - --from sidemantic --to sidemantic --output -
```

`rewrite`, `query`, and `explain` read SQL from stdin when their SQL argument is
`-`. Migration and pre-aggregation query sources do the same. Query and
single-file code generation write to stdout when output is `-`.

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
