# CLI deprecation policy

Sidemantic keeps scripts working while the CLI evolves. A legacy command or
flag remains functional for at least two minor releases after its deprecation is
announced. Its warning names the supported replacement and the first release in
which removal may occur.

Warnings are human-facing diagnostics. They are written to stderr, never
stdout, and are suppressed by `--quiet`, `--plain`, and machine output formats
(`csv`, `json`, and `jsonl`). Machine-readable stdout therefore remains valid
and uncontaminated. Help and shell-completion discovery do not warn.

During the compatibility window, the legacy spelling keeps its documented
behavior, exit codes, and output schema. A security issue may require a faster
removal, but the release notes will call out the exception and provide a safe
migration path. Once the target release is reached, removal may happen in that
release or any later release; it is not postponed silently to an earlier
release.

## Current lifecycle

| Legacy surface | Replacement | Deprecated | Earliest removal |
| --- | --- | --- | --- |
| `sidemantic gen` | `sidemantic generate` | 0.10.0 | 0.12.0 |
| `sidemantic migrator` | `sidemantic migrate generate` or `migrate check` | 0.10.0 | 0.12.0 |
| `sidemantic export-native` | `sidemantic convert --to sidemantic` | 0.10.0 | 0.12.0 |
| `sidemantic explain-sql` | `sidemantic explain` | 0.10.0 | 0.12.0 |
| `sidemantic api-serve` | `sidemantic server api` | 0.10.0 | 0.12.0 |
| `sidemantic mcp-serve` | `sidemantic server mcp` | 0.10.0 | 0.12.0 |
| `sidemantic tree` | `sidemantic workbench` | 0.10.0 | 0.12.0 |
| `server postgres --password` | `server postgres --password-file` | 0.10.2 | 1.0.0 |
| `server api --auth-token` | `server api --auth-token-file` | 0.10.2 | 1.0.0 |
| `dashboard serve --output-dir` | Remove the ignored flag | 0.10.0 | 0.12.0 |
| `dashboard serve --warm-interaction-preaggregations` | Remove the ignored flag | 0.10.0 | 0.12.0 |

`sidemantic serve` was briefly (0.10.0-0.10.2) a hidden deprecated alias for
`sidemantic server postgres`. As of 0.11.0 it is the unified serving command
(web UI + HTTP API + MCP on one port); scripts that relied on the alias must
use `sidemantic server postgres` explicitly.

The registry in `sidemantic/cli_polish.py` is the executable source of truth.
Tests require every entry to specify its replacement, introduction release, and
target removal release. Update this table, the registry, release notes, and the
bundled plugin skills together whenever a lifecycle changes.

## Recovery behavior

Sidemantic may suggest a close command name, corrected option spelling, or a
safe next action. It never silently executes a suggested command and never adds
`--force` or another mutating option on the user's behalf. Review a suggestion
with `sidemantic help <command>` before running it.
