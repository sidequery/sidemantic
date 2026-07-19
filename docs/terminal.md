# Terminal help, paging, and completion

The installed CLI is its own offline documentation. Use `sidemantic help` for
the command index and pass a nested path for a specific workflow:

```console
sidemantic help validate
sidemantic help migrate generate
sidemantic help preagg recommend
```

`-h` and `--help` work at the same command levels. The generated
`sidemantic(1)` page is installed under `share/man/man1` by wheels, so standard
environment managers can expose it through `man sidemantic`. The checked-in
copy is generated from the live Click command tree:

```console
uv run python scripts/generate_man_page.py
```

A behavioral test fails when the generated page is stale.

## Paging

Long human-readable `info`, `explain`, `validate`, and pre-aggregation reports
use a pager only when stdout is an interactive terminal and the report exceeds
the terminal height. `--pager` requests paging for any non-empty human report;
`--no-pager` disables it. `SIDEMANTIC_PAGER` takes precedence over `PAGER`; if
neither is set, the platform default is used.

Paging is always disabled for redirected output, `TERM=dumb`, `--quiet`,
`--plain`, and the `csv`, `json`, or `jsonl` formats. It never changes the bytes
written to redirected stdout. Color follows the normal CLI color policy.

## Shell completion

Typer can install completion for Bash, Zsh, Fish, and PowerShell:

```console
sidemantic --install-completion
sidemantic --show-completion zsh
```

Completion covers command and option names plus relevant values:

- `convert --from` and `convert --to` use the live semantic-format registry;
- `preagg refresh --model` reads model names from the selected project;
- dashboard spec arguments complete YAML and JSON files;
- model, query, output, and source paths use shell-native file completion.

Project-aware completion is best effort. An incomplete or invalid project
returns no dynamic model candidates and never prints a traceback into the
shell's completion stream.
