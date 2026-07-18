# Command-line interface

Sidemantic is project-first. Run commands from a project root and use path or connection options
only to override discovery.

## Project conventions

```text
analytics/
├── sidemantic.yaml       # optional project configuration
├── models/               # semantic YAML, SQL, and imported model formats
├── queries/              # SQL used by migration commands
├── dashboard.yml         # declarative official-UI dashboard
└── data/
    └── warehouse.duckdb  # one .db or .duckdb file is discovered automatically
```

Sidemantic searches upward for `sidemantic.yaml`, `sidemantic.yml`, or `sidemantic.json`. Without a
config file it discovers the nearest conventional project root. A project may use
`dashboard.yaml` or `dashboard.json` instead of `dashboard.yml`.

Discovery rejects ambiguous dashboard specs or multiple database files. Resolve ambiguity with an
explicit spec, `--db`, or `--connection`. To select a config outside the discovered project, put the
global option before the command:

```bash
sidemantic --config path/to/sidemantic.yaml validate
```

## Everyday workflow

```bash
cd analytics
sidemantic validate
sidemantic info
sidemantic query "SELECT orders.status, orders.revenue FROM orders"
sidemantic dashboard validate
sidemantic dashboard serve
```

`dashboard serve` loads the declarative spec into the official React application. The experimental
cross-library renderers remain Python library examples; they are not a second CLI dashboard.

## Command families

| Command | Purpose |
| --- | --- |
| `validate [MODELS]` | Validate discovered or explicit semantic definitions |
| `info [MODELS]` | Summarize models, dimensions, metrics, and relationships |
| `query SQL` | Execute semantic SQL and emit CSV |
| `rewrite SQL` | Compile semantic SQL without executing it |
| `explain SQL` | Explain rewrite planning as JSON |
| `dashboard validate [SPEC]` | Validate the discovered or explicit dashboard |
| `dashboard serve [SPEC]` | Run the official dashboard application |
| `dashboard types` | Generate dashboard-authoring TypeScript definitions |
| `server api` | Run the HTTP/Arrow API and general explorer UI |
| `server postgres` | Run the PostgreSQL wire-protocol server |
| `server mcp` | Run the MCP server |
| `generate client` | Generate a typed TypeScript query-client schema |
| `generate sql [SOURCES]` | Generate bindings for semantic SQL literals |
| `migrate generate [QUERIES]` | Generate models and rewritten SQL |
| `migrate check [QUERIES]` | Check existing-model query coverage |
| `convert [SOURCE]` | Convert semantic formats through the shared registry |
| `preagg recommend` | Recommend pre-aggregations from query history |
| `preagg apply` | Add recommendations to model files |
| `preagg refresh` | Refresh materialized pre-aggregations |
| `workbench [MODELS]` | Open the terminal workbench |
| `lsp` | Run the semantic SQL language server |

Use `sidemantic COMMAND --help` for command-specific overrides. Common overrides are `--models`,
`--db`, and `--connection`; they do not replace the default project-root workflow.

## Compatibility names

Older releases exposed flat or differently grouped names such as `api-serve`, `serve`, `mcp-serve`,
`gen`, `migrator`, `export-native`, and `explain-sql`. They are compatibility aliases where still
available. New documentation and automation should use `server`, `generate`, `migrate`, `convert`,
and `explain`.
