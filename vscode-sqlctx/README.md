# sqlctx VS Code Extension

Language support for [sqlctx](https://github.com/sidequery/sqlctx) semantic layer definitions.

## Features

**For `.sqlctx.sql` files:**
- Syntax highlighting for MODEL, DIMENSION, METRIC, RELATIONSHIP, SEGMENT statements
- Autocompletion for keywords and properties
- Hover documentation
- Validation diagnostics

**For `.sqlctx.yaml` files:**
- JSON Schema validation via [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml)
- Autocompletion for all model/metric/dimension properties

**For `.sqlctx.py` and `sqlctx.py` files:**
- Autocompletion for `Model`, `Dimension`, `Metric`, `Relationship`, and `Segment` keyword arguments
- Hover documentation for constructors and keyword args
- Go-to-definition, references, rename, symbols, and signature help on definition names

## Requirements

Install the sqlctx CLI with LSP support:

```bash
uv pip install 'sqlctx[lsp] @ git+https://github.com/sidequery/sqlctx'
# or
pip install 'sqlctx[lsp] @ git+https://github.com/sidequery/sqlctx'
```

For YAML support, install the [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml).

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `sqlctx.lsp.enabled` | `true` | Enable the language server |
| `sqlctx.lsp.path` | `sqlctx` | Path to sqlctx CLI |
| `sqlctx.lsp.python.enabled` | `true` | Enable language server support for sqlctx Python definition files |

## File Associations

- `*.sqlctx.sql` - sqlctx SQL dialect (MODEL, METRIC, etc.)
- `*.sqlctx.yaml`, `*.sqlctx.yml`, `sqlctx.yaml` - YAML definitions
- `*.sqlctx.py`, `sqlctx.py` - Python definitions

## Development

```bash
cd vscode-sqlctx
bun install
bun run compile
bun run test
```

To test locally, press F5 in VS Code to launch the Extension Development Host.

## Publishing

```bash
bun run package  # Creates .vsix file
vsce publish     # Publish to marketplace (requires publisher token)
```
