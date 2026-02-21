# Sidemantic VS Code Extension

Language support for [Sidemantic](https://sidemantic.com) semantic layer definitions.

## Features

**For `.sidemantic.sql` files:**
- Syntax highlighting for MODEL, DIMENSION, METRIC, RELATIONSHIP, SEGMENT statements
- Autocompletion for keywords and properties
- Hover documentation
- Validation diagnostics

**For `.sidemantic.yaml` files:**
- JSON Schema validation via [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml)
- Autocompletion for all model/metric/dimension properties

## Requirements

Install the Sidemantic CLI with LSP support:

```bash
uv pip install sidemantic[lsp]
# or
pip install sidemantic[lsp]
```

For YAML support, install the [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml).

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `sidemantic.lsp.enabled` | `true` | Enable the language server |
| `sidemantic.lsp.path` | `sidemantic` | Path to sidemantic CLI |

## File Associations

- `*.sidemantic.sql` - Sidemantic SQL dialect (MODEL, METRIC, etc.)
- `*.sidemantic.yaml`, `*.sidemantic.yml`, `sidemantic.yaml` - YAML definitions

## Development

```bash
cd vscode-sidemantic
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
