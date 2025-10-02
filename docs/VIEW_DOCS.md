# Viewing Sidemantic Documentation

## Quick Start - Python Server (No Installation)

The easiest way to view the docs:

```bash
cd docs
uv run --with markdown python serve.py
```

This will:
- Start a local server on http://localhost:8000
- Automatically open your browser
- Convert .qmd files to HTML on the fly

Press Ctrl+C to stop the server.

## Option 2: Install Quarto (Full Featured)

For the full Quarto experience with all features:

### Install Quarto

```bash
# macOS
brew install --cask quarto

# Or download from https://quarto.org/docs/get-started/
```

### Render and Preview

```bash
cd docs
quarto render
quarto preview
```

This generates a complete static site in `_site/`.

## Option 3: Read as Markdown

All `.qmd` files are just markdown with YAML frontmatter. You can read them directly:

```bash
# View in terminal
cat docs/index.qmd
cat docs/getting-started.qmd
cat docs/features/parameters.qmd

# Or open in your editor
code docs/
```

## Documentation Structure

```
docs/
├── index.qmd                    # Homepage
├── getting-started.qmd          # Tutorial
├── concepts/
│   └── models.qmd              # Models guide
├── features/
│   ├── parameters.qmd          # Parameters
│   └── symmetric-aggregates.qmd # Symmetric aggregates
└── examples.qmd                # Code examples
```

## What's Included

✅ **8 documentation files** covering:
- Getting started guide
- Core concepts (Models)
- Features (Parameters, Symmetric Aggregates)
- Examples with runnable code
- API patterns and best practices

✅ **112 passing tests** backing all documented features

✅ **5 runnable examples** in `examples/` directory

## Navigation

When viewing in the browser:
- **Home**: Overview and quick start
- **Getting Started**: Step-by-step tutorial
- **Models**: Understanding the core abstraction
- **Parameters**: Dynamic user input
- **Symmetric Aggregates**: Preventing double-counting
- **Examples**: Complete code samples

## Troubleshooting

### Port already in use

```bash
# Kill existing server
pkill -f "serve.py"

# Or use a different port
# Edit serve.py and change PORT = 8000 to PORT = 8001
```

### Missing markdown package

```bash
uv pip install markdown
```

### Browser doesn't open

Manually navigate to: http://localhost:8000/index.qmd
