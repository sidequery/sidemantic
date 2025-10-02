# Sidemantic Documentation

Comprehensive documentation for the Sidemantic semantic layer framework.

## Viewing the Documentation

### Option 1: Install Quarto and Render

```bash
# Install Quarto
brew install quarto

# Render documentation
cd docs
quarto render

# View locally
quarto preview
```

### Option 2: Read Markdown Files

All `.qmd` files are readable as markdown:

- `index.qmd` - Homepage and overview
- `getting-started.qmd` - Installation and first steps
- `concepts/models.qmd` - Core model concepts
- `features/parameters.qmd` - Parameters feature
- `features/symmetric-aggregates.qmd` - Symmetric aggregates feature
- `examples.qmd` - Code examples

## Documentation Structure

```
docs/
├── _quarto.yml           # Quarto configuration
├── index.qmd            # Homepage
├── getting-started.qmd  # Getting started guide
├── concepts/            # Core concepts
│   ├── models.qmd
│   ├── metrics.qmd
│   ├── dimensions-measures.qmd
│   └── joins.qmd
├── features/            # Feature documentation
│   ├── parameters.qmd
│   ├── symmetric-aggregates.qmd
│   ├── table-calculations.qmd
│   └── advanced-metrics.qmd
├── api/                 # API reference
│   └── index.qmd
└── examples.qmd         # Examples