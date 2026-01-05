# Malloy Demo: LookML to Malloy

This demo converts LookML files into Malloy format and generates sample data for interactive exploration.

## Quick start

```bash
git clone https://github.com/sidequery/sidemantic && cd sidemantic
uv run examples/malloy_demo/run_demo.py
```

Then open the generated `malloy_output/thelook.malloy` in VS Code with the Malloy extension.

## What it demonstrates

- **LookML parsing**: Reads LookML view definitions (dimensions, measures, dimension_groups)
- **Format conversion**: Uses sidemantic to convert LookML to Malloy sources
- **Sample data generation**: Creates Parquet files with realistic e-commerce data

## What happens

1. Parses 3 LookML views (products, customers, orders)
2. Converts to Malloy format using sidemantic
3. Generates sample Parquet data (200 customers, 40 products, 5000 orders)
4. Adds sample queries and a dashboard example

## Files

```
malloy_demo/
├── run_demo.py          # Main script
├── lookml_input/        # Source LookML files
│   ├── products.lkml
│   ├── customers.lkml
│   └── orders.lkml
└── malloy_output/       # Generated
    ├── thelook.malloy   # Converted Malloy model with sample queries
    └── data/*.parquet   # Sample data (gitignored, regenerate with run_demo.py)
```

## Generated Malloy features

The output demonstrates:

- **Sources**: Tables with measures and calculated dimensions
- **Measures**: count(), sum(), avg() with filters
- **Dimension groups**: Time dimensions from LookML `dimension_group`
- **Sample queries**: Ready-to-run queries with `run:` syntax
- **Dashboard**: Nested queries with bar charts
