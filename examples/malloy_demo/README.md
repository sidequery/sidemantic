# Malloy Demo: LookML to Malloy

This demo converts LookML files into Malloy format and generates sample data for interactive exploration.

## Try it in your browser

Press `.` on this repo (or go to [github.dev](https://github.dev/sidequery/sidemantic)) to open in the web editor, then:

1. Install the [Malloy VS Code extension](https://marketplace.visualstudio.com/items?itemName=malloydata.malloy-vscode)
2. Open `examples/malloy_demo/malloy_output/thelook.malloy`
3. Click "Run" on any query

## Quick start (local)

```bash
git clone https://github.com/sidequery/sidemantic && cd sidemantic
uv run examples/malloy_demo/run_demo.py
```

Then open `malloy_output/thelook.malloy` in VS Code with the Malloy extension.

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
└── malloy_output/       # Generated (checked in for zero-setup browser demo)
    ├── thelook.malloy   # Converted Malloy model with sample queries
    └── data/*.parquet   # Sample data
```

## Generated Malloy features

The output demonstrates:

- **Sources**: Tables with measures and calculated dimensions
- **Measures**: count(), sum(), avg() with filters
- **Dimension groups**: Time dimensions from LookML `dimension_group`
- **Sample queries**: Ready-to-run queries with `run:` syntax
- **Dashboard**: Nested queries with bar charts
