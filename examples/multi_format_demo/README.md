# Multi-Format Semantic Layer Demo

This demo shows how sidemantic can unify metrics defined in different semantic layer formats:

- **Customers**: Defined in Cube format (`cube/customers.yml`)
- **Products**: Defined in Hex format (`hex/products.yml`)
- **Orders**: Defined in LookML format (`lookml/orders.lkml`)

The demo demonstrates querying across all three formats seamlessly, with automatic joins between models from different semantic layers.

## Structure

```
multi_format_demo/
├── README.md              # This file
├── demo.py                # Main demo script
├── generate_data.py       # Script to generate sample data
├── cube/
│   └── customers.yml      # Customer model in Cube format
├── hex/
│   └── products.yml       # Product model in Hex format
├── lookml/
│   └── orders.lkml        # Orders model in LookML format
└── data/
    └── ecommerce.db       # DuckDB database (generated)
```

## Running the Demo

1. Generate sample e-commerce data:
```bash
uv run examples/multi_format_demo/generate_data.py
```

2. Run the demo:
```bash
uv run examples/multi_format_demo/demo.py
```

## What It Shows

The demo executes five queries that demonstrate different aspects of cross-format integration:

### Query 1: Basic Order Metrics (LookML)
Simple metrics from the orders model defined in LookML:
- `orders.order_count`
- `orders.total_revenue`
- `orders.avg_order_value`

### Query 2: Revenue by Customer Region (LookML + Cube)
Joins orders (LookML) with customers (Cube) to show revenue by region:
- Uses metrics from orders
- Uses dimensions from customers
- Automatic join across formats

### Query 3: Product Performance Analysis (LookML + Hex)
Joins orders (LookML) with products (Hex) to show sales by category:
- Uses metrics from both orders and products
- Shows product category performance

### Query 4: Complete Cross-Format Analysis (All Three)
Combines all three semantic layer formats:
- Revenue and order metrics from LookML
- Customer region from Cube
- Product category from Hex
- Filters to completed orders only

### Query 5: Using Cube Segment Filter
Demonstrates using a segment defined in Cube format to filter query results:
- Uses the `north_region` segment from customers
- Shows how segments work across format boundaries

## Key Features Demonstrated

✓ **Multi-format loading**: Load semantic models from Cube, Hex, and LookML
✓ **Cross-format querying**: Query metrics and dimensions across different formats
✓ **Automatic joins**: sidemantic automatically joins models from different formats
✓ **Segments**: Use filter segments defined in one format with metrics from another
✓ **Relationship management**: Manually define relationships between models from different sources

## Technical Details

The demo shows how simple cross-format integration can be:

```python
from sidemantic import SemanticLayer, load_from_directory

# Point at a directory with mixed formats
layer = SemanticLayer(connection="duckdb:///data.db")
load_from_directory(layer, "semantic_models/")

# That's it! Query across all formats
layer.query(
    metrics=["orders.total_revenue"],
    dimensions=["customers.region"]
)
```

### How it works

`load_from_directory()` automatically:

1. **Discovers all semantic layer files** (.lkml, .yml, .yaml)
2. **Detects the format** (Cube, Hex, LookML, MetricFlow, etc.)
3. **Parses with the right adapter**
4. **Infers relationships** based on foreign key naming conventions:
   - `orders.customer_id` → `customers.id` (many-to-one)
   - `orders.product_id` → `products.id` (many-to-one)
   - Reverse relationships automatically added
5. **Builds the join graph** for seamless cross-format queries

No manual relationship definitions, no adapter instantiation, no graph building - just point and query.

## Sample Data

The demo uses a simple e-commerce schema:

- **customers**: 8 customers across 4 regions
- **products**: 8 products in 3 categories (Electronics, Furniture, Office Supplies)
- **orders**: 22 orders with various statuses (completed, pending, cancelled)

## Notes

- This demo requires DuckDB (handled automatically by sidemantic)
- The data generation is deterministic (uses fixed random seed)
- Relationships are automatically inferred from foreign key naming conventions (`*_id` pattern)
- If auto-detection doesn't work, relationships can still be added manually
