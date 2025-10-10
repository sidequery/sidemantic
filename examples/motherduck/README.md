# MotherDuck Example

This example demonstrates using Sidemantic with MotherDuck, a cloud-based DuckDB service.

## Features

- Connect to MotherDuck cloud database
- Define semantic models using YAML configuration
- Create and refresh pre-aggregations for fast queries
- Query with automatic pre-aggregation routing
- Data persists in the cloud

## Prerequisites

Set your MotherDuck token:
```bash
export MOTHERDUCK_TOKEN=your_token_here
```

Sign up at [motherduck.com](https://motherduck.com) to get a token.

## Setup

1. **Create sample data** (10,000 orders, 20 customers):
   ```bash
   uv run python setup_data.py
   ```

2. **Refresh pre-aggregations**:
   ```bash
   uv run python refresh_preaggs.py
   ```

3. **Run query examples**:
   ```bash
   uv run python query_examples.py
   ```

## Configuration

The semantic layer is defined in `sidemantic.yaml`:

- **Connection**: MotherDuck (`md:sidemantic_demo`)
- **Models**: orders, customers
- **Pre-aggregations**: daily_status, monthly_summary
- **Metrics**: revenue, order_count, avg_order_value
- **Dimensions**: status, order_date, region, tier

## Pre-Aggregations

Two pre-aggregations speed up common queries:

- **daily_status**: Orders aggregated by day and status
- **monthly_summary**: Orders aggregated by month and status

Pre-aggregations are automatically used when queries match their definition.

## Queries

The example includes queries demonstrating:

1. Revenue by status (uses pre-aggregation)
2. Revenue by month (uses pre-aggregation)
3. Daily trends with filters
4. Cross-model queries (joins)
5. SQL rewriting with `sql()` method

## Database Structure

```
sidemantic_demo/
├── analytics/
│   ├── orders (10,000 rows)
│   └── customers (20 rows)
└── preagg/
    ├── orders_daily_status
    └── orders_monthly_summary
```

## Benefits

- Queries run in the cloud
- Data persists between sessions
- Pre-aggregations make queries much faster
- Share database with your team
