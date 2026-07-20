"""Project scaffolding for `sidemantic init` and `sidemantic demo`.

Everything here ships inside the wheel so a bare `uvx sidemantic demo` works on
a machine that has never seen the repository.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

STARTER_CONFIG = """# Sidemantic project configuration.
# Docs: https://sidemantic.com/sidemantic/cli
models_dir: models

# Point at your data when you outgrow the starter model:
# connection:
#   type: duckdb
#   path: data/warehouse.duckdb
# connection:
#   type: files
#   paths:
#     - data/*.parquet
"""

STARTER_MODEL = """models:
  - name: orders
    # Inline sample rows so the project queries with zero database setup.
    # Replace `sql` with `table: your_schema.orders` to use real data.
    sql: |
      select * from (values
        (1, 'paid',    120.00),
        (2, 'paid',     80.00),
        (3, 'pending',  50.00)
      ) as t(id, status, amount)
    primary_key: id
    dimensions:
      - name: status
        type: categorical
        sql: status
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
"""

STARTER_TEST = """# Golden-query assertions run by `sidemantic test`.
tests:
  - name: starter revenue stays constant
    sql: SELECT orders.revenue FROM orders
    expect:
      value: 250.0
"""

DEMO_CONFIG = """# Sidemantic demo project.
models_dir: models
connection:
  type: duckdb
  path: data/demo.duckdb
"""

DEMO_MODEL_FILES = {
    "customers.yml": """models:
  - name: customers
    table: customers
    description: Demo customer accounts
    primary_key: id
    dimensions:
      - name: region
        type: categorical
      - name: signup_date
        type: time
        granularity: day
    metrics:
      - name: customer_count
        agg: count
""",
    "products.yml": """models:
  - name: products
    table: products
    description: Demo product catalog
    primary_key: id
    dimensions:
      - name: category
        type: categorical
    metrics:
      - name: product_count
        agg: count
      - name: avg_price
        agg: avg
        sql: price
""",
    "orders.yml": """models:
  - name: orders
    table: orders
    description: Demo order transactions
    primary_key: id
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
      - name: products
        type: many_to_one
        foreign_key: product_id
    dimensions:
      - name: status
        type: categorical
      - name: created_at
        type: time
        granularity: day
    metrics:
      - name: order_count
        agg: count
      - name: revenue
        agg: sum
        sql: amount
      - name: completed_revenue
        agg: sum
        sql: amount
        filters:
          - "status = 'completed'"
""",
}

DEMO_TEST_FILE = """# Golden-query assertions run by `sidemantic test`.
tests:
  - name: demo data loads
    sql: SELECT orders.order_count FROM orders
    expect:
      row_count: 1
  - name: revenue splits by status
    sql: SELECT orders.status, orders.revenue FROM orders ORDER BY orders.status
    expect:
      row_count: 3
"""


def create_demo_database(path: Path) -> None:
    """Materialize the deterministic demo e-commerce dataset as a DuckDB file."""

    import duckdb

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR, email VARCHAR, region VARCHAR, signup_date DATE)"
        )
        customers = [
            (1, "Alice Johnson", "alice@example.com", "North", "2023-01-15"),
            (2, "Bob Smith", "bob@example.com", "South", "2023-02-20"),
            (3, "Carol Davis", "carol@example.com", "East", "2023-03-10"),
            (4, "David Wilson", "david@example.com", "West", "2023-04-05"),
            (5, "Eve Martinez", "eve@example.com", "North", "2023-05-18"),
            (6, "Frank Brown", "frank@example.com", "South", "2023-06-22"),
            (7, "Grace Lee", "grace@example.com", "East", "2023-07-30"),
            (8, "Henry Taylor", "henry@example.com", "West", "2023-08-14"),
        ]
        con.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?)", customers)

        con.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR, category VARCHAR, price DECIMAL(10,2), cost DECIMAL(10,2))"
        )
        products = [
            (1, "Laptop Pro", "Electronics", 1299.99, 800.00),
            (2, "Wireless Mouse", "Electronics", 29.99, 15.00),
            (3, "Desk Chair", "Furniture", 249.99, 120.00),
            (4, "Standing Desk", "Furniture", 599.99, 350.00),
            (5, "Notebook Set", "Office Supplies", 12.99, 5.00),
            (6, "Pen Pack", "Office Supplies", 8.99, 3.00),
            (7, "Monitor 27", "Electronics", 399.99, 250.00),
            (8, "Keyboard Mechanical", "Electronics", 149.99, 80.00),
        ]
        con.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?)", products)

        con.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER,"
            " quantity INTEGER, amount DECIMAL(10,2), status VARCHAR, created_at TIMESTAMP)"
        )
        rng = random.Random(42)
        statuses = ["completed", "pending", "cancelled", "completed", "completed"]
        start_date = datetime.now() - timedelta(days=365)
        orders = []
        order_id = 1
        for day in range(0, 365, 2):
            order_date = start_date + timedelta(days=day)
            volume_multiplier = 1 + (day / 365)
            for _ in range(int(rng.randint(1, 3) * volume_multiplier)):
                customer_id = rng.randint(1, 8)
                product_id = rng.randint(1, 8)
                quantity = rng.randint(1, 3)
                amount = float(products[product_id - 1][3]) * quantity
                created_at = order_date.replace(hour=rng.randint(8, 20), minute=rng.randint(0, 59))
                orders.append((order_id, customer_id, product_id, quantity, amount, rng.choice(statuses), created_at))
                order_id += 1
        con.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", orders)
    finally:
        con.close()


@dataclass
class ScaffoldResult:
    """Files a scaffold produced, for user-facing reporting."""

    root: Path
    created: list[Path] = field(default_factory=list)


def _write_files(root: Path, files: dict[str, str], *, force: bool) -> ScaffoldResult:
    conflicts = [name for name in files if (root / name).exists()]
    if conflicts and not force:
        listing = ", ".join(sorted(conflicts))
        raise FileExistsError(f"Refusing to overwrite existing files in {root}: {listing} (pass --force to replace)")
    result = ScaffoldResult(root=root)
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        result.created.append(target)
    return result


def scaffold_starter_project(directory: Path, *, force: bool = False) -> ScaffoldResult:
    """Write a minimal, immediately queryable project (no database needed)."""

    files = {
        "sidemantic.yaml": STARTER_CONFIG,
        "models/orders.yml": STARTER_MODEL,
        "tests/orders.yml": STARTER_TEST,
    }
    return _write_files(directory, files, force=force)


def scaffold_demo_project(directory: Path, *, force: bool = False) -> ScaffoldResult:
    """Write the full demo project: models, golden tests, and a DuckDB database."""

    files = {"sidemantic.yaml": DEMO_CONFIG, "tests/demo.yml": DEMO_TEST_FILE}
    files.update({f"models/{name}": content for name, content in DEMO_MODEL_FILES.items()})
    db_path = directory / "data" / "demo.duckdb"
    if db_path.exists() and not force:
        raise FileExistsError(
            f"Refusing to overwrite existing files in {directory}: data/demo.duckdb (pass --force to replace)"
        )
    result = _write_files(directory, files, force=force)
    create_demo_database(db_path)
    result.created.append(db_path)
    return result
