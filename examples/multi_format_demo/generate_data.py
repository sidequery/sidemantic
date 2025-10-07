#!/usr/bin/env python3
"""
Generate sample e-commerce data for the multi-format demo.

This script creates three tables:
- customers: Customer information
- products: Product catalog
- orders: Customer orders with product details
"""

import random
from datetime import datetime, timedelta

import duckdb

# Create database connection
con = duckdb.connect("examples/multi_format_demo/data/ecommerce.db")

# Create customers table
con.execute("""
    CREATE OR REPLACE TABLE customers (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        email VARCHAR,
        region VARCHAR,
        signup_date DATE
    )
""")

# Generate customer data
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

con.executemany(
    "INSERT INTO customers VALUES (?, ?, ?, ?, ?)",
    customers,
)

# Create products table
con.execute("""
    CREATE OR REPLACE TABLE products (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        category VARCHAR,
        price DECIMAL(10,2),
        cost DECIMAL(10,2)
    )
""")

# Generate product data
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

con.executemany(
    "INSERT INTO products VALUES (?, ?, ?, ?, ?)",
    products,
)

# Create orders table
con.execute("""
    CREATE OR REPLACE TABLE orders (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        amount DECIMAL(10,2),
        status VARCHAR,
        created_at TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customers(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
""")

# Generate order data with better timeseries distribution
random.seed(42)
orders = []
order_id = 1
statuses = ["completed", "pending", "cancelled", "completed", "completed"]  # Weight towards completed

# Generate orders over the past year with increasing volume
start_date = datetime.now() - timedelta(days=365)

# Create ~200 orders distributed over the year
for day in range(0, 365, 2):  # Every other day
    order_date = start_date + timedelta(days=day)

    # Simulate varying order volume (1-3 orders per day, higher towards recent months)
    volume_multiplier = 1 + (day / 365)  # Increases from 1 to 2 over the year
    num_orders_today = int(random.randint(1, 3) * volume_multiplier)

    for _ in range(num_orders_today):
        customer_id = random.randint(1, 8)
        product_id = random.randint(1, 8)
        quantity = random.randint(1, 3)
        product_price = [p[3] for p in products if p[0] == product_id][0]
        amount = float(product_price) * quantity
        status = random.choice(statuses)

        # Add some time variation within the day
        hour = random.randint(8, 20)
        minute = random.randint(0, 59)
        created_at = order_date.replace(hour=hour, minute=minute)

        orders.append((
            order_id,
            customer_id,
            product_id,
            quantity,
            amount,
            status,
            created_at,
        ))
        order_id += 1

con.executemany(
    "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)",
    orders,
)

# Show summary
print("Data generated successfully!\n")
print("Customers:", con.execute("SELECT COUNT(*) FROM customers").fetchone()[0])
print("Products:", con.execute("SELECT COUNT(*) FROM products").fetchone()[0])
print("Orders:", con.execute("SELECT COUNT(*) FROM orders").fetchone()[0])
print("\nOrder status breakdown:")
print(con.execute("SELECT status, COUNT(*) as count FROM orders GROUP BY status ORDER BY count DESC").df())

con.close()
