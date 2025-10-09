#!/usr/bin/env python
# /// script
# dependencies = ["duckdb"]
# ///
"""Create sample ecommerce database with realistic data.

Run with: uv run examples/ecommerce/data/create_db.py
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

# Configuration
NUM_CUSTOMERS = 200
NUM_PRODUCTS = 100
NUM_ORDERS = 500
MAX_ITEMS_PER_ORDER = 5

# Random data
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU", "JP"]
US_STATES = ["CA", "NY", "TX", "FL", "IL", "PA", "OH"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio"]
TIERS = ["bronze", "silver", "gold", "platinum"]
CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys"]
SUBCATEGORIES = {
    "Electronics": ["Phones", "Laptops", "Tablets", "Headphones"],
    "Clothing": ["Shirts", "Pants", "Dresses", "Shoes"],
    "Home & Garden": ["Furniture", "Kitchen", "Bedding", "Garden Tools"],
    "Sports": ["Fitness", "Outdoor", "Team Sports", "Water Sports"],
    "Books": ["Fiction", "Non-Fiction", "Children", "Reference"],
    "Toys": ["Action Figures", "Dolls", "Board Games", "Educational"],
}
BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "BrandF"]
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]


def create_database():
    """Create and populate the ecommerce database."""
    db_path = Path(__file__).parent / "ecommerce.db"
    conn = duckdb.connect(str(db_path))

    # Create customers table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            email VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            country VARCHAR,
            state VARCHAR,
            city VARCHAR,
            tier VARCHAR,
            created_at TIMESTAMP,
            is_active BOOLEAN
        )
    """)

    # Generate customers
    print(f"Generating {NUM_CUSTOMERS} customers...")
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        country = random.choice(COUNTRIES)
        state = random.choice(US_STATES) if country == "US" else None
        created_at = datetime.now() - timedelta(days=random.randint(1, 730))
        is_active = random.random() > 0.3  # 70% active

        customers.append(
            (
                i,
                f"customer{i}@example.com",
                f"First{i}",
                f"Last{i}",
                country,
                state,
                random.choice(CITIES),
                random.choice(TIERS),
                created_at,
                is_active,
            )
        )

    conn.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", customers)

    # Create products table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            brand VARCHAR,
            price DECIMAL(10, 2),
            is_active BOOLEAN
        )
    """)

    # Generate products
    print(f"Generating {NUM_PRODUCTS} products...")
    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        category = random.choice(CATEGORIES)
        subcategory = random.choice(SUBCATEGORIES[category])
        price = round(random.uniform(10, 1000), 2)
        is_active = random.random() > 0.2  # 80% active

        products.append((i, f"Product {i}", category, subcategory, random.choice(BRANDS), price, is_active))

    conn.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?)", products)

    # Create orders table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            status VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            total_amount DECIMAL(10, 2),
            is_first_order BOOLEAN,
            payment_method VARCHAR
        )
    """)

    # Track customer order counts for is_first_order
    customer_orders = {}

    # Generate orders
    print(f"Generating {NUM_ORDERS} orders...")
    orders = []
    for i in range(1, NUM_ORDERS + 1):
        customer_id = random.randint(1, NUM_CUSTOMERS)
        is_first_order = customer_orders.get(customer_id, 0) == 0
        customer_orders[customer_id] = customer_orders.get(customer_id, 0) + 1

        created_at = datetime.now() - timedelta(days=random.randint(1, 365))
        updated_at = created_at + timedelta(days=random.randint(0, 10))
        status = random.choice(ORDER_STATUSES)

        # Calculate total_amount (will be updated after creating order_items)
        orders.append(
            (
                i,
                customer_id,
                status,
                created_at,
                updated_at,
                0.0,  # Placeholder, will update
                is_first_order,
                random.choice(PAYMENT_METHODS),
            )
        )

    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", orders)

    # Create order_items table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price DECIMAL(10, 2),
            discount_amount DECIMAL(10, 2)
        )
    """)

    # Generate order items
    print("Generating order items...")
    order_items = []
    order_item_id = 1
    order_totals = {}

    for order_id in range(1, NUM_ORDERS + 1):
        num_items = random.randint(1, MAX_ITEMS_PER_ORDER)
        order_total = 0

        for _ in range(num_items):
            product_id = random.randint(1, NUM_PRODUCTS)
            # Get product price
            price = float(conn.execute(f"SELECT price FROM products WHERE product_id = {product_id}").fetchone()[0])
            quantity = random.randint(1, 5)
            discount_amount = round(random.uniform(0, price * 0.3), 2) if random.random() > 0.7 else 0

            item_total = (price * quantity) - discount_amount
            order_total += item_total

            order_items.append((order_item_id, order_id, product_id, quantity, price, discount_amount))
            order_item_id += 1

        order_totals[order_id] = round(order_total, 2)

    conn.executemany("INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?)", order_items)

    # Update order totals
    print("Updating order totals...")
    for order_id, total in order_totals.items():
        conn.execute(f"UPDATE orders SET total_amount = {total} WHERE order_id = {order_id}")

    # Print summary
    print("\n" + "=" * 60)
    print("Database created successfully!")
    print("=" * 60)
    print(f"Location: {db_path}")
    print(f"Customers: {conn.execute('SELECT COUNT(*) FROM customers').fetchone()[0]}")
    print(f"Products: {conn.execute('SELECT COUNT(*) FROM products').fetchone()[0]}")
    print(f"Orders: {conn.execute('SELECT COUNT(*) FROM orders').fetchone()[0]}")
    print(f"Order Items: {conn.execute('SELECT COUNT(*) FROM order_items').fetchone()[0]}")
    print(f"Total Revenue: ${conn.execute('SELECT SUM(total_amount) FROM orders').fetchone()[0]:,.2f}")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    create_database()
