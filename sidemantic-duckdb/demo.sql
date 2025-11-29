-- Sidemantic DuckDB Extension Demo
-- Run with: ./build/release/duckdb < demo.sql

-- Load the extension
LOAD 'build/release/extension/sidemantic/sidemantic.duckdb_extension';

-- ============================================
-- Create sample data
-- ============================================
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    status VARCHAR,
    amount DECIMAL(10,2),
    order_date DATE
);

INSERT INTO orders VALUES
    (1, 100, 'completed', 150.00, '2024-01-15'),
    (2, 101, 'completed', 75.50, '2024-01-16'),
    (3, 100, 'pending', 200.00, '2024-01-17'),
    (4, 102, 'completed', 50.00, '2024-01-18'),
    (5, 101, 'cancelled', 125.00, '2024-01-19');

-- ============================================
-- Define semantic model (old property syntax)
-- ============================================
SEMANTIC CREATE MODEL orders (
    name orders,
    table orders,
    primary_key order_id
);

-- ============================================
-- Add metrics and dimensions (CREATE syntax)
-- ============================================
SEMANTIC CREATE METRIC revenue AS SUM(amount);
SEMANTIC CREATE METRIC order_count AS COUNT(*);
SEMANTIC CREATE METRIC avg_order_value AS AVG(amount);

SEMANTIC CREATE DIMENSION status AS status;
SEMANTIC CREATE DIMENSION order_date AS order_date;

-- ============================================
-- Query using semantic layer
-- ============================================

-- Total revenue
SELECT '--- Total Revenue ---';
SEMANTIC SELECT orders.revenue FROM orders;

-- Order count
SELECT '--- Order Count ---';
SEMANTIC SELECT orders.order_count FROM orders;

-- Revenue by status
SELECT '--- Revenue by Status ---';
SEMANTIC SELECT orders.status, orders.revenue FROM orders;

-- Multiple metrics with dimension
SELECT '--- Full Report by Status ---';
SEMANTIC SELECT
    orders.status,
    orders.revenue,
    orders.order_count,
    orders.avg_order_value
FROM orders;

-- ============================================
-- Check what's defined
-- ============================================
SELECT '--- Loaded Models ---';
SELECT * FROM sidemantic_models();

-- See the rewritten SQL
SELECT '--- Rewritten SQL Example ---';
SELECT sidemantic_rewrite_sql('SELECT orders.revenue, orders.status FROM orders');
