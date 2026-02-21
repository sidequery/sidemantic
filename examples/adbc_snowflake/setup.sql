-- Setup script for ADBC + Snowflake demo
-- Run this in Snowflake to create test data

-- Create schema if needed
CREATE SCHEMA IF NOT EXISTS demo;

-- Create orders table
CREATE OR REPLACE TABLE demo.orders (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    status VARCHAR(50),
    region VARCHAR(50),
    amount NUMBER(10,2),
    order_date TIMESTAMP
);

-- Insert sample data
INSERT INTO demo.orders (order_id, customer_id, status, region, amount, order_date)
VALUES
    (1, 101, 'completed', 'North', 150.00, '2024-01-15 10:30:00'),
    (2, 102, 'completed', 'South', 275.50, '2024-01-15 14:22:00'),
    (3, 103, 'pending', 'East', 89.99, '2024-01-16 09:15:00'),
    (4, 101, 'completed', 'North', 432.00, '2024-01-16 16:45:00'),
    (5, 104, 'cancelled', 'West', 67.25, '2024-01-17 11:00:00'),
    (6, 105, 'completed', 'South', 299.99, '2024-01-17 13:30:00'),
    (7, 102, 'completed', 'East', 185.00, '2024-01-18 10:00:00'),
    (8, 106, 'pending', 'North', 520.00, '2024-01-18 15:20:00'),
    (9, 103, 'completed', 'West', 145.75, '2024-01-19 09:45:00'),
    (10, 107, 'completed', 'South', 88.50, '2024-01-19 14:10:00'),
    (11, 108, 'completed', 'North', 675.00, '2024-01-20 11:30:00'),
    (12, 101, 'pending', 'East', 234.99, '2024-01-20 16:00:00'),
    (13, 109, 'completed', 'West', 412.50, '2024-01-21 10:15:00'),
    (14, 110, 'cancelled', 'South', 55.00, '2024-01-21 12:45:00'),
    (15, 102, 'completed', 'North', 189.99, '2024-01-22 09:30:00'),
    (16, 111, 'completed', 'East', 333.33, '2024-01-22 14:00:00'),
    (17, 103, 'completed', 'West', 267.80, '2024-01-23 11:20:00'),
    (18, 112, 'pending', 'South', 445.00, '2024-01-23 15:45:00'),
    (19, 104, 'completed', 'North', 128.50, '2024-01-24 10:00:00'),
    (20, 113, 'completed', 'East', 599.99, '2024-01-24 13:30:00');

-- Verify data
SELECT region, status, COUNT(*) as orders, SUM(amount) as revenue
FROM demo.orders
GROUP BY region, status
ORDER BY region, status;
