-- Total revenue by order status
SELECT
    status,
    SUM(total_amount) as total_revenue,
    COUNT(*) as order_count
FROM orders
GROUP BY status
ORDER BY total_revenue DESC;
