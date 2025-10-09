-- Customer order patterns (cross-model query)
SELECT
    c.region,
    c.customer_segment,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.region, c.customer_segment
ORDER BY total_spent DESC;
