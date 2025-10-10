-- High value orders analysis
SELECT
    status,
    payment_method,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_order_value,
    MAX(total_amount) as max_order_value
FROM orders
WHERE total_amount > 500
GROUP BY status, payment_method
ORDER BY avg_order_value DESC;
