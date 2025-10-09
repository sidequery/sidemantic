-- Cancelled orders analysis
SELECT
    cancellation_reason,
    COUNT(*) as cancelled_count,
    SUM(total_amount) as lost_revenue,
    AVG(total_amount) as avg_order_value
FROM orders
WHERE status = 'cancelled'
GROUP BY cancellation_reason
ORDER BY cancelled_count DESC;
