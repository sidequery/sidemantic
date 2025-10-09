-- Customer demographics analysis
SELECT
    region,
    age_group,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_lifetime_value
FROM customers
GROUP BY region, age_group
ORDER BY customer_count DESC;
