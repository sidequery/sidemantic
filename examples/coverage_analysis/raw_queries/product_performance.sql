-- Product sales performance
SELECT
    category,
    brand,
    COUNT(DISTINCT product_id) as product_count,
    SUM(units_sold) as total_units,
    SUM(revenue) as total_revenue,
    AVG(price) as avg_price
FROM products
GROUP BY category, brand
HAVING SUM(revenue) > 10000
ORDER BY total_revenue DESC
LIMIT 20;
