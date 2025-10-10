-- Inventory and sales analysis (table not in semantic layer)
SELECT
    warehouse_location,
    product_category,
    SUM(quantity_in_stock) as total_inventory,
    SUM(quantity_sold) as total_sold,
    AVG(reorder_point) as avg_reorder_point
FROM inventory
GROUP BY warehouse_location, product_category
ORDER BY total_inventory DESC;
