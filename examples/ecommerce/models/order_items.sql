-- Pure SQL semantic model definition
-- Order line items with product details, quantities, and discounts

MODEL (
    name order_items,
    table order_items,
    primary_key order_item_id,
    description 'Individual line items within orders'
);

-- Relationships
RELATIONSHIP (
    name orders,
    type many_to_one,
    foreign_key order_id
);

RELATIONSHIP (
    name products,
    type many_to_one,
    foreign_key product_id
);

-- Dimensions
DIMENSION (
    name discount_applied,
    type categorical,
    sql discount_amount > 0,
    description 'Whether a discount was applied'
);

-- Metrics
METRIC (
    name item_count,
    agg count,
    description 'Total number of order items'
);

METRIC (
    name quantity_sold,
    agg sum,
    sql quantity,
    description 'Total quantity of items sold'
);

METRIC (
    name gross_revenue,
    agg sum,
    sql price * quantity,
    description 'Gross revenue before discounts',
    format '$#,##0.00'
);

METRIC (
    name discount_amount_total,
    agg sum,
    sql discount_amount,
    description 'Total discount amount',
    format '$#,##0.00'
);

METRIC (
    name net_revenue,
    agg sum,
    sql (price * quantity) - discount_amount,
    description 'Net revenue after discounts',
    format '$#,##0.00'
);

METRIC (
    name avg_quantity_per_item,
    agg avg,
    sql quantity,
    description 'Average quantity per line item'
);

METRIC (
    name discounted_items,
    agg count,
    filters discount_amount > 0,
    description 'Number of items with discounts'
);

-- Segments
SEGMENT (
    name high_value_items,
    expression (price * quantity) - discount_amount > 100,
    description 'Line items with net value over $100'
);

SEGMENT (
    name discounted,
    expression discount_amount > 0,
    description 'Items with any discount applied'
);
