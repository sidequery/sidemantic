-- Pure SQL semantic model definition
-- Sidemantic supports defining models entirely in SQL using
-- MODEL(), DIMENSION(), RELATIONSHIP(), METRIC(), and SEGMENT() syntax

MODEL (
    name orders,
    table public.orders,
    primary_key order_id,
    description 'Customer orders'
);

-- Dimensions
DIMENSION (
    name order_id,
    type primary_key,
    sql order_id
);

DIMENSION (
    name customer_id,
    type foreign_key,
    sql customer_id
);

DIMENSION (
    name status,
    type categorical,
    sql status,
    description 'Order status'
);

DIMENSION (
    name order_date,
    type time,
    sql created_at,
    granularity day
);

-- Relationships
RELATIONSHIP (
    name customer,
    type many_to_one,
    foreign_key customer_id
);

-- Metrics
METRIC (
    name revenue,
    agg sum,
    sql amount,
    description 'Total order amount',
    format '$#,##0.00'
);

METRIC (
    name order_count,
    agg count,
    description 'Number of orders'
);

METRIC (
    name completed_revenue,
    agg sum,
    sql amount,
    filters status = 'completed',
    description 'Revenue from completed orders only'
);

METRIC (
    name avg_order_value,
    agg avg,
    sql amount,
    non_additive_dimension time
);

-- Segments
SEGMENT (
    name completed,
    expression status = 'completed',
    description 'Completed orders only'
);

SEGMENT (
    name high_value,
    expression amount > 100,
    description 'Orders over $100'
);

SEGMENT (
    name recent,
    expression created_at >= CURRENT_DATE - 30,
    description 'Orders from last 30 days'
);
