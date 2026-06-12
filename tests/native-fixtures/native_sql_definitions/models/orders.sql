MODEL (name orders, table orders, primary_key order_id);

DIMENSION (name status, type categorical);

METRIC (
    name total_revenue,
    agg sum,
    sql amount
);

SEGMENT (
    name completed,
    sql {model}.status = 'completed'
);
