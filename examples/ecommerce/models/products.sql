-- Pure SQL semantic model definition
-- Products catalog with categories, brands, and pricing tiers

MODEL (
    name products,
    table products,
    primary_key product_id,
    description 'Product catalog'
);

-- Dimensions
DIMENSION (
    name category,
    type categorical,
    sql category,
    description 'Product category'
);

DIMENSION (
    name subcategory,
    type categorical,
    sql subcategory,
    description 'Product subcategory'
);

DIMENSION (
    name brand,
    type categorical,
    sql brand,
    description 'Product brand'
);

DIMENSION (
    name product_name,
    type categorical,
    sql name,
    description 'Product name'
);

DIMENSION (
    name is_active,
    type categorical,
    sql is_active,
    description 'Whether product is currently available'
);

DIMENSION (
    name price_tier,
    type categorical,
    sql CASE
        WHEN price < 50 THEN 'budget'
        WHEN price < 200 THEN 'mid-range'
        WHEN price < 500 THEN 'premium'
        ELSE 'luxury'
    END,
    description 'Price tier based on product price'
);

-- Metrics
METRIC (
    name product_count,
    agg count,
    description 'Total number of products'
);

METRIC (
    name avg_price,
    agg avg,
    sql price,
    description 'Average product price',
    format '$#,##0.00'
);

METRIC (
    name active_product_count,
    agg count,
    filters is_active = true,
    description 'Number of active products'
);
