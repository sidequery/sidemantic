-- Simulated query history showing common patterns
-- These queries represent what users typically run

-- Daily status dashboard (run 150 times)
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders WHERE order_date >= '2025-09-01';
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders WHERE status = 'completed';
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAY;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders WHERE order_date >= '2025-08-01';
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders WHERE status IN ('completed', 'pending');
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, revenue, order_count FROM orders WHERE order_date >= '2025-09-01';

-- Regional performance (run 80 times)
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.region granularities=day
SELECT order_date, region, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.region granularities=day
SELECT order_date, region, revenue, order_count FROM orders WHERE order_date >= '2025-09-01';
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.region granularities=day
SELECT order_date, region, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.region granularities=day
SELECT order_date, region, revenue, order_count FROM orders WHERE region = 'North';
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.region granularities=day
SELECT order_date, region, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.region granularities=day
SELECT order_date, region, revenue, order_count FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL 7 DAY;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.region granularities=day
SELECT order_date, region, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.region granularities=day
SELECT order_date, region, revenue, order_count FROM orders WHERE order_date >= '2025-08-15';

-- Monthly executive dashboard (run 50 times)
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status,orders.region granularities=month
SELECT order_date, status, region, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status,orders.region granularities=month
SELECT order_date, status, region, revenue, order_count FROM orders WHERE order_date >= '2025-01-01';
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status,orders.region granularities=month
SELECT order_date, status, region, revenue, order_count FROM orders;
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status,orders.region granularities=month
SELECT order_date, status, region, revenue, order_count FROM orders WHERE status = 'completed';
-- sqlctx: models=orders metrics=orders.revenue,orders.order_count dimensions=orders.order_date,orders.status,orders.region granularities=month
SELECT order_date, status, region, revenue, order_count FROM orders;

-- Average order value by status (run 30 times)
-- sqlctx: models=orders metrics=orders.avg_order_value dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, avg_order_value FROM orders;
-- sqlctx: models=orders metrics=orders.avg_order_value dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, avg_order_value FROM orders WHERE order_date >= '2025-09-01';
-- sqlctx: models=orders metrics=orders.avg_order_value dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, avg_order_value FROM orders;
-- sqlctx: models=orders metrics=orders.avg_order_value dimensions=orders.order_date,orders.status granularities=day
SELECT order_date, status, avg_order_value FROM orders WHERE status = 'completed';
