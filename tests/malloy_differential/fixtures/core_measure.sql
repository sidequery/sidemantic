COPY (
  SELECT *
  FROM (
    VALUES
      (1, 'completed', 100.00),
      (2, 'completed', 250.00),
      (3, 'pending', 75.00),
      (4, 'cancelled', 40.00)
  ) AS orders(id, status, amount)
) TO 'data/orders.parquet' (FORMAT PARQUET);
