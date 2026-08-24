CREATE TABLE orders (
  order_id INTEGER,
  tenant_id INTEGER,
  active BOOLEAN,
  status VARCHAR,
  amount INTEGER
);

INSERT INTO orders VALUES
  (1, 1, true, 'kept', 10),
  (2, 1, false, 'inactive', 20),
  (3, 2, true, 'other_tenant', 30),
  (4, 1, true, 'also_kept', 15);
