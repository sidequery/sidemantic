CREATE TABLE items (
  id INTEGER,
  tenant_ref INTEGER,
  order_ref VARCHAR,
  category VARCHAR
);

INSERT INTO items VALUES
  (1, 1, 'O-1', 'retail'),
  (2, 1, 'O-1', 'retail'),
  (3, 1, 'O-2', 'retail'),
  (4, 2, 'O-1', 'other_tenant');

CREATE TABLE orders (
  id INTEGER,
  tenant_id INTEGER,
  order_number VARCHAR,
  amount INTEGER
);

INSERT INTO orders VALUES
  (10, 1, 'O-1', 100),
  (11, 1, 'O-2', 50),
  (12, 1, 'O-3', 30);
