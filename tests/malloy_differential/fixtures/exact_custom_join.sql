CREATE TABLE customers (
  id INTEGER,
  name VARCHAR,
  active BOOLEAN
);

INSERT INTO customers VALUES
  (1, 'kept', true),
  (2, 'inactive', false);

CREATE TABLE orders (
  id INTEGER,
  customer_id INTEGER,
  amount INTEGER
);

INSERT INTO orders VALUES
  (10, 1, 20),
  (11, 2, 30),
  (12, 3, 40);
