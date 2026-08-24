CREATE TABLE orders (
  order_id INTEGER,
  region VARCHAR,
  amount INTEGER
);

INSERT INTO orders VALUES
  (1, 'west', 10),
  (2, 'east', 20),
  (3, 'west', 30);
