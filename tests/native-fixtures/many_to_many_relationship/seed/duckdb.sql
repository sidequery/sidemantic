create table orders (
  order_id integer,
  amount integer
);

create table order_items (
  line_item_id integer,
  order_id integer,
  product_id integer
);

create table products (
  product_id integer,
  category varchar
);

insert into orders values
  (1, 100),
  (2, 50),
  (3, 80);

insert into order_items values
  (10, 1, 501),
  (11, 1, 502),
  (12, 2, 501),
  (13, 3, 503);

insert into products values
  (501, 'hardware'),
  (502, 'software'),
  (503, 'hardware');
