create table orders (
  order_id integer,
  status varchar,
  amount integer
);

insert into orders values
  (1, 'paid', 80),
  (2, 'paid', 120),
  (3, 'paid', 150),
  (4, 'refunded', 200);
