create table orders (
  order_id integer,
  status varchar,
  amount integer
);

insert into orders values
  (1, 'paid', 100),
  (2, 'paid', 50),
  (3, 'refunded', 25);
