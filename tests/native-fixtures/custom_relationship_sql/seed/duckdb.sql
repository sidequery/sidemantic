create table orders (
  order_id integer,
  customer_id integer,
  tenant_id integer,
  amount integer
);

create table customers (
  customer_id integer,
  tenant_id integer,
  country varchar
);

insert into orders values
  (1, 100, 1, 50),
  (2, 100, null, 70);

insert into customers values
  (100, 1, 'US'),
  (100, null, 'Global');
