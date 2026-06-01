create table orders (
  tenant_id integer,
  order_id integer,
  amount integer
);

create table order_items (
  tenant_id integer,
  order_id integer,
  product_id integer
);

create table products (
  tenant_id integer,
  product_id integer,
  category varchar
);

insert into orders values
  (1, 100, 100),
  (1, 101, 50),
  (2, 100, 200);

insert into order_items values
  (1, 100, 10),
  (1, 101, 11),
  (2, 100, 10);

insert into products values
  (1, 10, 'hardware'),
  (1, 11, 'services'),
  (2, 10, 'software');
