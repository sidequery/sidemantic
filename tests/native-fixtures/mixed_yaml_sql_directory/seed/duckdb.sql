create table orders (
    order_id integer,
    customer_id integer,
    amount integer
);

create table customers (
    customer_id integer,
    country varchar
);

insert into orders values
    (1, 101, 100),
    (2, 101, 180),
    (3, 102, 120);

insert into customers values
    (101, 'US'),
    (102, 'CA');
