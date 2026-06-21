create table customers (
    customer_id integer,
    country varchar
);

create table orders (
    order_id integer,
    customer_id integer,
    status varchar,
    amount integer
);

insert into customers values
    (1, 'CA'),
    (2, 'US'),
    (3, 'US');

insert into orders values
    (101, 1, 'paid', 120),
    (102, 2, 'paid', 200),
    (103, 3, 'paid', 80);
