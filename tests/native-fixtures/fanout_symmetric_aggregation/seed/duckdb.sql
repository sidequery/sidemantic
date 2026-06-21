create table customers (
    customer_id integer,
    country varchar,
    credit_limit integer
);

create table orders (
    order_id integer,
    customer_id integer,
    status varchar
);

insert into customers values
    (1, 'US', 1000),
    (2, 'CA', 500);

insert into orders values
    (101, 1, 'paid'),
    (102, 1, 'paid'),
    (103, 1, 'pending'),
    (104, 2, 'paid');
