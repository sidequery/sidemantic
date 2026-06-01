create table orders (
    order_id integer,
    status varchar,
    amount integer,
    created_at timestamp
);

create table customers (
    customer_id integer,
    country varchar
);

insert into orders values
    (1, 'paid', 100, timestamp '2026-01-01 10:00:00'),
    (2, 'paid', 150, timestamp '2026-01-02 10:00:00'),
    (3, 'refunded', 50, timestamp '2026-01-03 10:00:00');

insert into customers values
    (1, 'US');
