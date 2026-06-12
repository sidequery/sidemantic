create table orders (
    order_id integer,
    status varchar,
    amount integer,
    discount_amount integer
);

insert into orders values
    (1, 'paid', 100, 10),
    (2, 'paid', 200, 20),
    (3, 'refunded', 50, 5);
