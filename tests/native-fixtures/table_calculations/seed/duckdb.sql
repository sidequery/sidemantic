create table orders (
    order_id integer,
    status varchar,
    amount integer
);

insert into orders values
    (1, 'paid', 100),
    (2, 'paid', 150),
    (3, 'refunded', 50);
