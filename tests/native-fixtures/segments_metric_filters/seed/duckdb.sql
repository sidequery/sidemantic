create table orders (
    order_id integer,
    status varchar,
    amount integer
);

insert into orders values
    (1, 'completed', 120),
    (2, 'completed', 80),
    (3, 'pending', 200),
    (4, 'cancelled', 300),
    (5, 'completed', 180);
