create table orders (
    order_id integer,
    status varchar,
    amount integer
);

insert into orders values
    (1, 'completed', 100),
    (2, 'completed', 150),
    (3, 'cancelled', 50);
