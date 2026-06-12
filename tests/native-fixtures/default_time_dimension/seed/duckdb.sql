create table orders (
    order_id integer,
    amount integer,
    created_at timestamp
);

insert into orders values
    (1, 100, timestamp '2026-01-01 10:00:00'),
    (2, 150, timestamp '2026-01-18 10:00:00'),
    (3, 200, timestamp '2026-02-03 10:00:00');
