create table raw_orders (
    order_id integer,
    status varchar,
    amount integer,
    is_deleted boolean
);

insert into raw_orders values
    (1, 'paid', 100, false),
    (2, 'paid', 150, false),
    (3, 'refunded', 50, false),
    (4, 'paid', 999, true);
