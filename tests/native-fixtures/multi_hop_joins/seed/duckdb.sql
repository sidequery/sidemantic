create table order_items (
    line_item_id integer,
    order_id integer,
    product_id integer,
    line_amount integer
);

create table orders (
    order_id integer,
    customer_id integer
);

create table customers (
    customer_id integer,
    country varchar
);

insert into order_items values
    (10, 1, 100, 100),
    (11, 1, 101, 50),
    (12, 2, 100, 80);

insert into orders values
    (1, 501),
    (2, 502);

insert into customers values
    (501, 'US'),
    (502, 'CA');
