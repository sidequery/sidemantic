create table order_items (
    order_id integer,
    line_number integer,
    item_id integer
);

create table shipments (
    shipment_id integer,
    order_id integer,
    line_number integer
);

insert into order_items values
    (1, 1, 100),
    (1, 2, 101),
    (2, 1, 100),
    (2, 2, 102);

insert into shipments values
    (1, 1, 1),
    (2, 1, 2),
    (3, 2, 1);
