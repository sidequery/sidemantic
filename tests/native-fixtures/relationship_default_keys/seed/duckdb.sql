create table customers (
    id integer,
    country varchar
);

create table orders (
    id integer,
    status varchar
);

create table profiles (
    id integer,
    tier varchar
);

create table accounts (
    account_uid integer,
    region varchar
);

create table payments (
    payment_id integer,
    accounts_id integer,
    amount integer
);

create table vendors (
    vendor_uid integer,
    segment varchar
);

create table invoices (
    invoice_id integer,
    vendor_ref integer
);

insert into customers values
    (1, 'US'),
    (2, 'CA');

insert into orders values
    (1, 'paid'),
    (2, 'refunded');

insert into profiles values
    (1, 'gold'),
    (2, 'silver');

insert into accounts values
    (101, 'east'),
    (102, 'west');

insert into payments values
    (1001, 101, 40),
    (1002, 102, 60);

insert into vendors values
    (201, 'enterprise'),
    (202, 'midmarket');

insert into invoices values
    (3001, 201),
    (3002, 202);
