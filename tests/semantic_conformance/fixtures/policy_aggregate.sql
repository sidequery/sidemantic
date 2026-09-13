create table aggregate_accounts(id integer, tenant integer, tier varchar, quota integer, active boolean);
insert into aggregate_accounts values
    (1, 1, 'retail', 2, true),
    (2, 1, 'business', 4, true),
    (3, 2, 'retail', 10, true),
    (4, 1, 'retail', 20, false);
create table aggregate_purchases(id integer, account_id integer, amount integer, cost integer, deleted boolean);
insert into aggregate_purchases values
    (1, 1, 100, 11, false), (2, 1, 20, 3, false),
    (3, 2, 60, 7, false), (4, 3, 1000, 90, false),
    (5, 4, 800, 30, false), (6, 1, 999, 100, true);
