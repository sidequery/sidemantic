create table policy_values(id integer, subject varchar, amount integer);
insert into policy_values values
    (1, 'alice', 10), (2, 'bob', 20),
    (3, 'x'' OR ''1''=''1', 7), (4, 'O''Brien', 9);
create table policy_accounts(id integer, tenant integer, label varchar);
insert into policy_accounts values (1, 1, 'visible'), (2, 2, 'other tenant');
create table policy_purchases(id integer, account_id integer, amount integer);
insert into policy_purchases values (1, 1, 10), (2, 2, 20), (3, 999, 30), (4, null, 40);
create table migration_stops(id integer, city varchar);
insert into migration_stops values (1, 'SFO'), (2, 'LAX'), (3, 'JFK');
create table migration_journeys(id integer, origin_id integer, destination_id integer, archived_id integer);
insert into migration_journeys values (10, 1, 2, 123), (11, 1, 3, 124), (12, 2, null, null);
create table migration_sales(id integer, day date, category varchar, amount integer);
insert into migration_sales values
    (1, '2024-01-01', 'a', 100), (2, '2024-03-01', 'a', 180), (3, '2024-04-01', 'a', 210),
    (4, '2024-01-01', 'b', 10), (5, '2024-03-01', 'b', 30), (6, '2024-04-01', 'b', 50);
