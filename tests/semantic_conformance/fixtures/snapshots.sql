create table snapshot_rows (
    id integer, account varchar, region varchar, day date,
    balance integer, amount integer, eligible boolean, tenant integer
);
insert into snapshot_rows values
    (1, 'A', 'east', '2024-01-01', 100, 1, true, 1),
    (2, 'A', 'east', '2024-01-20', 150, 2, false, 1),
    (3, 'A', 'east', '2024-02-01', 170, 3, true, 1),
    (4, 'B', 'east', '2024-01-03', 50, 4, true, 1),
    (5, 'B', 'east', '2024-01-25', 80, 5, true, 1),
    (6, 'C', 'west', '2024-01-10', 30, 6, true, 1),
    (7, 'C', 'west', '2024-02-02', null, 7, true, 1),
    (8, 'D', 'west', null, 900, 8, true, 1),
    (9, 'A', 'east', '2024-03-01', 999, 100, true, 2);
