create table events (
  event_id integer,
  user_id varchar,
  event_type varchar,
  event_date date,
  region varchar,
  raw_platform varchar,
  amount integer
);

insert into events values
  (1, 'u1', 'signup', '2024-01-01', 'us', 'ios', 0),
  (2, 'u1', 'purchase', '2024-01-03', 'us', 'web', 100),
  (3, 'u1', 'active', '2024-01-02', 'us', 'ios', 0),
  (4, 'u1', 'active', '2024-01-05', 'us', 'web', 0),
  (5, 'u2', 'signup', '2024-01-05', 'eu', 'android', 0),
  (6, 'u2', 'purchase', '2024-01-07', 'eu', 'web', 75),
  (7, 'u2', 'active', '2024-01-07', 'eu', 'android', 0),
  (8, 'u3', 'signup', '2024-02-01', 'us', 'ios', 0),
  (9, 'u3', 'purchase', '2024-02-10', 'us', 'web', 200),
  (10, 'u4', 'signup', '2024-02-01', 'eu', 'ios', 0),
  (11, 'u4', 'purchase', '2024-02-05', 'eu', 'web', 150),
  (12, 'u4', 'active', '2024-02-07', 'eu', 'web', 0),
  (13, 'u5', 'purchase', '2024-02-10', 'us', 'web', 50);
