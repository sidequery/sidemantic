create table orders (
  order_id integer,
  created_at date,
  status varchar,
  amount integer
);

insert into orders values
  (1, date '2024-01-05', 'paid', 100),
  (2, date '2024-01-20', 'paid', 150),
  (3, date '2024-02-02', 'paid', 200),
  (4, date '2024-02-10', 'refunded', 50);

create table orders_preagg_monthly_status (
  created_at_month date,
  status varchar,
  total_revenue_raw integer,
  order_count_raw integer
);

insert into orders_preagg_monthly_status values
  (date '2024-01-01', 'paid', 250, 2),
  (date '2024-02-01', 'paid', 200, 1),
  (date '2024-02-01', 'refunded', 50, 1);
