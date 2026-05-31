model orders from orders (
  primary key (order_id)
  status
  created_at as created_at : time grain day
  sum(amount) as total_revenue
)

model customers from customers (
  primary key (customer_id)
  country
)
