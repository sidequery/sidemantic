export const MODEL_YAML = `models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: id
        type: numeric
      - name: customer_id
        type: numeric
      - name: product_id
        type: numeric
      - name: status
        type: categorical
      - name: created
        type: time
        sql: created_at
        supported_granularities: [day, month]
    metrics:
      - name: order_count
        agg: count
        description: Total number of orders
      - name: total_revenue
        agg: sum
        sql: amount
        description: Total revenue from all orders
      - name: avg_order_value
        agg: avg
        sql: amount
        description: Average order value
      - name: total_quantity
        agg: sum
        sql: quantity
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
      - name: products
        type: many_to_one
        foreign_key: product_id

  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: id
        type: numeric
      - name: name
        type: categorical
      - name: email
        type: categorical
      - name: region
        type: categorical
      - name: signup_date
        type: time
    metrics:
      - name: customer_count
        agg: count
        description: Total number of customers

  - name: products
    table: products
    primary_key: id
    dimensions:
      - name: id
        type: numeric
      - name: name
        type: categorical
      - name: category
        type: categorical
      - name: price
        type: numeric
      - name: cost
        type: numeric
    metrics:
      - name: product_count
        agg: count
        description: Total number of products
      - name: avg_price
        agg: avg
        sql: price
`;

export const METRICS = [
  { key: "orders.total_revenue", label: "Total Revenue", format: "currency" },
  { key: "orders.order_count", label: "Order Count", format: "number" },
  { key: "orders.avg_order_value", label: "Avg Order Value", format: "currency" },
  { key: "orders.total_quantity", label: "Total Quantity", format: "number" },
];

export const DIMENSIONS = [
  { key: "customers.region", label: "Customer Region" },
  { key: "orders.status", label: "Order Status" },
  { key: "products.category", label: "Product Category" },
  { key: "customers.name", label: "Top Customers" },
];

export const TIME_GRAINS = ["month", "day"];

function seededRandom(seed) {
  let value = seed;
  return () => {
    value |= 0;
    value = (value + 0x6d2b79f5) | 0;
    let next = Math.imul(value ^ (value >>> 15), 1 | value);
    next = (next + Math.imul(next ^ (next >>> 7), 61 | next)) ^ next;
    return ((next ^ (next >>> 14)) >>> 0) / 4294967296;
  };
}

function randomInt(random, min, max) {
  return Math.floor(random() * (max - min + 1)) + min;
}

export function createDemoData() {
  const customers = [
    [1, "Alice Johnson", "alice@example.com", "North", "2023-01-15"],
    [2, "Bob Smith", "bob@example.com", "South", "2023-02-20"],
    [3, "Carol Davis", "carol@example.com", "East", "2023-03-10"],
    [4, "David Wilson", "david@example.com", "West", "2023-04-05"],
    [5, "Eve Martinez", "eve@example.com", "North", "2023-05-18"],
    [6, "Frank Brown", "frank@example.com", "South", "2023-06-22"],
    [7, "Grace Lee", "grace@example.com", "East", "2023-07-30"],
    [8, "Henry Taylor", "henry@example.com", "West", "2023-08-14"],
  ].map(([id, name, email, region, signup_date]) => ({ id, name, email, region, signup_date }));

  const products = [
    [1, "Laptop Pro", "Electronics", 1299.99, 800.0],
    [2, "Wireless Mouse", "Electronics", 29.99, 15.0],
    [3, "Desk Chair", "Furniture", 249.99, 120.0],
    [4, "Standing Desk", "Furniture", 599.99, 350.0],
    [5, "Notebook Set", "Office Supplies", 12.99, 5.0],
    [6, "Pen Pack", "Office Supplies", 8.99, 3.0],
    [7, "Monitor 27", "Electronics", 399.99, 250.0],
    [8, "Keyboard Mechanical", "Electronics", 149.99, 80.0],
  ].map(([id, name, category, price, cost]) => ({ id, name, category, price, cost }));

  const random = seededRandom(42);
  const statuses = ["completed", "pending", "cancelled", "completed", "completed"];
  const orders = [];
  let id = 1;
  const startDate = Date.UTC(2025, 4, 22);

  for (let day = 0; day < 365; day += 2) {
    const volumeMultiplier = 1 + day / 365;
    const orderCount = Math.floor(randomInt(random, 1, 3) * volumeMultiplier);

    for (let index = 0; index < orderCount; index += 1) {
      const customer_id = randomInt(random, 1, customers.length);
      const product_id = randomInt(random, 1, products.length);
      const quantity = randomInt(random, 1, 3);
      const product = products.find((item) => item.id === product_id);
      const status = statuses[randomInt(random, 0, statuses.length - 1)];
      const hour = randomInt(random, 8, 20);
      const minute = randomInt(random, 0, 59);
      const createdAt = new Date(startDate + day * 24 * 60 * 60 * 1000);
      createdAt.setUTCHours(hour, minute, 0, 0);

      orders.push({
        id,
        customer_id,
        product_id,
        quantity,
        amount: Number((product.price * quantity).toFixed(2)),
        status,
        created_at: createdAt.toISOString().replace(".000Z", ""),
      });
      id += 1;
    }
  }

  return { customers, products, orders };
}
