import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev13.0/+esm";

function normalizeDuckValue(value) {
  if (typeof value === "bigint") return Number(value);
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  if (value && typeof value === "object" && typeof value.toString === "function") {
    const rendered = value.toString();
    return rendered === "[object Object]" ? value : rendered;
  }
  return value;
}

function arrowToRows(table) {
  const columns = table.schema.fields.map((field) => field.name);
  const rows = [];
  for (let rowIndex = 0; rowIndex < table.numRows; rowIndex += 1) {
    const row = table.get(rowIndex);
    const output = {};
    for (const column of columns) {
      let value = row?.[column];
      if (value === undefined && typeof row?.get === "function") value = row.get(column);
      if (value === undefined && typeof table.getChild === "function") {
        const child = table.getChild(column);
        if (child && typeof child.get === "function") value = child.get(rowIndex);
      }
      output[column] = normalizeDuckValue(value);
    }
    rows.push(output);
  }
  return { columns, rows };
}

async function loadDemoTables(db, conn, demoData) {
  await conn.query("drop table if exists orders");
  await conn.query("drop table if exists customers");
  await conn.query("drop table if exists products");

  await db.registerFileText("customers.json", JSON.stringify(demoData.customers));
  await db.registerFileText("products.json", JSON.stringify(demoData.products));
  await db.registerFileText("orders.json", JSON.stringify(demoData.orders));

  await conn.query(`
    create table customers as
    select
      id,
      name,
      email,
      region,
      cast(signup_date as date) as signup_date
    from read_json_auto('customers.json')
  `);
  await conn.query(`
    create table products as
    select
      id,
      name,
      category,
      price,
      cost
    from read_json_auto('products.json')
  `);
  await conn.query(`
    create table orders as
    select
      id,
      customer_id,
      product_id,
      quantity,
      amount,
      status,
      cast(created_at as timestamp) as created_at
    from read_json_auto('orders.json')
  `);
}

export async function createDuckDBRuntime(demoData) {
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }),
  );
  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  const conn = await db.connect();
  await loadDemoTables(db, conn, demoData);

  return {
    bundleName: bundle.mainModule.split("/").slice(-2).join("/"),
    async queryRows(sql) {
      return arrowToRows(await conn.query(sql));
    },
  };
}
