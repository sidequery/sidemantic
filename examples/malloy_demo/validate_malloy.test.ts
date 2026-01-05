/**
 * Validates the generated Malloy output using the official Malloy compiler.
 *
 * Run with: bun test examples/malloy_demo/validate_malloy.test.ts
 *
 * This test ensures the LookML to Malloy conversion produces syntactically
 * and semantically valid Malloy code that can be compiled by the Malloy compiler.
 */

import { describe, test, expect, beforeAll } from "bun:test";
import { SingleConnectionRuntime, URLReader } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { readFileSync, existsSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath, pathToFileURL } from "url";

const DEMO_DIR = dirname(import.meta.path);
const MALLOY_OUTPUT_DIR = resolve(DEMO_DIR, "malloy_output");
const MALLOY_FILE = resolve(MALLOY_OUTPUT_DIR, "thelook.malloy");

/**
 * URL Reader that converts file:// URLs to local file paths
 */
class LocalFileReader implements URLReader {
  async readURL(url: URL): Promise<string> {
    const filePath = fileURLToPath(url);
    return readFileSync(filePath, "utf-8");
  }
}

describe("Malloy Export Validation", () => {
  let malloySource: string;
  let runtime: SingleConnectionRuntime;
  let modelUrl: URL;

  beforeAll(async () => {
    // Ensure the output file exists
    if (!existsSync(MALLOY_FILE)) {
      throw new Error(
        `Malloy output file not found: ${MALLOY_FILE}\n` +
          `Run 'uv run examples/malloy_demo/run_demo.py' first to generate it.`
      );
    }

    // Read the generated Malloy file
    malloySource = readFileSync(MALLOY_FILE, "utf-8");

    // Create a DuckDB connection with the working directory set to malloy_output
    const connection = new DuckDBConnection(
      "duckdb",
      undefined,
      MALLOY_OUTPUT_DIR
    );

    const reader = new LocalFileReader();
    runtime = new SingleConnectionRuntime({
      connection,
      urlReader: reader,
    });

    modelUrl = pathToFileURL(MALLOY_FILE);
  });

  test("generated Malloy file should compile without errors", async () => {
    // This will throw if there are syntax or semantic errors
    const model = runtime.loadModel(modelUrl);
    const modelDef = await model.getModel();

    expect(modelDef).toBeDefined();
    expect(modelDef._modelDef).toBeDefined();
  });

  test("should define products source", async () => {
    const model = runtime.loadModel(modelUrl);
    const modelDef = await model.getModel();

    // Check that products source exists
    const sources = modelDef._modelDef.contents;
    expect(sources.products).toBeDefined();
  });

  test("should define customers source", async () => {
    const model = runtime.loadModel(modelUrl);
    const modelDef = await model.getModel();

    const sources = modelDef._modelDef.contents;
    expect(sources.customers).toBeDefined();
  });

  test("should define orders source", async () => {
    const model = runtime.loadModel(modelUrl);
    const modelDef = await model.getModel();

    const sources = modelDef._modelDef.contents;
    expect(sources.orders).toBeDefined();
  });

  test("should be able to run a simple query on products", async () => {
    const model = runtime.loadModel(modelUrl);
    const query = model.loadQuery(`
      run: products -> {
        group_by: category
        aggregate: product_count
      }
    `);

    const result = await query.run();
    expect(result.data).toBeDefined();
    expect(result.data.value.length).toBeGreaterThan(0);
  });

  test("should be able to run a query on customers", async () => {
    const model = runtime.loadModel(modelUrl);
    const query = model.loadQuery(`
      run: customers -> {
        group_by: region
        aggregate: customer_count
      }
    `);

    const result = await query.run();
    expect(result.data).toBeDefined();
    expect(result.data.value.length).toBeGreaterThan(0);
  });

  test("should be able to run a time-grouped query on orders", async () => {
    const model = runtime.loadModel(modelUrl);
    const query = model.loadQuery(`
      run: orders -> {
        group_by: created_month
        aggregate: total_revenue
      }
    `);

    const result = await query.run();
    expect(result.data).toBeDefined();
    expect(result.data.value.length).toBeGreaterThan(0);
  });

  test("should be able to use multiple measures in queries", async () => {
    const model = runtime.loadModel(modelUrl);
    const query = model.loadQuery(`
      run: orders -> {
        group_by: status
        aggregate:
          order_count
          total_revenue
      }
    `);

    const result = await query.run();
    expect(result.data).toBeDefined();
    expect(result.data.value.length).toBeGreaterThan(0);
  });

  test("should be able to use filtered measures", async () => {
    const model = runtime.loadModel(modelUrl);
    const query = model.loadQuery(`
      run: orders -> {
        aggregate:
          total_revenue
          completed_revenue
      }
    `);

    const result = await query.run();
    expect(result.data).toBeDefined();
    expect(result.data.value.length).toBeGreaterThan(0);

    // Completed revenue should be less than or equal to total revenue
    const row = result.data.value[0] as Record<string, number>;
    expect(row.completed_revenue).toBeLessThanOrEqual(row.total_revenue);
  });

  test("generated file should not have circular references", () => {
    // Check for the pattern that causes circular references: "name is name"
    // where a field is defined as itself without any transformation
    const lines = malloySource.split("\n");
    const circularPattern = /^\s*(\w+)\s+is\s+\1\s*$/;

    const circularRefs = lines.filter((line) => circularPattern.test(line));

    expect(circularRefs).toEqual([]);
  });

  test("time dimensions should have proper truncation", () => {
    // Time dimensions should use Malloy's truncation syntax like .day, .month, .year
    expect(malloySource).toContain("created_date is created_at.day");
    expect(malloySource).toContain("created_month is created_at.month");
    expect(malloySource).toContain("created_year is created_at.year");
  });

  test("passthrough dimensions should not be exported", () => {
    // Passthrough dimensions like "name is name" should not appear
    // because Malloy auto-exposes columns from the underlying table
    expect(malloySource).not.toMatch(/\bname is name\b/);
    expect(malloySource).not.toMatch(/\bcategory is category\b/);
    expect(malloySource).not.toMatch(/\bemail is email\b/);
    expect(malloySource).not.toMatch(/\bamount is amount\b/);
    expect(malloySource).not.toMatch(/\bstatus is status\b/);
  });

  test("measures should be properly defined with aggregations", () => {
    expect(malloySource).toContain("product_count is count()");
    expect(malloySource).toContain("customer_count is count()");
    expect(malloySource).toContain("order_count is count()");
    expect(malloySource).toContain("total_revenue is sum(amount)");
    expect(malloySource).toContain("avg_order_value is avg(amount)");
  });
});
