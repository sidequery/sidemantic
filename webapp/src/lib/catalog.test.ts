import { expect, test } from "bun:test";
import { buildCatalogFromDescribe } from "./catalog";
import { formatCompact, formatValue } from "./format";

test("describe formats reach model and graph metric displays, with explicit formats taking precedence", () => {
  const catalog = buildCatalogFromDescribe({
    models: [{
      name: "orders",
      dimensions: [{ name: "amount", value_format_name: "usd" }],
      metrics: [
        { name: "arppu", type: "ratio", value_format_name: "decimal_2" },
        { name: "arpu", type: "ratio", value_format_name: "percent", format: "#,##0.00" },
        { name: "aov", type: "ratio", value_format_name: "usd" },
      ],
    }],
    metrics: [
      { name: "aov", type: "ratio", value_format_name: "usd" },
      { name: "conversion", type: "ratio", value_format_name: "percent_2" },
      { name: "arpu", type: "ratio", value_format_name: "percent", format: "#,##0.00" },
    ],
  });
  expect(formatValue(0.59, catalog.models[0].dimensions[0])).toBe("$0.59");
  expect(formatValue(160907.2, catalog.models[0].metrics[0])).toBe("160,907.20");
  expect(formatValue(0.59, catalog.models[0].metrics[1])).toBe("0.59");
  expect(formatValue(0.59, catalog.models[0].metrics[2])).toBe("$0.59");
  expect(formatCompact(160907.2, catalog.graphMetrics[0])).toBe("$160.9K");
  expect(formatValue(0.59, catalog.graphMetrics[1])).toBe("59.00%");
  expect(formatValue(0.59, catalog.graphMetrics[2])).toBe("0.59");
});
