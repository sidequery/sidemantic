import { expect, test } from "bun:test";
import { buildCatalogFromDescribe, joinedDimensionsForModel, withJoinablePairs } from "./catalog";
import { formatCompact, formatValue } from "./format";
import { catalogDimTypes, composeFilters, includeFilter } from "./queries";

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

test("joined dimensions preserve types and refs, label owners, and respect public visibility and reachability", () => {
  const catalog = withJoinablePairs(buildCatalogFromDescribe({ models: [
    { name: "activity", dimensions: [{ name: "arm" }] },
    { name: "assignment", dimensions: [
      { name: "arm", label: "Test arm", type: "numeric" },
      { name: "active", type: "boolean" },
      { name: "secret", public: false },
    ] },
    { name: "unrelated", dimensions: [{ name: "arm" }] },
  ] }), { joinable_pairs: [
    { from: "assignment", to: "activity", hops: 2 },
    { from: "activity", to: "assignment" },
    { from: "activity", to: "activity" },
  ] });
  expect(joinedDimensionsForModel(catalog, "activity").map(({ ref, label, type }) => ({ ref, label, type }))).toEqual([
    { ref: "assignment.arm", label: "Test arm · Assignment", type: "numeric" },
    { ref: "assignment.active", label: "Active · Assignment", type: "boolean" },
  ]);
  expect(joinedDimensionsForModel(catalog, "unrelated")).toEqual([]);
  expect(composeFilters({ "assignment.arm": includeFilter(["2"]), "assignment.active": includeFilter(["true"]) }, {
    types: catalogDimTypes(catalog),
  })).toEqual(["assignment.arm = 2", "assignment.active = true"]);
});
