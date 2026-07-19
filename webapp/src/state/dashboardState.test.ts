import { describe, expect, test } from "bun:test";
import type { DashboardDocument } from "../data/dashboardTypes";
import { decodeDashboardState, encodeDashboardState, rowsToCsv } from "./dashboardState";

const document: DashboardDocument = {
  schema: "sidemantic.dashboard.v1",
  title: "Revenue",
  tabs: [
    {
      id: "overview",
      charts: [{ id: "trend", query: { metrics: ["orders.revenue"], dimensions: ["orders.region"] } }],
    },
    {
      id: "customers",
      charts: [{ id: "tiers", query: { metrics: ["customers.customer_count"], dimensions: ["customers.tier"] } }],
    },
  ],
};

describe("dashboard URL state", () => {
  test("round-trips the active tab and allowed filters", () => {
    const encoded = encodeDashboardState(
      { tab: "customers", filters: { "orders.region": "North", "customers.tier": "Gold" } },
      document,
    );
    expect(decodeDashboardState(encoded, document)).toEqual({
      tab: "customers",
      filters: { "orders.region": "North", "customers.tier": "Gold" },
    });
  });

  test("rejects unknown tabs, dimensions, and non-string filter values", () => {
    const filters = encodeURIComponent(JSON.stringify({ "orders.region": "North", "orders.secret": "x", "customers.tier": 4 }));
    expect(decodeDashboardState(`?tab=missing&dashboard_filters=${filters}`, document)).toEqual({
      tab: "overview",
      filters: { "orders.region": "North" },
    });
  });
});

test("rowsToCsv quotes commas, quotes, and newlines", () => {
  expect(rowsToCsv(["name", "amount"], [{ name: "A, \"quoted\" row", amount: 12 }, { name: "two\nlines", amount: null }])).toBe(
    'name,amount\r\n"A, ""quoted"" row",12\r\n"two\nlines",',
  );
});
