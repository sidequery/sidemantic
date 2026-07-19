import { describe, expect, test } from "bun:test";
import type { DashboardChart, DashboardDocument } from "../data/dashboardTypes";
import { NULL_TOKEN } from "../data/types";
import {
  dashboardFilterValue,
  dashboardResultColumn,
  decodeDashboardState,
  encodeDashboardState,
  rowsToCsv,
  selectableDashboardDimension,
  shouldUseExplorer,
} from "./dashboardState";

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

describe("dashboard routing", () => {
  test("preserves legacy root explorer and pivot links", () => {
    expect(shouldUseExplorer("/", "?view=explore&model=orders")).toBe(true);
    expect(shouldUseExplorer("/", "?view=pivot&model=orders")).toBe(true);
    expect(shouldUseExplorer("/explore", "")).toBe(true);
    expect(shouldUseExplorer("/", "?tab=overview")).toBe(false);
  });
});

describe("dashboard selections", () => {
  type SelectInteraction = NonNullable<DashboardChart["interactions"]>["select"];
  const chart = (select: SelectInteraction): DashboardChart => ({
    id: "status",
    query: { metrics: ["orders.order_count"], dimensions: ["orders.status", "orders.country"] },
    interactions: { select },
  });

  test("honors disabled and field-scoped select interactions", () => {
    expect(selectableDashboardDimension(chart(false), "orders.status")).toBe(false);
    expect(selectableDashboardDimension(chart(true), "orders.status")).toBe(true);
    expect(selectableDashboardDimension(chart({ fields: ["orders.country"] }), "orders.status")).toBe(false);
    expect(selectableDashboardDimension(chart({ fields: ["orders.status"] }), "orders.status")).toBe(true);
  });

  test("preserves the null sentinel instead of filtering for its display label", () => {
    expect(dashboardFilterValue(null)).toBe(NULL_TOKEN);
    expect(dashboardFilterValue(undefined)).toBe(NULL_TOKEN);
    expect(dashboardFilterValue("—")).toBe("—");
  });

  test("resolves exact qualified aliases before ambiguous suffixes", () => {
    const columns = ["orders_status", "customers_status", "revenue"];
    expect(dashboardResultColumn("customers.status", columns)).toBe("customers_status");
    expect(dashboardResultColumn("orders.status", columns)).toBe("orders_status");
    expect(dashboardResultColumn("orders.revenue", columns)).toBe("revenue");
  });
});
