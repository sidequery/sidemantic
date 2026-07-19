import { describe, expect, test } from "bun:test";
import type { DashboardChart, DashboardDocument } from "../data/dashboardTypes";
import { NULL_TOKEN } from "../data/types";
import {
  brushableDashboardDimension,
  dashboardCategorySeries,
  dashboardDrillDimension,
  dashboardExploreUrl,
  dashboardFilterValue,
  dashboardMetricRefs,
  dashboardRangeFilter,
  dashboardResultColumn,
  dashboardStructuredQuery,
  dashboardTimeSeries,
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
      {
        tab: "customers",
        filters: { "orders.region": "North", "customers.tier": "Gold" },
        ranges: { "orders.region": { from: "A", to: "Z" } },
      },
      document,
    );
    expect(decodeDashboardState(encoded, document)).toEqual({
      tab: "customers",
      filters: { "orders.region": "North", "customers.tier": "Gold" },
      ranges: { "orders.region": { from: "A", to: "Z" } },
    });
  });

  test("rejects unknown tabs, dimensions, and non-string filter values", () => {
    const filters = encodeURIComponent(JSON.stringify({ "orders.region": "North", "orders.secret": "x", "customers.tier": 4 }));
    expect(decodeDashboardState(`?tab=missing&dashboard_filters=${filters}`, document)).toEqual({
      tab: "overview",
      filters: { "orders.region": "North" },
      ranges: {},
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
    expect(dashboardDrillDimension(chart({ fields: ["orders.country"] }))).toBe("orders.country");
  });

  test("honors horizontal brush interactions", () => {
    const brushed: DashboardChart = {
      id: "trend",
      query: { metrics: ["orders.order_count"], dimensions: ["orders.created_at__month", "orders.region"] },
      interactions: { brush: { fields: ["orders.created_at__month"], channel: "x" } },
    };
    expect(brushableDashboardDimension(brushed, "orders.created_at__month")).toBe(true);
    expect(brushableDashboardDimension(brushed, "orders.region")).toBe(false);
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

  test("preserves every encoded y metric", () => {
    const chart: DashboardChart = {
      id: "trend",
      query: { metrics: ["orders.revenue", "orders.order_count"], dimensions: ["orders.created_at__month"] },
      encoding: { y: ["orders.revenue", "orders.order_count"] },
    };
    expect(dashboardMetricRefs(chart)).toEqual(["orders.revenue", "orders.order_count"]);
  });

  test("preserves the backend pre-aggregation default unless the document overrides it", () => {
    const chart: DashboardChart = { id: "revenue", query: { metrics: ["orders.revenue"] } };
    const implicit = dashboardStructuredQuery(document, chart, {}, {});
    expect(implicit).not.toHaveProperty("usePreaggregations");

    const explicit = dashboardStructuredQuery(
      { ...document, defaults: { query: { use_preaggregations: false } } },
      chart,
      {},
      {},
    );
    expect(explicit.usePreaggregations).toBe(false);
  });

  test("adds shared brush ranges to dashboard queries", () => {
    const chart: DashboardChart = {
      id: "trend",
      query: { metrics: ["orders.revenue"], dimensions: ["orders.created_at__month"] },
    };
    const query = dashboardStructuredQuery(
      document,
      chart,
      {},
      {},
      { "orders.created_at__month": { from: "2026-01-01", to: "2026-03-01" } },
    );
    expect(query.filters).toContain(
      "orders.created_at >= '2026-01-01' AND orders.created_at < '2026-04-01'",
    );
    expect(
      dashboardRangeFilter("orders.created_at__day", { from: "2026-03-01", to: "2026-03-02" }),
    ).toBe("orders.created_at >= '2026-03-01' AND orders.created_at < '2026-03-03'");
  });

  test("expands selected time buckets against their base dimension", () => {
    const chart: DashboardChart = {
      id: "trend",
      query: { metrics: ["orders.revenue"], dimensions: ["orders.created_at__month"] },
    };
    const selected = dashboardStructuredQuery(
      document,
      chart,
      { "orders.created_at__month": "2026-02-01" },
      {},
    );
    expect(selected.filters).toContain(
      "orders.created_at >= '2026-02-01' AND orders.created_at < '2026-03-01'",
    );

    const selectedNull = dashboardStructuredQuery(
      document,
      chart,
      { "orders.created_at__month": NULL_TOKEN },
      {},
    );
    expect(selectedNull.filters).toContain("orders.created_at IS NULL");
  });

  test("carries brushed time buckets into explorer links", () => {
    const chart: DashboardChart = {
      id: "trend",
      query: { metrics: ["orders.revenue"], dimensions: ["orders.created_at__month"] },
      encoding: { x: "orders.created_at__month" },
    };
    const url = new URL(
      dashboardExploreUrl(chart, {
        tab: "overview",
        filters: { "orders.region": "West" },
        ranges: { "orders.created_at__month": { from: "2026-01-01", to: "2026-03-01" } },
      }),
      "https://example.test",
    );
    expect(url.searchParams.get("from")).toBe("2026-01-01");
    expect(url.searchParams.get("to")).toBe("2026-03-31");
    expect(JSON.parse(url.searchParams.get("filters") ?? "{}")).toEqual({ "orders.region": ["West"] });
  });

  test("separates categorical series before plotting bars", () => {
    const series = dashboardCategorySeries(
      [
        { status: "Open", region: "West", count: 10 },
        { status: "Open", region: "East", count: 12 },
      ],
      "status",
      "count",
      ["region"],
    );
    expect(series).toEqual([
      { label: "West", data: [{ label: "Open", filterValue: "Open", value: 10 }] },
      { label: "East", data: [{ label: "Open", filterValue: "Open", value: 12 }] },
    ]);
  });

  test("keeps time-series dimension combinations separate and aligns their buckets", () => {
    const series = dashboardTimeSeries(
      [
        { month: "2026-01", region: "West", revenue: 10 },
        { month: "2026-01", region: "East", revenue: 20 },
        { month: "2026-02", region: "West", revenue: 15 },
      ],
      "month",
      "revenue",
      ["region"],
    );
    expect(series.map((entry) => entry.label)).toEqual(["West", "East"]);
    expect(series[0].points).toEqual([
      { x: "2026-01", y: 10 },
      { x: "2026-02", y: 15 },
    ]);
    expect(series[1].points[0]).toEqual({ x: "2026-01", y: 20 });
    expect(Number.isNaN(series[1].points[1].y)).toBe(true);
  });
});
