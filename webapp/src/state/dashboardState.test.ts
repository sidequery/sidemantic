import { describe, expect, test } from "bun:test";
import type { DashboardChart, DashboardDocument } from "../data/dashboardTypes";
import { NULL_TOKEN } from "../data/types";
import {
  brushableDashboardDimension,
  dashboardCategorySeries,
  dashboardCategorySelection,
  dashboardChartType,
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
        filterSources: { "customers.tier": "customers:tiers" },
        rangeSources: { "orders.region": "overview:trend" },
      },
      document,
    );
    expect(decodeDashboardState(encoded, document)).toEqual({
      tab: "customers",
      filters: { "orders.region": "North", "customers.tier": "Gold" },
      ranges: { "orders.region": { from: "A", to: "Z" } },
      filterSources: { "customers.tier": "customers:tiers" },
      rangeSources: { "orders.region": "overview:trend" },
      chartFilters: {},
      chartRanges: {},
    });
  });

  test("rejects unknown tabs, dimensions, and non-string filter values", () => {
    const filters = encodeURIComponent(JSON.stringify({ "orders.region": "North", "orders.secret": "x", "customers.tier": 4 }));
    expect(decodeDashboardState(`?tab=missing&dashboard_filters=${filters}`, document)).toEqual({
      tab: "overview",
      filters: { "orders.region": "North" },
      ranges: {},
      filterSources: {},
      rangeSources: {},
      chartFilters: {},
      chartRanges: {},
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

  test("infers auto time series from semantic dimension metadata", () => {
    const autoChart = (dimension: string): DashboardChart => ({
      id: "trend",
      type: "auto",
      query: { metrics: ["orders.revenue"], dimensions: [dimension] },
      encoding: { x: dimension },
    });
    const types = { "orders.created_at": "time", "orders.region": "categorical" };

    expect(dashboardChartType(autoChart("orders.created_at"), types)).toBe("line");
    expect(dashboardChartType(autoChart("orders.created_at__month"), types)).toBe("line");
    expect(dashboardChartType(autoChart("orders.region"), types)).toBe("bar");
    expect(dashboardChartType(autoChart("orders.unknown__month"), types)).toBe("bar");
  });

  test("defaults time-series queries to ascending x order", () => {
    const auto: DashboardChart = {
      id: "trend",
      type: "auto",
      query: { metrics: ["orders.revenue"], dimensions: ["orders.created_at"] },
    };
    expect(dashboardStructuredQuery(document, auto, {}, { "orders.created_at": "time" }).orderBy).toEqual([
      "orders.created_at ASC",
    ]);

    const explicit = { ...auto, query: { ...auto.query, order_by: ["orders.created_at DESC"] } };
    expect(dashboardStructuredQuery(document, explicit, {}, { "orders.created_at": "time" }).orderBy).toEqual([
      "orders.created_at DESC",
    ]);

    const categorical = { ...auto, query: { ...auto.query, dimensions: ["orders.region"] } };
    expect(dashboardStructuredQuery(document, categorical, {}, { "orders.region": "categorical" }).orderBy).toBeUndefined();
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
      dashboardExploreUrl(document, chart, {
        tab: "overview",
        filters: { "orders.region": "West" },
        ranges: { "orders.created_at__month": { from: "2026-01-01", to: "2026-03-01" } },
        filterSources: {},
        rangeSources: {},
      }),
      "https://example.test",
    );
    expect(url.searchParams.get("from")).toBe("2026-01-01");
    expect(url.searchParams.get("to")).toBe("2026-03-31");
    expect(JSON.parse(url.searchParams.get("filters") ?? "{}")).toEqual({ "orders.region": ["West"] });
  });

  test("expands selected time buckets in explorer links", () => {
    const chart: DashboardChart = {
      id: "trend",
      query: { metrics: ["orders.revenue"], dimensions: ["orders.created_at__month"] },
      encoding: { x: "orders.created_at__month" },
    };
    const url = new URL(
      dashboardExploreUrl(document, chart, {
        tab: "overview",
        filters: { "orders.created_at__month": "2026-02-01" },
        ranges: {},
        filterSources: {},
        rangeSources: {},
      }),
      "https://example.test",
    );
    expect(url.searchParams.get("from")).toBe("2026-02-01");
    expect(url.searchParams.get("to")).toBe("2026-02-28");
    expect(url.searchParams.has("filters")).toBe(false);
  });

  test("carries a model-scoped time range into categorical explorer links", () => {
    const chart: DashboardChart = {
      id: "status",
      query: { metrics: ["orders.revenue"], dimensions: ["orders.status"] },
      encoding: { x: "orders.status" },
    };
    const url = new URL(
      dashboardExploreUrl(
        document,
        chart,
        {
          tab: "overview",
          filters: {},
          ranges: {
            "customers.created_at__month": { from: "2025-01-01", to: "2025-02-01" },
            "orders.created_at": { from: "2026-01-01", to: "2026-03-31" },
          },
          filterSources: {},
          rangeSources: {},
        },
        { "orders.created_at": "time" },
      ),
      "https://example.test",
    );
    expect(url.searchParams.get("from")).toBe("2026-01-01");
    expect(url.searchParams.get("to")).toBe("2026-03-31");
  });

  test("keeps tab-scoped interaction state out of charts on other tabs", () => {
    const scopedDocument: DashboardDocument = {
      ...document,
      defaults: { interactions: { scope: "tab" } },
      tabs: [
        {
          id: "small",
          charts: [
            { id: "small-revenue", query: { metrics: ["orders.revenue"], dimensions: ["orders.region"] } },
          ],
        },
        {
          id: "large",
          charts: [
            {
              id: "large-revenue",
              query: { metrics: ["orders_200k.revenue"], dimensions: ["orders_200k.region"] },
            },
          ],
        },
      ],
    };
    const largeChart = scopedDocument.tabs[1].charts[0];
    const query = dashboardStructuredQuery(
      scopedDocument,
      largeChart,
      { "orders.region": "West", "orders_200k.region": "East" },
      {},
      {
        "orders.created_at__month": { from: "2026-01-01", to: "2026-02-01" },
        "orders_200k.region": { from: "A", to: "Z" },
      },
    );
    expect(query.filters).toContain("orders_200k.region = 'East'");
    expect(query.filters).toContain("orders_200k.region >= 'A' AND orders_200k.region <= 'Z'");
    expect(query.filters?.some((filter) => filter.includes("orders.region") || filter.includes("orders.created_at"))).toBe(
      false,
    );
  });

  test("keeps repeated chart-scoped dimensions isolated by source", () => {
    const first: DashboardChart = {
      id: "first",
      query: { metrics: ["orders.revenue"], dimensions: ["orders.region", "orders.created_at__month"] },
    };
    const second: DashboardChart = {
      id: "second",
      query: { metrics: ["orders.order_count"], dimensions: ["orders.region", "orders.created_at__month"] },
    };
    const scopedDocument: DashboardDocument = {
      ...document,
      defaults: { interactions: { scope: "chart" } },
      tabs: [{ id: "overview", charts: [first, second] }],
    };
    const state = {
      tab: "overview",
      filters: {},
      ranges: {},
      filterSources: {},
      rangeSources: {},
      chartFilters: {
        "overview:first": { "orders.region": "West" },
        "overview:second": { "orders.region": "East" },
      },
      chartRanges: {
        "overview:first": { "orders.created_at__month": { from: "2026-01-01", to: "2026-02-01" } },
        "overview:second": { "orders.created_at__month": { from: "2026-04-01", to: "2026-05-01" } },
      },
    };

    expect(dashboardStructuredQuery(scopedDocument, first, state.filters, {}, state.ranges, state).filters).toContain(
      "orders.region = 'West'",
    );
    expect(dashboardStructuredQuery(scopedDocument, second, state.filters, {}, state.ranges, state).filters).toContain(
      "orders.region = 'East'",
    );
    expect(dashboardStructuredQuery(scopedDocument, first, state.filters, {}, state.ranges, state).filters).toContain(
      "orders.created_at >= '2026-01-01' AND orders.created_at < '2026-03-01'",
    );
    expect(dashboardStructuredQuery(scopedDocument, second, state.filters, {}, state.ranges, state).filters).toContain(
      "orders.created_at >= '2026-04-01' AND orders.created_at < '2026-06-01'",
    );
    expect(new URL(dashboardExploreUrl(scopedDocument, first, state), "https://example.test").searchParams.has("filters")).toBe(
      true,
    );
    expect(
      JSON.parse(new URL(dashboardExploreUrl(scopedDocument, second, state), "https://example.test").searchParams.get("filters") ?? "{}"),
    ).toEqual({ "orders.region": ["East"] });
    expect(decodeDashboardState(encodeDashboardState(state, scopedDocument), scopedDocument).chartFilters).toEqual(
      state.chartFilters,
    );
    expect(decodeDashboardState(encodeDashboardState(state, scopedDocument), scopedDocument).chartRanges).toEqual(
      state.chartRanges,
    );
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
      { label: "West", filterValues: ["West"], data: [{ label: "Open", filterValue: "Open", value: 10 }] },
      { label: "East", filterValues: ["East"], data: [{ label: "Open", filterValue: "Open", value: 12 }] },
    ]);
  });

  test("includes selectable series dimensions in grouped bar selections", () => {
    const chart: DashboardChart = {
      id: "status",
      query: {
        metrics: ["orders.order_count"],
        dimensions: ["orders.status", "orders.region", "orders.channel"],
      },
      interactions: { select: { fields: ["orders.status", "orders.region"] } },
    };
    expect(
      dashboardCategorySelection(
        chart,
        "orders.status",
        "Open",
        ["orders.region", "orders.channel"],
        ["West", "Retail"],
      ),
    ).toEqual({ "orders.status": "Open", "orders.region": "West" });
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
