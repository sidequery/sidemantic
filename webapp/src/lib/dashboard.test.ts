import { describe, expect, test } from "bun:test";
import type { Catalog, DashboardSpec } from "../data/types";
import { dashboardTabConfig } from "./dashboard";

const catalog: Catalog = {
  models: [
    {
      name: "orders",
      label: "Orders",
      metrics: [
        { ref: "orders.revenue", name: "revenue", model: "orders", label: "Revenue" },
        { ref: "orders.order_count", name: "order_count", model: "orders", label: "Order Count" },
      ],
      dimensions: [
        { ref: "orders.created_at", name: "created_at", model: "orders", label: "Created", type: "time" },
        { ref: "orders.region", name: "region", model: "orders", label: "Region", type: "categorical" },
        { ref: "orders.channel", name: "channel", model: "orders", label: "Channel", type: "categorical" },
      ],
      timeDimension: { ref: "orders.created_at", name: "created_at", model: "orders", label: "Created", type: "time" },
      defaultGrain: "day",
    },
  ],
  graphMetrics: [],
};

const dashboard: DashboardSpec = {
  title: "Revenue dashboard",
  tabs: [
    {
      id: "overview",
      label: "Overview",
      charts: [
        {
          id: "revenue",
          title: "Monthly revenue",
          query: {
            metrics: ["orders.revenue", "orders.order_count"],
            dimensions: ["orders.created_at__month", "orders.region"],
          },
          encoding: { x: "orders.created_at__month", y: "orders.revenue" },
        },
      ],
    },
    {
      id: "channels",
      charts: [
        {
          id: "orders",
          query: { metrics: ["orders.order_count"], dimensions: ["orders.channel"] },
          encoding: { x: "orders.channel", y: "orders.order_count" },
        },
      ],
    },
  ],
};

describe("dashboardTabConfig", () => {
  test("maps semantic fields onto canonical Explore configuration", () => {
    const config = dashboardTabConfig(catalog, dashboard);
    expect(config?.id).toBe("overview");
    expect(config?.selectedMetric).toBe("orders.revenue");
    expect(config?.grain).toBe("month");
    expect(config?.metrics.map((metric) => metric.ref)).toEqual(["orders.revenue", "orders.order_count"]);
    expect(config?.dimensions.map((dimension) => dimension.ref)).toEqual(["orders.created_at", "orders.region"]);
  });

  test("selects a requested dashboard tab", () => {
    const config = dashboardTabConfig(catalog, dashboard, "channels");
    expect(config?.id).toBe("channels");
    expect(config?.selectedMetric).toBe("orders.order_count");
    expect(config?.dimensions.map((dimension) => dimension.ref)).toEqual(["orders.channel"]);
  });

  test("preserves segments and honors the ordered y-metric encoding", () => {
    const segmented: DashboardSpec = {
      title: "Segmented orders",
      tabs: [
        {
          id: "segmented",
          charts: [
            {
              id: "orders",
              query: {
                metrics: ["orders.revenue", "orders.order_count"],
                dimensions: ["orders.created_at__month"],
                segments: ["orders.completed"],
                use_preaggregations: false,
              },
              encoding: {
                x: "orders.created_at__month",
                y: ["orders.order_count", "orders.revenue"],
              },
            },
          ],
        },
      ],
    };

    const config = dashboardTabConfig(catalog, segmented);
    expect(config?.segments).toEqual(["orders.completed"]);
    expect(config?.usePreaggregations).toBe(false);
    expect(config?.selectedMetric).toBe("orders.order_count");
  });

  test("accepts the camel-case pre-aggregation override", () => {
    const configured: DashboardSpec = {
      title: "Raw orders",
      tabs: [
        {
          id: "raw",
          charts: [
            {
              id: "orders",
              query: { metrics: ["orders.order_count"], usePreaggregations: false },
            },
          ],
        },
      ],
    };

    expect(dashboardTabConfig(catalog, configured)?.usePreaggregations).toBe(false);
  });

  test("selects the owner model for a graph-only metric", () => {
    const multiModelCatalog: Catalog = {
      models: [
        {
          name: "customers",
          label: "Customers",
          metrics: [{ ref: "customers.count", name: "count", model: "customers", label: "Customers" }],
          dimensions: [],
        },
        catalog.models[0],
      ],
      graphMetrics: [
        {
          ref: "gross_margin_rate",
          name: "gross_margin_rate",
          ownerModel: "orders",
          label: "Gross Margin Rate",
        },
      ],
    };
    const graphMetricDashboard: DashboardSpec = {
      title: "Margin dashboard",
      tabs: [
        {
          id: "margin",
          charts: [
            {
              id: "gross-margin",
              query: { metrics: ["gross_margin_rate"] },
              encoding: { y: "gross_margin_rate" },
            },
          ],
        },
      ],
    };

    const config = dashboardTabConfig(multiModelCatalog, graphMetricDashboard);
    expect(config?.model.name).toBe("orders");
    expect(config?.metrics.map((metric) => metric.ref)).toEqual(["gross_margin_rate"]);
    expect(config?.selectedMetric).toBe("gross_margin_rate");
  });

  test("keeps metrics from a model related to the dashboard dimension model", () => {
    const crossModelCatalog: Catalog = {
      models: [
        {
          name: "customers",
          label: "Customers",
          metrics: [{ ref: "customers.count", name: "count", model: "customers", label: "Customers" }],
          dimensions: [
            {
              ref: "customers.country",
              name: "country",
              model: "customers",
              label: "Country",
              type: "categorical",
            },
          ],
        },
        catalog.models[0],
      ],
      graphMetrics: [],
      joinablePairs: [{ from: "customers", to: "orders" }],
    };
    const crossModelDashboard: DashboardSpec = {
      title: "Revenue by customer country",
      tabs: [
        {
          id: "country",
          charts: [
            {
              id: "revenue",
              query: { metrics: ["orders.revenue"], dimensions: ["customers.country"] },
              encoding: { x: "customers.country", y: "orders.revenue" },
            },
          ],
        },
      ],
    };

    const config = dashboardTabConfig(crossModelCatalog, crossModelDashboard);
    expect(config?.model.name).toBe("customers");
    expect(config?.metrics.map((metric) => metric.ref)).toEqual(["orders.revenue"]);
    expect(config?.selectedMetric).toBe("orders.revenue");
  });
});
