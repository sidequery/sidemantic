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
});
