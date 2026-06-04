import { defineDashboard, type DashboardRenderer } from "./sidemantic.generated";

const renderer: DashboardRenderer = "vega-lite";

export default defineDashboard({
  schema: "sidemantic.dashboard.v1",
  title: "Revenue Performance Explorer",
  defaults: {
    renderer,
    query: {
      interactionPreaggregations: true,
    },
    interactions: {
      scope: "tab",
    },
  },
  tabs: [
    {
      id: "overview",
      label: "Overview",
      sourceRecordCount: 50000,
      charts: [
        {
          id: "revenue_explorer",
          title: "Revenue Explorer",
          type: "line",
          query: {
            metrics: ["orders.revenue", "orders.gross_margin", "orders.order_count"],
            dimensions: [
              "orders.created_at__month",
              "orders.region",
              "orders.channel",
              "orders.customer_tier",
              "orders.product_line",
            ],
            orderBy: ["orders.created_at__month"],
            interactionPreaggregations: true,
          },
          encoding: {
            x: "orders.created_at__month",
            y: "orders.revenue",
            color: "orders.region",
          },
          interactions: {
            brush: {
              fields: ["orders.created_at__month"],
              channel: "x",
            },
            select: {
              fields: ["orders.region", "orders.channel", "orders.customer_tier", "orders.product_line"],
            },
          },
        },
      ],
    },
  ],
});

