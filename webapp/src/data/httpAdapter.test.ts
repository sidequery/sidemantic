import { afterEach, describe, expect, mock, test } from "bun:test";
import { HttpBackend } from "./httpAdapter";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("HttpBackend.getDashboard", () => {
  test("returns null when the dashboard endpoint is absent", async () => {
    globalThis.fetch = mock(async () => new Response("Not found", { status: 404 })) as typeof fetch;

    await expect(new HttpBackend().getDashboard()).resolves.toBeNull();
  });

  test("ignores an embedded host's HTML SPA fallback", async () => {
    globalThis.fetch = mock(
      async () => new Response("<!doctype html><title>Sidemantic</title>", { headers: { "Content-Type": "text/html" } }),
    ) as typeof fetch;

    await expect(new HttpBackend().getDashboard()).resolves.toBeNull();
  });

  test("returns a JSON dashboard document", async () => {
    const dashboard = { schema: "sidemantic.dashboard.v1", title: "Revenue", tabs: [] } as const;
    globalThis.fetch = mock(async () => Response.json(dashboard)) as typeof fetch;

    await expect(new HttpBackend().getDashboard()).resolves.toEqual(dashboard);
  });

  test("normalizes validator-accepted scalar query fields", async () => {
    const dashboard = {
      title: "Revenue",
      tabs: [{
        id: "overview",
        charts: [{
          id: "revenue",
          query: {
            metrics: "orders.revenue",
            dimensions: "orders.status",
            filters: "orders.status = 'completed'",
            segments: "orders.completed",
            order_by: "orders.revenue DESC",
          },
          interactions: { select: { fields: "orders.status" }, brush: { fields: "orders.created_at", channel: "x" } },
        }],
      }],
    };
    globalThis.fetch = mock(async () => Response.json(dashboard)) as typeof fetch;

    const normalized = await new HttpBackend().getDashboard();
    expect(normalized?.tabs[0].charts[0].query).toMatchObject({
      metrics: ["orders.revenue"],
      dimensions: ["orders.status"],
      filters: ["orders.status = 'completed'"],
      segments: ["orders.completed"],
      order_by: ["orders.revenue DESC"],
    });
    expect(normalized?.tabs[0].charts[0].interactions).toMatchObject({
      select: { fields: ["orders.status"] },
      brush: { fields: ["orders.created_at"], channel: "x" },
    });
  });
});
