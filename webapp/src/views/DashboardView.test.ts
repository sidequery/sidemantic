import { describe, expect, test } from "bun:test";
import type { QueryResult } from "../data/types";
import { dashboardResultAlias } from "./DashboardView";

describe("dashboardResultAlias", () => {
  test("uses model-qualified aliases when bare names collide", () => {
    const refs = ["orders.status", "customers.status", "orders.revenue"];
    const result: QueryResult = {
      columns: ["orders_status", "customers_status", "revenue"],
      rows: [],
      rowCount: 0,
      sql: "",
    };

    expect(dashboardResultAlias("orders.status", refs, result)).toBe("orders_status");
    expect(dashboardResultAlias("customers.status", refs, result)).toBe("customers_status");
    expect(dashboardResultAlias("orders.revenue", refs, result)).toBe("revenue");
  });

  test("keeps a bare alias for non-colliding fields", () => {
    expect(dashboardResultAlias("orders.created_at__month", ["orders.created_at__month"])).toBe(
      "created_at__month",
    );
  });
});
