import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { SidemanticBackend } from "../data/backend";
import type { Catalog } from "../data/types";
import { ExplorerProvider, searchForState, useExplorer } from "./ExplorerContext";
import { initialStateFromCatalog } from "./explorerState";

// This suite runs without a DOM, so it is exactly the environment a host's server render sees:
// touching `window` during render would throw here.
const catalog: Catalog = {
  models: [
    {
      name: "orders",
      label: "Orders",
      metrics: [{ ref: "orders.revenue", name: "revenue", model: "orders", label: "Revenue" }],
      dimensions: [
        { ref: "orders.created_at", name: "created_at", model: "orders", label: "Created", type: "time" },
        { ref: "orders.region", name: "region", model: "orders", label: "Region", type: "categorical" },
      ],
      timeDimension: { ref: "orders.created_at", name: "created_at", model: "orders", label: "Created", type: "time" },
      defaultGrain: "month",
    },
  ],
  graphMetrics: [],
};

const backend = {
  health: async () => true,
  getCatalog: async () => catalog,
  getDashboard: async () => null,
  compile: async () => "",
  runQuery: async () => ({ columns: [], rows: [], rowCount: 0, sql: "" }),
} satisfies SidemanticBackend;

function Probe() {
  const { state } = useExplorer();
  return <span>{`${state.view}|${state.model}|${state.selectedMetric}|${state.grain}`}</span>;
}

function render(initialSearch?: string) {
  return renderToStaticMarkup(
    <ExplorerProvider catalog={catalog} backend={backend} initialSearch={initialSearch}>
      <Probe />
    </ExplorerProvider>,
  );
}

describe("ExplorerProvider hydration", () => {
  test("renders without a window when no search is supplied, landing on the catalog default", () => {
    expect(render()).toContain("home|orders|orders.revenue|month");
  });

  test("hydrates from a host-supplied search instead of window.location", () => {
    expect(render("?view=pivot&grain=week")).toContain("pivot|orders|orders.revenue|week");
  });

  test("accepts a search without the leading question mark", () => {
    expect(render("view=explore")).toContain("explore|orders|orders.revenue|month");
  });
});

test("searchForState mirrors state as a location.search-shaped string", () => {
  expect(searchForState(initialStateFromCatalog(catalog))).toBe("?view=home&model=orders&metric=orders.revenue&grain=month");
});
