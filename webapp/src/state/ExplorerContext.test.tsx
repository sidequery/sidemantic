import { afterEach, describe, expect, setSystemTime, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { SidemanticBackend } from "../data/backend";
import type { Catalog, DashboardSpec } from "../data/types";
import { dashboardTabConfig } from "../lib/dashboard";
import { composeFilters } from "../lib/queries";
import { presetRange } from "../lib/time";
import { ExplorerProvider, searchForState, stateForSearch, useExplorer } from "./ExplorerContext";
import { explorerReducer, initialStateFromCatalog } from "./explorerState";

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

describe("stateForSearch", () => {
  const initial = initialStateFromCatalog(catalog);

  test("decodes a search into the state the provider would mount with", () => {
    const state = stateForSearch("?view=pivot&grain=week", catalog, null, initial);
    expect(state.view).toBe("pivot");
    expect(state.grain).toBe("week");
  });

  test("round-trips through searchForState, so a navigation echo mirrors its own state", () => {
    const search = "?view=explore&model=orders&metric=orders.revenue&grain=day";
    expect(searchForState(stateForSearch(search, catalog, null, initial))).toBe(search);
  });
});

describe("rolling date selection", () => {
  afterEach(() => setSystemTime());

  test("the first hydrated Explore and Pivot queries have a 28-day bound", () => {
    setSystemTime(new Date("2026-10-19T12:00:00Z"));
    for (const view of ["explore", "pivot"]) {
      const state = stateForSearch(`?view=${view}`, catalog, null, initialStateFromCatalog(catalog));
      const filters = composeFilters(state.filters, {
        timeRef: catalog.models[0].timeDimension?.ref,
        range: state.dateRange,
      });
      expect(filters).toEqual([
        "orders.created_at >= cast('2026-09-22' as date)",
        "orders.created_at < cast('2026-10-20' as date)",
      ]);
    }
  });

  test("a shared default remains rolling after midnight, even with a cached base", () => {
    setSystemTime(new Date("2026-10-19T12:00:00Z"));
    const initial = initialStateFromCatalog(catalog);
    const search = searchForState(initial);
    expect(search).not.toContain("from=");
    expect(search).not.toContain("to=");
    setSystemTime(new Date("2026-10-20T12:00:00Z"));
    const restored = stateForSearch(search, catalog, null, initial);
    expect(restored.dateRange).toEqual({ from: "2026-09-23", to: "2026-10-20" });
    expect(searchForState(restored)).toBe(search);
    expect(searchForState(initial)).toBe(search);
  });

  test("All time survives reload and reset restores a freshly resolved default", () => {
    setSystemTime(new Date("2026-10-19T12:00:00Z"));
    const initial = initialStateFromCatalog(catalog);
    const all = explorerReducer(initial, { type: "setDateRange" });
    const search = searchForState(all);
    expect(search).toContain("range=all");
    setSystemTime(new Date("2026-10-20T12:00:00Z"));
    const restored = stateForSearch(search, catalog, null, initial);
    expect(restored.dateRange).toBeUndefined();
    expect(searchForState(restored)).toBe(search);
    expect(explorerReducer(restored, { type: "reset", initial }).dateRange)
      .toEqual({ from: "2026-09-23", to: "2026-10-20" });
  });

  test("custom dates equal to today's default stay absolute, as do chart brushes", () => {
    setSystemTime(new Date("2026-10-19T12:00:00Z"));
    const initial = initialStateFromCatalog(catalog);
    for (const range of [initial.dateRange!, { from: "2026-10-19T01:00:00", to: "2026-10-19T02:00:00" }]) {
      const custom = explorerReducer(initial, { type: "setDateRange", range });
      const search = searchForState(custom);
      expect(search).toContain("from=");
      setSystemTime(new Date("2026-10-20T12:00:00Z"));
      expect(stateForSearch(search, catalog, null, initial).dateRange).toEqual(range);
    }
  });

  test("selecting a preset keeps its rolling meaning and changing other controls preserves it", () => {
    setSystemTime(new Date("2026-10-19T12:00:00Z"));
    const initial = initialStateFromCatalog(catalog);
    for (const days of [7, 28, 90, 180, 365]) {
      setSystemTime(new Date("2026-10-19T12:00:00Z"));
      const preset = explorerReducer(initial, { type: "setDateRange", preset: days });
      const pivot = explorerReducer(preset, { type: "setView", view: "pivot" });
      const changed = explorerReducer(pivot, { type: "setGrain", grain: "day" });
      const search = searchForState(changed);
      setSystemTime(new Date("2026-10-20T12:00:00Z"));
      const restored = stateForSearch(search, catalog, null, initial);
      expect(restored.dateRange).toEqual(presetRange(days));
      expect(restored.datePreset).toBe(days);
      expect(restored.view).toBe("pivot");
      expect(restored.grain).toBe("day");
    }
  });

  test("models without time emit no date filters and switching to time retains the selection", () => {
    const noTimeCatalog: Catalog = { ...catalog, models: [{ ...catalog.models[0], timeDimension: undefined }] };
    const initial = initialStateFromCatalog(noTimeCatalog);
    const noTime = stateForSearch("?view=explore", noTimeCatalog, null, initial);
    expect(composeFilters(noTime.filters, { range: noTime.dateRange })).toEqual([]);
    const switched = explorerReducer(noTime, {
      type: "setModel", model: "orders", metric: "orders.revenue", grain: "day",
    });
    expect(composeFilters(switched.filters, { range: switched.dateRange, timeRef: "orders.created_at" })).toHaveLength(2);
    const all = explorerReducer(switched, { type: "setDateRange" });
    expect(explorerReducer(all, { type: "setModel", model: "products", metric: "products.count", grain: "day" }).datePreset).toBe("all");
  });

  test("configured dashboards retain static filters without adding the Explore default", () => {
    const dashboard: DashboardSpec = {
      title: "Historical orders",
      tabs: [{ id: "history", charts: [{ id: "revenue", query: {
        metrics: "orders.revenue", filters: ["orders.created_at < '2020-01-01'"],
      } }] }],
    };
    const initial = initialStateFromCatalog(catalog, dashboard);
    expect(initial.dateRange).toBeUndefined();
    const config = dashboardTabConfig(catalog, dashboard)!;
    expect([...config.filters, ...composeFilters(initial.filters, {
      range: initial.dateRange, timeRef: config.timeDimension?.ref,
    })]).toEqual(["orders.created_at < '2020-01-01'"]);
    const explicit = explorerReducer(initial, { type: "setDateRange", preset: 28 });
    expect(searchForState(explicit)).toContain("range=28d");
    expect(stateForSearch(searchForState(explicit), catalog, dashboard, initial).datePreset).toBe(28);
    const tab = explorerReducer(explicit, {
      type: "setDashboardTab", tab: "history", model: "orders", metric: "orders.revenue", grain: "day",
    });
    expect(tab.dateRange).toBeUndefined();
    expect(tab.datePreset).toBeUndefined();
  });
});
