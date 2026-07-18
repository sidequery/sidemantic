import type { Catalog, CatalogModel, DashboardSpec, Grain } from "../data/types";
import { graphMetricsForModel } from "../lib/catalog";
import { dashboardTabConfig } from "../lib/dashboard";
import { isEmptyFilter, type FilterMode, type FilterState } from "../lib/queries";
import type { DateRange } from "../lib/time";

export type ViewKind = "home" | "explore" | "pivot";

// Extra per-row figure shown in each leaderboard, relative to the ranking metric. `none` keeps the
// bare ranked value (the pre-E2 behavior).
export type ContextColumn = "none" | "pctTotal" | "delta" | "deltaPct";

// Which window the current period is compared against. `previous` is the immediately-preceding
// equal window (the pre-E3 behavior); `off` disables comparison entirely; `custom` uses an
// explicit comparisonRange.
export type ComparisonMode = "off" | "previous" | "year" | "custom";

export type ExplorerState = {
  view: ViewKind;
  dashboardTab?: string;
  model: string; // primary fact model name
  selectedMetric: string; // metric ref ranking leaderboards / focused in detail
  filters: FilterState; // dimRef -> selected values
  grain: Grain;
  timezone: string; // IANA zone for time bucketing + label rendering; "UTC" default (E4)
  dateRange?: DateRange; // undefined = all time
  contextColumn: ContextColumn; // leaderboard context column (E2)
  comparison: ComparisonMode; // comparison window for deltas/overlay (E3)
  comparisonRange?: DateRange; // explicit window when comparison === "custom"
  pivotDims: string[];
  pivotMetrics: string[];
};

export type ExplorerAction =
  | { type: "hydrate"; state: ExplorerState }
  | { type: "setView"; view: ViewKind }
  | { type: "setDashboardTab"; tab: string; model: string; metric: string; grain: Grain }
  | { type: "setModel"; model: string; metric: string; grain: Grain }
  | { type: "setMetric"; metric: string }
  // `mode` forces include/exclude when toggling from the editor; the leaderboard omits it (keeping
  // the dimension's current mode, defaulting to include).
  | { type: "toggleFilter"; dim: string; value: string; mode?: FilterMode }
  | { type: "setFilterMode"; dim: string; mode: FilterMode }
  | { type: "setFilterPattern"; dim: string; pattern: string }
  | { type: "removeFilterValue"; dim: string; value: string }
  | { type: "removeFilterDim"; dim: string }
  | { type: "clearFilters" }
  | { type: "setGrain"; grain: Grain }
  | { type: "setTimezone"; timezone: string }
  | { type: "setDateRange"; range?: DateRange }
  | { type: "setContextColumn"; column: ContextColumn }
  | { type: "setComparison"; comparison: ComparisonMode; range?: DateRange }
  | { type: "setPivotDims"; dims: string[] }
  | { type: "setPivotMetrics"; metrics: string[] }
  | { type: "reset"; initial: ExplorerState };

function toggle(values: string[] | undefined, value: string): string[] {
  const list = values ?? [];
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value];
}

/** Write `filter` at `dim`, dropping the key when the filter would emit no SQL (keeps state minimal
 *  and the URL short, and lets `dirty`/pill rendering treat "no filters" as an empty map). */
function putFilter(filters: FilterState, dim: string, filter: FilterState[string]): FilterState {
  const next = { ...filters };
  if (isEmptyFilter(filter)) delete next[dim];
  else next[dim] = filter;
  return next;
}

export function explorerReducer(state: ExplorerState, action: ExplorerAction): ExplorerState {
  switch (action.type) {
    case "hydrate":
      return action.state;
    case "setView":
      return { ...state, view: action.view };
    case "setDashboardTab":
      return {
        ...state,
        view: "explore",
        dashboardTab: action.tab,
        model: action.model,
        selectedMetric: action.metric,
        grain: action.grain,
        filters: {},
        dateRange: undefined,
        pivotDims: [],
        pivotMetrics: [],
      };
    case "setModel":
      // Switching the primary model invalidates metric/filter selections.
      return {
        ...state,
        model: action.model,
        selectedMetric: action.metric,
        grain: action.grain,
        filters: {},
        pivotDims: [],
        pivotMetrics: [],
      };
    case "setMetric":
      return { ...state, selectedMetric: action.metric };
    case "toggleFilter": {
      // Toggle a value within the dimension's selection. The editor passes an explicit `mode`;
      // a leaderboard click keeps the current include/exclude mode (contains falls back to include,
      // which has discrete values).
      const current = state.filters[action.dim];
      const mode = action.mode ?? (current && current.mode !== "contains" ? current.mode : "include");
      // Reuse the existing value list only when its mode matches the target; otherwise start fresh
      // (a contains filter's values are inert and must not leak into an include/exclude toggle).
      const prior = current && current.mode === mode ? current.values : [];
      const values = toggle(prior, action.value);
      return { ...state, filters: putFilter(state.filters, action.dim, { mode, values }) };
    }
    case "setFilterMode": {
      // Preserve the value list across include<->exclude; contains keeps its pattern but its values
      // are inert. A dimension with no prior filter starts empty in the chosen mode.
      const current = state.filters[action.dim];
      const filter = { mode: action.mode, values: current?.values ?? [], pattern: current?.pattern };
      return { ...state, filters: putFilter(state.filters, action.dim, filter) };
    }
    case "setFilterPattern": {
      const current = state.filters[action.dim];
      const filter = { mode: "contains" as const, values: current?.values ?? [], pattern: action.pattern };
      return { ...state, filters: putFilter(state.filters, action.dim, filter) };
    }
    case "removeFilterValue": {
      const current = state.filters[action.dim];
      if (!current) return state;
      const values = current.values.filter((v) => v !== action.value);
      return { ...state, filters: putFilter(state.filters, action.dim, { ...current, values }) };
    }
    case "removeFilterDim": {
      const filters = { ...state.filters };
      delete filters[action.dim];
      return { ...state, filters };
    }
    case "clearFilters":
      return { ...state, filters: {} };
    case "setGrain":
      return { ...state, grain: action.grain };
    case "setTimezone":
      return { ...state, timezone: action.timezone };
    case "setDateRange":
      return { ...state, dateRange: action.range };
    case "setContextColumn":
      return { ...state, contextColumn: action.column };
    case "setComparison":
      // A custom window is only meaningful with an explicit range; drop a stale one otherwise so
      // switching back to "previous"/"year" doesn't carry the old custom bounds around.
      return {
        ...state,
        comparison: action.comparison,
        comparisonRange: action.comparison === "custom" ? action.range : undefined,
      };
    case "setPivotDims":
      return { ...state, pivotDims: action.dims };
    case "setPivotMetrics":
      return { ...state, pivotMetrics: action.metrics };
    case "reset":
      // Reset clears the scoped controls (filters, date range, ...) but stays on the current view.
      // The catalog-derived `initial` lands on "home", which would otherwise eject Explore/Pivot
      // to the index on every Reset.
      return { ...action.initial, view: state.view };
    default:
      return state;
  }
}

/** First model that has a time dimension, else the first model. */
export function primaryModel(catalog: Catalog): CatalogModel | undefined {
  return catalog.models.find((m) => m.timeDimension) ?? catalog.models[0];
}

/** Default ranking metric for a model: its first metric, else the first graph metric. */
export function defaultMetric(model: CatalogModel | undefined, catalog: Catalog): string {
  return model?.metrics[0]?.ref ?? (model ? graphMetricsForModel(catalog, model.name)[0]?.ref : undefined) ?? "";
}

/** Derive a fresh initial state from the loaded catalog. */
export function initialStateFromCatalog(catalog: Catalog, dashboard?: DashboardSpec | null): ExplorerState {
  const configured = dashboardTabConfig(catalog, dashboard);
  const model = configured?.model ?? primaryModel(catalog);
  const metric = configured?.selectedMetric ?? defaultMetric(model, catalog);
  return {
    // A configured dashboard opens directly; otherwise land on the model index.
    view: configured ? "explore" : "home",
    dashboardTab: configured?.id,
    model: model?.name ?? "",
    selectedMetric: metric,
    filters: {},
    grain: configured?.grain ?? (model?.defaultGrain as Grain) ?? "month",
    timezone: "UTC",
    dateRange: undefined,
    contextColumn: "none",
    comparison: "previous", // preserve the pre-E3 previous-period comparison as the default
    comparisonRange: undefined,
    pivotDims: [],
    // Left empty so the pivot falls back to the active metric for whatever model is selected,
    // rather than pinning the primary model's first metric across model switches.
    pivotMetrics: [],
  };
}
