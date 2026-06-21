import type { Catalog, CatalogModel, Grain } from "../data/types";
import type { FilterState } from "../lib/queries";
import type { DateRange } from "../lib/time";

export type ViewKind = "explore" | "pivot";

export type ExplorerState = {
  view: ViewKind;
  model: string; // primary fact model name
  selectedMetric: string; // metric ref ranking leaderboards / focused in detail
  filters: FilterState; // dimRef -> selected values
  grain: Grain;
  dateRange?: DateRange; // undefined = all time
  pivotDims: string[];
  pivotMetrics: string[];
};

export type ExplorerAction =
  | { type: "hydrate"; state: ExplorerState }
  | { type: "setView"; view: ViewKind }
  | { type: "setModel"; model: string; metric: string; grain: Grain }
  | { type: "setMetric"; metric: string }
  | { type: "toggleFilter"; dim: string; value: string }
  | { type: "removeFilterValue"; dim: string; value: string }
  | { type: "removeFilterDim"; dim: string }
  | { type: "clearFilters" }
  | { type: "setGrain"; grain: Grain }
  | { type: "setDateRange"; range?: DateRange }
  | { type: "setPivotDims"; dims: string[] }
  | { type: "setPivotMetrics"; metrics: string[] }
  | { type: "reset"; initial: ExplorerState };

function toggle(values: string[] | undefined, value: string): string[] {
  const list = values ?? [];
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value];
}

export function explorerReducer(state: ExplorerState, action: ExplorerAction): ExplorerState {
  switch (action.type) {
    case "hydrate":
      return action.state;
    case "setView":
      return { ...state, view: action.view };
    case "setModel":
      // Switching the primary model invalidates metric/filter selections.
      return {
        ...state,
        model: action.model,
        selectedMetric: action.metric,
        grain: action.grain,
        filters: {},
        pivotDims: [],
        pivotMetrics: [action.metric],
      };
    case "setMetric":
      return { ...state, selectedMetric: action.metric };
    case "toggleFilter": {
      const next = toggle(state.filters[action.dim], action.value);
      const filters = { ...state.filters };
      if (next.length) filters[action.dim] = next;
      else delete filters[action.dim];
      return { ...state, filters };
    }
    case "removeFilterValue": {
      const next = (state.filters[action.dim] ?? []).filter((v) => v !== action.value);
      const filters = { ...state.filters };
      if (next.length) filters[action.dim] = next;
      else delete filters[action.dim];
      return { ...state, filters };
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
    case "setDateRange":
      return { ...state, dateRange: action.range };
    case "setPivotDims":
      return { ...state, pivotDims: action.dims };
    case "setPivotMetrics":
      return { ...state, pivotMetrics: action.metrics };
    case "reset":
      return action.initial;
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
  return model?.metrics[0]?.ref ?? catalog.graphMetrics[0]?.ref ?? "";
}

/** Derive a fresh initial state from the loaded catalog. */
export function initialStateFromCatalog(catalog: Catalog): ExplorerState {
  const model = primaryModel(catalog);
  const metric = defaultMetric(model, catalog);
  return {
    view: "explore",
    model: model?.name ?? "",
    selectedMetric: metric,
    filters: {},
    grain: (model?.defaultGrain as Grain) ?? "month",
    dateRange: undefined,
    pivotDims: [],
    // Left empty so the pivot falls back to the active metric for whatever model is selected,
    // rather than pinning the primary model's first metric across model switches.
    pivotMetrics: [],
  };
}
