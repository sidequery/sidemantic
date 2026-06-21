import type { FilterState } from "../lib/queries";
import type { DateRange } from "../lib/time";
import type { ExplorerState, ViewKind } from "./explorerState";

// Only selections + filters live in the URL — never result rows. This keeps shared links small
// and reproducible (the approved state contract).

const VIEWS: ViewKind[] = ["explore", "pivot"];

function parseJson<T>(value: string | null): T | undefined {
  if (!value) return undefined;
  try {
    return JSON.parse(value) as T;
  } catch {
    return undefined;
  }
}

export function encodeState(state: ExplorerState): string {
  const params = new URLSearchParams();
  params.set("view", state.view);
  if (state.model) params.set("model", state.model);
  if (state.selectedMetric) params.set("metric", state.selectedMetric);
  if (state.grain) params.set("grain", state.grain);
  if (state.dateRange) {
    params.set("from", state.dateRange.from);
    params.set("to", state.dateRange.to);
  }
  if (Object.keys(state.filters).length) params.set("filters", JSON.stringify(state.filters));
  if (state.pivotDims.length) params.set("pdims", JSON.stringify(state.pivotDims));
  if (state.pivotMetrics.length) params.set("pmetrics", JSON.stringify(state.pivotMetrics));
  return params.toString();
}

/** Decode URL params onto a catalog-derived base state. Unknown/missing params fall back. */
export function decodeState(search: string, base: ExplorerState): ExplorerState {
  const params = new URLSearchParams(search);
  const next: ExplorerState = { ...base };

  const view = params.get("view");
  if (view && VIEWS.includes(view as ViewKind)) next.view = view as ViewKind;
  const model = params.get("model");
  if (model) next.model = model;
  const metric = params.get("metric");
  if (metric) next.selectedMetric = metric;
  const grain = params.get("grain");
  if (grain) next.grain = grain as ExplorerState["grain"];

  const from = params.get("from");
  const to = params.get("to");
  if (from && to) next.dateRange = { from, to } satisfies DateRange;

  const filters = parseJson<FilterState>(params.get("filters"));
  if (filters) next.filters = filters;
  const pdims = parseJson<string[]>(params.get("pdims"));
  if (pdims) next.pivotDims = pdims;
  const pmetrics = parseJson<string[]>(params.get("pmetrics"));
  if (pmetrics) next.pivotMetrics = pmetrics;

  return next;
}
