import type { FilterState } from "../lib/queries";
import { ALL_GRAINS, type DateRange } from "../lib/time";
import type { ExplorerState, ViewKind } from "./explorerState";

// Only selections + filters live in the URL — never result rows. This keeps shared links small
// and reproducible (the approved state contract).

const VIEWS: ViewKind[] = ["home", "explore", "pivot"];
const GRAINS = new Set<string>(ALL_GRAINS);

function parseJson(value: string | null): unknown {
  if (!value) return undefined;
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

// Accept only YYYY-MM-DD calendar dates, so a hand-edited ?from/&to can't seed the date helpers
// with values that parse to NaN and break series/leaderboard queries.
function isIsoDate(value: string | null): value is string {
  return value != null && /^\d{4}-\d{2}-\d{2}$/.test(value) && !Number.isNaN(Date.parse(value));
}

// Reject hand-edited/malformed URLs (e.g. {"orders.status":"CA"}) so the dashboard hydrates with a
// valid filter map instead of crashing downstream on `values.map(...)`.
function isFilterState(value: unknown): value is FilterState {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  return Object.values(value as Record<string, unknown>).every(isStringArray);
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
  if (grain && GRAINS.has(grain)) next.grain = grain as ExplorerState["grain"];

  const from = params.get("from");
  const to = params.get("to");
  if (isIsoDate(from) && isIsoDate(to) && from <= to) next.dateRange = { from, to } satisfies DateRange;

  const filters = parseJson(params.get("filters"));
  if (isFilterState(filters)) next.filters = filters;
  const pdims = parseJson(params.get("pdims"));
  if (isStringArray(pdims)) next.pivotDims = pdims;
  const pmetrics = parseJson(params.get("pmetrics"));
  if (isStringArray(pmetrics)) next.pivotMetrics = pmetrics;

  return next;
}
