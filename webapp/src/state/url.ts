import { isEmptyFilter, type DimFilter, type FilterMode, type FilterState } from "../lib/queries";
import { ALL_GRAINS, type DateRange } from "../lib/time";
import type { ExplorerState, ViewKind } from "./explorerState";

// Only selections + filters live in the URL — never result rows. This keeps shared links small
// and reproducible (the approved state contract).

const VIEWS: ViewKind[] = ["explore", "pivot"];
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

const FILTER_MODES = new Set<FilterMode>(["include", "exclude", "contains"]);

// Coerce one dimension's serialized filter into a valid DimFilter, or null if unusable.
// Backward-compatible: a bare string array (the pre-mode format) deserializes as an include filter,
// so old shared links keep working. The new format is a { mode, values, pattern? } object.
function coerceDimFilter(value: unknown): DimFilter | null {
  if (isStringArray(value)) return { mode: "include", values: value };
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  const obj = value as Record<string, unknown>;
  if (!FILTER_MODES.has(obj.mode as FilterMode)) return null;
  const mode = obj.mode as FilterMode;
  const values = isStringArray(obj.values) ? obj.values : [];
  const pattern = typeof obj.pattern === "string" ? obj.pattern : undefined;
  const filter: DimFilter = { mode, values, pattern };
  // Drop filters that would emit no SQL (e.g. a hand-edited empty include list).
  return isEmptyFilter(filter) ? null : filter;
}

// Reject hand-edited/malformed URLs (e.g. {"orders.status":42}) so the dashboard hydrates with a
// valid filter map instead of crashing downstream. Any unusable entry drops that dimension.
function parseFilterState(value: unknown): FilterState | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  const out: FilterState = {};
  for (const [dim, raw] of Object.entries(value as Record<string, unknown>)) {
    const filter = coerceDimFilter(raw);
    if (filter) out[dim] = filter;
  }
  return out;
}

/** Serialize a filter map. Include-only filters (the common case, and every leaderboard click)
 *  collapse to a bare value array so links stay short and identical to the pre-mode format. */
function serializeFilters(filters: FilterState): Record<string, string[] | DimFilter> {
  const out: Record<string, string[] | DimFilter> = {};
  for (const [dim, filter] of Object.entries(filters)) {
    out[dim] = filter.mode === "include" ? filter.values : filter;
  }
  return out;
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
  if (Object.keys(state.filters).length) params.set("filters", JSON.stringify(serializeFilters(state.filters)));
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

  const filters = parseFilterState(parseJson(params.get("filters")));
  if (filters) next.filters = filters;
  const pdims = parseJson(params.get("pdims"));
  if (isStringArray(pdims)) next.pivotDims = pdims;
  const pmetrics = parseJson(params.get("pmetrics"));
  if (isStringArray(pmetrics)) next.pivotMetrics = pmetrics;

  return next;
}
