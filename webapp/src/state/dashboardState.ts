import type { DashboardChart, DashboardDocument, DashboardTab } from "../data/dashboardTypes";
import { aliasOf, NULL_TOKEN, type ResultRow, type StructuredQuery } from "../data/types";
import { displayDimValue, sqlLiteral } from "../lib/format";
import { filterExprs, type DimTypes } from "../lib/queries";

export type DashboardRange = { from: string; to: string };

export type DashboardViewState = {
  tab: string;
  filters: Record<string, string>;
  ranges: Record<string, DashboardRange>;
  filterSources: Record<string, string>;
  rangeSources: Record<string, string>;
};

export type SavedDashboardView = {
  name: string;
  state: DashboardViewState;
};

const FILTER_PARAM = "dashboard_filters";
const RANGE_PARAM = "dashboard_ranges";
const FILTER_SOURCE_PARAM = "dashboard_filter_sources";
const RANGE_SOURCE_PARAM = "dashboard_range_sources";

export function shouldUseExplorer(pathname: string, search: string): boolean {
  if (pathname === "/explore") return true;
  const view = new URLSearchParams(search).get("view");
  return view === "explore" || view === "pivot";
}

export function selectableDashboardDimension(chart: DashboardChart, dimension: string): boolean {
  if (!dimension || !(chart.query.dimensions ?? []).includes(dimension)) return false;
  const select = chart.interactions?.select;
  if (select === true) return true;
  if (!select) return false;
  return !select.fields?.length || select.fields.includes(dimension);
}

export function dashboardDrillDimension(chart: DashboardChart): string | undefined {
  return (chart.query.dimensions ?? []).find((dimension) => selectableDashboardDimension(chart, dimension));
}

export function brushableDashboardDimension(chart: DashboardChart, dimension: string): boolean {
  if (!dimension || !(chart.query.dimensions ?? []).includes(dimension)) return false;
  const brush = chart.interactions?.brush;
  if (brush === true) return true;
  if (!brush || (brush.channel && brush.channel !== "x")) return false;
  return !brush.fields?.length || brush.fields.includes(dimension);
}

export function dashboardFilterValue(value: unknown): string {
  return value == null ? NULL_TOKEN : String(value);
}

function safeResultAlias(value: string): string {
  let alias = value.trim().replaceAll(/[^A-Za-z0-9_]/g, "_").replaceAll(/^_+|_+$/g, "") || "field";
  if (/^\d/.test(alias)) alias = `field_${alias}`;
  return alias;
}

export function dashboardResultColumn(ref: string, columns: string[]): string {
  const leafAlias = aliasOf(ref);
  const qualifiedAlias = safeResultAlias(ref);
  if (qualifiedAlias !== leafAlias && columns.includes(qualifiedAlias)) return qualifiedAlias;
  if (columns.includes(leafAlias)) return leafAlias;
  return columns.find((column) => column.endsWith(leafAlias)) ?? leafAlias;
}

export function dashboardMetricRefs(chart: DashboardChart): string[] {
  const encoded = chart.encoding?.y;
  if (Array.isArray(encoded) && encoded.length) return encoded;
  if (typeof encoded === "string") return [encoded];
  return chart.query.metrics.slice(0, 1);
}

const TIME_GRAINS = new Set(["second", "minute", "hour", "day", "week", "month", "quarter", "year"]);

function timeGrain(dimension: string): { baseDimension: string; grain: string } | null {
  const separator = dimension.lastIndexOf("__");
  if (separator < 0) return null;
  const grain = dimension.slice(separator + 2);
  if (!TIME_GRAINS.has(grain)) return null;
  return { baseDimension: dimension.slice(0, separator), grain };
}

export function dashboardChartType(chart: DashboardChart, types: DimTypes): "bar" | "line" | "area" {
  if (chart.type && chart.type !== "auto") return chart.type;
  const x = chart.encoding?.x ?? chart.query.dimensions?.[0] ?? "";
  const grained = timeGrain(x);
  const semanticType = types[x] ?? (grained ? types[grained.baseDimension] : undefined);
  return semanticType === "time" ? "line" : "bar";
}

function nextBucketStart(value: string, grain: string): string | null {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  if (grain === "second") date.setUTCSeconds(date.getUTCSeconds() + 1);
  else if (grain === "minute") date.setUTCMinutes(date.getUTCMinutes() + 1);
  else if (grain === "hour") date.setUTCHours(date.getUTCHours() + 1);
  else if (grain === "day") date.setUTCDate(date.getUTCDate() + 1);
  else if (grain === "week") date.setUTCDate(date.getUTCDate() + 7);
  else if (grain === "month") date.setUTCMonth(date.getUTCMonth() + 1);
  else if (grain === "quarter") date.setUTCMonth(date.getUTCMonth() + 3);
  else if (grain === "year") date.setUTCFullYear(date.getUTCFullYear() + 1);
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? date.toISOString().slice(0, 10) : date.toISOString();
}

export function dashboardRangeFilter(dimension: string, range: DashboardRange): string {
  const grained = timeGrain(dimension);
  if (grained) {
    const exclusiveEnd = nextBucketStart(range.to, grained.grain);
    if (exclusiveEnd) {
      return `${grained.baseDimension} >= ${sqlLiteral(range.from)} AND ${grained.baseDimension} < ${sqlLiteral(exclusiveEnd)}`;
    }
    return `${grained.baseDimension} >= ${sqlLiteral(range.from)} AND ${grained.baseDimension} <= ${sqlLiteral(range.to)}`;
  }
  return `${dimension} >= ${sqlLiteral(range.from)} AND ${dimension} <= ${sqlLiteral(range.to)}`;
}

function dashboardSelectionFilter(dimension: string, value: string, types: DimTypes): string[] {
  const grained = timeGrain(dimension);
  if (grained) {
    if (value === NULL_TOKEN) return [`${grained.baseDimension} IS NULL`];
    return [dashboardRangeFilter(dimension, { from: value, to: value })];
  }
  return filterExprs({ [dimension]: { mode: "include", values: [value] } }, { types });
}

export function dashboardChartScopeKey(document: DashboardDocument, chart: DashboardChart): string {
  const tab = document.tabs.find((candidate) => candidate.charts.includes(chart));
  return `${tab?.id ?? "dashboard"}:${chart.id}`;
}

function scopedDashboardInteractions(
  document: DashboardDocument,
  chart: DashboardChart,
  state: Pick<DashboardViewState, "filters" | "ranges" | "filterSources" | "rangeSources">,
): Pick<DashboardViewState, "filters" | "ranges"> {
  const scope = document.defaults?.interactions?.scope ?? "dashboard";
  if (scope === "dashboard") return state;
  if (scope === "chart") {
    const source = dashboardChartScopeKey(document, chart);
    return {
      filters: Object.fromEntries(
        Object.entries(state.filters).filter(([dimension]) => state.filterSources[dimension] === source),
      ),
      ranges: Object.fromEntries(
        Object.entries(state.ranges).filter(([dimension]) => state.rangeSources[dimension] === source),
      ),
    };
  }
  const charts = document.tabs.find((tab) => tab.charts.includes(chart))?.charts ?? [chart];
  const dimensions = new Set(charts.flatMap((candidate) => candidate.query.dimensions ?? []));
  return {
    filters: Object.fromEntries(Object.entries(state.filters).filter(([dimension]) => dimensions.has(dimension))),
    ranges: Object.fromEntries(Object.entries(state.ranges).filter(([dimension]) => dimensions.has(dimension))),
  };
}

export function dashboardStructuredQuery(
  document: DashboardDocument,
  chart: DashboardChart,
  filters: DashboardViewState["filters"],
  types: DimTypes,
  ranges: DashboardViewState["ranges"] = {},
  sources: Pick<DashboardViewState, "filterSources" | "rangeSources"> = { filterSources: {}, rangeSources: {} },
): StructuredQuery {
  const scoped = scopedDashboardInteractions(document, chart, { filters, ranges, ...sources });
  const x = chart.encoding?.x ?? chart.query.dimensions?.[0] ?? "";
  const explicitOrder = chart.query.order_by ?? chart.query.orderBy;
  const defaultTimeOrder = x && ["line", "area"].includes(dashboardChartType(chart, types)) ? [`${x} ASC`] : undefined;
  const request: StructuredQuery = {
    metrics: chart.query.metrics,
    dimensions: chart.query.dimensions,
    filters: [
      ...(chart.query.filters ?? []),
      ...Object.entries(scoped.filters).flatMap(([dimension, value]) =>
        dashboardSelectionFilter(dimension, value, types),
      ),
      ...Object.entries(scoped.ranges).map(([dimension, range]) => dashboardRangeFilter(dimension, range)),
    ],
    segments: chart.query.segments,
    orderBy: explicitOrder ?? defaultTimeOrder,
    limit: chart.query.limit ?? 500,
  };
  const usePreaggregations =
    chart.query.use_preaggregations ??
    chart.query.usePreaggregations ??
    document.defaults?.query?.use_preaggregations ??
    document.defaults?.query?.usePreaggregations;
  if (usePreaggregations !== undefined) request.usePreaggregations = usePreaggregations;
  return request;
}

function explorerDate(value: string): string | null {
  const match = /^(\d{4}-\d{2}-\d{2})/.exec(value);
  return match?.[1] ?? null;
}

function explorerRange(dimension: string, range: DashboardRange): DashboardRange | null {
  const from = explorerDate(range.from);
  if (!from) return null;
  const grained = timeGrain(dimension);
  if (!grained) {
    const to = explorerDate(range.to);
    return to ? { from, to } : null;
  }
  const exclusiveEnd = nextBucketStart(range.to, grained.grain);
  if (!exclusiveEnd) return null;
  const inclusiveEnd = new Date(exclusiveEnd);
  inclusiveEnd.setUTCMilliseconds(inclusiveEnd.getUTCMilliseconds() - 1);
  return { from, to: inclusiveEnd.toISOString().slice(0, 10) };
}

export function dashboardExploreUrl(document: DashboardDocument, chart: DashboardChart, state: DashboardViewState): string {
  const encoded = chart.encoding?.y;
  const metric = (Array.isArray(encoded) ? encoded[0] : encoded) ?? chart.query.metrics[0] ?? "";
  const dimensions = chart.query.dimensions ?? [];
  const model = metric.includes(".") ? metric.split(".")[0] : dimensions[0]?.split(".")[0] ?? "";
  const scoped = scopedDashboardInteractions(document, chart, state);
  const explorerFilters = Object.fromEntries(Object.entries(scoped.filters).map(([dimension, value]) => [dimension, [value]]));
  const params = new URLSearchParams({ view: "explore", model, metric });
  if (Object.keys(explorerFilters).length) params.set("filters", JSON.stringify(explorerFilters));

  const xDimension = chart.encoding?.x ?? dimensions[0];
  const range = xDimension ? scoped.ranges[xDimension] : undefined;
  const dateRange = xDimension && range ? explorerRange(xDimension, range) : null;
  if (dateRange) {
    params.set("from", dateRange.from);
    params.set("to", dateRange.to);
  }
  return `/explore?${params}`;
}

export type DashboardTimeSeries = {
  label: string;
  points: { x: string; y: number }[];
};

export type DashboardCategorySeries = {
  label: string;
  data: { label: string; filterValue: string; value: number }[];
};

export function dashboardCategorySeries(
  rows: ResultRow[],
  xColumn: string,
  yColumn: string,
  seriesColumns: string[],
): DashboardCategorySeries[] {
  const grouped = new Map<string, DashboardCategorySeries>();
  for (const row of rows) {
    const value = Number(row[yColumn]);
    if (!Number.isFinite(value)) continue;
    const seriesValues = seriesColumns.map((column) => dashboardFilterValue(row[column]));
    const key = JSON.stringify(seriesValues);
    const label = seriesValues.length ? seriesValues.map(displayDimValue).join(" · ") : "Current";
    const series = grouped.get(key) ?? { label, data: [] };
    const filterValue = dashboardFilterValue(row[xColumn]);
    series.data.push({ label: displayDimValue(filterValue), filterValue, value });
    grouped.set(key, series);
  }
  return [...grouped.values()];
}

export function dashboardTimeSeries(
  rows: ResultRow[],
  xColumn: string,
  yColumn: string,
  seriesColumns: string[],
): DashboardTimeSeries[] {
  const xValues: string[] = [];
  const seenX = new Set<string>();
  const grouped = new Map<string, { label: string; values: Map<string, number> }>();

  for (const row of rows) {
    if (row[xColumn] == null) continue;
    const x = String(row[xColumn]);
    const y = Number(row[yColumn]);
    if (!Number.isFinite(y)) continue;
    if (!seenX.has(x)) {
      seenX.add(x);
      xValues.push(x);
    }
    const seriesValues = seriesColumns.map((column) => dashboardFilterValue(row[column]));
    const key = JSON.stringify(seriesValues);
    const label = seriesValues.length ? seriesValues.map(displayDimValue).join(" · ") : "Current";
    const series = grouped.get(key) ?? { label, values: new Map<string, number>() };
    series.values.set(x, y);
    grouped.set(key, series);
  }

  return [...grouped.values()].map((series) => ({
    label: series.label,
    points: xValues.map((x) => ({ x, y: series.values.get(x) ?? Number.NaN })),
  }));
}

function validFilters(value: unknown, allowedDimensions: Set<string>): Record<string, string> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return {};
  const filters: Record<string, string> = {};
  for (const [dimension, filterValue] of Object.entries(value as Record<string, unknown>)) {
    if (allowedDimensions.has(dimension) && typeof filterValue === "string") filters[dimension] = filterValue;
  }
  return filters;
}

function validRanges(value: unknown, allowedDimensions: Set<string>): Record<string, DashboardRange> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return {};
  const ranges: Record<string, DashboardRange> = {};
  for (const [dimension, candidate] of Object.entries(value as Record<string, unknown>)) {
    if (!allowedDimensions.has(dimension) || typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) continue;
    const range = candidate as { from?: unknown; to?: unknown };
    if (typeof range.from === "string" && typeof range.to === "string") {
      ranges[dimension] = { from: range.from, to: range.to };
    }
  }
  return ranges;
}

function validSources(value: unknown, allowedDimensions: Set<string>): Record<string, string> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).filter(
      (entry): entry is [string, string] => allowedDimensions.has(entry[0]) && typeof entry[1] === "string",
    ),
  );
}

export function dashboardDimensions(document: DashboardDocument): Set<string> {
  return new Set(document.tabs.flatMap((tab) => tab.charts.flatMap((chart) => chart.query.dimensions ?? [])));
}

export function decodeDashboardState(search: string, document: DashboardDocument): DashboardViewState {
  const params = new URLSearchParams(search);
  const fallbackTab = document.tabs[0]?.id ?? "";
  const requestedTab = params.get("tab");
  const tab = document.tabs.some((candidate) => candidate.id === requestedTab) ? requestedTab ?? fallbackTab : fallbackTab;
  let parsedFilters: unknown;
  let parsedRanges: unknown;
  let parsedFilterSources: unknown;
  let parsedRangeSources: unknown;
  try {
    parsedFilters = JSON.parse(params.get(FILTER_PARAM) ?? "{}");
  } catch {
    parsedFilters = {};
  }
  try {
    parsedRanges = JSON.parse(params.get(RANGE_PARAM) ?? "{}");
  } catch {
    parsedRanges = {};
  }
  try {
    parsedFilterSources = JSON.parse(params.get(FILTER_SOURCE_PARAM) ?? "{}");
  } catch {
    parsedFilterSources = {};
  }
  try {
    parsedRangeSources = JSON.parse(params.get(RANGE_SOURCE_PARAM) ?? "{}");
  } catch {
    parsedRangeSources = {};
  }
  const dimensions = dashboardDimensions(document);
  return {
    tab,
    filters: validFilters(parsedFilters, dimensions),
    ranges: validRanges(parsedRanges, dimensions),
    filterSources: validSources(parsedFilterSources, dimensions),
    rangeSources: validSources(parsedRangeSources, dimensions),
  };
}

export function encodeDashboardState(state: DashboardViewState, document: DashboardDocument): string {
  const params = new URLSearchParams();
  if (state.tab && state.tab !== document.tabs[0]?.id) params.set("tab", state.tab);
  if (Object.keys(state.filters).length) params.set(FILTER_PARAM, JSON.stringify(state.filters));
  if (Object.keys(state.ranges).length) params.set(RANGE_PARAM, JSON.stringify(state.ranges));
  const filterSources = Object.fromEntries(
    Object.entries(state.filterSources).filter(([dimension]) => dimension in state.filters),
  );
  const rangeSources = Object.fromEntries(
    Object.entries(state.rangeSources).filter(([dimension]) => dimension in state.ranges),
  );
  if (Object.keys(filterSources).length) params.set(FILTER_SOURCE_PARAM, JSON.stringify(filterSources));
  if (Object.keys(rangeSources).length) params.set(RANGE_SOURCE_PARAM, JSON.stringify(rangeSources));
  return params.toString();
}

export function dashboardStorageKey(document: DashboardDocument): string {
  const identity = `${document.schema ?? "sidemantic.dashboard.v1"}:${document.title}`;
  return `sidemantic-dashboard-views:${identity}`;
}

export function loadSavedDashboardViews(document: DashboardDocument): SavedDashboardView[] {
  try {
    const parsed = JSON.parse(localStorage.getItem(dashboardStorageKey(document)) ?? "[]") as unknown;
    if (!Array.isArray(parsed)) return [];
    const dimensions = dashboardDimensions(document);
    return parsed.flatMap((item): SavedDashboardView[] => {
      if (typeof item !== "object" || item === null || Array.isArray(item)) return [];
      const candidate = item as { name?: unknown; state?: unknown };
      if (typeof candidate.name !== "string" || !candidate.name.trim()) return [];
      if (typeof candidate.state !== "object" || candidate.state === null || Array.isArray(candidate.state)) return [];
      const rawState = candidate.state as {
        tab?: unknown;
        filters?: unknown;
        ranges?: unknown;
        filterSources?: unknown;
        rangeSources?: unknown;
      };
      const tab = document.tabs.some((entry) => entry.id === rawState.tab)
        ? String(rawState.tab)
        : document.tabs[0]?.id ?? "";
      return [{
        name: candidate.name,
        state: {
          tab,
          filters: validFilters(rawState.filters, dimensions),
          ranges: validRanges(rawState.ranges, dimensions),
          filterSources: validSources(rawState.filterSources, dimensions),
          rangeSources: validSources(rawState.rangeSources, dimensions),
        },
      }];
    });
  } catch {
    return [];
  }
}

export function storeSavedDashboardViews(document: DashboardDocument, views: SavedDashboardView[]): boolean {
  try {
    localStorage.setItem(dashboardStorageKey(document), JSON.stringify(views));
    return true;
  } catch {
    return false;
  }
}

function csvCell(value: unknown): string {
  if (value == null) return "";
  const text = String(value);
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function rowsToCsv(columns: string[], rows: ResultRow[]): string {
  return [columns.map(csvCell).join(","), ...rows.map((row) => columns.map((column) => csvCell(row[column])).join(","))].join("\r\n");
}

export function tabLabel(tab: DashboardTab): string {
  return tab.label?.trim() || tab.id;
}
