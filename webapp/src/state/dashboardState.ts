import type { DashboardChart, DashboardDocument, DashboardTab } from "../data/dashboardTypes";
import { aliasOf, NULL_TOKEN, type ResultRow, type StructuredQuery } from "../data/types";
import { filterExprs, type DimTypes, type FilterState } from "../lib/queries";

export type DashboardViewState = {
  tab: string;
  filters: Record<string, string>;
};

export type SavedDashboardView = {
  name: string;
  state: DashboardViewState;
};

const FILTER_PARAM = "dashboard_filters";

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

export function dashboardStructuredQuery(
  document: DashboardDocument,
  chart: DashboardChart,
  filters: DashboardViewState["filters"],
  types: DimTypes,
): StructuredQuery {
  const filterState: FilterState = Object.fromEntries(
    Object.entries(filters).map(([dimension, value]) => [dimension, { mode: "include", values: [value] }]),
  );
  const request: StructuredQuery = {
    metrics: chart.query.metrics,
    dimensions: chart.query.dimensions,
    filters: [...(chart.query.filters ?? []), ...filterExprs(filterState, { types })],
    segments: chart.query.segments,
    orderBy: chart.query.order_by ?? chart.query.orderBy,
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

export type DashboardTimeSeries = {
  label: string;
  points: { x: string; y: number }[];
};

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
    const label = seriesValues.length ? seriesValues.map((value) => (value === NULL_TOKEN ? "—" : value)).join(" · ") : "Current";
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

export function dashboardDimensions(document: DashboardDocument): Set<string> {
  return new Set(document.tabs.flatMap((tab) => tab.charts.flatMap((chart) => chart.query.dimensions ?? [])));
}

export function decodeDashboardState(search: string, document: DashboardDocument): DashboardViewState {
  const params = new URLSearchParams(search);
  const fallbackTab = document.tabs[0]?.id ?? "";
  const requestedTab = params.get("tab");
  const tab = document.tabs.some((candidate) => candidate.id === requestedTab) ? requestedTab ?? fallbackTab : fallbackTab;
  let parsedFilters: unknown;
  try {
    parsedFilters = JSON.parse(params.get(FILTER_PARAM) ?? "{}");
  } catch {
    parsedFilters = {};
  }
  return { tab, filters: validFilters(parsedFilters, dashboardDimensions(document)) };
}

export function encodeDashboardState(state: DashboardViewState, document: DashboardDocument): string {
  const params = new URLSearchParams();
  if (state.tab && state.tab !== document.tabs[0]?.id) params.set("tab", state.tab);
  if (Object.keys(state.filters).length) params.set(FILTER_PARAM, JSON.stringify(state.filters));
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
      const rawState = candidate.state as { tab?: unknown; filters?: unknown };
      const tab = document.tabs.some((entry) => entry.id === rawState.tab)
        ? String(rawState.tab)
        : document.tabs[0]?.id ?? "";
      return [{ name: candidate.name, state: { tab, filters: validFilters(rawState.filters, dimensions) } }];
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
