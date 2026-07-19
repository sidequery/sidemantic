import type { DashboardChart, DashboardDocument, DashboardTab } from "../data/dashboardTypes";
import { NULL_TOKEN, type ResultRow } from "../data/types";

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
