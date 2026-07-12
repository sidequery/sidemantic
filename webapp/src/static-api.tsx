import { createElement, Fragment, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { flushSync } from "react-dom";
import { DataTable } from "./components/DataTable";
import { FilterPill } from "./components/FilterPill";
import { Leaderboard } from "./components/Leaderboard";
import { MetricCard } from "./components/MetricCard";
import { QueryDebugPanel } from "./components/QueryDebugPanel";
import { EmptyState, ErrorState, LoadingState } from "./components/States";
import { aliasForSemanticRef, formatUiValue, normalizeFilterValue } from "./lib/uiCore.js";
export {
  aliasForSemanticRef,
  formatUiCompact as formatCompact,
  formatUiValue as formatValue,
  labelize,
  normalizeFilterValue,
  removeFilterDimension,
  removeFilterValue,
  toggleFilterValue,
} from "./lib/uiCore.js";
export { applyTheme, applyThemeTokens, getTheme, toggleTheme } from "./lib/theme";

type Row = Record<string, string | number | boolean | null>;
type Query = {
  dimensions?: string[];
  metrics?: string[];
  output_aliases?: Record<string, string>;
  result?: { columns?: string[]; rows?: Row[]; sample_rows?: Row[] };
};

const roots = new WeakMap<Element, Root>();

function mount(container: Element, node: ReactNode) {
  let root = roots.get(container);
  if (!root) {
    container.replaceChildren();
    root = createRoot(container);
    roots.set(container, root);
  }
  flushSync(() => root!.render(node));
}

function rowsOf(query: Query) {
  return query.result?.sample_rows ?? query.result?.rows ?? [];
}

function aliasFor(query: Query, ref = "") {
  return query.output_aliases?.[ref] ?? aliasForSemanticRef(ref);
}

function resolveFormat(format: any, context: unknown): any {
  return typeof format === "function" ? format(context) : format;
}

export function metricConfigFor(metrics: any[] = [], metricKey: string) {
  return metrics.find((metric) => metric.key === metricKey) ?? metrics[0] ?? {};
}

export function metricValueFormat(metrics: any[] = [], metricKey: string) {
  return metricConfigFor(metrics, metricKey).format === "currency"
    ? { currency: "USD", maximumFractionDigits: 0, style: "currency" }
    : { maximumFractionDigits: 0 };
}

export function renderSelectOptions(select: HTMLSelectElement, options: any[], selectedValue: string, config: Record<string, any> = {}) {
  select.replaceChildren(...options.map((option) => {
    const value = String(config.value?.(option) ?? option.key ?? option.value ?? option);
    const node = document.createElement("option");
    node.value = value;
    node.textContent = String(config.label?.(option) ?? option.label ?? value);
    node.selected = value === selectedValue;
    return node;
  }));
}

export function syncScrollPosition(source: HTMLElement, target: HTMLElement) {
  target.scrollLeft = source.scrollLeft;
  target.scrollTop = source.scrollTop;
}

export function filterZeroMetricRows(result: {columns?: string[]; rows?: Row[]; sample_rows?: Row[]}, metricKey: string) {
  return { columns: result.columns ?? [], rows: (result.rows ?? result.sample_rows ?? []).filter((row) => Number(row[metricKey]) !== 0) };
}

export function highlightCode(element: HTMLElement, source: string) {
  // Highlighting presentation belongs to QueryDebugPanel; editable source remains safe plain text.
  element.textContent = source;
}

export function toComponentResult(result: { columns?: string[]; rows?: Row[]; sample_rows?: Row[] } = {}) {
  return { columns: result.columns ?? [], sample_rows: result.sample_rows ?? result.rows ?? [] };
}

export function toComponentQuery({ dimensions = [], metrics = [], result, outputAliases }: {
  dimensions?: string[]; metrics?: string[]; result?: Query["result"]; outputAliases?: Record<string, string>;
} = {}): Query {
  return { dimensions, metrics, output_aliases: outputAliases, result: toComponentResult(result) };
}

export function renderMetricCards(container: Element, query: Query, options: Record<string, any> = {}) {
  const row = rowsOf(query)[0] ?? {};
  mount(container, createElement(Fragment, null, ...(query.metrics ?? []).map((metric) => {
    const key = aliasFor(query, metric);
    const format = typeof options.valueFormat === "function" ? options.valueFormat({ metric, key, value: row[key] }) : options.valueFormat;
    return createElement(MetricCard, {
      key: metric,
      metric,
      label: options.labels?.[metric] ?? aliasForSemanticRef(metric).replaceAll("_", " "),
      value: row[key],
      format,
      delta: options.deltas?.[metric],
      selected: options.selectedMetric === metric,
      onSelect: options.onSelect ? () => options.onSelect({ metric, key, value: row[key] }) : undefined,
    });
  })));
}

export function renderMetricSummaryCards(container: Element, config: Record<string, any> = {}) {
  const totals = config.totals?.rows?.[0] ?? {};
  const series = config.seriesRows ?? [];
  mount(container, createElement(Fragment, null, ...(config.metrics ?? []).map((metric: Record<string, any>) => {
    const key = metric.key;
    const alias = aliasForSemanticRef(key);
    return createElement(MetricCard, {
      key,
      metric: key,
      label: metric.label ?? alias.replaceAll("_", " "),
      value: totals[alias],
      format: resolveFormat(config.valueFormat, key),
      selected: config.selectedMetric === key,
      sparkValues: series.map((row: Row) => Number(row[alias]) || 0),
      sparkLabels: series.map((row: Row) => String(row[config.timeKey] ?? "")),
      onSparkBrush: config.onBrush ? (range: { from: string; to: string } | null) => config.onBrush(range?.from ?? null, range?.to ?? null) : undefined,
      onSelect: config.onSelect ? () => config.onSelect({ metric: key, key: alias, value: totals[alias] }) : undefined,
    });
  })));
}

export function renderLeaderboard(container: Element, query: Query, options: Record<string, any> = {}) {
  const dimension = query.dimensions?.[0] ?? "";
  const metric = options.metricRef ?? query.metrics?.[0] ?? "";
  const dimensionKey = aliasFor(query, dimension);
  const metricKey = aliasFor(query, metric);
  const allRows = rowsOf(query);
  const expanded = options.expanded ?? (container as any).__sdmExpanded ?? false;
  mount(container, createElement(Leaderboard, {
    dimension,
    title: options.dimensionLabel ?? dimensionKey.replaceAll("_", " "),
    metricLabel: options.metricLabel ?? metricKey.replaceAll("_", " "),
    rows: allRows.map((row) => ({ value: normalizeFilterValue(row[dimensionKey]), metric: Number(row[metricKey]) || 0 })),
    selectedValues: options.selectedValues ?? (options.selectedValue === undefined ? [] : [options.selectedValue]),
    collapsedLimit: options.limit || allRows.length,
    expanded,
    formatMetric: (value: number) => formatUiValue(value, typeof options.valueFormat === "function" ? options.valueFormat({ metric, key: metricKey, value }) : options.valueFormat),
    onToggle: options.onSelect ? (value: string) => options.onSelect({ dimension, value, row: allRows.find((row) => normalizeFilterValue(row[dimensionKey]) === value) }) : undefined,
    onExpandedChange: options.expandable ? (next: boolean) => {
      (container as any).__sdmExpanded = next;
      options.onToggleExpand?.(next);
      renderLeaderboard(container, query, options);
    } : undefined,
  }));
}

export function renderDimensionLeaderboardCards(container: Element, dimensions: any[], config: Record<string, any> = {}) {
  const expandedDimension = (container as any).__sdmExpandedDim as string | undefined;
  const visible = expandedDimension ? dimensions.filter((item) => (item.key ?? item) === expandedDimension) : dimensions;
  mount(container, createElement(Fragment, null, ...visible.map((item) => {
    const dimension = item.key ?? item;
    const result = config.resultForDimension?.(item) ?? { rows: [] };
    const query = toComponentQuery({ dimensions: [dimension], metrics: [config.metricRef], result });
    const rows = rowsOf(query);
    const expanded = expandedDimension === dimension;
    return createElement(Leaderboard, {
      key: dimension,
      dimension,
      title: item.label ?? aliasForSemanticRef(dimension).replaceAll("_", " "),
      metricLabel: config.metricLabel?.(item) ?? config.metricName ?? aliasForSemanticRef(config.metricRef),
      rows: rows.map((row) => ({ value: normalizeFilterValue(row[aliasFor(query, dimension)]), metric: Number(row[aliasFor(query, config.metricRef)]) || 0 })),
      selectedValues: config.selectedValuesForDimension?.(item) ?? [],
      collapsedLimit: config.limit ?? 6,
      expanded,
      formatMetric: (value: number) => formatUiValue(value, resolveFormat(config.valueFormat, { metric: config.metricRef, value })),
      onToggle: config.onSelect ? (value: string) => config.onSelect({ dimension, value, row: rows.find((row) => normalizeFilterValue(row[aliasFor(query, dimension)]) === value) }) : undefined,
      onExpandedChange: config.expandable === false ? undefined : (next: boolean) => {
        (container as any).__sdmExpandedDim = next ? dimension : undefined;
        renderDimensionLeaderboardCards(container, dimensions, config);
      },
    });
  })));
}

export function renderFilterPills(container: Element, filters: Record<string, unknown[]>, onRemove?: (filter: {dimension: string; value: string}) => void, options: Record<string, any> = {}) {
  const pills = Object.entries(filters ?? {}).flatMap(([dimension, values]) => (values ?? []).map((value) => {
    const normalized = normalizeFilterValue(value);
    return createElement(FilterPill, { key: `${dimension}:${normalized}`, dimension, value: normalized, onRemove: onRemove ? () => onRemove({ dimension, value: normalized }) : undefined });
  }));
  mount(container, pills.length ? createElement(Fragment, null, ...pills) : options.emptyLabel ? createElement("span", { className: "text-faint" }, options.emptyLabel) : null);
}

export function renderHighlightedQueryDebug(container: Element, queries: Record<string, {sql?: string} | string>) {
  mount(container, createElement(QueryDebugPanel, { queries: Object.fromEntries(Object.entries(queries ?? {}).map(([name, query]) => [name, typeof query === "string" ? query : query?.sql])) }));
}

export const renderQueryDebug = renderHighlightedQueryDebug;

export function renderDataPreview(container: Element, result: {columns?: string[]; rows?: Row[]; sample_rows?: Row[]}, options: Record<string, any> = {}) {
  const columns = result?.columns ?? [];
  mount(container, createElement(DataTable, {
    columns: columns.map((key) => ({ key, label: key.replaceAll("_", " "), numeric: (result.sample_rows ?? result.rows ?? []).some((row) => typeof row[key] === "number") })),
    rows: result.sample_rows ?? result.rows ?? [],
    pageSize: options.pageSize || 50,
    renderCell: (_column: unknown, value: unknown) => formatUiValue(value),
  }));
}

export function renderState(container: Element, state: {kind: string; message: string}) {
  const Component = state.kind === "error" ? ErrorState : state.kind === "loading" ? LoadingState : EmptyState;
  mount(container, createElement(Component, { message: state.message }));
}

export function renderValidationState(stateElement: HTMLElement, listElement: HTMLElement, errors: string[] = []) {
  stateElement.textContent = errors.length ? "Invalid" : "Valid";
  stateElement.dataset.valid = String(errors.length === 0);
  listElement.replaceChildren(...errors.map((error) => Object.assign(document.createElement("li"), { textContent: error })));
}

export function setControlsDisabled(selector: string, disabled: boolean) {
  document.querySelectorAll<HTMLButtonElement | HTMLInputElement | HTMLSelectElement>(selector).forEach((control) => { control.disabled = disabled; });
}
