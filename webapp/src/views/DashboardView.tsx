import { useEffect, useMemo, useState } from "react";
import { ColumnChart } from "../components/ColumnChart";
import { DataTable, type Column } from "../components/DataTable";
import { ErrorState, LoadingState } from "../components/States";
import { FilterPill } from "../components/FilterPill";
import { Leaderboard } from "../components/Leaderboard";
import { MetricCard } from "../components/MetricCard";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { ThemeToggle } from "../components/ThemeToggle";
import { TimeSeriesChart, type BrushRange } from "../components/TimeSeriesChart";
import type { SidemanticBackend } from "../data/backend";
import {
  NULL_TOKEN,
  aliasOf,
  type Catalog,
  type CatalogMetric,
  type DashboardChart,
  type DashboardChartType,
  type DashboardSpec,
  type QueryResult,
  type ResultRow,
  type StructuredQuery,
} from "../data/types";
import { displayDimValue, formatValue, labelize } from "../lib/format";
import { dimTypes, filterExprs, includeFilter, type FilterState } from "../lib/queries";
import { dateOnly, endOfBucket, timeFilters } from "../lib/time";
import { useQueryActive } from "../state/queryActivity";
import { useQueryResult } from "../state/useQueryResult";

type Selection = { values: string[]; sourceChart: string };
type Selections = Record<string, Selection>;
type TimeSelection = { from: string; to: string; sourceChart: string };
type TimeSelections = Record<string, TimeSelection>;
type InteractionScope = "chart" | "tab" | "dashboard";

const DASHBOARD_FILTERS_PARAM = "dashboardFilters";
const DASHBOARD_RANGES_PARAM = "dashboardRanges";

function parseStored<T>(name: string, fallback: T): T {
  try {
    const raw = new URLSearchParams(window.location.search).get(name);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
}

function normalizedValue(value: unknown): string {
  return value == null ? NULL_TOKEN : String(value);
}

function stripGrain(ref: string): string {
  const marker = ref.lastIndexOf("__");
  return marker > ref.lastIndexOf(".") ? ref.slice(0, marker) : ref;
}

function grainOf(ref: string): "hour" | "day" | "week" | "month" | "quarter" | "year" {
  const grain = ref.split("__").at(-1);
  return grain === "hour" || grain === "week" || grain === "month" || grain === "quarter" || grain === "year"
    ? grain
    : "day";
}

function chartKind(chart: DashboardChart): DashboardChartType {
  if (chart.type && chart.type !== "auto") return chart.type;
  const dimensions = chart.query.dimensions ?? [];
  if (!dimensions.length) return "kpi";
  if (dimensions.some((ref) => /__(hour|day|week|month|quarter|year)$/.test(ref))) return "line";
  return "bar";
}

function yMetrics(chart: DashboardChart): string[] {
  const encoded = chart.encoding?.y;
  if (Array.isArray(encoded)) return encoded;
  if (encoded) return [encoded];
  return chart.query.metrics;
}

/** Resolve the generator's collision-safe result alias for one semantic ref. */
export function dashboardResultAlias(ref: string, selectedRefs: string[], result?: QueryResult): string {
  const bare = aliasOf(ref);
  if (result?.columns.includes(bare)) return bare;
  const duplicates = selectedRefs.filter((candidate) => aliasOf(candidate) === bare).length;
  if (duplicates > 1 && ref.includes(".")) {
    const prefixed = `${ref.split(".", 1)[0]}_${bare}`;
    if (!result || result.columns.includes(prefixed)) return prefixed;
  }
  return result?.columns.find((column) => column === ref || column.endsWith(`_${bare}`)) ?? bare;
}

function metricInfo(catalog: Catalog, ref: string): CatalogMetric | undefined {
  for (const model of catalog.models) {
    const found = model.metrics.find((metric) => metric.ref === ref);
    if (found) return found;
  }
  return catalog.graphMetrics.find((metric) => metric.ref === ref || metric.name === ref);
}

function dimensionLabel(catalog: Catalog, ref: string): string {
  const base = stripGrain(ref);
  for (const model of catalog.models) {
    const found = model.dimensions.find((dimension) => dimension.ref === base);
    if (found) return found.label;
  }
  return labelize(aliasOf(ref));
}

function allDimensionTypes(catalog: Catalog) {
  return dimTypes(catalog.models.flatMap((model) => model.dimensions));
}

function selectionFilterState(
  selections: Selections,
  chartId: string,
  scope: InteractionScope,
): FilterState {
  const filters: FilterState = {};
  for (const [dimension, selection] of Object.entries(selections)) {
    if (scope === "chart" && selection.sourceChart !== chartId) continue;
    // Keep a source chart's complete categorical context visible so selected
    // bars/rows can be highlighted while every peer chart is filtered.
    if (scope !== "chart" && selection.sourceChart === chartId) continue;
    filters[dimension] = includeFilter(selection.values);
  }
  return filters;
}

function chartQuery(
  chart: DashboardChart,
  selections: Selections,
  ranges: TimeSelections,
  scope: InteractionScope,
  dimensionTypes: Record<string, string>,
  defaultUsePreaggregations: boolean | undefined,
): StructuredQuery {
  const query = chart.query;
  const filters = [
    ...(query.filters ?? []),
    ...filterExprs(selectionFilterState(selections, chart.id, scope), { types: dimensionTypes }),
  ];
  for (const [dimension, range] of Object.entries(ranges)) {
    if (scope === "chart" && range.sourceChart !== chart.id) continue;
    filters.push(...timeFilters(stripGrain(dimension), { from: dateOnly(range.from), to: dateOnly(range.to) }));
  }
  return {
    metrics: query.metrics,
    dimensions: query.dimensions,
    filters,
    segments: query.segments,
    orderBy: query.order_by ?? query.orderBy,
    limit: query.limit,
    usePreaggregations:
      query.use_preaggregations ?? query.usePreaggregations ?? defaultUsePreaggregations,
  };
}

function panelSpan(chart: DashboardChart, kind: DashboardChartType): string {
  if (chart.layout?.colSpan === 1) return "lg:col-span-1";
  if (chart.layout?.colSpan === 2 || kind === "line" || kind === "area" || kind === "table") {
    return "lg:col-span-2";
  }
  return "lg:col-span-1";
}

function ChartPanel({
  chart,
  catalog,
  backend,
  selections,
  ranges,
  scope,
  defaultUsePreaggregations,
  onToggle,
  onBrush,
}: {
  chart: DashboardChart;
  catalog: Catalog;
  backend: SidemanticBackend;
  selections: Selections;
  ranges: TimeSelections;
  scope: InteractionScope;
  defaultUsePreaggregations?: boolean;
  onToggle: (dimension: string, value: string, sourceChart: string) => void;
  onBrush: (dimension: string, range: BrushRange | null, sourceChart: string) => void;
}) {
  const kind = chartKind(chart);
  const dimensionTypes = useMemo(() => allDimensionTypes(catalog), [catalog]);
  const query = useMemo(
    () => chartQuery(chart, selections, ranges, scope, dimensionTypes, defaultUsePreaggregations),
    [chart, selections, ranges, scope, dimensionTypes, defaultUsePreaggregations],
  );
  const state = useQueryResult(backend, query);
  const result = state.result;
  const rows = result?.rows ?? [];
  const dimensions = chart.query.dimensions ?? [];
  const allRefs = [...dimensions, ...chart.query.metrics];
  const metricRefs = yMetrics(chart);
  const xRef = chart.encoding?.x ?? dimensions[0];
  const colorRef = chart.encoding?.color;
  const interactionsEnabled = chart.interactions?.crossfilter !== false;
  const selectConfig = chart.interactions?.select;
  const selectFields =
    typeof selectConfig === "object" && selectConfig.fields?.length
      ? selectConfig.fields
      : xRef
        ? [xRef]
        : [];
  const selectableDimension = selectFields[0];
  const brushEnabled = interactionsEnabled && Boolean(chart.interactions?.brush);

  function metricFormat(ref: string) {
    const info = metricInfo(catalog, ref);
    return { format: info?.format, type: info?.type };
  }

  function metricLabel(ref: string): string {
    return metricInfo(catalog, ref)?.label ?? labelize(aliasOf(ref));
  }

  function renderKpis() {
    const row = rows[0] ?? {};
    return (
      <div className="grid gap-px bg-line sm:grid-cols-2 xl:grid-cols-4">
        {chart.query.metrics.map((metric) => {
          const key = dashboardResultAlias(metric, allRefs, result);
          return (
            <MetricCard
              key={metric}
              metric={metric}
              label={metricLabel(metric)}
              value={row[key]}
              format={metricFormat(metric)}
              loading={state.loading}
            />
          );
        })}
      </div>
    );
  }

  function renderLines() {
    if (!xRef) return null;
    const xKey = dashboardResultAlias(xRef, allRefs, result);
    const colorKey = colorRef ? dashboardResultAlias(colorRef, allRefs, result) : undefined;
    const grouped = new Map<string, ResultRow[]>();
    for (const row of rows) {
      const group = colorKey ? displayDimValue(normalizedValue(row[colorKey])) : "";
      grouped.set(group, [...(grouped.get(group) ?? []), row]);
    }
    if (!grouped.size) grouped.set("", []);
    return (
      <div className="grid gap-3">
        {[...grouped.entries()].flatMap(([group, groupRows]) =>
          metricRefs.map((metric) => {
            const metricKey = dashboardResultAlias(metric, allRefs, result);
            const points = groupRows
              .map((row) => ({ x: String(row[xKey] ?? ""), y: Number(row[metricKey]) }))
              .filter((point) => point.x && Number.isFinite(point.y));
            const title = [metricLabel(metric), group].filter(Boolean).join(" · ");
            return (
              <section key={`${group}:${metric}`} className="min-w-0">
                {(metricRefs.length > 1 || group) && <h4 className="mb-1 text-2xs font-medium text-faint">{title}</h4>}
                <TimeSeriesChart
                  points={points}
                  formatValue={(value) => formatValue(value, metricFormat(metric))}
                  ariaLabel={`${chart.title ?? title}, ${points.length} points`}
                  onBrush={
                    brushEnabled
                      ? (range) =>
                          onBrush(
                            xRef,
                            range
                              ? { from: range.from, to: endOfBucket(range.to, grainOf(xRef)) }
                              : null,
                            chart.id,
                          )
                      : undefined
                  }
                />
              </section>
            );
          }),
        )}
      </div>
    );
  }

  function renderBars() {
    if (!xRef) return null;
    const xKey = dashboardResultAlias(xRef, allRefs, result);
    const colorKey = colorRef ? dashboardResultAlias(colorRef, allRefs, result) : undefined;
    const selectedValues = selectableDimension ? selections[selectableDimension]?.values ?? [] : [];
    return (
      <div className="grid gap-3">
        {metricRefs.map((metric) => {
          const metricKey = dashboardResultAlias(metric, allRefs, result);
          const labelToValue = new Map<string, string>();
          const data = rows.map((row) => {
            const raw = normalizedValue(row[xKey]);
            const label = colorKey
              ? `${displayDimValue(raw)} · ${displayDimValue(normalizedValue(row[colorKey]))}`
              : displayDimValue(raw);
            labelToValue.set(label, raw);
            return { label, value: Number(row[metricKey]) || 0 };
          });
          const selectedLabels = [...labelToValue.entries()]
            .filter(([, value]) => selectedValues.includes(value))
            .map(([label]) => label);
          return (
            <section key={metric}>
              {metricRefs.length > 1 && <h4 className="mb-1 text-2xs font-medium text-faint">{metricLabel(metric)}</h4>}
              <ColumnChart
                data={data}
                selectedLabels={selectedLabels}
                onToggle={
                  interactionsEnabled && selectableDimension
                    ? (label) => onToggle(selectableDimension, labelToValue.get(label) ?? label, chart.id)
                    : undefined
                }
                ariaLabel={`${chart.title ?? metricLabel(metric)}, ${data.length} categories`}
              />
            </section>
          );
        })}
      </div>
    );
  }

  function renderLeaderboard() {
    const dimension = selectableDimension ?? xRef ?? dimensions[0];
    const metric = metricRefs[0];
    if (!dimension || !metric) return null;
    const dimKey = dashboardResultAlias(dimension, allRefs, result);
    const metricKey = dashboardResultAlias(metric, allRefs, result);
    return (
      <Leaderboard
        dimension={dimension}
        title={dimensionLabel(catalog, dimension)}
        metricLabel={metricLabel(metric)}
        rows={rows.map((row) => ({ value: normalizedValue(row[dimKey]), metric: Number(row[metricKey]) || 0 }))}
        selectedValues={selections[dimension]?.values ?? []}
        loading={state.loading}
        formatMetric={(value) => formatValue(value, metricFormat(metric))}
        collapsedLimit={chart.query.limit ?? 10}
        onToggle={interactionsEnabled ? (value) => onToggle(dimension, value, chart.id) : undefined}
      />
    );
  }

  function renderTable() {
    const columns: Column[] = (result?.columns ?? []).map((key) => ({
      key,
      label: labelize(key),
      numeric: rows.some((row) => typeof row[key] === "number"),
    }));
    return (
      <DataTable
        columns={columns}
        rows={rows}
        loading={state.loading}
        pageSize={Math.min(chart.query.limit ?? 50, 100)}
        renderCell={(_column, value) => formatValue(value)}
      />
    );
  }

  let content = null;
  if (!result && state.loading) content = <LoadingState message="Running semantic query…" />;
  else if (state.error) content = <ErrorState message={state.error} />;
  else if (kind === "kpi") content = renderKpis();
  else if (kind === "line" || kind === "area") content = renderLines();
  else if (kind === "bar") content = renderBars();
  else if (kind === "leaderboard") content = renderLeaderboard();
  else content = renderTable();

  return (
    <article
      data-dashboard-chart={chart.id}
      data-chart-type={kind}
      className={`${panelSpan(chart, kind)} min-w-0 border border-line bg-surface`}
    >
      <header className="flex min-h-10 items-center justify-between gap-3 border-b border-line px-3 py-2">
        <div className="min-w-0">
          <h3 className="truncate text-sm font-semibold text-ink">{chart.title ?? labelize(chart.id)}</h3>
          <p className="truncate text-2xs text-faint">
            {chart.query.metrics.map(metricLabel).join(" · ")}
          </p>
        </div>
        <span className="shrink-0 text-2xs text-faint">{state.loading ? "Updating…" : `${result?.rowCount ?? 0} rows`}</span>
      </header>
      <div className="min-w-0 p-3">{content}</div>
      <QueryDebugPanel
        queries={{ [chart.id]: result?.sql }}
        inputs={{
          [chart.id]: {
            metrics: query.metrics,
            dimensions: query.dimensions,
            filters: query.filters,
          },
        }}
      />
    </article>
  );
}

function QueryStatus() {
  const active = useQueryActive();
  return (
    <span className="flex items-center gap-1.5 text-2xs text-faint" aria-live="polite">
      <span aria-hidden="true" className={`inline-block size-2 rounded-full ${active ? "bg-faint motion-safe:animate-pulse" : "bg-accent"}`} />
      {active ? "Querying" : "Live"}
    </span>
  );
}

export function DashboardView({
  dashboard,
  catalog,
  backend,
}: {
  dashboard: DashboardSpec;
  catalog: Catalog;
  backend: SidemanticBackend;
}) {
  const initialTab = new URLSearchParams(window.location.search).get("tab");
  const [activeTabId, setActiveTabId] = useState(
    dashboard.tabs.some((tab) => tab.id === initialTab) ? initialTab! : dashboard.tabs[0]?.id ?? "",
  );
  const [selections, setSelections] = useState<Selections>(() => parseStored(DASHBOARD_FILTERS_PARAM, {}));
  const [ranges, setRanges] = useState<TimeSelections>(() => parseStored(DASHBOARD_RANGES_PARAM, {}));
  const [copied, setCopied] = useState(false);
  const scope = dashboard.defaults?.interactions?.scope ?? "tab";
  const activeTab = dashboard.tabs.find((tab) => tab.id === activeTabId) ?? dashboard.tabs[0];
  const defaultUsePreaggregations =
    dashboard.defaults?.query?.use_preaggregations ?? dashboard.defaults?.query?.usePreaggregations;

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    params.set("tab", activeTabId);
    if (Object.keys(selections).length) params.set(DASHBOARD_FILTERS_PARAM, JSON.stringify(selections));
    else params.delete(DASHBOARD_FILTERS_PARAM);
    if (Object.keys(ranges).length) params.set(DASHBOARD_RANGES_PARAM, JSON.stringify(ranges));
    else params.delete(DASHBOARD_RANGES_PARAM);
    const query = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}`);
  }, [activeTabId, selections, ranges]);

  function switchTab(tabId: string) {
    if (scope === "tab") {
      setSelections({});
      setRanges({});
    }
    setActiveTabId(tabId);
  }

  function toggleSelection(dimension: string, value: string, sourceChart: string) {
    setSelections((current) => {
      const existing = current[dimension]?.values ?? [];
      const values = existing.includes(value) ? existing.filter((item) => item !== value) : [...existing, value];
      if (!values.length) {
        const next = { ...current };
        delete next[dimension];
        return next;
      }
      return { ...current, [dimension]: { values, sourceChart } };
    });
  }

  function setBrush(dimension: string, range: BrushRange | null, sourceChart: string) {
    setRanges((current) => {
      if (!range) {
        const next = { ...current };
        delete next[dimension];
        return next;
      }
      return { ...current, [dimension]: { ...range, sourceChart } };
    });
  }

  if (!activeTab) return <ErrorState title="Invalid dashboard" message="No dashboard tabs were configured." />;

  return (
    <div className="flex h-screen min-w-0 flex-col overflow-hidden bg-bg text-ink">
      <header className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-line bg-surface px-4 py-2.5">
        <div className="min-w-0">
          <p className="text-2xs font-semibold uppercase tracking-wide text-faint">Sidemantic dashboard</p>
          <h1 className="truncate text-lg font-semibold text-ink">{dashboard.title}</h1>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={async () => {
              await navigator.clipboard.writeText(window.location.href);
              setCopied(true);
              window.setTimeout(() => setCopied(false), 1200);
            }}
            className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink"
          >
            {copied ? "Copied" : "Copy link"}
          </button>
          <ThemeToggle />
          <QueryStatus />
        </div>
      </header>

      <nav className="flex shrink-0 items-center gap-1 overflow-x-auto border-b border-line bg-surface px-4" aria-label="Dashboard tabs">
        {dashboard.tabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={tab.id === activeTab.id}
            onClick={() => switchTab(tab.id)}
            className="min-h-10 whitespace-nowrap border-b-2 border-transparent px-3 text-xs font-medium text-muted hover:text-ink aria-selected:border-accent aria-selected:text-ink"
          >
            {tab.label ?? labelize(tab.id)}
          </button>
        ))}
      </nav>

      {Object.keys(selections).length || Object.keys(ranges).length ? (
        <div className="flex min-h-10 shrink-0 flex-wrap items-center gap-2 border-b border-line bg-surface px-4 py-1.5">
          <span className="text-2xs font-semibold uppercase tracking-wide text-faint">Filters</span>
          {Object.entries(selections).flatMap(([dimension, selection]) =>
            selection.values.map((value) => (
              <FilterPill
                key={`${dimension}:${value}`}
                dimension={dimension}
                dimensionLabel={dimensionLabel(catalog, dimension)}
                value={displayDimValue(value)}
                onRemove={() => toggleSelection(dimension, value, selection.sourceChart)}
              />
            )),
          )}
          {Object.entries(ranges).map(([dimension, range]) => (
            <FilterPill
              key={dimension}
              dimension={dimension}
              dimensionLabel={dimensionLabel(catalog, dimension)}
              value={`${dateOnly(range.from)} – ${dateOnly(range.to)}`}
              onRemove={() => setBrush(dimension, null, range.sourceChart)}
            />
          ))}
          <button
            type="button"
            onClick={() => {
              setSelections({});
              setRanges({});
            }}
            className="text-2xs text-muted hover:text-ink hover:underline"
          >
            Clear all
          </button>
        </div>
      ) : null}

      <main className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-5">
        <section className="mx-auto grid max-w-7xl grid-cols-1 gap-4 lg:grid-cols-2" role="tabpanel">
          {activeTab.charts.map((chart) => (
            <ChartPanel
              key={chart.id}
              chart={chart}
              catalog={catalog}
              backend={backend}
              selections={selections}
              ranges={ranges}
              scope={scope}
              defaultUsePreaggregations={defaultUsePreaggregations}
              onToggle={toggleSelection}
              onBrush={setBrush}
            />
          ))}
        </section>
      </main>
    </div>
  );
}
