import { useEffect, useMemo, useState } from "react";
import { AppShell } from "../components/AppShell";
import { ColumnChart } from "../components/ColumnChart";
import { DataTable, type Column } from "../components/DataTable";
import { MetricCard } from "../components/MetricCard";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { EmptyState, ErrorState, LoadingState } from "../components/States";
import { ThemeToggle } from "../components/ThemeToggle";
import { TimeSeriesChart } from "../components/TimeSeriesChart";
import type { SidemanticBackend } from "../data/backend";
import type { DashboardChart, DashboardDocument } from "../data/dashboardTypes";
import type { Catalog, ResultRow } from "../data/types";
import { displayDimValue, formatValue, labelize } from "../lib/format";
import { dimTypes } from "../lib/queries";
import {
  brushableDashboardDimension,
  decodeDashboardState,
  dashboardCategorySeries,
  dashboardCategorySelection,
  dashboardChartType,
  dashboardChartScopeKey,
  dashboardDrillDimension,
  dashboardExploreUrl,
  dashboardFilterValue,
  dashboardMetricRefs,
  dashboardResultColumn,
  dashboardScopedInteractions,
  dashboardStructuredQuery,
  dashboardTimeSeries,
  encodeDashboardState,
  loadSavedDashboardViews,
  rowsToCsv,
  selectableDashboardCategory,
  storeSavedDashboardViews,
  tabLabel,
  type DashboardViewState,
  type SavedDashboardView,
} from "../state/dashboardState";
import { useQueryActive } from "../state/queryActivity";
import { useQueryResult } from "../state/useQueryResult";

function DashboardQueryStatus() {
  const active = useQueryActive();
  return <span className="text-2xs text-faint">{active ? "Querying…" : "Live data"}</span>;
}

function metricFormat(catalog: Catalog, ref: string) {
  for (const model of catalog.models) {
    const metric = model.metrics.find((candidate) => candidate.ref === ref);
    if (metric) return { format: metric.format, type: metric.type };
  }
  const metric = catalog.graphMetrics.find((candidate) => candidate.ref === ref);
  return { format: metric?.format, type: metric?.type };
}

function dimensionTypes(catalog: Catalog) {
  return dimTypes(catalog.models.flatMap((model) => model.dimensions));
}

function firstY(chart: DashboardChart): string {
  const encoded = chart.encoding?.y;
  return (Array.isArray(encoded) ? encoded[0] : encoded) ?? chart.query.metrics[0] ?? "";
}

function CsvDownload({ chart, columns, rows }: { chart: DashboardChart; columns: string[]; rows: ResultRow[] }) {
  const csv = rowsToCsv(columns, rows);
  return (
    <a
      href={`data:text/csv;charset=utf-8,${encodeURIComponent(csv)}`}
      download={`${chart.id}.csv`}
      className="border border-line px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink"
    >
      Export CSV
    </a>
  );
}

function ChartDetails({
  chart,
  columns,
  rows,
  loading,
  onDrill,
}: {
  chart: DashboardChart;
  columns: string[];
  rows: ResultRow[];
  loading: boolean;
  onDrill: (dimension: string, value: string) => void;
}) {
  const tableColumns: Column[] = columns.map((column) => ({
    key: column,
    label: labelize(column),
    numeric: chart.query.metrics.some((metric) => dashboardResultColumn(metric, columns) === column),
  }));
  const firstDrillDimension = dashboardDrillDimension(chart);
  const firstDrillColumn = firstDrillDimension ? dashboardResultColumn(firstDrillDimension, columns) : undefined;
  const canDrill = Boolean(firstDrillDimension);

  return (
    <div className="border-t border-line bg-surface-soft p-3" data-testid={`chart-details-${chart.id}`}>
      <div className="mb-2 flex items-center justify-between gap-2">
        <span className="text-2xs font-semibold uppercase tracking-wide text-faint">Drill details</span>
        {canDrill && firstDrillDimension && firstDrillColumn ? (
          <span className="text-2xs text-faint">Choose a {labelize(firstDrillDimension)} value to filter every chart.</span>
        ) : null}
      </div>
      <DataTable
        columns={tableColumns}
        rows={rows}
        loading={loading}
        pageSize={20}
        renderCell={(_column, value) => String(value ?? "—")}
      />
      {canDrill && firstDrillDimension && firstDrillColumn && rows.length ? (
        <div className="mt-2 flex flex-wrap gap-1" aria-label={`Drill by ${labelize(firstDrillDimension)}`}>
          {rows.slice(0, 12).map((row, index) => {
            const value = dashboardFilterValue(row[firstDrillColumn]);
            return (
              <button
                key={`${String(value)}-${index}`}
                type="button"
                onClick={() => onDrill(firstDrillDimension, value)}
                className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-accent hover:text-ink"
              >
                Filter to {displayDimValue(value)}
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function DashboardChartPanel({
  document,
  chart,
  catalog,
  backend,
  state,
  setFilters,
  setRange,
}: {
  document: DashboardDocument;
  chart: DashboardChart;
  catalog: Catalog;
  backend: SidemanticBackend;
  state: DashboardViewState;
  setFilters: (filters: Record<string, string>) => void;
  setRange: (dimension: string, range: { from: string; to: string } | null) => void;
}) {
  const [detailsOpen, setDetailsOpen] = useState(false);
  const types = useMemo(() => dimensionTypes(catalog), [catalog]);
  const request = useMemo(
    () => dashboardStructuredQuery(document, chart, state.filters, types, state.ranges, state),
    [chart, document, state, types],
  );
  const query = useQueryResult(backend, request);
  const rows = query.result?.rows ?? [];
  const columns = query.result?.columns ?? [];
  const dimensions = chart.query.dimensions ?? [];
  const xRef = chart.encoding?.x ?? dimensions[0] ?? "";
  const yRefs = dashboardMetricRefs(chart);
  const yRef = yRefs[0] ?? firstY(chart);
  const xColumn = dashboardResultColumn(xRef, columns);
  const scopedInteractions = dashboardScopedInteractions(document, chart, state);
  const chartType = dashboardChartType(chart, types);
  const canBrush = brushableDashboardDimension(chart, xRef);
  const chartTitle = chart.title?.trim() || labelize(chart.id);
  const seriesRefs = [chart.encoding?.color, ...dimensions.filter((dimension) => dimension !== xRef)].filter(
    (dimension, index, refs): dimension is string => Boolean(dimension) && refs.indexOf(dimension) === index,
  );
  const canSelect = selectableDashboardCategory(chart, xRef, seriesRefs);
  const seriesColumns = seriesRefs.map((dimension) => dashboardResultColumn(dimension, columns));

  let visualization: React.ReactNode;
  if (query.loading && !query.result) {
    visualization = <LoadingState title={`Loading ${chartTitle}`} />;
  } else if (query.error) {
    visualization = <ErrorState title={`Could not load ${chartTitle}`} message={query.error} />;
  } else if (!rows.length) {
    visualization = <EmptyState title={chartTitle} message="The query returned no rows." />;
  } else if (!dimensions.length) {
    const total = rows[0];
    visualization = (
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
        {chart.query.metrics.map((metric) => (
          <MetricCard
            key={metric}
            metric={metric}
            label={labelize(metric)}
            value={total[dashboardResultColumn(metric, columns)]}
            format={metricFormat(catalog, metric)}
          />
        ))}
      </div>
    );
  } else if (chartType === "line" || chartType === "area") {
    visualization = (
      <div className="grid grid-cols-[repeat(auto-fit,minmax(260px,1fr))] gap-3">
        {yRefs.map((metric) => {
          const series = dashboardTimeSeries(
            rows,
            xColumn,
            dashboardResultColumn(metric, columns),
            seriesColumns,
          );
          const primary = series[0] ?? { label: "Current", points: [] };
          const format = metricFormat(catalog, metric);
          return (
            <div key={metric} className="min-w-0">
              {yRefs.length > 1 ? <h3 className="mb-1 text-2xs font-semibold text-muted">{labelize(metric)}</h3> : null}
              <TimeSeriesChart
                points={primary.points}
                seriesLabel={primary.label}
                additionalSeries={series.slice(1)}
                formatValue={(value) => formatValue(value, format)}
                ariaLabel={`${chartTitle}, ${labelize(metric)}, ${series.length} series and ${primary.points.length} buckets`}
                onBrush={canBrush ? (range) => setRange(xRef, range) : undefined}
              />
            </div>
          );
        })}
      </div>
    );
  } else {
    visualization = (
      <div className="grid grid-cols-[repeat(auto-fit,minmax(260px,1fr))] gap-3">
        {yRefs.map((metric) => {
          const yColumn = dashboardResultColumn(metric, columns);
          const series = dashboardCategorySeries(rows, xColumn, yColumn, seriesColumns);
          return series.map((entry, seriesIndex) => {
            const data = entry.data.slice(0, 30);
            const showSeriesLabel = yRefs.length > 1 || series.length > 1;
            const seriesLabel = [yRefs.length > 1 ? labelize(metric) : "", series.length > 1 ? entry.label : ""]
              .filter(Boolean)
              .join(" · ");
            return (
              <div key={`${metric}-${entry.label}-${seriesIndex}`} className="min-w-0">
                {showSeriesLabel ? <h3 className="mb-1 text-2xs font-semibold text-muted">{seriesLabel}</h3> : null}
                <ColumnChart
                  data={data}
                  ariaLabel={`${chartTitle}, ${seriesLabel || labelize(metric)}, ${data.length} categories`}
                  selectedLabel={scopedInteractions.filters[xRef]}
                  onSelect={canSelect ? (value) => {
                    setFilters(dashboardCategorySelection(chart, xRef, value, seriesRefs, entry.filterValues));
                    setDetailsOpen(true);
                  } : undefined}
                />
              </div>
            );
          });
        })}
      </div>
    );
  }

  return (
    <article className="min-w-0 overflow-hidden border border-line bg-surface" data-testid="dashboard-chart" data-chart-id={chart.id}>
      <header className="flex min-h-12 flex-wrap items-center justify-between gap-2 border-b border-line px-3 py-2">
        <div className="min-w-0">
          <h2 className="truncate text-sm font-semibold text-ink">{chartTitle}</h2>
          <p className="text-2xs text-faint">{labelize(yRef)}{xRef ? ` by ${labelize(xRef)}` : ""}</p>
        </div>
        <div className="flex flex-wrap gap-1">
          <button
            type="button"
            aria-expanded={detailsOpen}
            onClick={() => setDetailsOpen((open) => !open)}
            className="border border-line px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink"
          >
            {detailsOpen ? "Hide details" : "Drill details"}
          </button>
          <a href={dashboardExploreUrl(document, chart, state, types)} className="border border-line px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink">
            Explore from here
          </a>
          <CsvDownload chart={chart} columns={columns} rows={rows} />
        </div>
      </header>
      <div className="min-h-64 overflow-hidden p-3">{visualization}</div>
      {detailsOpen ? (
        <ChartDetails
          chart={chart}
          columns={columns}
          rows={rows}
          loading={query.loading}
          onDrill={(dimension, value) => setFilters({ [dimension]: value })}
        />
      ) : null}
      {query.result?.sql ? <div className="border-t border-line px-3 py-2"><QueryDebugPanel queries={{ Query: query.result.sql }} /></div> : null}
    </article>
  );
}

function SavedViews({
  document,
  state,
  onLoad,
}: {
  document: DashboardDocument;
  state: DashboardViewState;
  onLoad: (state: DashboardViewState) => void;
}) {
  const [name, setName] = useState("");
  const [views, setViews] = useState<SavedDashboardView[]>(() => loadSavedDashboardViews(document));
  const [storageError, setStorageError] = useState(false);

  function persist(next: SavedDashboardView[]) {
    setViews(next);
    setStorageError(!storeSavedDashboardViews(document, next));
  }

  return (
    <details className="relative">
      <summary className="cursor-pointer list-none border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink">
        Saved views
      </summary>
      <div className="absolute right-0 z-50 mt-1 w-72 border border-line bg-surface p-3 shadow-[var(--shadow)]">
        <p className="mb-2 text-2xs text-faint">Stored only in this browser. Nothing is written to the server.</p>
        <div className="flex gap-1">
          <input
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="View name"
            aria-label="Saved view name"
            className="min-w-0 flex-1 border border-line bg-bg px-2 py-1 text-xs text-ink"
          />
          <button
            type="button"
            disabled={!name.trim()}
            onClick={() => {
              const next = [...views.filter((view) => view.name !== name.trim()), { name: name.trim(), state }];
              persist(next);
              setName("");
            }}
            className="border border-line px-2 py-1 text-2xs text-muted hover:text-ink disabled:opacity-40"
          >
            Save
          </button>
        </div>
        {storageError ? <p role="alert" className="mt-2 text-2xs text-danger">Browser storage is unavailable.</p> : null}
        <div className="mt-2 flex flex-col gap-1">
          {views.length ? views.map((view) => (
            <div key={view.name} className="flex items-center justify-between gap-2 border-t border-line pt-1">
              <button type="button" onClick={() => onLoad(view.state)} className="min-w-0 flex-1 truncate text-left text-xs text-muted hover:text-ink">
                {view.name}
              </button>
              <button type="button" aria-label={`Delete saved view ${view.name}`} onClick={() => persist(views.filter((entry) => entry.name !== view.name))} className="text-2xs text-faint hover:text-danger">
                Delete
              </button>
            </div>
          )) : <span className="text-2xs text-faint">No local views yet.</span>}
        </div>
      </div>
    </details>
  );
}

function ShareUrlButton() {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      title="Copies the active tab, filters, and brushed ranges. Data is queried live; local saved views are not shared."
      onClick={async () => {
        await navigator.clipboard.writeText(window.location.href);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1200);
      }}
      className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink"
    >
      {copied ? "Share URL copied" : "Copy share URL"}
    </button>
  );
}

export function DashboardDocumentView({
  document,
  catalog,
  backend,
}: {
  document: DashboardDocument;
  catalog: Catalog;
  backend: SidemanticBackend;
}) {
  const [state, setState] = useState<DashboardViewState>(() => decodeDashboardState(window.location.search, document));
  const activeTab = document.tabs.find((tab) => tab.id === state.tab) ?? document.tabs[0];

  useEffect(() => {
    window.document.title = document.title;
  }, [document.title]);

  useEffect(() => {
    const query = encodeDashboardState(state, document);
    window.history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}`);
  }, [document, state]);

  function setFilters(chart: DashboardChart, filters: Record<string, string>) {
    const source = dashboardChartScopeKey(document, chart);
    if ((document.defaults?.interactions?.scope ?? "dashboard") === "chart") {
      setState((current) => ({
        ...current,
        chartFilters: {
          ...(current.chartFilters ?? {}),
          [source]: { ...(current.chartFilters?.[source] ?? {}), ...filters },
        },
      }));
      return;
    }
    setState((current) => ({
      ...current,
      filters: { ...current.filters, ...filters },
      filterSources: {
        ...current.filterSources,
        ...Object.fromEntries(Object.keys(filters).map((dimension) => [dimension, source])),
      },
    }));
  }

  function setRange(chart: DashboardChart, dimension: string, range: { from: string; to: string } | null) {
    const source = dashboardChartScopeKey(document, chart);
    if ((document.defaults?.interactions?.scope ?? "dashboard") === "chart") {
      setState((current) => {
        const chartRanges = { ...(current.chartRanges ?? {}) };
        const ranges = { ...(chartRanges[source] ?? {}) };
        if (range) ranges[dimension] = range;
        else delete ranges[dimension];
        if (Object.keys(ranges).length) chartRanges[source] = ranges;
        else delete chartRanges[source];
        return { ...current, chartRanges };
      });
      return;
    }
    setState((current) => {
      const ranges = { ...current.ranges };
      const rangeSources = { ...current.rangeSources };
      if (range) {
        ranges[dimension] = range;
        rangeSources[dimension] = source;
      } else {
        delete ranges[dimension];
        delete rangeSources[dimension];
      }
      return { ...current, ranges, rangeSources };
    });
  }

  const toolbar = (
    <>
      <SavedViews document={document} state={state} onLoad={setState} />
      <ShareUrlButton />
      <ThemeToggle />
      <DashboardQueryStatus />
    </>
  );
  const chartFilterEntries = Object.entries(state.chartFilters ?? {}).flatMap(([source, values]) =>
    Object.entries(values).map(([dimension, value]) => ({ source, dimension, value })),
  );
  const chartRangeEntries = Object.entries(state.chartRanges ?? {}).flatMap(([source, values]) =>
    Object.entries(values).map(([dimension, range]) => ({ source, dimension, range })),
  );
  const filters = Object.entries(state.filters).length || Object.entries(state.ranges).length || chartFilterEntries.length || chartRangeEntries.length ? (
    <>
      <span className="shrink-0 text-2xs font-semibold uppercase tracking-wide text-faint">Filters</span>
      {Object.entries(state.filters).map(([dimension, value]) => (
        <button
          key={dimension}
          type="button"
          aria-label={`Remove filter ${labelize(dimension)} ${value}`}
          onClick={() => setState((current) => {
            const next = { ...current.filters };
            const filterSources = { ...current.filterSources };
            delete next[dimension];
            delete filterSources[dimension];
            return { ...current, filters: next, filterSources };
          })}
          className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-danger"
        >
          {labelize(dimension)} = {value} ×
        </button>
      ))}
      {Object.entries(state.ranges).map(([dimension, range]) => (
        <button
          key={`range-${dimension}`}
          type="button"
          aria-label={`Remove range ${labelize(dimension)} ${range.from} to ${range.to}`}
          onClick={() =>
            setState((current) => {
              const ranges = { ...current.ranges };
              const rangeSources = { ...current.rangeSources };
              delete ranges[dimension];
              delete rangeSources[dimension];
              return { ...current, ranges, rangeSources };
            })
          }
          className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-danger"
        >
          {labelize(dimension)}: {range.from}–{range.to} ×
        </button>
      ))}
      {chartFilterEntries.map(({ source, dimension, value }) => (
        <button
          key={`chart-filter-${source}-${dimension}`}
          type="button"
          aria-label={`Remove filter ${labelize(dimension)} ${value} from ${source}`}
          onClick={() => setState((current) => {
            const chartFilters = { ...(current.chartFilters ?? {}) };
            const values = { ...(chartFilters[source] ?? {}) };
            delete values[dimension];
            if (Object.keys(values).length) chartFilters[source] = values;
            else delete chartFilters[source];
            return { ...current, chartFilters };
          })}
          className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-danger"
        >
          {labelize(dimension)} = {value} · {source} ×
        </button>
      ))}
      {chartRangeEntries.map(({ source, dimension, range }) => (
        <button
          key={`chart-range-${source}-${dimension}`}
          type="button"
          aria-label={`Remove range ${labelize(dimension)} ${range.from} to ${range.to} from ${source}`}
          onClick={() => setState((current) => {
            const chartRanges = { ...(current.chartRanges ?? {}) };
            const values = { ...(chartRanges[source] ?? {}) };
            delete values[dimension];
            if (Object.keys(values).length) chartRanges[source] = values;
            else delete chartRanges[source];
            return { ...current, chartRanges };
          })}
          className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-danger"
        >
          {labelize(dimension)}: {range.from}–{range.to} · {source} ×
        </button>
      ))}
      <button type="button" onClick={() => setState((current) => ({ ...current, filters: {}, ranges: {}, filterSources: {}, rangeSources: {}, chartFilters: {}, chartRanges: {} }))} className="text-2xs text-muted hover:text-ink">
        Clear all
      </button>
    </>
  ) : undefined;

  return (
    <AppShell
      brand={<div className="min-w-0"><h1 className="truncate text-sm font-semibold text-ink">{document.title}</h1><p className="text-2xs text-faint">Declarative dashboard</p></div>}
      toolbar={toolbar}
      filters={filters}
      rail={null}
      showRail={false}
    >
      <div data-testid="dashboard-document" className="flex min-h-full flex-col">
        <nav className="flex shrink-0 gap-1 overflow-x-auto border-b border-line bg-surface px-3 pt-2" aria-label="Dashboard tabs">
          {document.tabs.map((tab) => (
            <button
              key={tab.id}
              type="button"
              role="tab"
              aria-selected={tab.id === activeTab?.id}
              data-tab-id={tab.id}
              onClick={() =>
                setState((current) => {
                  if (current.tab === tab.id) return current;
                  if ((document.defaults?.interactions?.scope ?? "dashboard") === "dashboard") {
                    return { ...current, tab: tab.id };
                  }
                  return { ...current, tab: tab.id, filters: {}, ranges: {}, filterSources: {}, rangeSources: {}, chartFilters: {}, chartRanges: {} };
                })
              }
              className="border border-b-0 border-line px-3 py-2 text-xs text-muted aria-selected:border-accent aria-selected:bg-bg aria-selected:text-ink"
            >
              {tabLabel(tab)}
            </button>
          ))}
        </nav>
        {activeTab ? (
          <section className="grid min-w-0 grid-cols-1 gap-3 p-3 xl:grid-cols-2" data-testid="dashboard-chart-grid" data-tab-id={activeTab.id}>
            {activeTab.charts.map((chart) => (
              <DashboardChartPanel
                key={chart.id}
                document={document}
                chart={chart}
                catalog={catalog}
                backend={backend}
                state={state}
                setFilters={(filters) => setFilters(chart, filters)}
                setRange={(dimension, range) => setRange(chart, dimension, range)}
              />
            ))}
          </section>
        ) : <EmptyState title="No dashboard tabs" message="This dashboard document has no tabs to render." />}
      </div>
    </AppShell>
  );
}
