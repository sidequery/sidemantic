import { useMemo, useState } from "react";
import { ColumnChart } from "../components/ColumnChart";
import { DashboardShell } from "../components/DashboardShell";
import { DataTable, type Column } from "../components/DataTable";
import { DateRangeControl } from "../components/DateRangeControl";
import { DataPreviewTable, LineChart } from "../components/DistributionAdapters";
import { FilterPill } from "../components/FilterPill";
import { GrainSelect } from "../components/GrainSelect";
import { Leaderboard, type LeaderboardRow } from "../components/Leaderboard";
import { MetricCard } from "../components/MetricCard";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { Sparkline } from "../components/Sparkline";
import { EmptyState, ErrorState, LoadingState, StatusDot } from "../components/States";
import { ThemeToggle } from "../components/ThemeToggle";
import { TimeSeriesChart } from "../components/TimeSeriesChart";
import { ViewSwitcher } from "../components/ViewSwitcher";
import type { Grain } from "../data/types";
import type { DateRange } from "../lib/time";
import type { ViewKind } from "../state/explorerState";
import { formatValue } from "../lib/format";

const leaderboardSets: Array<{ dimension: string; title: string; rows: LeaderboardRow[] }> = [
  {
    dimension: "orders.region",
    title: "Customer Region",
    rows: [
      ["East", 83262], ["South", 80579], ["West", 71561], ["North", 52889],
    ].map(([value, metric]) => ({ value: String(value), metric: Number(metric) })),
  },
  {
    dimension: "orders.status",
    title: "Order Status",
    rows: [["completed", 170379], ["cancelled", 64637], ["pending", 53275]].map(([value, metric]) => ({ value: String(value), metric: Number(metric) })),
  },
  {
    dimension: "products.category",
    title: "Product Category",
    rows: [["Electronics", 202015], ["Furniture", 83848], ["Office Supplies", 2428]].map(([value, metric]) => ({ value: String(value), metric: Number(metric) })),
  },
  {
    dimension: "customers.name",
    title: "Top Customers",
    rows: [
      ["Bob Smith", 46690], ["Carol Davis", 42227], ["Henry Taylor", 41633], ["Grace Lee", 41035],
      ["Frank Brown", 33890], ["David Wilson", 29928], ["Alice Johnson", 27114], ["Emma Martinez", 24802],
    ].map(([value, metric]) => ({ value: String(value), metric: Number(metric) })),
  },
];

const tableColumns: Column[] = [
  { key: "region", label: "Region", sortable: true },
  { key: "revenue", label: "Revenue", numeric: true, sortable: true },
  { key: "orders", label: "Orders", numeric: true, sortable: true },
];
const tableRows = Array.from({ length: 17 }, (_, index) => ({
  region: ["East", "South", "West", "North"][index % 4],
  revenue: 91000 - index * 2875,
  orders: 420 - index * 11,
}));

export function ComponentGallery() {
  const [selectedMetric, setSelectedMetric] = useState("orders.revenue");
  const [selected, setSelected] = useState<Record<string, string[]>>({});
  const [expanded, setExpanded] = useState<string | null>(null);
  const [galleryFilters, setGalleryFilters] = useState<Record<string, string[]>>({
    "orders.region": ["East", "West"],
    "orders.status": ["completed"],
  });
  const [dateRange, setDateRange] = useState<DateRange | undefined>({ from: "2025-01-01", to: "2025-06-30" });
  const [grain, setGrain] = useState<Grain>("month");
  const [view, setView] = useState<ViewKind>("explore");
  const [comparison, setComparison] = useState<"off" | "previous" | "year" | "custom">("previous");
  const [sort, setSort] = useState<{ key: string; dir: "asc" | "desc" }>({ key: "revenue", dir: "desc" });
  const sortedRows = useMemo(
    () => [...tableRows].sort((left, right) => {
      const a = left[sort.key as keyof typeof left];
      const b = right[sort.key as keyof typeof right];
      const result = typeof a === "number" && typeof b === "number" ? a - b : String(a).localeCompare(String(b));
      return sort.dir === "asc" ? result : -result;
    }),
    [sort],
  );

  return (
    <main className="min-h-screen bg-bg text-ink">
      <header className="sticky top-0 z-20 flex items-center justify-between border-b border-line bg-surface px-4 py-3">
        <div>
          <h1 className="text-sm font-semibold">Sidemantic component gallery</h1>
          <p className="text-2xs text-faint">Canonical React primitives · WASM leaderboard design</p>
        </div>
        <ThemeToggle />
      </header>

      <div className="space-y-8 p-4">
        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-faint">Metric cards</h2>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
            <MetricCard metric="orders.revenue" label="Revenue" value={288291} format={{ format: "currency" }} selected={selectedMetric === "orders.revenue"} sparkValues={[31, 42, 38, 55, 60, 74]} onSelect={setSelectedMetric} />
            <MetricCard metric="orders.count" label="Order Count" value={447} selected={selectedMetric === "orders.count"} sparkValues={[21, 34, 28, 41, 44, 51]} onSelect={setSelectedMetric} />
            <MetricCard metric="orders.margin" label="Gross Margin" value={0.324} format={{ format: "percent" }} sparkValues={[24, 26, 29, 27, 31, 32]} />
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-faint">Leaderboard cards</h2>
          <div className="grid grid-cols-[repeat(auto-fit,minmax(220px,1fr))] gap-0 border-l border-t border-line">
            {leaderboardSets
              .filter((item) => expanded === null || expanded === item.dimension)
              .map((item) => (
                <Leaderboard
                  key={item.dimension}
                  dimension={item.dimension}
                  title={item.title}
                  metricLabel="Total Revenue"
                  rows={item.rows}
                  selectedValues={selected[item.dimension]}
                  formatMetric={(value) => formatValue(value, { format: "currency" })}
                  onToggle={(value) => setSelected((current) => {
                    const values = current[item.dimension] ?? [];
                    return { ...current, [item.dimension]: values.includes(value) ? values.filter((entry) => entry !== value) : [...values, value] };
                  })}
                  expanded={expanded === item.dimension}
                  onExpandedChange={(next) => setExpanded(next ? item.dimension : null)}
                />
              ))}
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-faint">Filters and controls</h2>
          <div className="space-y-3 border border-line bg-surface p-3">
            <div data-testid="gallery-filter-pills" className="flex min-h-8 flex-wrap items-center gap-2">
              {Object.entries(galleryFilters).flatMap(([dimension, values]) =>
                values.map((value) => (
                  <FilterPill
                    key={`${dimension}:${value}`}
                    dimension={dimension}
                    value={value}
                    onRemove={() => setGalleryFilters((current) => ({
                      ...current,
                      [dimension]: current[dimension].filter((entry) => entry !== value),
                    }))}
                  />
                )),
              )}
              {Object.values(galleryFilters).every((values) => values.length === 0) ? (
                <span className="text-2xs text-faint">No active filters</span>
              ) : null}
            </div>
            <div className="flex flex-wrap items-center gap-3 border-t border-line pt-3">
              <DateRangeControl range={dateRange} onChange={setDateRange} comparison={comparison} onComparisonChange={setComparison} />
              <GrainSelect grain={grain} options={["day", "week", "month", "quarter", "year"]} onChange={setGrain} />
              <ViewSwitcher view={view} onChange={setView} />
              <span className="inline-flex items-center gap-1.5 text-2xs text-muted"><StatusDot status="ok" /> Ready</span>
              <span className="inline-flex items-center gap-1.5 text-2xs text-muted"><StatusDot status="loading" /> Updating</span>
              <button
                type="button"
                onClick={() => setGalleryFilters({ "orders.region": ["East", "West"], "orders.status": ["completed"] })}
                className="min-h-11 border border-line px-3 text-2xs text-muted hover:bg-surface-soft"
              >
                Reset filters
              </button>
            </div>
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-faint">Charts</h2>
          <div className="grid gap-3 xl:grid-cols-2">
            <article className="border border-line bg-surface p-3">
              <h3 className="mb-2 text-xs font-semibold">Sparkline</h3>
              <Sparkline
                values={[31, 42, 38, 55, 60, 74, 69, 83]}
                labels={["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"]}
                ariaLabel="Eight month revenue trend"
              />
            </article>
            <article className="border border-line bg-surface p-3">
              <h3 className="mb-2 text-xs font-semibold">Column chart</h3>
              <ColumnChart data={[{ label: "East", value: 83 }, { label: "South", value: 81 }, { label: "West", value: 72 }, { label: "Returns", value: -18 }]} />
            </article>
            <article className="border border-line bg-surface p-3">
              <h3 className="mb-2 text-xs font-semibold">Line chart compatibility export</h3>
              <LineChart data={[{ label: "Jan", value: 31 }, { label: "Feb", value: 42 }, { label: "Mar", value: 38 }, { label: "Apr", value: 55 }, { label: "May", value: 60 }]} />
            </article>
            <article className="border border-line bg-surface p-3">
              <h3 className="mb-2 text-xs font-semibold">Interactive time series</h3>
              <TimeSeriesChart
                points={[31, 42, 38, 55, 60, 74].map((y, index) => ({ x: `2025-0${index + 1}-01`, y }))}
                comparison={[27, 35, 33, 48, 52, 61].map((y, index) => ({ x: `2024-0${index + 1}-01`, y }))}
                formatValue={(value) => `$${value.toLocaleString()}k`}
              />
            </article>
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-faint">Analytical table</h2>
          <DataTable
            columns={tableColumns}
            rows={sortedRows}
            pageSize={6}
            sortKey={sort.key}
            sortDir={sort.dir}
            onSort={(key) => setSort((current) => ({ key, dir: current.key === key && current.dir === "desc" ? "asc" : "desc" }))}
            renderCell={(column, value) => column.numeric ? Number(value).toLocaleString() : String(value)}
          />
          <div className="mt-3">
            <h3 className="mb-2 text-xs font-semibold text-muted">Distribution-friendly data preview</h3>
            <DataPreviewTable result={{ columns: ["region", "revenue", "orders"], sample_rows: tableRows.slice(0, 8) }} pageSize={4} />
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-faint">Query inspection</h2>
          <QueryDebugPanel
            inputs={{ Revenue: { metrics: ["orders.revenue"], dimensions: ["orders.region"], filters: ["orders.status = 'completed'"] } }}
            queries={{ Revenue: "SELECT orders.region, SUM(orders.revenue) AS revenue\nFROM orders\nWHERE orders.status = 'completed'\nGROUP BY orders.region\nORDER BY revenue DESC" }}
          />
        </section>

        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-faint">States</h2>
          <div className="grid gap-2 lg:grid-cols-3">
            <LoadingState message="Loading metric results…" />
            <EmptyState message="No matching rows." />
            <ErrorState message="The query could not be completed." />
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-faint">Dashboard shell</h2>
          <div className="overflow-hidden border border-line bg-surface">
            <DashboardShell
              eyebrow="Embedded shell example"
              title="Revenue overview"
              status={<span className="inline-flex items-center gap-1.5"><StatusDot status="ok" /> Connected</span>}
              toolbar={<><FilterPill dimension="orders.region" value="East" /><GrainSelect grain={grain} options={["day", "month", "quarter"]} onChange={setGrain} /></>}
            >
              <div className="grid gap-2 sm:grid-cols-2">
                <MetricCard metric="orders.revenue" label="Revenue" value={288291} format={{ format: "currency" }} sparkValues={[31, 42, 38, 55, 60]} />
                <EmptyState title="Optional panel" message="Shell content is composed from the same primitives." />
              </div>
            </DashboardShell>
          </div>
        </section>
      </div>
    </main>
  );
}
