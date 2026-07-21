import { useMemo, useState } from "react";
import { BarLineCombo } from "../components/BarLineCombo";
import { Button } from "../components/Button";
import { ColumnChart } from "../components/ColumnChart";
import { Combobox } from "../components/Combobox";
import { DashboardShell } from "../components/DashboardShell";
import { DataTable, type Column } from "../components/DataTable";
import { DatePicker, type DatePickerRange } from "../components/DatePicker";
import { DateRangeControl } from "../components/DateRangeControl";
import { DataPreviewTable, LineChart } from "../components/DistributionAdapters";
import { DonutChart } from "../components/DonutChart";
import { FilterPill } from "../components/FilterPill";
import { GrainSelect } from "../components/GrainSelect";
import { HeatmapChart } from "../components/HeatmapChart";
import { HistogramChart } from "../components/HistogramChart";
import { Leaderboard, type LeaderboardRow } from "../components/Leaderboard";
import { MetricCard } from "../components/MetricCard";
import { NetworkChart } from "../components/NetworkChart";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { ScatterChart } from "../components/ScatterChart";
import { Select } from "../components/Select";
import { Sparkline } from "../components/Sparkline";
import { StackedAreaChart } from "../components/StackedAreaChart";
import { EmptyState, ErrorState, LoadingState, StatusDot } from "../components/States";
import { Switch } from "../components/Switch";
import { Tabs } from "../components/Tabs";
import { ThemeToggle } from "../components/ThemeToggle";
import { TimeSeriesChart } from "../components/TimeSeriesChart";
import { Tooltip } from "../components/Tooltip";
import { ViewSwitcher } from "../components/ViewSwitcher";
import { WaterfallChart } from "../components/WaterfallChart";
import type { Grain } from "../data/types";
import { rowsToSeries } from "../lib/rows";
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

const MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"];
// Long-form rows in query-result shape, pivoted through rowsToSeries for the stacked area demo.
const planRows = MONTH_LABELS.flatMap((month, index) => [
  { month, plan: "Basic", revenue: 120 + index * 26 },
  { month, plan: "Pro", revenue: 60 + index * 24 },
  { month, plan: "Enterprise", revenue: index < 2 ? 0 : 10 + index * 12 },
]);
const scatterPoints = Array.from({ length: 48 }, (_, index) => ({
  x: index + 1,
  y: (index + 1) * 1.6 + ((Math.sin(index * 78.233) * 43758.5453) % 18),
  label: `Account ${index + 1}`,
  series: index % 2 ? "Returning" : "New",
}));
const histogramValues = Array.from({ length: 400 }, (_, index) => Math.abs((Math.sin(index * 12.9898) * 43758.5453) % 1) * 100);
const heatmapCells = ["East", "South", "West", "North"].flatMap((region, row) =>
  MONTH_LABELS.map((month, index) => ({ x: month, y: region, value: (row + 1) * (index + 1) * 7 - (index % 4 === 3 ? 80 : 0) })),
);

const METRIC_OPTIONS = [
  { value: "orders.revenue", label: "Revenue" },
  { value: "orders.count", label: "Order count" },
  { value: "customers.count", label: "Customer count" },
  { value: "orders.margin", label: "Gross margin" },
];

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
  const [pickedMetric, setPickedMetric] = useState<string | null>("orders.revenue");
  const [pickedMetrics, setPickedMetrics] = useState<string[]>(["orders.revenue", "orders.count"]);
  const [pickedFruit, setPickedFruit] = useState("apple");
  const [compactRows, setCompactRows] = useState(true);
  const [galleryTab, setGalleryTab] = useState("explore");
  const [pickedDate, setPickedDate] = useState<string | null>("2025-06-15");
  const [pickedRange, setPickedRange] = useState<DatePickerRange | null>({ from: "2025-06-01", to: "2025-06-21" });
  const stackedSeries = useMemo(() => rowsToSeries(planRows, { x: "month", y: "revenue", series: "plan" }), []);
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
          <p className="text-2xs text-faint">
            Canonical React primitives · <a className="text-accent hover:underline" href="/typography">typography workbench →</a>
          </p>
        </div>
        <ThemeToggle />
      </header>

      <div className="space-y-8 p-4">
        <section>
          <h2 className="mb-2 text-xs font-medium text-muted">Metric cards</h2>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
            <MetricCard metric="orders.revenue" label="Revenue" value={288291} format={{ format: "currency" }} selected={selectedMetric === "orders.revenue"} sparkValues={[31, 42, 38, 55, 60, 74]} onSelect={setSelectedMetric} />
            <MetricCard metric="orders.count" label="Order Count" value={447} selected={selectedMetric === "orders.count"} sparkValues={[21, 34, 28, 41, 44, 51]} onSelect={setSelectedMetric} />
            <MetricCard
              metric="orders.margin"
              label="Gross Margin"
              value={0.324}
              format={{ format: "percent" }}
              delta={{ label: "+1.1pt", tone: "positive" }}
              comparison="vs previous month"
              sparkValues={[24, 26, 29, 27, 31, 32]}
              selected={selectedMetric === "orders.margin"}
              onSelect={setSelectedMetric}
            />
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-medium text-muted">Leaderboard cards</h2>
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
          <h2 className="mb-2 text-xs font-medium text-muted">Filters and controls</h2>
          <div className="space-y-3">
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
            <div className="flex flex-wrap items-center gap-3">
              <DateRangeControl range={dateRange} onChange={setDateRange} comparison={comparison} onComparisonChange={setComparison} />
              <GrainSelect grain={grain} options={["day", "week", "month", "quarter", "year"]} onChange={setGrain} />
              <ViewSwitcher view={view} onChange={setView} />
              <span className="inline-flex items-center gap-1.5 text-2xs text-muted"><StatusDot status="ok" /> Ready</span>
              <span className="inline-flex items-center gap-1.5 text-2xs text-muted"><StatusDot status="loading" /> Updating</span>
              <button
                type="button"
                onClick={() => setGalleryFilters({ "orders.region": ["East", "West"], "orders.status": ["completed"] })}
                className="inline-flex h-7 items-center rounded-full border border-line px-3 text-xs text-muted hover:bg-surface-soft"
              >
                Reset filters
              </button>
            </div>
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-medium text-muted">Charts</h2>
          <div className="grid gap-3 xl:grid-cols-2">
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Sparkline</h3>
              <Sparkline
                values={[31, 42, 38, 55, 60, 74, 69, 83]}
                labels={["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"]}
                ariaLabel="Eight month revenue trend"
              />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Column chart</h3>
              <ColumnChart data={[{ label: "East", value: 83 }, { label: "South", value: 81 }, { label: "West", value: 72 }, { label: "Returns", value: -18 }]} />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Line chart compatibility export</h3>
              <LineChart data={[{ label: "Jan", value: 31 }, { label: "Feb", value: 42 }, { label: "Mar", value: 38 }, { label: "Apr", value: 55 }, { label: "May", value: 60 }]} />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Interactive time series</h3>
              <TimeSeriesChart
                points={[31, 42, 38, 55, 60, 74].map((y, index) => ({ x: `2025-0${index + 1}-01`, y }))}
                comparison={[27, 35, 33, 48, 52, 61].map((y, index) => ({ x: `2024-0${index + 1}-01`, y }))}
                formatValue={(value) => `$${value.toLocaleString()}k`}
              />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Donut</h3>
              <DonutChart data={[{ label: "iOS", value: 4200 }, { label: "Android", value: 3100 }, { label: "Web", value: 1900 }, { label: "Other", value: 400 }]} />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Stacked area (long rows via rowsToSeries)</h3>
              <StackedAreaChart labels={stackedSeries.labels} series={stackedSeries.series} height={200} />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Bar + line combo (dual axis)</h3>
              <BarLineCombo
                data={MONTH_LABELS.map((label, index) => ({ label, bar: 200 + index * 42, line: 0.02 + index * 0.004 }))}
                barLabel="Revenue"
                lineLabel="Conversion"
                formatLine={(value) => `${(value * 100).toFixed(1)}%`}
                height={200}
              />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Histogram</h3>
              <HistogramChart values={histogramValues} bins={20} />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Scatter</h3>
              <ScatterChart points={scatterPoints} xLabel="Sessions" yLabel="Orders" height={200} />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Heatmap</h3>
              <HeatmapChart cells={heatmapCells} height={200} />
            </article>
            <article className="min-w-0">
              <h3 className="mb-2 text-xs font-semibold">Waterfall</h3>
              <WaterfallChart
                data={[
                  { label: "Gross", value: 1000, isTotal: true },
                  { label: "Refunds", value: -140 },
                  { label: "Discounts", value: -90 },
                  { label: "Upsells", value: 130 },
                  { label: "Net", value: 900, isTotal: true },
                ]}
                height={200}
              />
            </article>
            <article className="min-w-0 xl:col-span-2">
              <h3 className="mb-2 text-xs font-semibold">Network</h3>
              <NetworkChart
                nodes={[
                  { id: "orders", group: "fact" },
                  { id: "payments", group: "fact" },
                  { id: "customers", group: "dimension" },
                  { id: "products", group: "dimension" },
                  { id: "regions", group: "dimension" },
                ]}
                links={[
                  { source: "orders", target: "customers", weight: 3 },
                  { source: "orders", target: "products", weight: 2 },
                  { source: "orders", target: "regions" },
                  { source: "payments", target: "orders", weight: 2 },
                  { source: "payments", target: "customers" },
                ]}
                height={260}
              />
            </article>
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-medium text-muted">Inputs</h2>
          <div className="space-y-3">
            <div className="flex flex-wrap items-center gap-3">
              <Button variant="primary">Primary</Button>
              <Button>Secondary</Button>
              <Button variant="ghost">Ghost</Button>
              <Button variant="danger">Danger</Button>
              <Button size="sm">Small</Button>
              <Select
                label="Fruit"
                value={pickedFruit}
                onChange={setPickedFruit}
                options={[{ value: "apple", label: "Apple" }, { value: "banana", label: "Banana" }, { value: "cherry", label: "Cherry" }]}
              />
              <Switch checked={compactRows} onChange={setCompactRows} label="Compact rows" />
              <Tabs
                tabs={[{ key: "explore", label: "Explore" }, { key: "pivot", label: "Pivot" }, { key: "sql", label: "SQL" }]}
                active={galleryTab}
                onChange={setGalleryTab}
              />
              <Tooltip content="Revenue in USD, net of refunds">
                <span className="cursor-help text-2xs text-muted underline decoration-dotted">What is revenue?</span>
              </Tooltip>
            </div>
            <div className="flex flex-wrap items-start gap-3">
              <Combobox value={pickedMetric} onChange={setPickedMetric} options={METRIC_OPTIONS} ariaLabel="Metric" />
              <Combobox multiple values={pickedMetrics} onChange={setPickedMetrics} options={METRIC_OPTIONS} ariaLabel="Metrics" />
              <DatePicker mode="single" value={pickedDate} onChange={setPickedDate} ariaLabel="Date" />
              <DatePicker mode="range" value={pickedRange} onChange={setPickedRange} ariaLabel="Range" />
            </div>
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-medium text-muted">Analytical table</h2>
          <DataTable
            columns={tableColumns}
            rows={sortedRows}
            pageSize={6}
            sortKey={sort.key}
            sortDir={sort.dir}
            onSort={(key) => setSort((current) => ({ key, dir: current.key === key && current.dir === "desc" ? "asc" : "desc" }))}
            renderCell={(column, value) => column.numeric ? Number(value).toLocaleString() : String(value)}
            searchable
            totals={{ region: "count", revenue: "sum", orders: "sum" }}
          />
          <div className="mt-3">
            <h3 className="mb-2 text-xs font-semibold text-muted">Distribution-friendly data preview</h3>
            <DataPreviewTable result={{ columns: ["region", "revenue", "orders"], sample_rows: tableRows.slice(0, 8) }} pageSize={4} />
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-medium text-muted">Query inspection</h2>
          <QueryDebugPanel
            inputs={{ Revenue: { metrics: ["orders.revenue"], dimensions: ["orders.region"], filters: ["orders.status = 'completed'"] } }}
            queries={{ Revenue: "SELECT orders.region, SUM(orders.revenue) AS revenue\nFROM orders\nWHERE orders.status = 'completed'\nGROUP BY orders.region\nORDER BY revenue DESC" }}
          />
        </section>

        <section>
          <h2 className="mb-2 text-xs font-medium text-muted">States</h2>
          <div className="grid gap-2 lg:grid-cols-3">
            <LoadingState message="Loading metric results…" />
            <EmptyState message="No matching rows." />
            <ErrorState message="The query could not be completed." />
          </div>
        </section>

        <section>
          <h2 className="mb-2 text-xs font-medium text-muted">Dashboard shell</h2>
          {/* No wrapper card: the shell composes cards itself, and nesting containers is off limits. */}
          <div>
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
