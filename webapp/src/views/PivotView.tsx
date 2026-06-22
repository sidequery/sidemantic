import { useMemo, useState } from "react";
import { aliasOf, type CatalogMetric } from "../data/types";
import { DataTable, type Column } from "../components/DataTable";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { EmptyState, ErrorState } from "../components/States";
import { formatValue, labelize } from "../lib/format";
import { graphMetricsForModel } from "../lib/catalog";
import { composeFilters, dimTypes, pivotGroup, previewRows } from "../lib/queries";
import { useExplorer } from "../state/ExplorerContext";
import { useQueryResult } from "../state/useQueryResult";

function toggleInArray(list: string[], item: string): string[] {
  return list.includes(item) ? list.filter((x) => x !== item) : [...list, item];
}

function Chip({ active, label, onClick }: { active: boolean; label: string; onClick: () => void }) {
  return (
    <button
      type="button"
      data-selected={active || undefined}
      onClick={onClick}
      className="border border-line bg-surface px-2 py-0.5 text-2xs text-muted hover:border-faint data-[selected=true]:border-accent data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent"
    >
      {label}
    </button>
  );
}

export function PivotView() {
  const { state, dispatch, catalog, backend } = useExplorer();
  const model = catalog.models.find((m) => m.name === state.model);
  const dims = model?.dimensions ?? [];
  const graphMetrics = graphMetricsForModel(catalog, state.model);
  const allMetrics: CatalogMetric[] = useMemo(
    () => [...(model?.metrics ?? []), ...graphMetrics],
    [model, graphMetrics],
  );
  const timeRef = model?.timeDimension?.ref;

  // Sanitize selections against the current model so a deep-link from another model (or a stale
  // default) never pins fields this model doesn't have.
  const validDim = (ref: string) => dims.some((d) => d.ref === ref);
  const validMetric = (ref: string) => allMetrics.some((m) => m.ref === ref);
  const pivotDims = state.pivotDims.filter(validDim);
  const sanitizedMetrics = state.pivotMetrics.filter(validMetric);
  const fallbackMetric = validMetric(state.selectedMetric) ? state.selectedMetric : model?.metrics[0]?.ref;
  const pivotMetrics = sanitizedMetrics.length ? sanitizedMetrics : fallbackMetric ? [fallbackMetric] : [];

  const [ungrouped, setUngrouped] = useState(false);
  const [sort, setSort] = useState<{ ref: string; dir: "asc" | "desc" } | null>(null);

  const types = useMemo(() => dimTypes(dims), [dims]);
  const baseFilters = useMemo(
    () => composeFilters(state.filters, { timeRef, range: state.dateRange, types }),
    [state.filters, timeRef, state.dateRange, types],
  );

  const effectiveSort = sort ?? (pivotMetrics[0] ? { ref: pivotMetrics[0], dir: "desc" as const } : null);
  const orderBy = !ungrouped && effectiveSort ? [`${effectiveSort.ref} ${effectiveSort.dir.toUpperCase()}`] : undefined;

  const query =
    pivotMetrics.length || pivotDims.length
      ? ungrouped
        ? previewRows({ dimensions: pivotDims, metrics: pivotMetrics }, baseFilters, 50)
        : pivotGroup(pivotMetrics, pivotDims, baseFilters, orderBy, 500)
      : null;
  const { result, loading, error } = useQueryResult(backend, query);

  const metricByRef = useMemo(() => new Map(allMetrics.map((m) => [m.ref, m])), [allMetrics]);
  const refByAlias = useMemo(() => {
    const map = new Map<string, string>();
    for (const ref of [...pivotDims, ...pivotMetrics]) map.set(aliasOf(ref), ref);
    return map;
  }, [pivotDims, pivotMetrics]);

  const columns: Column[] = [
    ...pivotDims.map((ref) => ({ key: aliasOf(ref), label: dims.find((d) => d.ref === ref)?.label ?? labelize(ref), numeric: false })),
    ...pivotMetrics.map((ref) => ({
      key: aliasOf(ref),
      label: metricByRef.get(ref)?.label ?? labelize(ref),
      numeric: true,
      sortable: !ungrouped,
    })),
  ];

  function onSort(key: string) {
    const ref = refByAlias.get(key);
    if (!ref) return;
    setSort((prev) => (prev && prev.ref === ref ? { ref, dir: prev.dir === "desc" ? "asc" : "desc" } : { ref, dir: "desc" }));
  }

  function renderCell(column: Column, value: unknown): string {
    if (column.numeric) {
      const metric = metricByRef.get(refByAlias.get(column.key) ?? "");
      return formatValue(value, { format: metric?.format, type: metric?.type });
    }
    return value === null || value === undefined || value === "" ? "—" : String(value);
  }

  if (!model) return <div className="p-4"><EmptyState message="No model available." /></div>;

  return (
    <div className="flex flex-col gap-4 p-4">
      <section className="flex flex-col gap-2 border border-line bg-surface p-3">
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="mr-1 text-2xs font-semibold uppercase tracking-wide text-faint">Group by</span>
          {dims.map((dim) => (
            <Chip
              key={dim.ref}
              active={pivotDims.includes(dim.ref)}
              label={dim.label}
              onClick={() => dispatch({ type: "setPivotDims", dims: toggleInArray(pivotDims, dim.ref) })}
            />
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="mr-1 text-2xs font-semibold uppercase tracking-wide text-faint">Metrics</span>
          {allMetrics.map((metric) => (
            <Chip
              key={metric.ref}
              active={pivotMetrics.includes(metric.ref)}
              label={metric.label}
              onClick={() => dispatch({ type: "setPivotMetrics", metrics: toggleInArray(pivotMetrics, metric.ref) })}
            />
          ))}
        </div>
        <label className="flex items-center gap-1.5 text-2xs text-muted">
          <input type="checkbox" checked={ungrouped} onChange={(event) => setUngrouped(event.target.checked)} />
          Raw rows (ungrouped, first 50)
        </label>
      </section>

      {error ? (
        <ErrorState message={error} />
      ) : columns.length === 0 ? (
        <EmptyState title="Nothing selected" message="Pick at least one metric or dimension above." />
      ) : (
        <DataTable
          columns={columns}
          rows={result?.rows ?? []}
          loading={loading}
          sortKey={effectiveSort ? aliasOf(effectiveSort.ref) : undefined}
          sortDir={effectiveSort?.dir}
          onSort={onSort}
          renderCell={renderCell}
        />
      )}

      <QueryDebugPanel queries={{ Pivot: result?.sql }} />
    </div>
  );
}
