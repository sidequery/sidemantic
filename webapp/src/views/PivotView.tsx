import { useMemo, useState } from "react";
import { aliasOf, type CatalogMetric } from "../data/types";
import { DataTable, type Column } from "../components/DataTable";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { EmptyState, ErrorState } from "../components/States";
import { formatValue, labelize } from "../lib/format";
import { graphMetricsForModel } from "../lib/catalog";
import { composeFilters, dimTypes, pivotGroup } from "../lib/queries";
import { useExplorer } from "../state/ExplorerContext";
import { useQueryResult } from "../state/useQueryResult";

export function PivotView() {
  const { state, catalog, backend } = useExplorer();
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

  const [sort, setSort] = useState<{ ref: string; dir: "asc" | "desc" } | null>(null);

  const types = useMemo(() => dimTypes(dims), [dims]);
  const baseFilters = useMemo(
    () => composeFilters(state.filters, { timeRef, range: state.dateRange, types }),
    [state.filters, timeRef, state.dateRange, types],
  );

  const effectiveSort = sort ?? (pivotMetrics[0] ? { ref: pivotMetrics[0], dir: "desc" as const } : null);
  const orderBy = effectiveSort ? [`${effectiveSort.ref} ${effectiveSort.dir.toUpperCase()}`] : undefined;
  const query = pivotMetrics.length || pivotDims.length ? pivotGroup(pivotMetrics, pivotDims, baseFilters, orderBy, 500) : null;
  // Stamp the selected timezone so any time-bucketed pivot dimension truncates in-zone server-side
  // (elided when UTC, matching the pre-E4 request shape).
  const tzQuery = query ? { ...query, timezone: state.timezone } : null;
  const { result, loading, error } = useQueryResult(backend, tzQuery);

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
      sortable: true,
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
