import { useMemo, useState } from "react";
import { aliasOf } from "../data/types";
import { labelize } from "../lib/format";
import { composeFilters, dimTypes, previewRows } from "../lib/queries";
import { useExplorer } from "../state/ExplorerContext";
import { useQueryResult } from "../state/useQueryResult";
import { DataTable, type Column } from "./DataTable";

/** Shared Explore/Pivot raw-row preview. It overlays the stage and never creates a third view state. */
export function RowPreviewDrawer() {
  const [open, setOpen] = useState(false);
  const { state, catalog, backend } = useExplorer();
  const model = catalog.models.find((candidate) => candidate.name === state.model);
  const dimensions = model?.dimensions ?? [];
  const validDimensions = state.pivotDims.filter((ref) => dimensions.some((dimension) => dimension.ref === ref));
  const selectedDimensions = validDimensions.length ? validDimensions : dimensions.slice(0, 6).map((dimension) => dimension.ref);
  const timeRef = model?.timeDimension?.ref;
  const types = useMemo(() => dimTypes(dimensions), [dimensions]);
  const filters = useMemo(
    () => composeFilters(state.filters, { timeRef, range: state.dateRange, types }),
    [state.filters, timeRef, state.dateRange, types],
  );
  // A row preview is raw detail, so aggregate/shared metrics are intentionally excluded. Mixing
  // them into an ungrouped query produces invalid SQL for aggregate metric expressions and makes
  // the preview depend on whichever KPI happens to be selected.
  const query = open && model ? previewRows({ dimensions: selectedDimensions, metrics: [] }, filters, 50) : null;
  const { result, loading, error } = useQueryResult(backend, query);
  const columns: Column[] = [
    ...selectedDimensions.map((ref) => ({
      key: aliasOf(ref),
      label: dimensions.find((dimension) => dimension.ref === ref)?.label ?? labelize(ref),
    })),
  ];

  return (
    <section
      data-testid="row-preview-drawer"
      data-open={open || undefined}
      className="absolute inset-x-0 bottom-0 z-30 border-t border-line bg-surface shadow-[0_-8px_24px_rgba(0,0,0,0.16)]"
    >
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
        className="flex h-8 w-full items-center justify-between px-3 text-left text-2xs font-semibold uppercase tracking-wide text-muted hover:bg-surface-soft hover:text-ink"
      >
        <span>Rows preview</span>
        <span aria-hidden="true">{open ? "▼" : "▲"}</span>
      </button>
      {open ? (
        <div className="max-h-[45vh] overflow-auto border-t border-line">
          {error ? <p role="alert" className="p-3 text-xs text-danger">{error}</p> : columns.length === 0 ? (
            <p className="p-3 text-xs text-faint">No preview fields are available.</p>
          ) : (
            <DataTable
              columns={columns}
              rows={result?.rows ?? []}
              loading={loading}
              pageSize={10}
              renderCell={(_column, value) => (value == null || value === "" ? "—" : String(value))}
            />
          )}
        </div>
      ) : null}
    </section>
  );
}
