import { useEffect, useMemo, useState } from "react";
import type { ResultRow } from "../data/types";
import { paginateRows } from "../lib/uiCore.js";

export type Column = {
  key: string; // result column alias
  label: string;
  numeric?: boolean;
  sortable?: boolean;
};

export type TotalKind = "sum" | "avg" | "min" | "max" | "count";

type DataTableProps = {
  columns: Column[];
  rows: ResultRow[];
  loading?: boolean;
  sortKey?: string;
  sortDir?: "asc" | "desc";
  onSort?: (key: string) => void;
  renderCell: (column: Column, value: unknown) => string;
  pageSize?: number;
  /** Client-side substring filter over every column's raw values. */
  searchable?: boolean;
  /** Aggregate footer per column key, e.g. { revenue: "sum", growth: "avg" }. */
  totals?: Partial<Record<string, TotalKind>>;
  /** Keep the header row visible while the body scrolls. */
  stickyHeader?: boolean;
};

/** Column aggregate over the (filtered) rows; non-finite values are skipped. Exported for tests. */
export function columnTotal(rows: ResultRow[], key: string, kind: TotalKind): number {
  if (kind === "count") return rows.length;
  const values = rows.map((row) => (typeof row[key] === "number" ? (row[key] as number) : Number(row[key]))).filter(Number.isFinite);
  if (values.length === 0) return Number.NaN;
  if (kind === "min") return Math.min(...values);
  if (kind === "max") return Math.max(...values);
  const sum = values.reduce((total, value) => total + value, 0);
  return kind === "avg" ? sum / values.length : sum;
}

const TOTAL_LABEL: Record<TotalKind, string> = { sum: "Σ", avg: "avg", min: "min", max: "max", count: "n" };

export function DataTable({
  columns,
  rows,
  loading,
  sortKey,
  sortDir,
  onSort,
  renderCell,
  pageSize = 50,
  searchable,
  totals,
  stickyHeader = true,
}: DataTableProps) {
  const [page, setPage] = useState(0);
  const [search, setSearch] = useState("");

  const filteredRows = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!searchable || !needle) return rows;
    return rows.filter((row) => columns.some((column) => String(row[column.key] ?? "").toLowerCase().includes(needle)));
  }, [rows, columns, search, searchable]);

  const { paginate, pageCount, safePage, start, visibleRows } = paginateRows(filteredRows, page, pageSize);

  useEffect(() => {
    setPage(0);
  }, [rows, pageSize, sortKey, sortDir, search]);

  const hasTotals = totals && columns.some((column) => totals[column.key]);

  return (
    <div className="overflow-hidden border border-line bg-surface">
      {searchable ? (
        <div className="flex items-center gap-2 border-b border-line px-3 py-1.5">
          <input
            type="search"
            aria-label="Search rows"
            placeholder="Search…"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            className="w-full max-w-64 rounded-full border border-line bg-surface px-2.5 py-1 text-2xs text-ink placeholder:text-faint"
          />
          {search ? (
            <span className="whitespace-nowrap text-2xs text-faint tnum">
              {filteredRows.length.toLocaleString()} of {rows.length.toLocaleString()}
            </span>
          ) : null}
        </div>
      ) : null}
      <div className="overflow-auto">
        <table className="w-max min-w-full border-collapse text-xs" data-testid="pivot-table">
          <thead>
          <tr className="bg-surface-soft">
            {columns.map((column) => {
              const active = sortKey === column.key;
              return (
                <th
                  key={column.key}
                  className={`max-w-80 whitespace-nowrap border-b border-line bg-surface-soft px-3 py-1.5 font-semibold text-faint ${
                    column.numeric ? "min-w-32 text-right" : "min-w-40 text-left"
                  } ${stickyHeader ? "sticky top-0 z-10" : ""}`}
                >
                  {column.sortable && onSort ? (
                    <button
                      type="button"
                      onClick={() => onSort(column.key)}
                      aria-label={`Sort by ${column.label}${active ? `, currently ${sortDir === "asc" ? "ascending" : "descending"}` : ""}`}
                      className={`table-sort inline-flex max-w-full items-center gap-1 whitespace-nowrap hover:text-ink ${active ? "text-ink" : ""}`}
                    >
                      <span className="truncate">{column.label}</span>
                      <span aria-hidden="true" className="text-[9px]">
                        {active ? (sortDir === "asc" ? "▲" : "▼") : "↕"}
                      </span>
                    </button>
                  ) : (
                    <span className="block truncate" title={column.label}>{column.label}</span>
                  )}
                </th>
              );
            })}
          </tr>
          </thead>
          <tbody>
          {loading && filteredRows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="px-3 py-6 text-center text-faint">
                Loading…
              </td>
            </tr>
          ) : filteredRows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="px-3 py-6 text-center text-faint">
                {search ? "No matching rows" : "No rows"}
              </td>
            </tr>
          ) : (
            visibleRows.map((row, index) => (
              <tr key={start + index} className="hover:bg-surface-soft">
                {columns.map((column) => {
                  const cellText = renderCell(column, row[column.key]);
                  return (
                    <td
                      key={column.key}
                      className={`max-w-80 whitespace-nowrap border-b border-line px-3 py-1.5 text-muted ${
                        column.numeric ? "min-w-32 text-right font-mono tnum text-ink" : "min-w-40"
                      }`}
                    >
                      <span className="block max-w-80 truncate" title={cellText}>{cellText}</span>
                    </td>
                  );
                })}
              </tr>
            ))
          )}
          </tbody>
          {hasTotals && filteredRows.length > 0 ? (
            <tfoot>
              <tr className="bg-surface-soft" data-testid="table-totals">
                {columns.map((column) => {
                  const kind = totals?.[column.key];
                  if (!kind) return <td key={column.key} className="border-t border-line px-3 py-1.5" />;
                  const total = columnTotal(filteredRows, column.key, kind);
                  const text = kind === "count" ? total.toLocaleString() : Number.isFinite(total) ? renderCell(column, total) : "—";
                  return (
                    <td
                      key={column.key}
                      data-total={kind}
                      className={`whitespace-nowrap border-t border-line px-3 py-1.5 font-mono tnum font-medium text-ink ${
                        column.numeric ? "text-right" : "text-left"
                      }`}
                    >
                      <span aria-hidden="true" className="mr-1 text-2xs text-faint">{TOTAL_LABEL[kind]}</span>
                      {text}
                    </td>
                  );
                })}
              </tr>
            </tfoot>
          ) : null}
        </table>
      </div>
      {paginate ? (
        <div
          data-testid="pivot-table-pager"
          className="flex items-center justify-between gap-3 border-t border-line px-3 py-1 text-2xs text-faint"
        >
          <span className="tnum">
            {start + 1}–{Math.min(start + pageSize, filteredRows.length)} of {filteredRows.length.toLocaleString()}
            {loading ? " · Updating…" : ""}
          </span>
          <div className="flex gap-1">
            <button
              type="button"
              disabled={safePage === 0}
              onClick={() => setPage((value) => Math.max(0, value - 1))}
              className="table-pager-button px-2 py-1 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
            >
              Prev
            </button>
            <button
              type="button"
              disabled={safePage >= pageCount - 1}
              onClick={() => setPage((value) => Math.min(pageCount - 1, value + 1))}
              className="table-pager-button px-2 py-1 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
