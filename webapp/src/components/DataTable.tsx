import { useEffect, useState } from "react";
import type { ResultRow } from "../data/types";
import { paginateRows } from "../lib/uiCore.js";

export type Column = {
  key: string; // result column alias
  label: string;
  numeric?: boolean;
  sortable?: boolean;
};

type DataTableProps = {
  columns: Column[];
  rows: ResultRow[];
  loading?: boolean;
  sortKey?: string;
  sortDir?: "asc" | "desc";
  onSort?: (key: string) => void;
  renderCell: (column: Column, value: unknown) => string;
  pageSize?: number;
};

export function DataTable({ columns, rows, loading, sortKey, sortDir, onSort, renderCell, pageSize = 50 }: DataTableProps) {
  const [page, setPage] = useState(0);
  const { paginate, pageCount, safePage, start, visibleRows } = paginateRows(rows, page, pageSize);

  useEffect(() => {
    setPage(0);
  }, [rows, pageSize, sortKey, sortDir]);

  return (
    <div className="overflow-hidden border border-line bg-surface">
      <div className="overflow-auto">
        <table className="w-max min-w-full border-collapse text-xs" data-testid="pivot-table">
          <thead>
          <tr className="bg-surface-soft">
            {columns.map((column) => {
              const active = sortKey === column.key;
              return (
                <th
                  key={column.key}
                  className={`max-w-80 whitespace-nowrap border-b border-line px-3 py-1.5 font-semibold text-faint ${column.numeric ? "min-w-32 text-right" : "min-w-40 text-left"}`}
                >
                  {column.sortable && onSort ? (
                    <button
                      type="button"
                      onClick={() => onSort(column.key)}
                      aria-label={`Sort by ${column.label}${active ? `, currently ${sortDir === "asc" ? "ascending" : "descending"}` : ""}`}
                      className={`inline-flex min-h-11 max-w-full items-center gap-1 whitespace-nowrap hover:text-ink ${active ? "text-ink" : ""}`}
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
          {loading && rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="px-3 py-6 text-center text-faint">
                Loading…
              </td>
            </tr>
          ) : rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="px-3 py-6 text-center text-faint">
                No rows
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
        </table>
      </div>
      {paginate ? (
        <div
          data-testid="pivot-table-pager"
          className="flex min-h-11 items-center justify-between gap-3 border-t border-line px-3 text-2xs text-faint"
        >
          <span className="tnum">
            {start + 1}–{Math.min(start + pageSize, rows.length)} of {rows.length.toLocaleString()}
            {loading ? " · Updating…" : ""}
          </span>
          <div className="flex gap-1">
            <button
              type="button"
              disabled={safePage === 0}
              onClick={() => setPage((value) => Math.max(0, value - 1))}
              className="min-h-11 min-w-11 px-2 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
            >
              Prev
            </button>
            <button
              type="button"
              disabled={safePage >= pageCount - 1}
              onClick={() => setPage((value) => Math.min(pageCount - 1, value + 1))}
              className="min-h-11 min-w-11 px-2 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
