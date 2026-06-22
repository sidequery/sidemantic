import type { ResultRow } from "../data/types";

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
};

export function DataTable({ columns, rows, loading, sortKey, sortDir, onSort, renderCell }: DataTableProps) {
  return (
    <div className="overflow-auto border border-line bg-surface">
      <table className="w-full border-collapse text-xs" data-testid="pivot-table">
        <thead>
          <tr className="bg-surface-soft">
            {columns.map((column) => {
              const active = sortKey === column.key;
              return (
                <th
                  key={column.key}
                  className={`border-b border-line px-3 py-1.5 font-semibold text-faint ${column.numeric ? "text-right" : "text-left"}`}
                >
                  {column.sortable && onSort ? (
                    <button
                      type="button"
                      onClick={() => onSort(column.key)}
                      className={`inline-flex items-center gap-1 hover:text-ink ${active ? "text-ink" : ""}`}
                    >
                      {column.label}
                      <span aria-hidden="true" className="text-[9px]">
                        {active ? (sortDir === "asc" ? "▲" : "▼") : "↕"}
                      </span>
                    </button>
                  ) : (
                    column.label
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
            rows.map((row, index) => (
              <tr key={index} className="hover:bg-surface-soft">
                {columns.map((column) => (
                  <td
                    key={column.key}
                    className={`border-b border-line px-3 py-1.5 text-muted last:border-b-0 ${
                      column.numeric ? "text-right font-mono tnum text-ink" : ""
                    }`}
                  >
                    {renderCell(column, row[column.key])}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
