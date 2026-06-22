import { useState } from "react";
import { formatValue, labelize, type SidemanticQueryResult } from "./types";

type DataPreviewTableProps = {
  result?: SidemanticQueryResult;
  // Rows per page. Pagination controls only appear when the result has more rows than this.
  pageSize?: number;
};

export function DataPreviewTable({ result, pageSize = 10 }: DataPreviewTableProps) {
  const columns = result?.columns || [];
  const rows = result?.sample_rows || [];
  const paginate = pageSize > 0 && rows.length > pageSize;
  const pageCount = paginate ? Math.ceil(rows.length / pageSize) : 1;
  const [page, setPage] = useState(0);
  const safePage = Math.min(page, pageCount - 1);
  const start = paginate ? safePage * pageSize : 0;
  const visibleRows = paginate ? rows.slice(start, start + pageSize) : rows;

  return (
    <div className="overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm">
      <div className="overflow-auto">
        <table className="w-full border-collapse text-[13px]">
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column} className="max-w-[160px] truncate border-b border-slate-100 px-3 py-2 text-left text-xs font-semibold text-slate-600">
                  {labelize(column)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, rowIndex) => (
              <tr key={start + rowIndex}>
                {columns.map((column) => (
                  <td key={column} className="max-w-[160px] truncate border-b border-slate-100 px-3 py-2 text-slate-700">
                    {formatValue(row[column])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {paginate ? (
        <div
          data-testid="data-preview-pager"
          className="flex items-center justify-between gap-3 border-t border-slate-100 px-3 py-2 text-xs text-slate-500"
        >
          <span>
            {start + 1}–{Math.min(start + pageSize, rows.length)} of {rows.length.toLocaleString()}
          </span>
          <div className="flex gap-1">
            <button
              type="button"
              data-action="prev-page"
              disabled={safePage === 0}
              onClick={() => setPage((value) => Math.max(0, value - 1))}
              className="rounded border border-slate-200 px-2 py-1 hover:border-slate-300 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Prev
            </button>
            <button
              type="button"
              data-action="next-page"
              disabled={safePage >= pageCount - 1}
              onClick={() => setPage((value) => Math.min(pageCount - 1, value + 1))}
              className="rounded border border-slate-200 px-2 py-1 hover:border-slate-300 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
