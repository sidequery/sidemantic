import { formatValue, labelize, type SidemanticQueryResult } from "./types";

type DataPreviewTableProps = {
  result?: SidemanticQueryResult;
};

export function DataPreviewTable({ result }: DataPreviewTableProps) {
  const columns = result?.columns || [];
  const rows = result?.sample_rows || [];

  return (
    <div className="overflow-auto rounded-lg border border-slate-200 bg-white shadow-sm">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column} className="border-b border-slate-100 px-3 py-2 text-left text-xs font-semibold text-slate-600">
                {labelize(column)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {columns.map((column) => (
                <td key={column} className="border-b border-slate-100 px-3 py-2 text-slate-700">
                  {formatValue(row[column])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
