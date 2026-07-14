import { DataTable } from "./DataTable";
import { TimeSeriesChart } from "./TimeSeriesChart";
import { formatValue, labelize } from "../lib/format";

export type DistributionResult = {
  columns: string[];
  sample_rows: Record<string, string | number | boolean | null>[];
};

export function DataPreviewTable({ result, pageSize = 10 }: { result?: DistributionResult; pageSize?: number }) {
  const columns = result?.columns ?? [];
  return (
    <DataTable
      columns={columns.map((key) => ({ key, label: labelize(key), numeric: result?.sample_rows.some((row) => typeof row[key] === "number") }))}
      rows={result?.sample_rows ?? []}
      pageSize={pageSize}
      renderCell={(_column, value) => formatValue(value)}
    />
  );
}

export function LineChart({ data, height = 200, ariaLabel }: { data: { label: string; value: number }[]; height?: number; ariaLabel?: string }) {
  return (
    <div style={{ minHeight: height }}>
      <TimeSeriesChart
        points={data.map(({ label, value }) => ({ x: label, y: value }))}
        formatValue={(value) => formatValue(value)}
        ariaLabel={ariaLabel}
      />
    </div>
  );
}
