import { aliasFor, formatValue, labelize, type SidemanticQuerySpec, type ValueFormatOptions } from "./types";

// Extra (non-primary) columns rendered to the right of the ranked metric — e.g. a delta and delta %.
// `bar: true` draws a per-column magnitude bar (used in the expanded view); `signTone: true` colors
// negative values red. The primary metric always gets a bar.
export type LeaderboardColumn = {
  key: string;
  label: string;
  format?: ValueFormatOptions;
  bar?: boolean;
  signTone?: boolean;
};

type LeaderboardProps = {
  query: SidemanticQuerySpec;
  metricRef?: string;
  metricFormat?: ValueFormatOptions;
  extraColumns?: LeaderboardColumn[];
  selectedValue?: string;
  selectedValues?: string[];
  onSelect?: (selection: { dimension: string; value: string; row: Record<string, unknown> }) => void;
  // Collapsed view shows the top `limit` rows; the expanded view shows all of them.
  limit?: number;
  expanded?: boolean;
  onExpand?: () => void;
  onBack?: () => void;
};

function maxMagnitudeOf(values: number[]) {
  return Math.max(0, ...values.map((value) => Math.abs(value))) || 1;
}

export function Leaderboard({
  query,
  metricRef: selectedMetricRef,
  metricFormat,
  extraColumns = [],
  selectedValue,
  selectedValues = [],
  onSelect,
  limit = 6,
  expanded = false,
  onExpand,
  onBack,
}: LeaderboardProps) {
  const dimensionRef = query.dimensions?.[0] || "";
  const metricRef = selectedMetricRef || query.metrics?.[0] || "";
  const dimensionKey = aliasFor(query, dimensionRef);
  const metricKey = aliasFor(query, metricRef);
  const allRows = query.result?.sample_rows || [];
  const rows = expanded ? allRows : allRows.slice(0, limit);
  const selectedSet = new Set([...(selectedValues || []), ...(selectedValue === undefined ? [] : [selectedValue])]);

  const numeric = (row: Record<string, unknown>, key: string) => {
    const value = Number(row[key]);
    return Number.isFinite(value) ? value : 0;
  };
  const metricValues = rows.map((row) => numeric(row, metricKey));
  const maxMagnitude = maxMagnitudeOf(metricValues);
  const barColumns = [{ key: metricKey, bar: true }, ...extraColumns].filter((column) => column.bar);
  const columnMax: Record<string, number> = {};
  for (const column of barColumns) {
    columnMax[column.key] = maxMagnitudeOf(rows.map((row) => numeric(row, column.key)));
  }

  const headerCells = [
    { key: metricKey, label: labelize(metricKey) },
    ...extraColumns.map((column) => ({ key: column.key, label: column.label })),
  ];

  // Expanded: full-width table, one column per metric, each metric column carries its own bar.
  if (expanded) {
    return (
      <section data-testid="dimension-leaderboard" data-expanded="true" className="rounded-lg border border-slate-200 bg-white shadow-sm">
        <div className="flex flex-wrap items-center gap-3 border-b border-slate-200 px-3 py-2">
          {onBack ? (
            <button
              type="button"
              data-action="leaderboard-back"
              onClick={onBack}
              className="inline-flex items-center gap-1 text-sm font-medium text-indigo-600 hover:text-indigo-700"
            >
              <span aria-hidden="true">←</span> All dimensions
            </button>
          ) : null}
          <h2 className="text-sm font-semibold text-slate-950">{labelize(dimensionKey)}</h2>
        </div>
        <div className="overflow-auto">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                <th className="border-b border-slate-100 px-3 py-2 text-left text-xs font-semibold text-slate-600">
                  {labelize(dimensionKey)}
                </th>
                {headerCells.map((cell) => (
                  <th key={cell.key} className="border-b border-slate-100 px-3 py-2 text-right text-xs font-semibold text-slate-600">
                    {cell.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const value = String(row[dimensionKey] ?? "");
                const selected = selectedSet.has(value);
                return (
                  <tr
                    key={`${dimensionRef}:${value}`}
                    data-dimension={dimensionRef}
                    data-value={value}
                    data-selected={selected || undefined}
                    className="data-[selected=true]:bg-slate-100"
                  >
                    <td className="px-3 py-1 text-[11px] text-slate-700">{value || "—"}</td>
                    {[
                      { key: metricKey, label: labelize(metricKey), format: metricFormat, bar: true, signTone: false },
                      ...extraColumns,
                    ].map((column) => {
                        const cellValue = numeric(row, column.key);
                        const tone = cellValue < 0 ? "negative" : "positive";
                        const width = column.bar ? Math.round((Math.abs(cellValue) / (columnMax[column.key] || 1)) * 100) : 0;
                        return (
                          <td
                            key={column.key}
                            data-tone={column.signTone ? tone : undefined}
                            className="relative px-3 py-1 text-right text-[11px] tabular-nums text-slate-950 data-[tone=negative]:text-red-700"
                          >
                            {column.bar ? (
                              <span
                                aria-hidden="true"
                                className={`absolute inset-y-0 right-0 ${tone === "negative" ? "bg-[#b91c1c]/10" : "bg-[#6b7cff]/15"}`}
                                style={{ width: `${width}%` }}
                              />
                            ) : null}
                            <span className="relative">{formatValue(cellValue, column.format)}</span>
                          </td>
                        );
                      },
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    );
  }

  // Collapsed: compact ranked rows with a single bar behind the primary metric, plus plain extra cells.
  const gridTemplate = `minmax(0,1fr) repeat(${1 + extraColumns.length}, auto)`;
  return (
    <section data-testid="dimension-leaderboard" className="rounded-lg border border-slate-200 bg-white shadow-sm">
      <div className="px-3 pb-2 pt-2.5">
        <h2 className="text-sm font-semibold text-slate-950">{labelize(dimensionKey)}</h2>
      </div>
      <div data-testid="leaderboard-rows">
        {rows.map((row, index) => {
          const value = String(row[dimensionKey] ?? "");
          const metricValue = metricValues[index] ?? 0;
          const tone = metricValue < 0 ? "negative" : "positive";
          const selected = selectedSet.has(value);
          const content = (
            <>
              <span
                aria-hidden="true"
                className={`absolute inset-y-0 left-0 ${tone === "negative" ? "bg-[#b91c1c]/10" : "bg-[#6b7cff]/15"}`}
                style={{ width: `${Math.round((Math.abs(metricValue) / maxMagnitude) * 100)}%` }}
              />
              <span className="relative min-w-0 truncate text-slate-700">{value || "—"}</span>
              <strong className="relative text-right font-semibold tabular-nums text-slate-950">
                {formatValue(metricValue, metricFormat)}
              </strong>
              {extraColumns.map((column) => {
                const cellValue = numeric(row, column.key);
                const cellTone = cellValue < 0 ? "negative" : "positive";
                return (
                  <span
                    key={column.key}
                    data-tone={column.signTone ? cellTone : undefined}
                    className="relative text-right tabular-nums text-slate-500 data-[tone=negative]:text-red-700"
                  >
                    {formatValue(cellValue, column.format)}
                  </span>
                );
              })}
            </>
          );

          if (onSelect) {
            return (
              <button
                key={`${dimensionRef}:${value}`}
                type="button"
                data-dimension={dimensionRef}
                data-value={value}
                data-selected={selected || undefined}
                data-tone={tone}
                onClick={() => onSelect({ dimension: dimensionRef, value, row })}
                style={{ gridTemplateColumns: gridTemplate }}
                className="relative grid w-full gap-2 overflow-hidden px-3 py-1 text-left text-[11px] leading-tight data-[selected=true]:bg-slate-100"
              >
                {content}
              </button>
            );
          }

          return (
            <div
              key={`${dimensionRef}:${value}`}
              data-dimension={dimensionRef}
              data-value={value}
              data-selected={selected || undefined}
              data-tone={tone}
              style={{ gridTemplateColumns: gridTemplate }}
              className="relative grid w-full gap-2 overflow-hidden px-3 py-1 text-left text-[11px] leading-tight data-[selected=true]:bg-slate-100"
            >
              {content}
            </div>
          );
        })}
      </div>
      {onExpand && allRows.length > 0 ? (
        <button
          type="button"
          data-action="leaderboard-expand"
          onClick={onExpand}
          className="w-full border-t border-slate-100 px-3 py-2 text-left text-xs font-medium text-slate-500 hover:text-indigo-600"
        >
          Expand table{allRows.length > rows.length ? ` (${allRows.length})` : ""}
        </button>
      ) : null}
    </section>
  );
}
