import { aliasFor, formatValue, labelize, type SidemanticQuerySpec } from "./types";

type LeaderboardProps = {
  query: SidemanticQuerySpec;
  selectedValue?: string;
  onSelect?: (selection: { dimension: string; value: string; row: Record<string, unknown> }) => void;
};

export function Leaderboard({ query, selectedValue, onSelect }: LeaderboardProps) {
  const dimensionRef = query.dimensions?.[0] || "";
  const metricRef = query.metrics?.[0] || "";
  const dimensionKey = aliasFor(query, dimensionRef);
  const metricKey = aliasFor(query, metricRef);
  const rows = query.result?.sample_rows || [];
  const metricValues = rows.map((row) => {
    const metricValue = Number(row[metricKey]);
    return Number.isFinite(metricValue) ? metricValue : 0;
  });
  const maxMagnitude = Math.max(0, ...metricValues.map((metricValue) => Math.abs(metricValue))) || 1;

  return (
    <section data-testid="dimension-leaderboard" className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-3 flex items-baseline justify-between gap-3">
        <h2 className="text-sm font-semibold text-slate-950">{labelize(dimensionKey)}</h2>
        <p className="text-xs text-slate-500">Ranked by {labelize(metricKey)}</p>
      </div>
      <div data-testid="leaderboard-rows">
        {rows.map((row, index) => {
          const rawValue = row[dimensionKey];
          const value = String(rawValue ?? "");
          const metricValue = metricValues[index] ?? 0;
          const tone = metricValue < 0 ? "negative" : "positive";
          const selected = selectedValue !== undefined && selectedValue === value;
          const content = (
            <>
              <span
                aria-hidden="true"
                className={`absolute inset-y-1 left-0 ${tone === "negative" ? "bg-red-100" : "bg-indigo-100"}`}
                style={{ width: `${Math.round((Math.abs(metricValue) / maxMagnitude) * 100)}%` }}
              />
              <span className="relative min-w-0 truncate text-slate-700">{value || "—"}</span>
              <strong className="relative font-semibold text-slate-950">{formatValue(metricValue)}</strong>
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
                className="relative grid w-full grid-cols-[minmax(0,1fr)_auto] gap-3 overflow-hidden border-t border-slate-100 px-2 py-2 text-left text-sm first:border-t-0 data-[selected=true]:bg-indigo-50"
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
              className="relative grid w-full grid-cols-[minmax(0,1fr)_auto] gap-3 overflow-hidden border-t border-slate-100 px-2 py-2 text-left text-sm first:border-t-0 data-[selected=true]:bg-indigo-50"
            >
              {content}
            </div>
          );
        })}
      </div>
    </section>
  );
}
