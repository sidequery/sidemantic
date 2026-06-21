export type LeaderboardRow = { value: string; metric: number };

type LeaderboardProps = {
  dimension: string; // ref, exposed as data-dimension on rows
  title: string;
  metricLabel: string;
  rows: LeaderboardRow[];
  selectedValues?: string[];
  loading?: boolean;
  formatMetric: (value: number) => string;
  onToggle?: (value: string) => void;
};

export function Leaderboard({
  dimension,
  title,
  metricLabel,
  rows,
  selectedValues = [],
  loading,
  formatMetric,
  onToggle,
}: LeaderboardProps) {
  const selected = new Set(selectedValues);
  const maxMagnitude = Math.max(1, ...rows.map((row) => Math.abs(row.metric)));

  return (
    <section data-testid="dimension-leaderboard" data-dimension={dimension} className="flex flex-col border border-line bg-surface">
      <header className="flex items-baseline justify-between gap-3 border-b border-line px-3 py-2">
        <h3 className="truncate text-xs font-semibold text-ink">{title}</h3>
        <p className="shrink-0 text-2xs text-faint">Ranked by {metricLabel}</p>
      </header>
      <div data-testid="leaderboard-rows">
        {loading && rows.length === 0 ? (
          <div className="space-y-2 p-3">
            {[0, 1, 2, 3].map((i) => (
              <div key={i} className="skeleton h-5 w-full" />
            ))}
          </div>
        ) : rows.length === 0 ? (
          <p className="px-3 py-4 text-xs text-faint">No values</p>
        ) : (
          rows.map((row) => {
            const tone = row.metric < 0 ? "negative" : "positive";
            const isSelected = selected.has(row.value);
            const width = `${Math.round((Math.abs(row.metric) / maxMagnitude) * 100)}%`;
            return (
              <button
                key={`${dimension}:${row.value}`}
                type="button"
                data-dimension={dimension}
                data-value={row.value}
                data-selected={isSelected || undefined}
                data-tone={tone}
                onClick={() => onToggle?.(row.value)}
                className="relative grid w-full grid-cols-[minmax(0,1fr)_auto] items-center gap-3 overflow-hidden border-b border-line px-3 py-1.5 text-left text-xs last:border-b-0 hover:bg-surface-soft data-[selected=true]:bg-accent-soft"
              >
                <span
                  aria-hidden="true"
                  className={`absolute inset-y-0 left-0 ${tone === "negative" ? "bg-danger-soft" : "bg-accent-soft"}`}
                  style={{ width }}
                />
                <span className="relative min-w-0 truncate text-muted">{row.value || "—"}</span>
                <strong className="relative font-mono tnum font-medium text-ink">{formatMetric(row.metric)}</strong>
              </button>
            );
          })
        )}
      </div>
    </section>
  );
}
