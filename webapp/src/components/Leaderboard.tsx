import { displayDimValue, type Tone } from "../lib/format";
import type { ContextColumn } from "../state/explorerState";

export type LeaderboardRow = {
  value: string;
  metric: number;
  /** Optional context figure (% of total / delta) rendered beside the metric, already formatted. */
  context?: { label: string; tone: Tone };
};

type ContextOption = { key: ContextColumn; label: string; title: string };

type LeaderboardProps = {
  dimension: string; // ref, exposed as data-dimension on rows
  title: string;
  metricLabel: string;
  rows: LeaderboardRow[];
  selectedValues?: string[];
  loading?: boolean;
  formatMetric: (value: number) => string;
  onToggle?: (value: string) => void;
  contextColumn?: ContextColumn;
  contextOptions?: ContextOption[];
  onContextColumn?: (column: ContextColumn) => void;
};

const CONTEXT_TONE: Record<Tone, string> = {
  positive: "text-accent",
  negative: "text-danger",
  neutral: "text-faint",
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
  contextColumn = "none",
  contextOptions,
  onContextColumn,
}: LeaderboardProps) {
  const selected = new Set(selectedValues);
  const maxMagnitude = Math.max(1, ...rows.map((row) => Math.abs(row.metric)));
  const showContext = contextColumn !== "none";
  // Grid gains a third, right-aligned column only when a context column is active, so the plain
  // leaderboard keeps its exact two-column layout.
  const rowGrid = showContext
    ? "grid-cols-[minmax(0,1fr)_auto_auto]"
    : "grid-cols-[minmax(0,1fr)_auto]";

  return (
    <section data-testid="dimension-leaderboard" data-dimension={dimension} className="flex flex-col border border-line bg-surface">
      <header className="flex items-center justify-between gap-3 border-b border-line px-3 py-2">
        <div className="flex min-w-0 items-baseline gap-2">
          <h3 className="truncate text-xs font-semibold text-ink">{title}</h3>
          <p className="hidden shrink-0 text-2xs text-faint sm:block">Ranked by {metricLabel}</p>
        </div>
        {contextOptions && onContextColumn ? (
          <div
            role="group"
            aria-label="Context column"
            data-testid="leaderboard-context-toggle"
            className="flex shrink-0 overflow-hidden border border-line text-2xs"
          >
            {contextOptions.map((option) => (
              <button
                key={option.key}
                type="button"
                title={option.title}
                aria-pressed={contextColumn === option.key}
                data-context={option.key}
                data-active={contextColumn === option.key || undefined}
                onClick={() => onContextColumn(option.key)}
                className="border-l border-line px-1.5 py-0.5 font-mono text-faint first:border-l-0 hover:bg-surface-soft data-[active=true]:bg-accent-soft data-[active=true]:text-accent"
              >
                {option.label}
              </button>
            ))}
          </div>
        ) : null}
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
                className={`relative grid w-full ${rowGrid} items-center gap-3 overflow-hidden border-b border-line px-3 py-1.5 text-left text-xs last:border-b-0 hover:bg-surface-soft data-[selected=true]:bg-accent-soft`}
              >
                <span
                  aria-hidden="true"
                  className={`absolute inset-y-0 left-0 ${tone === "negative" ? "bg-danger-soft" : "bg-accent-soft"}`}
                  style={{ width }}
                />
                <span className="relative min-w-0 truncate text-muted">{displayDimValue(row.value)}</span>
                <strong className="relative font-mono tnum font-medium text-ink">{formatMetric(row.metric)}</strong>
                {showContext ? (
                  <span
                    data-testid="leaderboard-context"
                    data-tone={row.context?.tone ?? "neutral"}
                    className={`relative w-14 text-right font-mono tnum text-2xs ${CONTEXT_TONE[row.context?.tone ?? "neutral"]}`}
                  >
                    {row.context?.label ?? "—"}
                  </span>
                ) : null}
              </button>
            );
          })
        )}
      </div>
    </section>
  );
}
