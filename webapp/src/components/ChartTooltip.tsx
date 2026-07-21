import { type CSSProperties, type MouseEvent, type ReactNode, useState } from "react";

// Canonical React tooltip primitive. The webapp-builder copy is byte-for-byte synchronized so
// generated applications remain standalone without creating a second implementation.
export type ChartTooltipState = { content: ReactNode; x: number; y: number } | null;

export function useChartTooltip() {
  const [tip, setTip] = useState<ChartTooltipState>(null);
  const handlers = (content: ReactNode) => ({
    onMouseEnter: (event: MouseEvent) => setTip({ content, x: event.clientX, y: event.clientY }),
    onMouseMove: (event: MouseEvent) => setTip({ content, x: event.clientX, y: event.clientY }),
    onMouseLeave: () => setTip(null),
  });
  return { tip, handlers };
}

export function ChartTooltip({
  tip,
  position = "fixed",
  offset = 12,
  className,
  style,
}: {
  tip: ChartTooltipState;
  position?: "fixed" | "absolute";
  offset?: number;
  className?: string;
  style?: CSSProperties;
}) {
  if (!tip) return null;
  return (
    <div
      role="tooltip"
      style={{ position, left: tip.x + offset, top: tip.y + offset, pointerEvents: "none", zIndex: 50, ...style }}
      className={className || "rounded-md border border-line bg-surface px-2 py-1.5 text-xs text-ink shadow-[var(--shadow)]"}
    >
      {tip.content}
    </div>
  );
}

export type TooltipRow = { label: string; value: string; swatch?: string };

/** Standard tooltip body shared by the chart components: an optional mono heading plus aligned
 *  label/value rows, with an optional series color swatch per row. Pass the result to the
 *  `handlers(...)` content argument so every chart reads the same on hover. */
export function TooltipRows({ title, rows }: { title?: string; rows: TooltipRow[] }) {
  return (
    <div className="min-w-28">
      {title ? <div className="mb-0.5 font-mono text-faint">{title}</div> : null}
      {rows.map((row, index) => (
        <div key={index} className="flex items-center justify-between gap-3">
          <span className="flex items-center gap-1 text-muted">
            {row.swatch ? <span aria-hidden="true" className="inline-block size-2 rounded-sm" style={{ background: row.swatch }} /> : null}
            {row.label}
          </span>
          <span className="font-mono tnum font-medium text-ink">{row.value}</span>
        </div>
      ))}
    </div>
  );
}
