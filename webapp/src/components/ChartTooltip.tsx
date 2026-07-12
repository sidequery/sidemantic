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
      className={className || "rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-white shadow"}
    >
      {tip.content}
    </div>
  );
}
