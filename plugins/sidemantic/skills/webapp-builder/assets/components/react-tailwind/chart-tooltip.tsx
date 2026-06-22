import { type MouseEvent, useState } from "react";

// Shared hover-tooltip plumbing for the SVG charts. Dependency-free: a fixed-position div that
// follows the cursor. Spread `handlers(text)` onto any SVG shape and render <ChartTooltip tip={tip} />
// once per chart.
export type ChartTooltipState = { text: string; x: number; y: number } | null;

export function useChartTooltip() {
  const [tip, setTip] = useState<ChartTooltipState>(null);
  const handlers = (text: string) => ({
    onMouseEnter: (event: MouseEvent) => setTip({ text, x: event.clientX, y: event.clientY }),
    onMouseMove: (event: MouseEvent) => setTip({ text, x: event.clientX, y: event.clientY }),
    onMouseLeave: () => setTip(null),
  });
  return { tip, handlers };
}

export function ChartTooltip({ tip }: { tip: ChartTooltipState }) {
  if (!tip) return null;
  return (
    <div
      role="tooltip"
      style={{ position: "fixed", left: tip.x + 12, top: tip.y + 12, pointerEvents: "none", zIndex: 50 }}
      className="rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-white shadow"
    >
      {tip.text}
    </div>
  );
}
