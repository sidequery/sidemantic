import { useEffect, useRef, useState } from "react";
import { ChartTooltip, useChartTooltip } from "./chart-tooltip";
import { formatValue } from "./types";

type SparklineProps = {
  values: number[];
  labels?: string[];
  height?: number;
  ariaLabel?: string;
};

// Compact trend line — intentionally axis-less (use LineChart when you need axes). Responsive width
// (1:1 viewBox, no aspect distortion), area fill, endpoint marker + hover tooltip, and an a11y label.
export function Sparkline({ values, labels, height = 56, ariaLabel }: SparklineProps) {
  const ref = useRef<SVGSVGElement>(null);
  const [width, setWidth] = useState(160);
  const { tip, handlers } = useChartTooltip();

  useEffect(() => {
    const node = ref.current;
    if (!node || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) setWidth(Math.max(40, entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const numbers = values.filter(Number.isFinite);
  if (numbers.length < 2) {
    return <svg ref={ref} role="img" aria-label={ariaLabel || "No trend data"} className="h-14 w-full" viewBox={`0 0 ${width} ${height}`} />;
  }

  const pad = 4;
  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const span = max - min || 1;
  const coordinates = numbers.map((value, index) => ({
    x: pad + (index / (numbers.length - 1)) * (width - pad * 2),
    y: pad + (1 - (value - min) / span) * (height - pad * 2),
  }));
  const points = coordinates.map(({ x, y }) => `${x.toFixed(1)},${y.toFixed(1)}`);
  const last = coordinates.at(-1)!;
  const lastValue = numbers.at(-1)!;
  const lastLabel = labels?.[numbers.length - 1];
  const summary = ariaLabel || `Trend of ${numbers.length} points, latest ${formatValue(lastValue)}`;

  return (
    <>
      <svg ref={ref} role="img" aria-label={summary} className="h-14 w-full overflow-hidden text-slate-500" viewBox={`0 0 ${width} ${height}`}>
        <path
          d={`M ${coordinates[0].x.toFixed(1)} ${(height - pad).toFixed(1)} L ${points.join(" L ")} L ${last.x.toFixed(1)} ${(height - pad).toFixed(1)} Z`}
          className="fill-slate-500/10"
        />
        <path d={`M ${points.join(" L ")}`} fill="none" stroke="currentColor" strokeWidth="2" />
        <circle cx={last.x} cy={last.y} r="2.5" className="fill-[#6b7cff]" {...handlers(`${lastLabel ? `${lastLabel}: ` : ""}${formatValue(lastValue)}`)} />
      </svg>
      <ChartTooltip tip={tip} />
    </>
  );
}
