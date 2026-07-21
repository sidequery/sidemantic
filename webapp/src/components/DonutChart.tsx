import { useEffect, useRef, useState } from "react";
import { ChartTooltip, TooltipRows, useChartTooltip } from "./ChartTooltip";
import { formatCompact, formatValue } from "../lib/format";
import { observeWidth, vizColor } from "../lib/viz";

export type DonutDatum = { label: string; value: number };

export type DonutSegment = DonutDatum & {
  /** Fraction of the positive total, 0..1. */
  share: number;
  startAngle: number;
  endAngle: number;
  colorIndex: number;
};

type DonutChartProps = {
  data: DonutDatum[];
  height?: number;
  /** Text under the center total; the total itself is the sum of positive values. */
  centerLabel?: string;
  format?: (value: number) => string;
  ariaLabel?: string;
};

const TAU = Math.PI * 2;

/** Angular layout for the donut. Non-finite and non-positive values are dropped — a donut only
 *  reads as part-of-whole for positive parts. Exported for tests and adapters. */
export function donutSegments(data: DonutDatum[]): DonutSegment[] {
  const positive = data
    .map((item, index) => ({ ...item, colorIndex: index }))
    .filter((item) => Number.isFinite(item.value) && item.value > 0);
  const total = positive.reduce((sum, item) => sum + item.value, 0);
  if (total <= 0) return [];
  let angle = -Math.PI / 2;
  return positive.map((item) => {
    const share = item.value / total;
    const startAngle = angle;
    angle += share * TAU;
    return { label: item.label, value: item.value, share, startAngle, endAngle: angle, colorIndex: item.colorIndex };
  });
}

function arcPath(cx: number, cy: number, r0: number, r1: number, a0: number, a1: number): string {
  // Cap just under a full turn: an arc with identical endpoints renders as empty.
  const sweep = Math.min(a1 - a0, TAU - 0.0001);
  const b1 = a0 + sweep;
  const large = sweep > Math.PI ? 1 : 0;
  const x0o = cx + r1 * Math.cos(a0);
  const y0o = cy + r1 * Math.sin(a0);
  const x1o = cx + r1 * Math.cos(b1);
  const y1o = cy + r1 * Math.sin(b1);
  const x0i = cx + r0 * Math.cos(b1);
  const y0i = cy + r0 * Math.sin(b1);
  const x1i = cx + r0 * Math.cos(a0);
  const y1i = cy + r0 * Math.sin(a0);
  return [
    `M ${x0o.toFixed(2)} ${y0o.toFixed(2)}`,
    `A ${r1} ${r1} 0 ${large} 1 ${x1o.toFixed(2)} ${y1o.toFixed(2)}`,
    `L ${x0i.toFixed(2)} ${y0i.toFixed(2)}`,
    `A ${r0} ${r0} 0 ${large} 0 ${x1i.toFixed(2)} ${y1i.toFixed(2)}`,
    "Z",
  ].join(" ");
}

// Part-of-whole donut: theme palette arcs, center total, side legend with shares, hover tooltips.
export function DonutChart({ data, height = 200, centerLabel = "Total", format = formatValue, ariaLabel }: DonutChartProps) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(360);
  const { tip, handlers } = useChartTooltip();
  useEffect(() => observeWidth(ref.current, 220, setWidth), []);

  const segments = donutSegments(data);
  const total = segments.reduce((sum, segment) => sum + segment.value, 0);
  const size = Math.min(height, width * 0.55);
  const cx = size / 2;
  const cy = height / 2;
  const r1 = size / 2 - 6;
  const r0 = Math.max(r1 * 0.62, r1 - 34);
  const summary = ariaLabel || `Donut chart, ${segments.length} segments totaling ${formatCompact(total)}`;

  if (segments.length === 0) {
    return <div className="grid h-[200px] place-items-center text-xs text-faint">No positive values to chart.</div>;
  }

  return (
    <div ref={ref} className="flex w-full items-center gap-4" style={{ height }}>
      <svg role="img" aria-label={summary} width={size} height={height} className="shrink-0 overflow-hidden">
        {segments.map((segment) => (
          <path
            key={segment.label}
            d={arcPath(cx, cy, r0, r1, segment.startAngle, segment.endAngle)}
            fill={vizColor(segment.colorIndex)}
            stroke="var(--surface)"
            strokeWidth={1}
            data-label={segment.label}
            data-value={segment.value}
            {...handlers(
              <TooltipRows
                title={segment.label}
                rows={[
                  { label: "Value", value: format(segment.value), swatch: vizColor(segment.colorIndex) },
                  { label: "Share", value: `${(segment.share * 100).toFixed(1)}%` },
                ]}
              />,
            )}
          />
        ))}
        <text x={cx} y={cy - 4} textAnchor="middle" className="fill-ink font-mono tnum text-sm font-medium">
          {formatCompact(total)}
        </text>
        <text x={cx} y={cy + 12} textAnchor="middle" className="fill-faint text-[10px]">
          {centerLabel}
        </text>
      </svg>
      <ul className="min-w-0 flex-1 space-y-1 text-2xs">
        {segments.map((segment) => (
          <li key={segment.label} className="flex items-center gap-2" data-label={segment.label}>
            <span aria-hidden="true" className="inline-block size-2 shrink-0" style={{ background: vizColor(segment.colorIndex) }} />
            <span className="truncate text-muted">{segment.label}</span>
            <span className="ml-auto font-mono tnum text-ink">{format(segment.value)}</span>
            <span className="w-10 text-right font-mono tnum text-faint">{(segment.share * 100).toFixed(1)}%</span>
          </li>
        ))}
      </ul>
      <ChartTooltip tip={tip} />
    </div>
  );
}
