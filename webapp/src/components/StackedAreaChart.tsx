import { useEffect, useRef, useState } from "react";
import { formatCompact, formatValue } from "../lib/format";
import { axisTicks, observeWidth, vizColor } from "../lib/viz";

export type StackedSeries = {
  name: string;
  /** Values aligned to the shared `labels` axis; missing/non-finite entries stack as zero. */
  values: number[];
};

type StackedAreaChartProps = {
  labels: string[];
  series: StackedSeries[];
  height?: number;
  format?: (value: number) => string;
  formatLabel?: (label: string) => string;
  ariaLabel?: string;
};

const MARGIN = { top: 12, right: 14, bottom: 26, left: 48 };

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

// Composition over time: bottom-up stacked bands in the palette order, legend, and a crosshair
// readout of every band at the hovered bucket. Negative values clamp to zero — stacking only
// reads for nonnegative parts.
export function StackedAreaChart({
  labels,
  series,
  height = 240,
  format = formatValue,
  formatLabel = (label) => label,
  ariaLabel,
}: StackedAreaChartProps) {
  const container = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(640);
  const [hover, setHover] = useState<number | null>(null);
  useEffect(() => observeWidth(container.current, 240, setWidth), []);

  const count = labels.length;
  if (count < 2 || series.length === 0) {
    return <div className="grid h-[240px] place-items-center text-xs text-faint">Not enough data to chart.</div>;
  }

  const clamped = series.map((entry) => ({
    name: entry.name,
    values: labels.map((_, index) => {
      const value = entry.values[index];
      return Number.isFinite(value) && value > 0 ? value : 0;
    }),
  }));
  // cumulative[s][i] = top of band s at bucket i.
  const cumulative: number[][] = [];
  let previous = labels.map(() => 0);
  for (const entry of clamped) {
    const top = entry.values.map((value, index) => previous[index] + value);
    cumulative.push(top);
    previous = top;
  }
  const maxTotal = Math.max(...previous, 1);
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const xFor = (index: number) => MARGIN.left + (index / (count - 1)) * plotW;
  const yFor = (value: number) => MARGIN.top + (1 - value / maxTotal) * plotH;
  const labelEvery = Math.max(1, Math.ceil(count / 8));
  const summary = ariaLabel || `Stacked area chart, ${series.length} series over ${count} points`;

  function bandPath(bandIndex: number): string {
    const top = cumulative[bandIndex];
    const bottom = bandIndex === 0 ? labels.map(() => 0) : cumulative[bandIndex - 1];
    const forward = top.map((value, index) => `${xFor(index).toFixed(1)},${yFor(value).toFixed(1)}`).join(" L ");
    const backward = [...bottom.keys()]
      .reverse()
      .map((index) => `${xFor(index).toFixed(1)},${yFor(bottom[index]).toFixed(1)}`)
      .join(" L ");
    return `M ${forward} L ${backward} Z`;
  }

  function onMove(event: React.MouseEvent<SVGSVGElement>) {
    const rect = event.currentTarget.getBoundingClientRect();
    const px = ((event.clientX - rect.left) / rect.width) * width;
    const index = Math.round(((px - MARGIN.left) / plotW) * (count - 1));
    setHover(index >= 0 && index < count ? index : null);
  }

  const tooltipLeft = hover != null ? clamp(xFor(hover), 90, width - 90) : 0;

  return (
    <div ref={container} className="relative w-full">
      <div className="mb-1 flex flex-wrap items-center gap-3 text-2xs text-faint">
        {clamped.map((entry, index) => (
          <span key={entry.name} className="flex items-center gap-1">
            <span aria-hidden="true" className="inline-block size-2" style={{ background: vizColor(index) }} />
            {entry.name}
          </span>
        ))}
      </div>
      <svg
        role="img"
        aria-label={summary}
        className="w-full overflow-hidden"
        style={{ height }}
        viewBox={`0 0 ${width} ${height}`}
        onMouseMove={onMove}
        onMouseLeave={() => setHover(null)}
      >
        {axisTicks(0, maxTotal, 4).map((tick, index) => {
          const y = yFor(tick);
          return (
            <g key={index}>
              <line x1={MARGIN.left} x2={width - MARGIN.right} y1={y} y2={y} className="stroke-line" />
              <text x={MARGIN.left - 6} y={y + 3} textAnchor="end" className="fill-faint text-[10px]">
                {formatCompact(tick)}
              </text>
            </g>
          );
        })}
        {clamped.map((entry, index) => (
          <path key={entry.name} d={bandPath(index)} fill={vizColor(index)} fillOpacity={0.8} data-series={entry.name} />
        ))}
        {hover != null ? (
          <line x1={xFor(hover)} x2={xFor(hover)} y1={MARGIN.top} y2={height - MARGIN.bottom} className="stroke-faint" strokeDasharray="3 3" />
        ) : null}
        {labels.map((label, index) =>
          index % labelEvery === 0 || index === count - 1 ? (
            <text key={label} x={xFor(index)} y={height - 8} textAnchor="middle" className="fill-faint font-mono text-[10px]">
              {formatLabel(label)}
            </text>
          ) : null,
        )}
      </svg>
      {hover != null ? (
        <div
          className="pointer-events-none absolute top-8 z-20 -translate-x-1/2 whitespace-nowrap border border-line bg-surface px-2 py-1.5 text-2xs shadow-[var(--shadow)]"
          style={{ left: tooltipLeft }}
        >
          <div className="mb-0.5 font-mono text-faint">{formatLabel(labels[hover])}</div>
          {clamped.map((entry, index) => (
            <div key={entry.name} className="flex items-center justify-between gap-3">
              <span className="flex items-center gap-1 text-muted">
                <span aria-hidden="true" className="inline-block size-2" style={{ background: vizColor(index) }} />
                {entry.name}
              </span>
              <span className="font-mono tnum text-ink">{format(entry.values[hover])}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}
