import { useEffect, useRef, useState } from "react";
import { ChartTooltip, TooltipRows, useChartTooltip } from "./ChartTooltip";
import { formatCompact } from "../lib/format";
import { axisTicks, observeWidth } from "../lib/viz";

export type HistogramBin = { x0: number; x1: number; count: number };

type HistogramChartProps = {
  values: number[];
  /** Bin count; defaults to Sturges' rule capped at 40. */
  bins?: number;
  height?: number;
  format?: (value: number) => string;
  ariaLabel?: string;
};

/** Equal-width binning over the finite values. The last bin is closed on both ends so the max
 *  value lands inside it. Exported for tests and adapters. */
export function binValues(values: number[], bins?: number): HistogramBin[] {
  const finite = values.filter(Number.isFinite);
  if (finite.length === 0) return [];
  const min = Math.min(...finite);
  const max = Math.max(...finite);
  const count = Math.max(1, Math.floor(bins ?? Math.min(40, Math.ceil(Math.log2(finite.length) + 1))));
  if (min === max) return [{ x0: min, x1: max, count: finite.length }];
  const width = (max - min) / count;
  const result: HistogramBin[] = Array.from({ length: count }, (_, index) => ({
    x0: min + index * width,
    x1: min + (index + 1) * width,
    count: 0,
  }));
  for (const value of finite) {
    const index = Math.min(count - 1, Math.floor((value - min) / width));
    result[index].count += 1;
  }
  return result;
}

const MARGIN = { top: 12, right: 14, bottom: 26, left: 44 };

// Distribution of one numeric field: contiguous bars, count y-axis, range-labeled tooltips.
export function HistogramChart({ values, bins, height = 200, format = formatCompact, ariaLabel }: HistogramChartProps) {
  const ref = useRef<SVGSVGElement>(null);
  const [width, setWidth] = useState(640);
  const { tip, handlers } = useChartTooltip();
  useEffect(() => observeWidth(ref.current, 160, setWidth), []);

  const data = binValues(values, bins);
  if (data.length === 0) {
    return <div className="grid h-[200px] place-items-center text-xs text-faint">No numeric values to chart.</div>;
  }

  const maxCount = Math.max(...data.map((bin) => bin.count), 1);
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const yFor = (count: number) => MARGIN.top + (1 - count / maxCount) * plotH;
  const slot = plotW / data.length;
  const ticks = axisTicks(0, maxCount, 4);
  const labelEvery = Math.max(1, Math.ceil(data.length / 6));
  const summary = ariaLabel || `Histogram, ${data.length} bins from ${format(data[0].x0)} to ${format(data[data.length - 1].x1)}`;

  return (
    <>
      <svg ref={ref} role="img" aria-label={summary} className="w-full overflow-hidden" style={{ height }} viewBox={`0 0 ${width} ${height}`}>
        {ticks.map((tick, index) => {
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
        {data.map((bin, index) => {
          const x = MARGIN.left + slot * index;
          const y = yFor(bin.count);
          return (
            <rect
              key={index}
              x={x + 0.5}
              y={y}
              width={Math.max(1, slot - 1)}
              height={MARGIN.top + plotH - y}
              data-x0={bin.x0}
              data-x1={bin.x1}
              data-count={bin.count}
              className="fill-chart-primary"
              {...handlers(
                <TooltipRows
                  title={`${format(bin.x0)} – ${format(bin.x1)}`}
                  rows={[{ label: "Count", value: bin.count.toLocaleString() }]}
                />,
              )}
            />
          );
        })}
        {data.map((bin, index) =>
          index % labelEvery === 0 ? (
            <text key={index} x={MARGIN.left + slot * index} y={height - 8} textAnchor="middle" className="fill-muted text-[10px]">
              {format(bin.x0)}
            </text>
          ) : null,
        )}
      </svg>
      <ChartTooltip tip={tip} />
    </>
  );
}
