import { useEffect, useRef, useState } from "react";
import { ChartTooltip, TooltipRows, useChartTooltip } from "./ChartTooltip";
import { formatCompact } from "../lib/format";
import { axisTicks, observeWidth, vizColor } from "../lib/viz";

export type ScatterPoint = {
  x: number;
  y: number;
  label?: string;
  /** Optional series key; each distinct series gets its own palette color and legend entry. */
  series?: string;
};

type ScatterChartProps = {
  points: ScatterPoint[];
  height?: number;
  xLabel?: string;
  yLabel?: string;
  formatX?: (value: number) => string;
  formatY?: (value: number) => string;
  ariaLabel?: string;
};

const MARGIN = { top: 12, right: 14, bottom: 30, left: 48 };

// Numeric x/y correlation view: gridlines on both axes, palette-colored series, hover tooltips.
export function ScatterChart({
  points,
  height = 240,
  xLabel,
  yLabel,
  formatX = formatCompact,
  formatY = formatCompact,
  ariaLabel,
}: ScatterChartProps) {
  const ref = useRef<SVGSVGElement>(null);
  const [width, setWidth] = useState(640);
  const { tip, handlers } = useChartTooltip();
  useEffect(() => observeWidth(ref.current, 200, setWidth), []);

  const finite = points.filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
  if (finite.length === 0) {
    return <div className="grid h-[240px] place-items-center text-xs text-faint">No points to chart.</div>;
  }

  const series = [...new Set(finite.map((point) => point.series ?? ""))];
  const xs = finite.map((point) => point.x);
  const ys = finite.map((point) => point.y);
  const xMin = Math.min(...xs);
  const xMax = Math.max(...xs);
  const yMin = Math.min(...ys);
  const yMax = Math.max(...ys);
  const xSpan = xMax - xMin || 1;
  const ySpan = yMax - yMin || 1;
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const xFor = (value: number) => MARGIN.left + ((value - xMin) / xSpan) * plotW;
  const yFor = (value: number) => MARGIN.top + (1 - (value - yMin) / ySpan) * plotH;
  const summary = ariaLabel || `Scatter plot, ${finite.length} points${xLabel && yLabel ? ` of ${yLabel} by ${xLabel}` : ""}`;

  return (
    <>
      {series.length > 1 ? (
        <div className="mb-1 flex flex-wrap items-center gap-3 text-2xs text-faint">
          {series.map((name, index) => (
            <span key={name || "(default)"} className="flex items-center gap-1">
              <span aria-hidden="true" className="inline-block size-2 rounded-full" style={{ background: vizColor(index) }} />
              {name || "(default)"}
            </span>
          ))}
        </div>
      ) : null}
      <svg ref={ref} role="img" aria-label={summary} className="w-full overflow-hidden" style={{ height }} viewBox={`0 0 ${width} ${height}`}>
        {axisTicks(yMin, yMax, 4).map((tick, index) => {
          const y = yFor(tick);
          return (
            <g key={`y${index}`}>
              <line x1={MARGIN.left} x2={width - MARGIN.right} y1={y} y2={y} className="stroke-line" />
              <text x={MARGIN.left - 6} y={y + 3} textAnchor="end" className="fill-faint text-[10px]">
                {formatY(tick)}
              </text>
            </g>
          );
        })}
        {axisTicks(xMin, xMax, 5).map((tick, index) => {
          const x = xFor(tick);
          return (
            <g key={`x${index}`}>
              <line x1={x} x2={x} y1={MARGIN.top} y2={height - MARGIN.bottom} className="stroke-line" />
              <text x={x} y={height - 14} textAnchor="middle" className="fill-faint text-[10px]">
                {formatX(tick)}
              </text>
            </g>
          );
        })}
        {xLabel ? (
          <text x={MARGIN.left + plotW / 2} y={height - 2} textAnchor="middle" className="fill-muted text-[10px]">
            {xLabel}
          </text>
        ) : null}
        {yLabel ? (
          <text
            x={10}
            y={MARGIN.top + plotH / 2}
            textAnchor="middle"
            transform={`rotate(-90 10 ${MARGIN.top + plotH / 2})`}
            className="fill-muted text-[10px]"
          >
            {yLabel}
          </text>
        ) : null}
        {finite.map((point, index) => (
          <circle
            key={index}
            cx={xFor(point.x)}
            cy={yFor(point.y)}
            r={3.5}
            fill={vizColor(series.indexOf(point.series ?? ""))}
            fillOpacity={0.75}
            data-x={point.x}
            data-y={point.y}
            data-label={point.label}
            {...handlers(
              <TooltipRows
                title={point.label ?? point.series}
                rows={[
                  { label: xLabel ?? "x", value: formatX(point.x) },
                  { label: yLabel ?? "y", value: formatY(point.y) },
                ]}
              />,
            )}
          />
        ))}
      </svg>
      <ChartTooltip tip={tip} />
    </>
  );
}
