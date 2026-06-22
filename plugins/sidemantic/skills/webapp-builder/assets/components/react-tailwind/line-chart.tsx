import { useEffect, useRef, useState } from "react";
import { ChartTooltip, useChartTooltip } from "./chart-tooltip";
import { axisTicks, formatCompact, formatValue } from "./types";

type LineChartDatum = {
  label: string;
  value: number;
};

type LineChartProps = {
  data: LineChartDatum[];
  height?: number;
  ariaLabel?: string;
};

const MARGIN = { top: 12, right: 14, bottom: 26, left: 44 };

// Full-size time-series line for the `metricSeries` shape: responsive width (no aspect distortion),
// y-axis gridlines + compact labels, first/mid/last x labels, per-point hover tooltips, and an a11y
// summary. Sparkline is the compact, axis-less variant.
export function LineChart({ data, height = 200, ariaLabel }: LineChartProps) {
  const ref = useRef<SVGSVGElement>(null);
  const [width, setWidth] = useState(640);
  const { tip, handlers } = useChartTooltip();

  useEffect(() => {
    const node = ref.current;
    if (!node || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) setWidth(Math.max(160, entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const values = data.map((item) => (Number.isFinite(item.value) ? item.value : 0));
  if (values.length < 2) {
    return <svg role="img" aria-label={ariaLabel || "No series data"} className="h-[200px] w-full" viewBox={`0 0 ${width} ${height}`} />;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const xForIndex = (index: number) => MARGIN.left + (index / (values.length - 1)) * plotW;
  const yForValue = (value: number) => MARGIN.top + (1 - (value - min) / span) * plotH;
  const coordinates = values.map((value, index) => ({ x: xForIndex(index), y: yForValue(value) }));
  const points = coordinates.map(({ x, y }) => `${x.toFixed(1)},${y.toFixed(1)}`);
  const baselineY = MARGIN.top + plotH;
  const ticks = axisTicks(min, max, 4);
  const labelIndexes = [0, Math.floor((values.length - 1) / 2), values.length - 1].filter(
    (value, index, all) => all.indexOf(value) === index,
  );
  const summary = ariaLabel || `Line chart, ${values.length} points, ${formatCompact(min)} to ${formatCompact(max)}`;

  return (
    <>
      <svg ref={ref} role="img" aria-label={summary} className="h-[200px] w-full overflow-hidden" viewBox={`0 0 ${width} ${height}`}>
        {ticks.map((tick, index) => {
          const y = yForValue(tick);
          return (
            <g key={index}>
              <line x1={MARGIN.left} x2={width - MARGIN.right} y1={y} y2={y} className="stroke-slate-100" />
              <text x={MARGIN.left - 6} y={y + 3} textAnchor="end" className="fill-slate-400 text-[10px]">
                {formatCompact(tick)}
              </text>
            </g>
          );
        })}
        <path
          d={`M ${coordinates[0].x.toFixed(1)} ${baselineY.toFixed(1)} L ${points.join(" L ")} L ${coordinates.at(-1)!.x.toFixed(1)} ${baselineY.toFixed(1)} Z`}
          className="fill-[#6b7cff]/15"
        />
        <path d={`M ${points.join(" L ")}`} fill="none" strokeWidth="2" className="stroke-[#6b7cff]" />
        {coordinates.map((point, index) => (
          <circle
            key={data[index]?.label ?? index}
            cx={point.x}
            cy={point.y}
            r="3"
            className="fill-[#6b7cff]"
            {...handlers(`${data[index]?.label ?? ""}: ${formatValue(values[index])}`)}
          />
        ))}
        {labelIndexes.map((index) => (
          <text
            key={`label-${index}`}
            x={xForIndex(index)}
            y={height - 8}
            textAnchor={index === 0 ? "start" : index === values.length - 1 ? "end" : "middle"}
            className="fill-slate-500 text-[10px]"
          >
            {String(data[index]?.label ?? "").slice(0, 12)}
          </text>
        ))}
      </svg>
      <ChartTooltip tip={tip} />
    </>
  );
}
