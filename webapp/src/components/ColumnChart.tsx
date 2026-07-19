import { useEffect, useRef, useState } from "react";
import { ChartTooltip, useChartTooltip } from "./ChartTooltip";
import { formatCompact, formatValue } from "../lib/format";

type ColumnChartDatum = {
  label: string;
  value: number;
};

type ColumnChartProps = {
  data: ColumnChartDatum[];
  height?: number;
  ariaLabel?: string;
  selectedLabel?: string;
  onSelect?: (label: string) => void;
};

const MARGIN = { top: 12, right: 14, bottom: 26, left: 44 };

function axisTicks(min: number, max: number, count = 4) {
  if (!(max > min)) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, index) => min + step * index);
}

// Categorical bars with a zero baseline, responsive width (no aspect distortion), y-axis gridlines +
// compact labels, per-bar x labels and hover tooltips, and an a11y summary. Negatives draw below the
// baseline in red.
export function ColumnChart({ data, height = 200, ariaLabel, selectedLabel, onSelect }: ColumnChartProps) {
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
  const min = Math.min(0, ...values);
  const max = Math.max(0, ...values);
  const span = max - min || 1;
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const yForValue = (value: number) => MARGIN.top + (1 - (value - min) / span) * plotH;
  const baselineY = yForValue(0);
  const slot = plotW / Math.max(data.length, 1);
  const barWidth = Math.max(8, Math.min(48, slot * 0.62));
  const ticks = axisTicks(min, max, 4);
  const summary = ariaLabel || `Bar chart, ${data.length} categories, up to ${formatCompact(max)}`;

  return (
    <>
      <svg ref={ref} role="img" aria-label={summary} className="h-[200px] w-full overflow-hidden" viewBox={`0 0 ${width} ${height}`}>
        {ticks.map((tick, index) => {
          const y = yForValue(tick);
          return (
            <g key={index}>
              <line x1={MARGIN.left} x2={width - MARGIN.right} y1={y} y2={y} className="stroke-line" />
              <text x={MARGIN.left - 6} y={y + 3} textAnchor="end" className="fill-faint text-[10px]">
                {formatCompact(tick)}
              </text>
            </g>
          );
        })}
        <line x1={MARGIN.left} x2={width - MARGIN.right} y1={baselineY} y2={baselineY} className="stroke-line" />
        {data.map((item, index) => {
          const value = values[index] ?? 0;
          const valueY = yForValue(value);
          const barHeight = Math.abs(valueY - baselineY);
          const x = MARGIN.left + slot * index + (slot - barWidth) / 2;
          const y = Math.min(valueY, baselineY);
          return (
            <g
              key={item.label}
              role={onSelect ? "button" : undefined}
              tabIndex={onSelect ? 0 : undefined}
              aria-label={onSelect ? `Filter to ${item.label}` : undefined}
              aria-pressed={onSelect ? selectedLabel === item.label : undefined}
              onClick={onSelect ? () => onSelect(item.label) : undefined}
              onKeyDown={onSelect ? (event) => {
                if (event.key === "Enter" || event.key === " ") onSelect(item.label);
              } : undefined}
              className={onSelect ? "cursor-pointer" : undefined}
            >
              <rect
                x={x}
                y={y}
                width={barWidth}
                height={barHeight}
                rx="3"
                data-label={item.label}
                data-value={value}
                data-tone={value < 0 ? "negative" : "positive"}
                className={selectedLabel === item.label ? "fill-accent" : value < 0 ? "fill-danger" : "fill-chart-primary"}
                {...handlers(`${item.label}: ${formatValue(value)}`)}
              />
              <text x={x + barWidth / 2} y={height - 8} textAnchor="middle" className="fill-muted text-[10px]">
                {item.label.slice(0, 8)}
              </text>
            </g>
          );
        })}
      </svg>
      <ChartTooltip tip={tip} />
    </>
  );
}
