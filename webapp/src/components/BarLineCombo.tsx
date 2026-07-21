import { useEffect, useRef, useState } from "react";
import { ChartTooltip, TooltipRows, useChartTooltip } from "./ChartTooltip";
import { formatCompact, formatValue } from "../lib/format";
import { axisTicks, observeWidth, vizColor } from "../lib/viz";

export type BarLineDatum = {
  label: string;
  bar: number;
  line: number;
};

type BarLineComboProps = {
  data: BarLineDatum[];
  barLabel?: string;
  lineLabel?: string;
  height?: number;
  formatBar?: (value: number) => string;
  formatLine?: (value: number) => string;
  ariaLabel?: string;
};

const MARGIN = { top: 14, right: 48, bottom: 26, left: 48 };

// Dual-axis composition: categorical bars on the left axis plus an independently scaled line on
// the right axis (e.g. revenue bars with a conversion-rate line).
export function BarLineCombo({
  data,
  barLabel = "Bars",
  lineLabel = "Line",
  height = 220,
  formatBar = formatValue,
  formatLine = formatValue,
  ariaLabel,
}: BarLineComboProps) {
  const ref = useRef<SVGSVGElement>(null);
  const [width, setWidth] = useState(640);
  const { tip, handlers } = useChartTooltip();
  useEffect(() => observeWidth(ref.current, 240, setWidth), []);

  if (data.length === 0) {
    return <div className="grid h-[220px] place-items-center text-xs text-faint">No data to chart.</div>;
  }

  const bars = data.map((item) => (Number.isFinite(item.bar) ? item.bar : 0));
  const lines = data.map((item) => (Number.isFinite(item.line) ? item.line : 0));
  const barMin = Math.min(0, ...bars);
  const barMax = Math.max(0, ...bars);
  const barSpan = barMax - barMin || 1;
  const lineMin = Math.min(...lines);
  const lineMax = Math.max(...lines);
  const lineSpan = lineMax - lineMin || 1;
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const yBar = (value: number) => MARGIN.top + (1 - (value - barMin) / barSpan) * plotH;
  const yLine = (value: number) => MARGIN.top + (1 - (value - lineMin) / lineSpan) * plotH;
  const slot = plotW / data.length;
  const barWidth = Math.max(8, Math.min(48, slot * 0.55));
  const xCenter = (index: number) => MARGIN.left + slot * index + slot / 2;
  const baselineY = yBar(0);
  const linePath = data.map((_, index) => `${xCenter(index).toFixed(1)},${yLine(lines[index]).toFixed(1)}`).join(" L ");
  const lineColor = vizColor(1);
  const summary = ariaLabel || `Combo chart, ${data.length} categories: ${barLabel} bars with a ${lineLabel} line`;

  return (
    <>
      <div className="mb-1 flex items-center gap-3 text-2xs text-faint">
        <span className="flex items-center gap-1">
          <span aria-hidden="true" className="inline-block h-2 w-3 bg-chart-primary" /> {barLabel}
        </span>
        <span className="flex items-center gap-1">
          <span aria-hidden="true" className="inline-block h-0.5 w-3" style={{ background: lineColor }} /> {lineLabel}
        </span>
      </div>
      <svg ref={ref} role="img" aria-label={summary} className="w-full overflow-hidden" style={{ height }} viewBox={`0 0 ${width} ${height}`}>
        {axisTicks(barMin, barMax, 4).map((tick, index) => {
          const y = yBar(tick);
          return (
            <g key={index}>
              <line x1={MARGIN.left} x2={width - MARGIN.right} y1={y} y2={y} className="stroke-line" />
              <text x={MARGIN.left - 6} y={y + 3} textAnchor="end" className="fill-faint text-[10px]">
                {formatCompact(tick)}
              </text>
            </g>
          );
        })}
        {axisTicks(lineMin, lineMax, 4).map((tick, index) => (
          <text
            key={index}
            x={width - MARGIN.right + 6}
            y={yLine(tick) + 3}
            textAnchor="start"
            className="text-[10px]"
            fill={lineColor}
          >
            {formatLine(tick)}
          </text>
        ))}
        <line x1={MARGIN.left} x2={width - MARGIN.right} y1={baselineY} y2={baselineY} className="stroke-line" />
        {data.map((item, index) => {
          const value = bars[index];
          const valueY = yBar(value);
          return (
            <g key={item.label}>
              <rect
                x={xCenter(index) - barWidth / 2}
                y={Math.min(valueY, baselineY)}
                width={barWidth}
                height={Math.abs(valueY - baselineY)}
                data-label={item.label}
                data-bar={value}
                data-line={lines[index]}
                className={value < 0 ? "fill-danger" : "fill-chart-primary"}
                {...handlers(
                  <TooltipRows
                    title={item.label}
                    rows={[
                      { label: barLabel, value: formatBar(value), swatch: "var(--chart-primary)" },
                      { label: lineLabel, value: formatLine(lines[index]), swatch: lineColor },
                    ]}
                  />,
                )}
              />
              <text x={xCenter(index)} y={height - 8} textAnchor="middle" className="fill-muted text-[10px]">
                {item.label.slice(0, 8)}
              </text>
            </g>
          );
        })}
        <path d={`M ${linePath}`} fill="none" stroke={lineColor} strokeWidth={1.75} />
        {data.map((item, index) => (
          <circle
            key={item.label}
            cx={xCenter(index)}
            cy={yLine(lines[index])}
            r={3}
            fill={lineColor}
            {...handlers(
              <TooltipRows
                title={item.label}
                rows={[
                  { label: barLabel, value: formatBar(bars[index]), swatch: "var(--chart-primary)" },
                  { label: lineLabel, value: formatLine(lines[index]), swatch: lineColor },
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
