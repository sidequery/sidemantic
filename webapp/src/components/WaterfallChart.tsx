import { useEffect, useRef, useState } from "react";
import { ChartTooltip, TooltipRows, useChartTooltip } from "./ChartTooltip";
import { formatCompact, formatValue } from "../lib/format";
import { axisTicks, observeWidth } from "../lib/viz";

export type WaterfallDatum = {
  label: string;
  value: number;
  /** Total bars restate the running sum from zero (e.g. "Net revenue") instead of adding to it. */
  isTotal?: boolean;
};

export type WaterfallStep = WaterfallDatum & {
  /** Bar extent: where this step's bar starts and ends on the value axis. */
  start: number;
  end: number;
};

type WaterfallChartProps = {
  data: WaterfallDatum[];
  height?: number;
  format?: (value: number) => string;
  ariaLabel?: string;
};

/** Running-sum layout. Delta bars float from the previous cumulative value; total bars restate the
 *  cumulative value from zero. Exported for tests and adapters. */
export function waterfallSteps(data: WaterfallDatum[]): WaterfallStep[] {
  let running = 0;
  return data.map((item) => {
    const value = Number.isFinite(item.value) ? item.value : 0;
    if (item.isTotal) {
      running = value;
      return { ...item, value, start: 0, end: value };
    }
    const start = running;
    running += value;
    return { ...item, value, start, end: running };
  });
}

const MARGIN = { top: 12, right: 14, bottom: 26, left: 48 };

// Contribution bridge: floating delta bars (accent up, danger down), restated total bars, dashed
// connectors between consecutive steps.
export function WaterfallChart({ data, height = 220, format = formatValue, ariaLabel }: WaterfallChartProps) {
  const ref = useRef<SVGSVGElement>(null);
  const [width, setWidth] = useState(640);
  const { tip, handlers } = useChartTooltip();
  useEffect(() => observeWidth(ref.current, 200, setWidth), []);

  const steps = waterfallSteps(data);
  if (steps.length === 0) {
    return <div className="grid h-[220px] place-items-center text-xs text-faint">No steps to chart.</div>;
  }

  const bounds = steps.flatMap((step) => [step.start, step.end]);
  const min = Math.min(0, ...bounds);
  const max = Math.max(0, ...bounds);
  const span = max - min || 1;
  const plotW = width - MARGIN.left - MARGIN.right;
  const plotH = height - MARGIN.top - MARGIN.bottom;
  const yFor = (value: number) => MARGIN.top + (1 - (value - min) / span) * plotH;
  const slot = plotW / steps.length;
  const barWidth = Math.max(10, Math.min(56, slot * 0.62));
  const ticks = axisTicks(min, max, 4);
  const summary = ariaLabel || `Waterfall chart, ${steps.length} steps ending at ${format(steps[steps.length - 1].end)}`;

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
        <line x1={MARGIN.left} x2={width - MARGIN.right} y1={yFor(0)} y2={yFor(0)} className="stroke-faint" />
        {steps.map((step, index) => {
          const x = MARGIN.left + slot * index + (slot - barWidth) / 2;
          const y0 = yFor(step.start);
          const y1 = yFor(step.end);
          const tone = step.isTotal ? "total" : step.value < 0 ? "negative" : "positive";
          const fill = step.isTotal ? "fill-faint" : step.value < 0 ? "fill-danger" : "fill-chart-primary";
          const next = steps[index + 1];
          return (
            <g key={`${step.label}-${index}`}>
              <rect
                x={x}
                y={Math.min(y0, y1)}
                width={barWidth}
                height={Math.max(1, Math.abs(y1 - y0))}
                data-label={step.label}
                data-value={step.value}
                data-tone={tone}
                className={fill}
                {...handlers(
                  <TooltipRows
                    title={step.label}
                    rows={
                      step.isTotal
                        ? [{ label: "Total", value: format(step.end) }]
                        : [
                            { label: "Change", value: format(step.value) },
                            { label: "Running", value: format(step.end) },
                          ]
                    }
                  />,
                )}
              />
              {next ? (
                <line
                  x1={x + barWidth}
                  x2={MARGIN.left + slot * (index + 1) + (slot - barWidth) / 2}
                  y1={y1}
                  y2={y1}
                  className="stroke-faint"
                  strokeDasharray="3 3"
                />
              ) : null}
              <text x={x + barWidth / 2} y={height - 8} textAnchor="middle" className="fill-muted text-[10px]">
                {step.label.slice(0, 9)}
              </text>
            </g>
          );
        })}
      </svg>
      <ChartTooltip tip={tip} />
    </>
  );
}
