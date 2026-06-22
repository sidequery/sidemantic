import { useEffect, useRef, useState } from "react";

export type SeriesPoint = { x: string; y: number };

export type BrushRange = { from: string; to: string };

type TimeSeriesChartProps = {
  points: SeriesPoint[];
  comparison?: SeriesPoint[];
  formatValue: (value: number) => string;
  formatAxis?: (value: number) => string;
  comparisonLabel?: string;
  /** Brush-to-zoom: bucket-start strings for the selected range, or null to clear. */
  onBrush?: (range: BrushRange | null) => void;
};

const HEIGHT = 280;
const PAD = { top: 14, right: 18, bottom: 26, left: 60 };

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

/** Full-size, interactive metric time series: hover crosshair + tooltip, an optional dashed
 *  previous-period overlay, and drag-to-zoom that sets the dashboard date range. */
export function TimeSeriesChart({
  points,
  comparison,
  formatValue,
  formatAxis = formatValue,
  comparisonLabel = "Previous",
  onBrush,
}: TimeSeriesChartProps) {
  const container = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const dragging = useRef(false);
  // Authoritative drag positions live in a ref so mouseup reads the latest end-x even when React
  // hasn't re-rendered between rapid move events; `brush` state only drives the visual rect.
  const brushRef = useRef<{ a: number; b: number } | null>(null);
  const [width, setWidth] = useState(820);
  const [hover, setHover] = useState<number | null>(null);
  const [brush, setBrush] = useState<{ a: number; b: number } | null>(null);

  useEffect(() => {
    const element = container.current;
    if (!element) return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) setWidth(Math.max(320, entry.contentRect.width));
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const count = points.length;
  const all = [...points, ...(comparison ?? [])].map((point) => point.y).filter(Number.isFinite);
  const empty = count < 2 || all.length === 0;

  const min = empty ? 0 : Math.min(0, ...all);
  const max = empty ? 1 : Math.max(...all);
  const span = max - min || 1;
  const plotW = width - PAD.left - PAD.right;
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const xAt = (index: number) => PAD.left + (count <= 1 ? 0 : (index / (count - 1)) * plotW);
  const yAt = (value: number) => PAD.top + (1 - (value - min) / span) * plotH;
  const indexAtX = (px: number) => clamp(Math.round(((px - PAD.left) / plotW) * (count - 1)), 0, count - 1);

  const pathFor = (series: SeriesPoint[]) =>
    series.map((point, index) => `${xAt(index).toFixed(1)},${yAt(point.y).toFixed(1)}`).join(" L ");
  // Gap-aware path: breaks the line at non-finite points (missing aligned buckets in the overlay).
  const gappedPath = (series: SeriesPoint[]) => {
    const segments: string[] = [];
    let run: string[] = [];
    series.forEach((point, index) => {
      if (Number.isFinite(point.y)) {
        run.push(`${xAt(index).toFixed(1)},${yAt(point.y).toFixed(1)}`);
      } else if (run.length) {
        segments.push(run.join(" L "));
        run = [];
      }
    });
    if (run.length) segments.push(run.join(" L "));
    return segments.map((segment) => `M ${segment}`).join(" ");
  };
  const line = pathFor(points);
  const area = empty ? "" : `M ${xAt(0).toFixed(1)},${yAt(min).toFixed(1)} L ${line} L ${xAt(count - 1).toFixed(1)},${yAt(min).toFixed(1)} Z`;

  function pxFromEvent(event: React.MouseEvent): number {
    const rect = svgRef.current?.getBoundingClientRect();
    return rect ? event.clientX - rect.left : 0;
  }
  function onMove(event: React.MouseEvent) {
    if (empty) return;
    const px = pxFromEvent(event);
    setHover(indexAtX(px));
    if (dragging.current && brushRef.current) {
      brushRef.current = { a: brushRef.current.a, b: px };
      setBrush({ ...brushRef.current });
    }
  }
  function onDown(event: React.MouseEvent) {
    if (empty || !onBrush) return;
    dragging.current = true;
    const px = pxFromEvent(event);
    brushRef.current = { a: px, b: px };
    setBrush({ ...brushRef.current });
  }
  function onUp() {
    const drag = brushRef.current;
    if (dragging.current && drag && onBrush) {
      const i0 = indexAtX(Math.min(drag.a, drag.b));
      const i1 = indexAtX(Math.max(drag.a, drag.b));
      if (i1 > i0) onBrush({ from: points[i0].x, to: points[i1].x });
    }
    dragging.current = false;
    brushRef.current = null;
    setBrush(null);
  }
  function onLeave() {
    setHover(null);
    dragging.current = false;
    brushRef.current = null;
    setBrush(null);
  }

  const labelEvery = Math.max(1, Math.ceil(count / 8));
  const ticks = [max, min + span * 0.66, min + span * 0.33, min];
  // Guard against a stale hover index left over from a larger/previous series (grain or filter change).
  const safeHover = hover != null && hover >= 0 && hover < count ? hover : null;
  const hoverCur = safeHover != null ? points[safeHover] : null;
  const hoverPrevRaw = safeHover != null ? (comparison?.[safeHover] ?? null) : null;
  const hoverPrev = hoverPrevRaw && Number.isFinite(hoverPrevRaw.y) ? hoverPrevRaw : null;
  const tooltipLeft = safeHover != null ? clamp(xAt(safeHover), 80, width - 80) : 0;
  const delta =
    hoverCur && hoverPrev && hoverPrev.y !== 0 ? ((hoverCur.y - hoverPrev.y) / Math.abs(hoverPrev.y)) * 100 : null;

  return (
    <div className="relative border border-line bg-surface text-accent">
      {/* legend */}
      <div className="absolute right-3 top-2 z-10 flex items-center gap-3 text-2xs text-faint">
        <span className="flex items-center gap-1">
          <span className="inline-block h-0.5 w-3 bg-accent" /> Current
        </span>
        {comparison?.length ? (
          <span className="flex items-center gap-1">
            <span className="inline-block h-0 w-3 border-t border-dashed border-faint" /> {comparisonLabel}
          </span>
        ) : null}
      </div>

      <div ref={container} className="w-full">
        {empty ? (
          <div className="grid h-[280px] place-items-center text-xs text-faint">Not enough data to chart.</div>
        ) : (
          <svg
            ref={svgRef}
            width={width}
            height={HEIGHT}
            className="block select-none"
            onMouseMove={onMove}
            onMouseDown={onDown}
            onMouseUp={onUp}
            onMouseLeave={onLeave}
            onDoubleClick={() => onBrush?.(null)}
          >
            <defs>
              <linearGradient id="ts-fill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="currentColor" stopOpacity={0.18} />
                <stop offset="100%" stopColor="currentColor" stopOpacity={0} />
              </linearGradient>
            </defs>

            {ticks.map((value, index) => (
              <g key={index}>
                <line x1={PAD.left} x2={width - PAD.right} y1={yAt(value)} y2={yAt(value)} className="stroke-line" />
                <text x={PAD.left - 8} y={yAt(value) + 3} textAnchor="end" className="fill-faint font-mono text-[10px]">
                  {formatAxis(value)}
                </text>
              </g>
            ))}

            {comparison && comparison.length >= 2 ? (
              <path d={gappedPath(comparison)} fill="none" className="stroke-faint" strokeWidth={1.25} strokeDasharray="4 3" />
            ) : null}

            <path d={area} fill="url(#ts-fill)" />
            <path d={`M ${line}`} fill="none" stroke="currentColor" strokeWidth={1.75} />

            {brush ? (
              <rect
                x={Math.min(brush.a, brush.b)}
                y={PAD.top}
                width={Math.abs(brush.b - brush.a)}
                height={plotH}
                className="fill-accent"
                opacity={0.12}
              />
            ) : null}

            {safeHover != null && hoverCur ? (
              <g>
                <line x1={xAt(safeHover)} x2={xAt(safeHover)} y1={PAD.top} y2={HEIGHT - PAD.bottom} className="stroke-faint" strokeDasharray="3 3" />
                {hoverPrev ? <circle cx={xAt(safeHover)} cy={yAt(hoverPrev.y)} r={3} className="fill-faint" /> : null}
                <circle cx={xAt(safeHover)} cy={yAt(hoverCur.y)} r={3.5} fill="currentColor" />
              </g>
            ) : null}

            {points.map((point, index) =>
              index % labelEvery === 0 || index === count - 1 ? (
                <text key={point.x} x={xAt(index)} y={HEIGHT - 8} textAnchor="middle" className="fill-faint font-mono text-[10px]">
                  {point.x}
                </text>
              ) : null,
            )}
          </svg>
        )}
      </div>

      {hoverCur ? (
        <div
          className="pointer-events-none absolute top-8 z-20 -translate-x-1/2 whitespace-nowrap border border-line bg-surface px-2 py-1.5 text-2xs shadow-[var(--shadow)]"
          style={{ left: tooltipLeft }}
        >
          <div className="mb-0.5 font-mono text-faint">{hoverCur.x}</div>
          <div className="flex items-center justify-between gap-3">
            <span className="text-muted">Current</span>
            <span className="font-mono tnum font-medium text-ink">{formatValue(hoverCur.y)}</span>
          </div>
          {hoverPrev ? (
            <div className="flex items-center justify-between gap-3">
              <span className="text-muted">{comparisonLabel}</span>
              <span className="font-mono tnum text-muted">{formatValue(hoverPrev.y)}</span>
            </div>
          ) : null}
          {delta != null ? (
            <div className={`mt-0.5 text-right font-mono ${delta > 0 ? "text-accent" : delta < 0 ? "text-danger" : "text-faint"}`}>
              {delta.toLocaleString(undefined, { maximumFractionDigits: 1, signDisplay: "exceptZero" })}%
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
