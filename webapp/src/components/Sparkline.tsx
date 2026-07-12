import { useEffect, useRef, useState } from "react";
import { ChartTooltip, type ChartTooltipState } from "./ChartTooltip";

export type SparklineBrushRange = { from: string; to: string };

export type SparklineProps = {
  values: number[];
  labels?: string[];
  height?: number;
  ariaLabel?: string;
  formatValue?: (value: number) => string;
  onHover?: (point: { index: number; label?: string; value: number } | null) => void;
  onBrush?: (range: SparklineBrushRange | null) => void;
};

/** Responsive KPI sparkline: accessible summary, hover crosshair/value and optional date brushing. */
export function Sparkline({
  values,
  labels,
  height = 44,
  ariaLabel,
  formatValue = (value) => value.toLocaleString(undefined, { maximumFractionDigits: 2 }),
  onHover,
  onBrush,
}: SparklineProps) {
  const containerRef = useRef<HTMLSpanElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const dragStart = useRef<number | null>(null);
  const [width, setWidth] = useState(200);
  const [hover, setHover] = useState<number | null>(null);
  const [brush, setBrush] = useState<{ a: number; b: number } | null>(null);
  const [tip, setTip] = useState<ChartTooltipState>(null);

  useEffect(() => {
    const node = containerRef.current;
    if (!node || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) setWidth(Math.max(40, entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const points = values
    .map((value, index) => ({ index, value }))
    .filter((point) => Number.isFinite(point.value));
  if (points.length < 2) {
    return <svg ref={svgRef} role="img" aria-label={ariaLabel || "No trend data"} className="h-11 w-full" viewBox={`0 0 ${width} ${height}`} />;
  }

  const pad = 3;
  const min = Math.min(...points.map((point) => point.value));
  const max = Math.max(...points.map((point) => point.value));
  const span = max - min || 1;
  const coordinates = points.map((point, index) => ({
    ...point,
    x: pad + (index / (points.length - 1)) * (width - pad * 2),
    y: pad + (1 - (point.value - min) / span) * (height - pad * 2),
  }));
  const line = coordinates.map(({ x, y }) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" L ");
  const area = `M ${coordinates[0].x.toFixed(1)},${height - pad} L ${line} L ${coordinates.at(-1)!.x.toFixed(1)},${height - pad} Z`;
  const latest = coordinates.at(-1)!;
  const summary = ariaLabel || `Trend of ${points.length} points, latest ${formatValue(latest.value)}`;

  function localX(event: React.PointerEvent<SVGSVGElement>) {
    const rect = svgRef.current?.getBoundingClientRect();
    return rect ? Math.max(pad, Math.min(width - pad, ((event.clientX - rect.left) / rect.width) * width)) : pad;
  }
  function indexAt(x: number) {
    return Math.max(0, Math.min(coordinates.length - 1, Math.round(((x - pad) / (width - pad * 2)) * (coordinates.length - 1))));
  }
  function move(event: React.PointerEvent<SVGSVGElement>) {
    const x = localX(event);
    const index = indexAt(x);
    const point = coordinates[index];
    setHover(index);
    setTip({
      content: `${labels?.[point.index] ? `${labels[point.index]}: ` : ""}${formatValue(point.value)}`,
      x: event.clientX,
      y: event.clientY,
    });
    onHover?.({ index: point.index, label: labels?.[point.index], value: point.value });
    if (dragStart.current !== null) setBrush({ a: dragStart.current, b: x });
  }
  function down(event: React.PointerEvent<SVGSVGElement>) {
    if (!onBrush || !labels?.length) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    const x = localX(event);
    dragStart.current = x;
    setBrush({ a: x, b: x });
  }
  function up(event: React.PointerEvent<SVGSVGElement>) {
    if (dragStart.current === null || !onBrush || !labels?.length) return;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
    const end = localX(event);
    if (Math.abs(end - dragStart.current) > 6) {
      const startPoint = coordinates[indexAt(Math.min(dragStart.current, end))];
      const endPoint = coordinates[indexAt(Math.max(dragStart.current, end))];
      onBrush({ from: labels[startPoint.index], to: labels[endPoint.index] });
    }
    dragStart.current = null;
    setBrush(null);
  }
  function leave() {
    setHover(null);
    setTip(null);
    onHover?.(null);
    if (dragStart.current === null) setBrush(null);
  }

  const hovered = hover === null ? null : coordinates[hover];
  return (
    <span ref={containerRef} className="relative block w-full">
      <svg
        ref={svgRef}
        role="img"
        aria-label={summary}
        className={`h-11 w-full overflow-hidden text-chart-primary ${onBrush ? "touch-none select-none" : ""}`}
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        onPointerMove={move}
        onPointerDown={down}
        onPointerUp={up}
        onPointerCancel={leave}
        onPointerLeave={leave}
        onDoubleClick={() => onBrush?.(null)}
      >
        <path d={area} fill="currentColor" opacity={0.1} />
        <path d={`M ${line}`} fill="none" stroke="currentColor" strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
        {brush ? (
          <rect x={Math.min(brush.a, brush.b)} y={0} width={Math.abs(brush.b - brush.a)} height={height} fill="currentColor" opacity={0.12} />
        ) : null}
        {hovered ? (
          <>
            <line x1={hovered.x} x2={hovered.x} y1={0} y2={height} stroke="currentColor" strokeWidth={1} opacity={0.45} />
            <circle cx={hovered.x} cy={hovered.y} r={2.5} fill="currentColor" />
          </>
        ) : (
          <circle cx={latest.x} cy={latest.y} r={2.25} fill="currentColor" />
        )}
      </svg>
      <ChartTooltip tip={tip} />
    </span>
  );
}
