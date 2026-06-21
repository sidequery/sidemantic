type SparklineProps = {
  values: number[];
  width?: number;
  height?: number;
};

/** Compact area sparkline used on KPI cards. Accent stroke over a faint fill. */
export function Sparkline({ values, width = 200, height = 44 }: SparklineProps) {
  const numbers = values.filter((value) => Number.isFinite(value));
  if (numbers.length < 2) {
    return <svg aria-hidden="true" className="h-11 w-full" viewBox={`0 0 ${width} ${height}`} />;
  }

  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const span = max - min || 1;
  const pad = 2;
  const points = numbers.map((value, index) => {
    const x = (index / (numbers.length - 1)) * width;
    const y = pad + (1 - (value - min) / span) * (height - pad * 2);
    return [x, y] as const;
  });
  const line = points.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" L ");
  const area = `M ${points[0][0].toFixed(1)},${height} L ${line} L ${points.at(-1)![0].toFixed(1)},${height} Z`;

  return (
    <svg aria-hidden="true" className="h-11 w-full overflow-hidden text-accent" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
      <path d={area} fill="currentColor" opacity={0.1} />
      <path d={`M ${line}`} fill="none" stroke="currentColor" strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
    </svg>
  );
}
