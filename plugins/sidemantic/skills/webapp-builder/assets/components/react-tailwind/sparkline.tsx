type SparklineProps = {
  values: number[];
  width?: number;
  height?: number;
};

export function Sparkline({ values, width = 160, height = 56 }: SparklineProps) {
  const numbers = values.filter(Number.isFinite);
  if (numbers.length < 2) {
    return <svg aria-hidden="true" className="h-14 w-full" viewBox={`0 0 ${width} ${height}`} />;
  }

  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const span = max - min || 1;
  const points = numbers.map((value, index) => {
    const x = (index / (numbers.length - 1)) * width;
    const y = height - ((value - min) / span) * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  return (
    <svg aria-hidden="true" className="h-14 w-full overflow-hidden" viewBox={`0 0 ${width} ${height}`}>
      <path d={`M ${points.join(" L ")}`} fill="none" stroke="currentColor" strokeWidth="2" className="text-indigo-600" />
    </svg>
  );
}
