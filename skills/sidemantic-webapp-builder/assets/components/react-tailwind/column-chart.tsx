type ColumnChartDatum = {
  label: string;
  value: number;
};

type ColumnChartProps = {
  data: ColumnChartDatum[];
  width?: number;
  height?: number;
};

export function ColumnChart({ data, width = 320, height = 160 }: ColumnChartProps) {
  const padX = 16;
  const padTop = 10;
  const padBottom = 28;
  const values = data.map((item) => (Number.isFinite(item.value) ? item.value : 0));
  const min = Math.min(0, ...values);
  const max = Math.max(0, ...values);
  const span = max - min || 1;
  const plotHeight = height - padTop - padBottom;
  const yForValue = (value: number) => padTop + (1 - (value - min) / span) * plotHeight;
  const baselineY = yForValue(0);
  const slot = (width - padX * 2) / Math.max(data.length, 1);
  const barWidth = Math.max(10, Math.min(42, slot * 0.56));

  return (
    <svg aria-hidden="true" className="h-40 w-full overflow-hidden" viewBox={`0 0 ${width} ${height}`}>
      {min < 0 ? (
        <line
          x1={padX}
          x2={width - padX}
          y1={baselineY}
          y2={baselineY}
          className="stroke-slate-200 [vector-effect:non-scaling-stroke]"
        />
      ) : null}
      {data.map((item, index) => {
        const value = values[index] ?? 0;
        const valueY = yForValue(value);
        const barHeight = Math.abs(valueY - baselineY);
        const x = padX + slot * index + (slot - barWidth) / 2;
        const y = Math.min(valueY, baselineY);

        return (
          <g key={item.label}>
            <rect
              x={x}
              y={y}
              width={barWidth}
              height={barHeight}
              rx="3"
              data-label={item.label}
              data-value={value}
              data-tone={value < 0 ? "negative" : "positive"}
              className={value < 0 ? "fill-red-700" : "fill-indigo-600"}
            />
            <text x={x + barWidth / 2} y={height - 8} textAnchor="middle" className="fill-slate-500 text-[10px]">
              {item.label.slice(0, 8)}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
