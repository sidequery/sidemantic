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
  const max = Math.max(...data.map((item) => item.value), 1);
  const padX = 16;
  const padTop = 10;
  const padBottom = 28;
  const slot = (width - padX * 2) / Math.max(data.length, 1);
  const barWidth = Math.max(10, Math.min(42, slot * 0.56));

  return (
    <svg aria-hidden="true" className="h-40 w-full overflow-hidden" viewBox={`0 0 ${width} ${height}`}>
      {data.map((item, index) => {
        const barHeight = ((height - padTop - padBottom) * item.value) / max;
        const x = padX + slot * index + (slot - barWidth) / 2;
        const y = height - padBottom - barHeight;

        return (
          <g key={item.label}>
            <rect
              x={x}
              y={y}
              width={barWidth}
              height={barHeight}
              rx="3"
              data-label={item.label}
              data-value={item.value}
              className="fill-indigo-600"
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
