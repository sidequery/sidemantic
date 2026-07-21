import type { ReactNode } from "react";
import { ChartTooltip, useChartTooltip } from "./ChartTooltip";

type TooltipProps = {
  /** Text or node shown on hover. */
  content: ReactNode;
  /** A single trigger element; hover handlers wrap it in an inline-flex span. */
  children: ReactNode;
  className?: string;
};

// Hover tooltip for arbitrary triggers (column-header hints, truncated labels, info glyphs),
// reusing the chart tooltip surface so every tooltip in an app reads the same. Cursor-anchored,
// non-interactive content only.
export function Tooltip({ content, children, className }: TooltipProps) {
  const { tip, handlers } = useChartTooltip();
  return (
    <>
      <span className={className ?? "inline-flex"} {...handlers(content)}>
        {children}
      </span>
      <ChartTooltip tip={tip} />
    </>
  );
}
