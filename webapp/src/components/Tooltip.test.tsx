import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { TooltipRows } from "./ChartTooltip";
import { Tooltip } from "./Tooltip";

describe("TooltipRows", () => {
  test("renders a heading plus aligned label/value rows with optional swatches", () => {
    const html = renderToStaticMarkup(
      <TooltipRows
        title="2026-01"
        rows={[
          { label: "Revenue", value: "$1.2K", swatch: "var(--viz-1)" },
          { label: "Orders", value: "42" },
        ]}
      />,
    );
    expect(html).toContain("2026-01");
    expect(html).toContain("Revenue");
    expect(html).toContain("$1.2K");
    expect(html).toContain("var(--viz-1)");
  });
});

describe("Tooltip", () => {
  test("wraps the trigger and shows nothing until hover", () => {
    const html = renderToStaticMarkup(
      <Tooltip content="Revenue in USD">
        <span>$1.2M</span>
      </Tooltip>,
    );
    expect(html).toContain("$1.2M");
    expect(html).not.toContain('role="tooltip"');
  });
});
