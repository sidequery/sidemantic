import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { MetricCard } from "./MetricCard";

describe("MetricCard", () => {
  test("formats raw values and renders a noninteractive article without a selection handler", () => {
    const html = renderToStaticMarkup(
      <MetricCard metric="orders.revenue" label="Revenue" value={1234.5} format={{ format: "usd" }} />,
    );
    expect(html).toStartWith("<article");
    expect(html).toContain("$1,234.50");
  });

  test("renders an accessible selectable button when a selection handler is supplied", () => {
    const html = renderToStaticMarkup(
      <MetricCard metric="orders.revenue" label="Revenue" value={12} selected onSelect={() => {}} />,
    );
    expect(html).toStartWith("<article");
    expect(html).toContain("<button");
    expect(html).toContain('aria-pressed="true"');
  });
});
