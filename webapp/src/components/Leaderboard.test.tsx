import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { Leaderboard } from "./Leaderboard";

const rows = Array.from({ length: 7 }, (_, index) => ({ value: `Value ${index + 1}`, metric: 7 - index }));

function render(expanded: boolean) {
  return renderToStaticMarkup(
    <Leaderboard
      dimension="orders.region"
      title="Region"
      metricLabel="Revenue"
      rows={rows}
      formatMetric={String}
      expanded={expanded}
      onExpandedChange={() => {}}
    />,
  );
}

describe("Leaderboard", () => {
  test("collapses long results and exposes an accessible expand control", () => {
    const html = render(false);
    expect(html.match(/<button/g)?.length).toBe(7); // six values plus the expand control
    expect(html).toContain('data-action="leaderboard-expand"');
    expect(html).toContain('aria-expanded="false"');
    expect(html).toContain("Expand table (7)");
    expect(html).toContain("Ranked by Revenue");
    expect(html).toContain("flex flex-col border border-line bg-surface");
    expect(html).not.toContain("min-h-60");
  });

  test("expanded mode renders every row and a back control", () => {
    const html = render(true);
    expect(html.match(/<button/g)?.length).toBe(8); // seven values plus the back control
    expect(html).toContain('data-action="leaderboard-back"');
    expect(html).toContain('aria-expanded="true"');
  });
});
