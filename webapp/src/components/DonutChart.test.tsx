import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { DonutChart, donutSegments } from "./DonutChart";

describe("donutSegments", () => {
  test("shares sum to 1 and angles tile the circle in order", () => {
    const segments = donutSegments([
      { label: "a", value: 30 },
      { label: "b", value: 60 },
      { label: "c", value: 10 },
    ]);
    expect(segments.map((segment) => segment.share).reduce((sum, share) => sum + share, 0)).toBeCloseTo(1);
    expect(segments[0].endAngle).toBeCloseTo(segments[1].startAngle);
    expect(segments[1].endAngle).toBeCloseTo(segments[2].startAngle);
    expect(segments[2].endAngle - segments[0].startAngle).toBeCloseTo(Math.PI * 2);
  });

  test("drops non-positive and non-finite values but keeps stable palette indices", () => {
    const segments = donutSegments([
      { label: "a", value: 5 },
      { label: "junk", value: -3 },
      { label: "nan", value: Number.NaN },
      { label: "b", value: 5 },
    ]);
    expect(segments.map((segment) => segment.label)).toEqual(["a", "b"]);
    expect(segments.map((segment) => segment.colorIndex)).toEqual([0, 3]);
  });
});

describe("DonutChart", () => {
  test("renders arcs with data attributes and a legend with shares", () => {
    const html = renderToStaticMarkup(
      <DonutChart data={[{ label: "iOS", value: 75 }, { label: "Android", value: 25 }]} />,
    );
    expect(html).toContain('data-label="iOS"');
    expect(html).toContain("75.0%");
    expect(html).toContain("Android");
  });

  test("falls back to an empty state without positive values", () => {
    const html = renderToStaticMarkup(<DonutChart data={[{ label: "a", value: 0 }]} />);
    expect(html).toContain("No positive values");
  });
});
