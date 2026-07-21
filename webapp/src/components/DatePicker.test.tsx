import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { DatePicker, monthGrid } from "./DatePicker";

describe("monthGrid", () => {
  test("pads to complete Sunday-start weeks and covers the whole month", () => {
    const weeks = monthGrid(2024, 1); // February 2024, leap year
    expect(weeks.every((week) => week.length === 7)).toBe(true);
    const inMonth = weeks.flat().filter((cell) => cell.inMonth);
    expect(inMonth).toHaveLength(29);
    expect(inMonth[0].iso).toBe("2024-02-01");
    expect(inMonth[28].iso).toBe("2024-02-29");
    // Feb 1 2024 is a Thursday: the first week starts with January padding.
    expect(weeks[0][0].iso).toBe("2024-01-28");
  });
});

describe("DatePicker", () => {
  test("marks the selected single date in the inline calendar", () => {
    const html = renderToStaticMarkup(<DatePicker mode="single" inline value="2024-02-14" onChange={() => {}} />);
    expect(html).toContain("February 2024");
    expect(html).toContain('aria-selected="true" data-date="2024-02-14"');
  });

  test("marks range edges and interior days differently", () => {
    const html = renderToStaticMarkup(
      <DatePicker mode="range" inline value={{ from: "2024-02-05", to: "2024-02-07" }} onChange={() => {}} />,
    );
    expect(html).toContain('aria-selected="true" data-date="2024-02-05"');
    expect(html).toContain('aria-selected="true" data-date="2024-02-06"');
    expect(html).toContain('aria-selected="false" data-date="2024-02-08"');
    // Edges fill solid; interior days use the soft wash.
    expect(html).toContain('data-date="2024-02-05" class="py-1 text-center font-mono tnum bg-accent text-surface');
    expect(html).toContain('data-date="2024-02-06" class="py-1 text-center font-mono tnum bg-accent-soft text-accent');
  });
});
