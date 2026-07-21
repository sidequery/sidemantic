import { describe, expect, test } from "bun:test";
import { waterfallSteps } from "./WaterfallChart";

describe("waterfallSteps", () => {
  test("floats deltas from the running sum and restates totals from zero", () => {
    const steps = waterfallSteps([
      { label: "Gross", value: 100, isTotal: true },
      { label: "Refunds", value: -20 },
      { label: "Upsells", value: 5 },
      { label: "Net", value: 85, isTotal: true },
    ]);
    expect(steps[0]).toMatchObject({ start: 0, end: 100 });
    expect(steps[1]).toMatchObject({ start: 100, end: 80 });
    expect(steps[2]).toMatchObject({ start: 80, end: 85 });
    expect(steps[3]).toMatchObject({ start: 0, end: 85 });
  });

  test("treats non-finite values as zero-height steps", () => {
    const steps = waterfallSteps([
      { label: "a", value: 10 },
      { label: "bad", value: Number.NaN },
      { label: "b", value: 2 },
    ]);
    expect(steps[1]).toMatchObject({ start: 10, end: 10 });
    expect(steps[2]).toMatchObject({ start: 10, end: 12 });
  });
});
