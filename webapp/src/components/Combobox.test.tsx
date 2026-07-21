import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { Combobox, filterOptions } from "./Combobox";

const OPTIONS = [
  { value: "orders.revenue", label: "Revenue" },
  { value: "orders.count", label: "Order count" },
  { value: "customers.count", label: "Customer count" },
];

describe("filterOptions", () => {
  test("matches case-insensitively on value or label", () => {
    expect(filterOptions(OPTIONS, "REV").map((option) => option.value)).toEqual(["orders.revenue"]);
    expect(filterOptions(OPTIONS, "count")).toHaveLength(2);
    expect(filterOptions(OPTIONS, "  ")).toHaveLength(3);
  });
});

describe("Combobox", () => {
  test("renders a closed combobox input with the selection as placeholder", () => {
    const html = renderToStaticMarkup(<Combobox value="orders.revenue" options={OPTIONS} onChange={() => {}} />);
    expect(html).toContain('role="combobox"');
    expect(html).toContain('aria-expanded="false"');
    expect(html).toContain('placeholder="Revenue"');
    expect(html).toContain('aria-label="Clear selection"');
  });

  test("multiple mode renders removable chips for each selected value", () => {
    const html = renderToStaticMarkup(
      <Combobox multiple values={["orders.revenue", "orders.count"]} options={OPTIONS} onChange={() => {}} />,
    );
    expect(html).toContain('data-chip="orders.revenue"');
    expect(html).toContain('data-chip="orders.count"');
    expect(html).toContain('aria-label="Remove Revenue"');
    expect(html).toContain('aria-label="Clear selection"');
  });
});
