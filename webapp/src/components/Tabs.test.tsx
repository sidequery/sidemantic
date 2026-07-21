import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { Switch } from "./Switch";
import { Tabs } from "./Tabs";

describe("Tabs", () => {
  test("renders a tablist with the active tab selected", () => {
    const html = renderToStaticMarkup(
      <Tabs tabs={[{ key: "explore", label: "Explore" }, { key: "pivot", label: "Pivot" }]} active="pivot" onChange={() => {}} />,
    );
    expect(html).toContain('role="tablist"');
    expect(html).toContain('data-tab="pivot" data-selected="true"');
    expect(html).toContain('aria-selected="false"');
  });
});

describe("Switch", () => {
  test("renders an accessible switch reflecting its checked state", () => {
    const on = renderToStaticMarkup(<Switch checked label="Compact" onChange={() => {}} />);
    expect(on).toContain('role="switch"');
    expect(on).toContain('aria-checked="true"');
    const off = renderToStaticMarkup(<Switch checked={false} label="Compact" onChange={() => {}} />);
    expect(off).toContain('aria-checked="false"');
  });
});
