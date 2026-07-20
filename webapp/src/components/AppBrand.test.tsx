import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { AppBrand } from "./AppBrand";

describe("AppBrand", () => {
  test("renders a dashboard title without home navigation", () => {
    const html = renderToStaticMarkup(
      <AppBrand dashboardTitle="Revenue dashboard" modelLabel="Orders" onHome={() => {}} />,
    );

    expect(html).toStartWith("<span");
    expect(html).toContain("Revenue dashboard");
    expect(html).not.toContain("<button");
    expect(html).not.toContain('aria-label="Home"');
    expect(html).not.toContain("Orders");
  });

  test("keeps ordinary explorer branding as a home button", () => {
    const html = renderToStaticMarkup(<AppBrand modelLabel="Orders" onHome={() => {}} />);

    expect(html).toStartWith("<button");
    expect(html).toContain('aria-label="Home"');
    expect(html).toContain("Sidemantic");
    expect(html).toContain("Orders");
  });
});
