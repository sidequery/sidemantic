import { describe, expect, test } from "bun:test";
import type { CatalogDimension } from "../data/types";
import { resolvePreviewDimensions } from "./RowPreviewDrawer";

describe("resolvePreviewDimensions", () => {
  test("uses the configured cross-model dashboard dimensions when no pivot selection is active", () => {
    const dimensions: CatalogDimension[] = [
      { ref: "customers.country", name: "country", model: "customers", label: "Country", type: "categorical" },
    ];

    expect(resolvePreviewDimensions(dimensions, [])).toEqual(["customers.country"]);
  });
});
