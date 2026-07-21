import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { columnTotal, DataTable, type Column } from "./DataTable";

const COLUMNS: Column[] = [
  { key: "name", label: "Name" },
  { key: "revenue", label: "Revenue", numeric: true },
];
const ROWS = [
  { name: "Acme", revenue: 100 },
  { name: "Globex", revenue: 50 },
  { name: "Initech", revenue: "not-a-number" },
];
const renderCell = (column: Column, value: unknown) => String(value);

describe("columnTotal", () => {
  test("aggregates finite numerics and counts all rows", () => {
    expect(columnTotal(ROWS, "revenue", "sum")).toBe(150);
    expect(columnTotal(ROWS, "revenue", "avg")).toBe(75);
    expect(columnTotal(ROWS, "revenue", "min")).toBe(50);
    expect(columnTotal(ROWS, "revenue", "max")).toBe(100);
    expect(columnTotal(ROWS, "revenue", "count")).toBe(3);
  });

  test("returns NaN when no value is numeric", () => {
    expect(Number.isNaN(columnTotal(ROWS, "name", "sum"))).toBe(true);
  });
});

describe("DataTable", () => {
  test("renders a totals footer and search input when configured", () => {
    const html = renderToStaticMarkup(
      <DataTable columns={COLUMNS} rows={ROWS} renderCell={renderCell} searchable totals={{ revenue: "sum" }} />,
    );
    expect(html).toContain('aria-label="Search rows"');
    expect(html).toContain('data-testid="table-totals"');
    expect(html).toContain('data-total="sum"');
    expect(html).toContain(">150<");
  });

  test("omits search and totals chrome by default", () => {
    const html = renderToStaticMarkup(<DataTable columns={COLUMNS} rows={ROWS} renderCell={renderCell} />);
    expect(html).not.toContain('aria-label="Search rows"');
    expect(html).not.toContain('data-testid="table-totals"');
  });
});
