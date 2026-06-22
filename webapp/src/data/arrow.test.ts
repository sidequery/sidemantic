import { describe, expect, test } from "bun:test";
import { tableFromArrays, tableToIPC } from "apache-arrow";
import { decodeArrow } from "./arrow";

describe("decodeArrow", () => {
  test("decodes columns and rows for common column types", () => {
    const table = tableFromArrays({
      name: ["a", "b"],
      value: [1.5, 2.5],
      big: [1n, 2n],
    });
    const { columns, rows } = decodeArrow(new Uint8Array(tableToIPC(table, "stream")));
    expect(columns).toEqual(["name", "value", "big"]);
    expect(rows).toHaveLength(2);
    // int64 values inside the safe-integer range are narrowed to plain numbers.
    expect(rows[0]).toEqual({ name: "a", value: 1.5, big: 1 });
    expect(rows[1]).toEqual({ name: "b", value: 2.5, big: 2 });
  });

  test("keeps int64 values beyond the safe-integer range as strings (no precision loss)", () => {
    const table = tableFromArrays({ big: [9007199254740993n] }); // 2^53 + 1
    const { rows } = decodeArrow(new Uint8Array(tableToIPC(table, "stream")));
    expect(rows[0].big).toBe("9007199254740993");
  });
});
