import { describe, expect, test } from "bun:test";
import {
  DateDay,
  DateMillisecond,
  Decimal,
  TimestampSecond,
  TimestampMillisecond,
  TimestampMicrosecond,
  TimestampNanosecond,
  Table,
  makeData,
  makeVector,
  vectorFromArray,
  tableFromArrays,
  tableToIPC,
} from "apache-arrow";
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


test("decodes logical dates and timestamps without mistaking numeric metrics for dates", () => {
  const millis = Date.parse("2024-01-01T03:00:00Z");
  const table = new Table({
    day: vectorFromArray([new Date("2024-01-01"), null], new DateDay()),
    day64: vectorFromArray([new Date("2024-01-01"), null], new DateMillisecond()),
    seconds: vectorFromArray([millis, null], new TimestampSecond()),
    millis: vectorFromArray([millis, null], new TimestampMillisecond()),
    micros: vectorFromArray([millis, null], new TimestampMicrosecond()),
    nanos: vectorFromArray([millis, null], new TimestampNanosecond()),
    midnight: vectorFromArray([Date.parse("2024-01-01"), null], new TimestampMicrosecond()),
    zoned: vectorFromArray([millis, null], new TimestampMicrosecond("UTC")),
    metric: vectorFromArray([millis, null]),
  });
  const { rows } = decodeArrow(tableToIPC(table, "stream"));
  expect(rows[0]).toEqual({
    day: "2024-01-01", day64: "2024-01-01", seconds: "2024-01-01T03:00:00",
    millis: "2024-01-01T03:00:00", micros: "2024-01-01T03:00:00", nanos: "2024-01-01T03:00:00",
    midnight: "2024-01-01T00:00:00", zoned: "2024-01-01T03:00:00Z", metric: millis,
  });
  expect(Object.values(rows[1]).every((value) => value === null)).toBe(true);
});

test.each([0, 2, 4, -2])("applies decimal scale %s to positive, negative, zero and null values", (scale) => {
  const table = new Table({ amount: makeVector(makeData({
    type: new Decimal(scale, 18, 128), length: 4, nullCount: 1,
    nullBitmap: new Uint8Array([0b0111]),
    data: new Uint32Array([1234, 0, 0, 0, 0xfffffb2e, 0xffffffff, 0xffffffff, 0xffffffff, 0, 0, 0, 0, 0, 0, 0, 0]),
  })) });
  expect(decodeArrow(tableToIPC(table, "stream")).rows.map((row) => row.amount))
    .toEqual([Number(`1234e${-scale}`), Number(`-1234e${-scale}`), 0, null]);
});
