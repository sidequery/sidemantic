import { DataType, tableFromIPC } from "apache-arrow";
import type { ResultRow } from "./types";

function normalize(value: unknown, type: DataType): string | number | boolean | null {
  if (value === null || value === undefined) return null;
  // Arrow JS exposes dates/timestamps as epoch milliseconds and decimals as unscaled
  // integers. Recover their logical values from the schema, matching the JSON transport.
  if (DataType.isDate(type)) return new Date(Number(value)).toISOString().slice(0, 10);
  if (DataType.isTimestamp(type)) {
    const iso = new Date(Number(value)).toISOString().replace(".000Z", "Z");
    return type.timezone ? iso : iso.slice(0, -1);
  }
  if (DataType.isDecimal(type)) return Number(`${String(value)}e${-type.scale}`);
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    return Number.isSafeInteger(asNumber) ? asNumber : value.toString();
  }
  if (value instanceof Date) {
    // Date-only columns (grain buckets) read better as ISO dates.
    const iso = value.toISOString();
    return iso.endsWith("T00:00:00.000Z") ? iso.slice(0, 10) : iso;
  }
  if (typeof value === "object") return String(value);
  return value as string | number | boolean;
}

/** Decode an Arrow IPC stream (the body of POST /query?format=arrow) into plain rows. */
export function decodeArrow(bytes: Uint8Array): { columns: string[]; rows: ResultRow[] } {
  const table = tableFromIPC(bytes);
  const columns = table.schema.fields.map((field) => field.name);
  const rows: ResultRow[] = new Array(table.numRows);
  for (let i = 0; i < table.numRows; i++) {
    const proxy = table.get(i);
    const row: ResultRow = {};
    for (const field of table.schema.fields) {
      row[field.name] = normalize(proxy ? (proxy as Record<string, unknown>)[field.name] : null, field.type);
    }
    rows[i] = row;
  }
  return { columns, rows };
}
