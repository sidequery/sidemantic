import { tableFromIPC } from "apache-arrow";
import type { ResultRow } from "./types";

function normalize(value: unknown): string | number | boolean | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "bigint") return Number(value);
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
    for (const col of columns) {
      row[col] = normalize(proxy ? (proxy as Record<string, unknown>)[col] : null);
    }
    rows[i] = row;
  }
  return { columns, rows };
}
