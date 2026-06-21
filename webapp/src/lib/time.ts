import type { Grain } from "../data/types";

export const ALL_GRAINS: Grain[] = ["hour", "day", "week", "month", "quarter", "year"];

/** Grains offered in the UI, restricted to a dimension's supported set when known. */
export function grainOptions(supported?: string[]): Grain[] {
  if (supported?.length) {
    const set = new Set(supported.map((g) => g.toLowerCase()));
    const filtered = ALL_GRAINS.filter((g) => set.has(g));
    if (filtered.length) return filtered;
  }
  return ["day", "week", "month", "quarter", "year"];
}

/** Inclusive day range, ISO dates (YYYY-MM-DD). */
export type DateRange = { from: string; to: string };

function isoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function parseISO(value: string): Date {
  return new Date(`${value}T00:00:00Z`);
}

export function addDays(value: string, days: number): string {
  const date = parseISO(value);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}

function daysInRange(range: DateRange): number {
  return Math.round((parseISO(range.to).getTime() - parseISO(range.from).getTime()) / 86_400_000) + 1;
}

/** The equal-length window immediately preceding `range`, for period-over-period comparison. */
export function previousRange(range: DateRange): DateRange {
  const length = daysInRange(range);
  const to = addDays(range.from, -1);
  const from = addDays(to, -(length - 1));
  return { from, to };
}

export type DatePreset = { key: string; label: string; days: number };

export const DATE_PRESETS: DatePreset[] = [
  { key: "7d", label: "Last 7 days", days: 7 },
  { key: "28d", label: "Last 28 days", days: 28 },
  { key: "90d", label: "Last 90 days", days: 90 },
  { key: "180d", label: "Last 180 days", days: 180 },
  { key: "365d", label: "Last 12 months", days: 365 },
];

export function presetRange(days: number, today: Date = new Date()): DateRange {
  const to = isoDate(today);
  return { from: addDays(to, -(days - 1)), to };
}

/** SQL filter expressions bounding a time dimension ref to a date range. The upper bound is
 *  exclusive (`< day after to`) so a timestamp column still includes the whole final day rather
 *  than truncating to its midnight. */
export function timeFilters(ref: string, range: DateRange): string[] {
  return [`${ref} >= cast('${range.from}' as date)`, `${ref} < cast('${addDays(range.to, 1)}' as date)`];
}

/** Inclusive last calendar day of the bucket that starts at `start` for a given grain.
 *  Used to turn a brushed bucket range into a precise date filter. */
export function endOfBucket(start: string, grain: Grain): string {
  const date = parseISO(start);
  switch (grain) {
    case "hour":
    case "day":
      return start;
    case "week":
      return addDays(start, 6);
    case "month":
      return isoDate(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0)));
    case "quarter": {
      const quarterEndMonth = Math.floor(date.getUTCMonth() / 3) * 3 + 3;
      return isoDate(new Date(Date.UTC(date.getUTCFullYear(), quarterEndMonth, 0)));
    }
    case "year":
      return isoDate(new Date(Date.UTC(date.getUTCFullYear(), 12, 0)));
    default:
      return start;
  }
}
