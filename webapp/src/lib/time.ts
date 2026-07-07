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

/** Date-only portion of a date or timestamp bucket label (hour grain yields "2024-01-02T03:00:00"). */
export function dateOnly(value: string): string {
  return value.slice(0, 10);
}

function normalizeBucketLabel(value: string): string {
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return `${trimmed}T00:00:00Z`;
  if (/[zZ]|[+-]\d{2}:?\d{2}$/.test(trimmed)) return trimmed;
  return `${trimmed.replace(" ", "T")}Z`;
}

function parseISO(value: string): Date {
  // Tolerate timestamp bucket labels by keeping only the date part.
  return new Date(`${dateOnly(value)}T00:00:00Z`);
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

/** Shift an ISO date back one calendar year, keeping the same month/day. Feb 29 has no counterpart
 *  in a non-leap year, so it clamps to Feb 28 (matching how spreadsheets/BI tools handle the leap
 *  day in year-over-year comparisons). */
function isoYearAgo(value: string): string {
  const date = parseISO(value);
  const year = date.getUTCFullYear() - 1;
  const month = date.getUTCMonth();
  const day = date.getUTCDate();
  // Feb 29 -> Feb 28 when the prior year isn't a leap year.
  const lastOfMonth = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
  return isoDate(new Date(Date.UTC(year, month, Math.min(day, lastOfMonth))));
}

/** The same month/day span one year earlier, for year-over-year comparison. Both endpoints shift
 *  back a year independently (clamping Feb 29), so the window keeps its calendar alignment rather
 *  than its exact day count. */
export function previousYearRange(range: DateRange): DateRange {
  return { from: isoYearAgo(range.from), to: isoYearAgo(range.to) };
}

// --- Timezone-aware bucket-label formatting (E4) ---------------------------------------------
// Bucket labels come off the wire as UTC instants ("2024-01-02" or "2024-01-02T03:00:00"). With a
// non-UTC zone selected, the backend already truncates in that zone, but the labels it returns are
// still wall-clock strings without an offset. We render them via Intl in the selected zone so an
// hour bucket shows the local hour and a day/week/month label shows the local calendar boundary.

// Reuse formatters (constructing Intl.DateTimeFormat is comparatively expensive) keyed by zone+grain.
const bucketFormatters = new Map<string, Intl.DateTimeFormat>();

function bucketFormatter(timeZone: string, grain: Grain): Intl.DateTimeFormat {
  const cacheKey = `${timeZone}|${grain}`;
  const cached = bucketFormatters.get(cacheKey);
  if (cached) return cached;
  // Hour grain needs the clock time; coarser grains only need the date part (in-zone).
  const options: Intl.DateTimeFormatOptions =
    grain === "hour"
      ? { timeZone, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }
      : { timeZone, year: "numeric", month: "2-digit", day: "2-digit" };
  const formatter = new Intl.DateTimeFormat("en-CA", options); // en-CA => ISO-ish YYYY-MM-DD ordering
  bucketFormatters.set(cacheKey, formatter);
  return formatter;
}

/** Render a UTC bucket label into `timeZone` for axis ticks / tooltips. UTC (or an unparseable
 *  label) passes through unchanged so nothing regresses in the default case. Uses Intl only — no
 *  date library. */
export function formatBucketLabel(label: string, grain: Grain, timeZone: string): string {
  if (!timeZone || timeZone === "UTC") return label;
  const instant = new Date(normalizeBucketLabel(label));
  if (Number.isNaN(instant.getTime())) return label;
  const parts = bucketFormatter(timeZone, grain).formatToParts(instant);
  const get = (type: string) => parts.find((part) => part.type === type)?.value ?? "";
  const date = `${get("year")}-${get("month")}-${get("day")}`;
  if (grain !== "hour") return date;
  return `${date} ${get("hour")}:${get("minute")}`;
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
 *  than truncating to its midnight.
 *
 *  TIMEZONE LIMITATION (E4): these bounds are UTC day boundaries — `cast('YYYY-MM-DD' as date)`
 *  compares against the raw (UTC) timestamp column, not the selected-zone local day. With a
 *  non-UTC zone selected the E4 change makes bucket *labels* and server-side *truncation* in-zone,
 *  but the range window itself is still cut at UTC midnight. So e.g. a "Last 7 days" window in
 *  America/New_York can include/exclude a few UTC-vs-local boundary hours at its edges. Full
 *  in-zone boundary reinterpretation (converting the column to the zone before comparing, or
 *  shifting the literals by the zone offset) is deferred — it needs the dimension's column
 *  expression and zone-shifted literals, a larger change than this item. */
export function timeFilters(ref: string, range: DateRange): string[] {
  return [`${ref} >= cast('${range.from}' as date)`, `${ref} < cast('${addDays(range.to, 1)}' as date)`];
}

/** Whole-grain-unit offset of bucket `label` from bucket `first`. Used to align a previous-period
 *  series to the current one by bucket position rather than ordinal index, so missing (sparse)
 *  buckets in either period don't shift the overlay. */
export function bucketOffset(first: string, label: string, grain: Grain): number {
  if (grain === "hour") {
    return Math.round((Date.parse(normalizeBucketLabel(label)) - Date.parse(normalizeBucketLabel(first))) / 3_600_000);
  }
  const a = parseISO(first);
  const b = parseISO(label);
  const months = (b.getUTCFullYear() - a.getUTCFullYear()) * 12 + (b.getUTCMonth() - a.getUTCMonth());
  switch (grain) {
    case "week":
      return Math.round((b.getTime() - a.getTime()) / (7 * 86_400_000));
    case "month":
      return months;
    case "quarter":
      return Math.round(months / 3);
    case "year":
      return b.getUTCFullYear() - a.getUTCFullYear();
    default:
      return Math.round((b.getTime() - a.getTime()) / 86_400_000); // day
  }
}

/** Inclusive last calendar day of the bucket that starts at `start` for a given grain.
 *  Used to turn a brushed bucket range into a precise date filter. */
export function endOfBucket(start: string, grain: Grain): string {
  const normalizedStart = dateOnly(start);
  const date = parseISO(normalizedStart);
  switch (grain) {
    case "hour":
    case "day":
      return normalizedStart;
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
