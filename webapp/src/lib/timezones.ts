// Timezone list helpers for the E4 selector. IANA zone ids only; formatting/bucketing uses the
// built-in Intl API (no date library). The curated set covers the zones an analyst reaches for
// most; the full set (typeahead) comes from Intl.supportedValuesOf when the engine supports it.

/** A short, curated set of common IANA zones for the dropdown, UTC first. */
export const COMMON_TIMEZONES: string[] = [
  "UTC",
  "America/Los_Angeles",
  "America/Denver",
  "America/Chicago",
  "America/New_York",
  "America/Sao_Paulo",
  "Europe/London",
  "Europe/Berlin",
  "Europe/Paris",
  "Europe/Moscow",
  "Asia/Dubai",
  "Asia/Kolkata",
  "Asia/Singapore",
  "Asia/Shanghai",
  "Asia/Tokyo",
  "Australia/Sydney",
];

// Cache the (potentially large) full list — building it hits the ICU tables once.
let allZones: string[] | null = null;

/** Every IANA zone the runtime knows, or the curated set when Intl.supportedValuesOf is missing
 *  (older Safari). Always includes "UTC". */
export function allTimezones(): string[] {
  if (allZones) return allZones;
  const supported =
    typeof Intl.supportedValuesOf === "function" ? (Intl.supportedValuesOf("timeZone") as string[]) : [];
  const set = new Set<string>(["UTC", ...supported]);
  allZones = [...set].sort();
  return allZones;
}

/** True when `zone` is a usable IANA id on this runtime (validated by constructing a formatter). */
export function isValidTimezone(zone: string): boolean {
  if (!zone) return false;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: zone });
    return true;
  } catch {
    return false;
  }
}

/** Current UTC offset for `zone` as a compact "+05:30" / "-08:00" / "+00:00" string, for labels.
 *  Uses Intl only. Returns "" if the zone can't be resolved. */
export function timezoneOffsetLabel(zone: string, at: Date = new Date()): string {
  try {
    const parts = new Intl.DateTimeFormat("en-US", { timeZone: zone, timeZoneName: "shortOffset" }).formatToParts(at);
    const name = parts.find((part) => part.type === "timeZoneName")?.value ?? "";
    // Intl yields e.g. "GMT+5:30" / "UTC"; normalize to a fixed-width signed HH:MM.
    const match = name.match(/([+-])(\d{1,2})(?::(\d{2}))?/);
    if (!match) return zone === "UTC" ? "+00:00" : "";
    const sign = match[1];
    const hours = match[2].padStart(2, "0");
    const minutes = match[3] ?? "00";
    return `${sign}${hours}:${minutes}`;
  } catch {
    return "";
  }
}
