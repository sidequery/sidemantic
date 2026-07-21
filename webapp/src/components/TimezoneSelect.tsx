import { useId, useMemo, useState } from "react";
import { COMMON_TIMEZONES, allTimezones, isValidTimezone, timezoneOffsetLabel } from "../lib/timezones";

type TimezoneSelectProps = {
  timezone: string;
  disabled?: boolean;
  onChange: (timezone: string) => void;
};

const SEARCH_SENTINEL = "__search__";

/** Compact timezone control matching GrainSelect/DateRangeControl. A native <select> exposes the
 *  curated common zones (plus whatever zone is currently selected, so a link-loaded zone always
 *  shows); picking "Search…" reveals a lightweight typeahead backed by a <datalist> over every
 *  IANA zone the runtime knows — no popup library, no date library. */
export function TimezoneSelect({ timezone, disabled, onChange }: TimezoneSelectProps) {
  const listId = useId();
  const [searching, setSearching] = useState(false);
  const [text, setText] = useState("");

  // Curated set + the active zone (deduped), so a non-common selected zone is still selectable.
  const options = useMemo(() => {
    const set = new Set<string>(COMMON_TIMEZONES);
    if (timezone) set.add(timezone);
    return [...set];
  }, [timezone]);

  const zones = useMemo(() => (searching ? allTimezones() : []), [searching]);

  function commitSearch(value: string) {
    const trimmed = value.trim();
    if (isValidTimezone(trimmed)) {
      onChange(trimmed);
      setSearching(false);
      setText("");
    }
  }

  if (searching) {
    return (
      <label className="flex items-center gap-1.5 text-2xs text-faint">
        <span className="hidden sm:inline">Zone</span>
        <input
          type="text"
          list={listId}
          autoFocus
          aria-label="Search timezone"
          placeholder="Region/City…"
          value={text}
          disabled={disabled}
          onChange={(event) => {
            setText(event.target.value);
            // Selecting from the datalist fires change with a full, valid value — commit it.
            if (allTimezones().includes(event.target.value)) commitSearch(event.target.value);
          }}
          onKeyDown={(event) => {
            if (event.key === "Enter") commitSearch(text);
            else if (event.key === "Escape") {
              setSearching(false);
              setText("");
            }
          }}
          onBlur={() => {
            setSearching(false);
            setText("");
          }}
          className="w-36 rounded border border-line bg-surface px-1.5 py-1 text-2xs text-ink disabled:opacity-50"
        />
        <datalist id={listId}>
          {zones.map((zone) => (
            <option key={zone} value={zone} />
          ))}
        </datalist>
      </label>
    );
  }

  const offset = timezoneOffsetLabel(timezone);
  return (
    <label className="flex items-center gap-1.5 text-2xs text-faint">
      <span className="hidden sm:inline">Zone</span>
      <select
        aria-label="Timezone"
        value={timezone}
        disabled={disabled}
        onChange={(event) => {
          if (event.target.value === SEARCH_SENTINEL) setSearching(true);
          else onChange(event.target.value);
        }}
        className="max-w-[11rem] rounded border border-line bg-surface px-1.5 py-1 text-2xs text-ink disabled:opacity-50"
      >
        {options.map((zone) => (
          <option key={zone} value={zone}>
            {zone}
            {zone === "UTC" ? "" : offset && zone === timezone ? ` (${offset})` : ""}
          </option>
        ))}
        <option value={SEARCH_SENTINEL}>Search…</option>
      </select>
    </label>
  );
}
