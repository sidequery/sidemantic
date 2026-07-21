import { useRef, useState } from "react";

export type DatePickerRange = { from: string; to: string };

export type DayCell = {
  /** ISO date, YYYY-MM-DD. Comparisons on these strings are safe lexicographically. */
  iso: string;
  day: number;
  inMonth: boolean;
};

type SingleProps = {
  mode?: "single";
  value: string | null;
  onChange: (value: string | null) => void;
};

type RangeProps = {
  mode: "range";
  value: DatePickerRange | null;
  onChange: (value: DatePickerRange | null) => void;
};

type DatePickerProps = (SingleProps | RangeProps) & {
  /** Inline renders the calendar directly; popover (default) wraps it in a summary control. */
  inline?: boolean;
  ariaLabel?: string;
  disabled?: boolean;
};

const WEEKDAYS = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];
const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

function toIso(year: number, monthIndex: number, day: number): string {
  return `${String(year).padStart(4, "0")}-${String(monthIndex + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/** Sunday-start calendar grid for one month, padded with the neighboring months' days so every
 *  week is complete. Exported for tests. */
export function monthGrid(year: number, monthIndex: number): DayCell[][] {
  const first = new Date(Date.UTC(year, monthIndex, 1));
  const start = new Date(first);
  start.setUTCDate(1 - first.getUTCDay());
  const weeks: DayCell[][] = [];
  const cursor = new Date(start);
  do {
    const week: DayCell[] = [];
    for (let day = 0; day < 7; day += 1) {
      week.push({
        iso: toIso(cursor.getUTCFullYear(), cursor.getUTCMonth(), cursor.getUTCDate()),
        day: cursor.getUTCDate(),
        inMonth: cursor.getUTCMonth() === monthIndex,
      });
      cursor.setUTCDate(cursor.getUTCDate() + 1);
    }
    weeks.push(week);
  } while (cursor.getUTCMonth() === monthIndex);
  return weeks;
}

function todayIso(): string {
  const now = new Date();
  return toIso(now.getFullYear(), now.getMonth(), now.getDate());
}

// Dependency-free calendar picker for single dates or ranges, inline or as a popover in the
// DateRangeControl idiom. Values are ISO YYYY-MM-DD strings — no date library, no timezone drift.
export function DatePicker(props: DatePickerProps) {
  const { inline, ariaLabel, disabled } = props;
  const isRange = props.mode === "range";
  const anchor = isRange ? (props.value?.from ?? null) : props.value;
  const [year, setYear] = useState(() => Number((anchor ?? todayIso()).slice(0, 4)));
  const [monthIndex, setMonthIndex] = useState(() => Number((anchor ?? todayIso()).slice(5, 7)) - 1);
  // Range building: first click sets the pending start, second click commits from/to.
  const [pending, setPending] = useState<string | null>(null);
  const details = useRef<HTMLDetailsElement>(null);

  function shiftMonth(delta: number) {
    const next = new Date(Date.UTC(year, monthIndex + delta, 1));
    setYear(next.getUTCFullYear());
    setMonthIndex(next.getUTCMonth());
  }

  function pick(iso: string) {
    if (!isRange) {
      props.onChange(iso);
      if (details.current) details.current.open = false;
      return;
    }
    if (!pending) {
      setPending(iso);
      return;
    }
    const [from, to] = pending <= iso ? [pending, iso] : [iso, pending];
    props.onChange({ from, to });
    setPending(null);
    if (details.current) details.current.open = false;
  }

  function isSelected(iso: string): boolean {
    if (isRange) {
      if (pending) return iso === pending;
      return props.value != null && iso >= props.value.from && iso <= props.value.to;
    }
    return iso === props.value;
  }

  function isEdge(iso: string): boolean {
    if (isRange) return pending ? iso === pending : props.value != null && (iso === props.value.from || iso === props.value.to);
    return iso === props.value;
  }

  const today = todayIso();
  const calendar = (
    <div className="w-56 select-none rounded-md border border-line bg-surface p-2 text-2xs" aria-label={ariaLabel ?? "Calendar"}>
      <div className="mb-1 flex items-center justify-between">
        <button type="button" aria-label="Previous month" onClick={() => shiftMonth(-1)} className="px-1.5 py-0.5 text-muted hover:bg-surface-soft hover:text-ink">
          ‹
        </button>
        <span className="font-medium text-ink">
          {MONTHS[monthIndex]} {year}
        </span>
        <button type="button" aria-label="Next month" onClick={() => shiftMonth(1)} className="px-1.5 py-0.5 text-muted hover:bg-surface-soft hover:text-ink">
          ›
        </button>
      </div>
      <div className="grid grid-cols-7 text-center text-faint">
        {WEEKDAYS.map((weekday) => (
          <span key={weekday} className="py-0.5">
            {weekday}
          </span>
        ))}
      </div>
      <div role="grid" aria-label={`${MONTHS[monthIndex]} ${year}`}>
        {monthGrid(year, monthIndex).map((week, weekIndex) => (
          <div key={weekIndex} role="row" className="grid grid-cols-7">
            {week.map((cell) => (
              <button
                key={cell.iso}
                type="button"
                role="gridcell"
                aria-selected={isSelected(cell.iso)}
                data-date={cell.iso}
                onClick={() => pick(cell.iso)}
                className={`py-1 text-center font-mono tnum ${
                  isEdge(cell.iso)
                    ? "bg-accent text-surface"
                    : isSelected(cell.iso)
                      ? "bg-accent-soft text-accent"
                      : cell.inMonth
                        ? "text-ink hover:bg-surface-soft"
                        : "text-faint hover:bg-surface-soft"
                } ${cell.iso === today ? "underline underline-offset-2" : ""}`}
              >
                {cell.day}
              </button>
            ))}
          </div>
        ))}
      </div>
      {isRange && pending ? <p className="mt-1 text-faint">Start {pending} — pick an end date.</p> : null}
      {(isRange ? props.value : props.value) != null ? (
        <button
          type="button"
          onClick={() => (isRange ? props.onChange(null) : props.onChange(null))}
          className="mt-1 w-full border border-line px-2 py-1 text-left text-muted hover:bg-surface-soft"
        >
          Clear
        </button>
      ) : null}
    </div>
  );

  if (inline) return calendar;

  const summary = isRange
    ? props.value
      ? `${props.value.from} → ${props.value.to}`
      : "Any dates"
    : (props.value ?? "Any date");

  return (
    <details ref={details} className="relative inline-block text-2xs">
      <summary
        className={`flex cursor-pointer items-center gap-1.5 rounded border border-line bg-surface px-2 py-1 text-ink ${
          disabled ? "pointer-events-none opacity-50" : ""
        }`}
      >
        <span className="text-faint">{ariaLabel ?? "Date"}</span>
        <span className="font-mono tnum">{summary}</span>
        <span aria-hidden="true" className="text-faint">
          ▾
        </span>
      </summary>
      <div className="absolute left-0 z-50 mt-1 shadow-[var(--shadow)]">{calendar}</div>
    </details>
  );
}
