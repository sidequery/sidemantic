import { useEffect, useId, useMemo, useRef, useState } from "react";

export type ComboboxOption = { value: string; label?: string };

type SingleProps = {
  multiple?: false;
  value: string | null;
  onChange: (value: string | null) => void;
};

type MultipleProps = {
  multiple: true;
  values: string[];
  onChange: (values: string[]) => void;
};

type ComboboxProps = (SingleProps | MultipleProps) & {
  options: ComboboxOption[];
  placeholder?: string;
  ariaLabel?: string;
  disabled?: boolean;
  /** Cap on rendered matches; a search box narrows rather than paging (the FilterEditor framing). */
  maxVisible?: number;
};

/** Case-insensitive substring match over value and label. Exported for tests. */
export function filterOptions(options: ComboboxOption[], query: string): ComboboxOption[] {
  const needle = query.trim().toLowerCase();
  if (!needle) return options;
  return options.filter(
    (option) => option.value.toLowerCase().includes(needle) || (option.label ?? "").toLowerCase().includes(needle),
  );
}

// Searchable select in the WAI-ARIA combobox pattern: the input filters the listbox, arrow keys
// move the active option, Enter commits, Escape closes. Single mode commits and closes; multiple
// mode toggles values, keeps the list open, and shows removable chips. No popup library.
// Use Combobox for multi-select or long lists where typeahead beats scanning; Select for a short
// static single choice.
export function Combobox(props: ComboboxProps) {
  const { options, placeholder = "Search…", ariaLabel, disabled, maxVisible = 50 } = props;
  const listId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);

  const matches = useMemo(() => filterOptions(options, query).slice(0, maxVisible), [options, query, maxVisible]);
  const selectedValues = props.multiple ? props.values : props.value != null ? [props.value] : [];
  const selectedSet = new Set(selectedValues);
  const labelFor = (value: string) => options.find((option) => option.value === value)?.label ?? value;

  useEffect(() => {
    if (!open) return;
    function onPointer(event: MouseEvent) {
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onPointer, true);
    return () => document.removeEventListener("mousedown", onPointer, true);
  }, [open]);

  function commit(option: ComboboxOption | undefined) {
    if (!option) return;
    if (props.multiple) {
      const next = selectedSet.has(option.value)
        ? props.values.filter((value) => value !== option.value)
        : [...props.values, option.value];
      props.onChange(next);
      // Stay open for further picks; keep the query so a narrowed list supports bulk selection.
      return;
    }
    props.onChange(option.value);
    setQuery("");
    setOpen(false);
  }

  function clear() {
    if (props.multiple) props.onChange([]);
    else props.onChange(null);
    setQuery("");
  }

  function onKeyDown(event: React.KeyboardEvent) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setOpen(true);
      setActiveIndex((index) => Math.min(index + 1, matches.length - 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((index) => Math.max(index - 1, 0));
    } else if (event.key === "Enter") {
      event.preventDefault();
      if (open) commit(matches[activeIndex]);
    } else if (event.key === "Escape") {
      setOpen(false);
    } else if (event.key === "Backspace" && props.multiple && !query && props.values.length) {
      props.onChange(props.values.slice(0, -1));
    }
  }

  const inputPlaceholder = !props.multiple && props.value != null ? labelFor(props.value) : placeholder;

  return (
    <div ref={rootRef} className="relative inline-flex min-w-40 flex-wrap items-center gap-1 text-2xs">
      {props.multiple
        ? props.values.map((value) => (
            <span
              key={value}
              data-chip={value}
              className="inline-flex items-center gap-1 rounded-full bg-surface-soft px-2 py-0.5 leading-4 text-muted"
            >
              <span className="max-w-32 truncate">{labelFor(value)}</span>
              <button
                type="button"
                aria-label={`Remove ${labelFor(value)}`}
                disabled={disabled}
                onClick={() => props.onChange(props.values.filter((entry) => entry !== value))}
                className="text-faint hover:text-ink"
              >
                ×
              </button>
            </span>
          ))
        : null}
      <span className="relative min-w-28 flex-1">
        <input
          type="text"
          role="combobox"
          aria-expanded={open}
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={open && matches[activeIndex] ? `${listId}-${activeIndex}` : undefined}
          aria-label={ariaLabel}
          disabled={disabled}
          placeholder={inputPlaceholder}
          value={query}
          onChange={(event) => {
            setQuery(event.target.value);
            setActiveIndex(0);
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={onKeyDown}
          className="w-full rounded-full border border-line bg-surface px-2.5 py-1 text-ink placeholder:text-faint disabled:opacity-50"
        />
        {selectedValues.length > 0 ? (
          <button
            type="button"
            aria-label="Clear selection"
            disabled={disabled}
            onClick={clear}
            className="absolute right-1 top-1/2 -translate-y-1/2 px-1 text-faint hover:text-ink"
          >
            ×
          </button>
        ) : null}
      </span>
      {open ? (
        <ul
          id={listId}
          role="listbox"
          aria-multiselectable={props.multiple || undefined}
          className="absolute left-0 top-full z-50 mt-1 max-h-56 w-full min-w-40 overflow-y-auto rounded-xl border border-line bg-surface p-1.5 shadow-[var(--shadow)]"
        >
          {matches.length === 0 ? <li className="px-1.5 py-1 text-faint">No matches</li> : null}
          {matches.map((option, index) => (
            <li
              key={option.value}
              id={`${listId}-${index}`}
              role="option"
              aria-selected={selectedSet.has(option.value)}
              data-active={index === activeIndex || undefined}
              onMouseEnter={() => setActiveIndex(index)}
              onMouseDown={(event) => {
                // mousedown (not click) so selection wins over the input's blur.
                event.preventDefault();
                commit(option);
              }}
              className="cursor-pointer truncate rounded-md px-2 py-1 text-muted data-[active=true]:bg-surface-soft data-[active=true]:text-ink"
            >
              {option.label ?? option.value}
              {selectedSet.has(option.value) ? <span className="float-right text-accent">✓</span> : null}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
