import { useEffect, useId, useMemo, useRef, useState } from "react";
import { aliasOf, NULL_TOKEN, type CatalogDimension, type CatalogModel } from "../data/types";
import { displayDimValue, sqlLiteral } from "../lib/format";
import { composeFilters, distinctValues, likeEscape, type FilterMode } from "../lib/queries";
import { useExplorer } from "../state/ExplorerContext";
import { useQueryResult } from "../state/useQueryResult";

const MODES: { mode: FilterMode; label: string }[] = [
  { mode: "include", label: "Include" },
  { mode: "exclude", label: "Exclude" },
  { mode: "contains", label: "Contains" },
];

// The DISTINCT list is intentionally shallow — a search box narrows it rather than paging through
// thousands of values. Matches the leaderboard's own top-N framing.
const VALUE_LIMIT = 50;
const SEARCH_DEBOUNCE_MS = 200;

/** Debounce a fast-changing value so we don't issue a query on every keystroke. */
function useDebounced<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);
  return debounced;
}

/**
 * Popover editor for one dimension's filter: mode (include/exclude/contains) + a searchable,
 * server-filtered checkbox list of distinct values (include/exclude), or a substring pattern
 * (contains). Traps focus and closes on Escape or outside click. Reads/writes explorer state
 * directly via the reducer; no local mirror of the committed filter.
 */
export function FilterEditor({
  dim,
  model,
  onClose,
}: {
  dim: CatalogDimension;
  model: CatalogModel;
  onClose: () => void;
}) {
  const { state, dispatch, backend } = useExplorer();
  const filter = state.filters[dim.ref];
  // Mode is editor-local: an empty exclude selection emits no SQL, so it can't live in committed
  // state, yet the user's mode choice must survive until they check a value. Seed from the committed
  // filter (include by default) and drive value-toggles through this mode.
  const [mode, setModeState] = useState<FilterMode>(filter?.mode ?? "include");
  const selected = useMemo(() => new Set(filter?.mode !== "contains" ? (filter?.values ?? []) : []), [filter]);

  const panelRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const labelId = useId();

  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounced(search, SEARCH_DEBOUNCE_MS);
  // Contains mode: the search box IS the pattern; a keystroke retypes the committed pattern.
  const pattern = filter?.mode === "contains" ? (filter.pattern ?? "") : "";
  const [patternDraft, setPatternDraft] = useState(pattern);
  const debouncedPattern = useDebounced(patternDraft, SEARCH_DEBOUNCE_MS);

  // Focus the search box on open; restore focus to the opener when the popover unmounts.
  useEffect(() => {
    const opener = document.activeElement as HTMLElement | null;
    searchRef.current?.focus();
    return () => opener?.focus?.();
  }, []);

  // Escape closes; outside pointer/click closes. Focus is trapped to the panel via onKeyDown below.
  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.stopPropagation();
        onClose();
      }
    }
    function onPointer(event: MouseEvent) {
      if (panelRef.current && !panelRef.current.contains(event.target as Node)) onClose();
    }
    document.addEventListener("keydown", onKey, true);
    document.addEventListener("mousedown", onPointer, true);
    return () => {
      document.removeEventListener("keydown", onKey, true);
      document.removeEventListener("mousedown", onPointer, true);
    };
  }, [onClose]);

  // Commit the contains pattern (debounced) so the dashboard updates as you type.
  useEffect(() => {
    if (mode !== "contains") return;
    if (debouncedPattern === pattern) return;
    dispatch({ type: "setFilterPattern", dim: dim.ref, pattern: debouncedPattern });
    // pattern is derived from committed state; re-running when it changes would loop.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [debouncedPattern, mode, dim.ref, dispatch]);

  const timeRef = model.timeDimension?.ref;
  // Distinct values for this dimension, honoring the surrounding crossfilter (minus this dim's own
  // filter) and the active date range, then narrowed by the search text via a server-side ILIKE.
  const valueFilters = useMemo(() => {
    const base = composeFilters(state.filters, { timeRef, range: state.dateRange, excludeDim: dim.ref });
    if (debouncedSearch.trim()) {
      // Cast to text so the search works on numeric/boolean dimensions too: DuckDB and
      // Postgres reject ILIKE on non-text operands.
      const pat = sqlLiteral(`%${likeEscape(debouncedSearch.trim())}%`);
      base.push(`CAST(${dim.ref} AS VARCHAR) ILIKE ${pat} ESCAPE '\\'`);
    }
    return base;
  }, [state.filters, timeRef, state.dateRange, dim.ref, debouncedSearch]);

  const listMode = mode !== "contains";
  const { result, loading, error } = useQueryResult(
    backend,
    listMode ? distinctValues(dim.ref, valueFilters, VALUE_LIMIT) : null,
  );

  const dimAlias = aliasOf(dim.ref);
  const values: string[] = useMemo(() => {
    if (!result) return [];
    return result.rows.map((row) => {
      const raw = row[dimAlias];
      return raw === null || raw === undefined ? NULL_TOKEN : String(raw);
    });
  }, [result, dimAlias]);
  // A kept result from the previous dimension carries a different column; treat as still loading.
  const stale = !!result && result.rows.length > 0 && !result.columns.includes(dimAlias);
  const showSkeleton = listMode && (loading || stale);

  function setMode(next: FilterMode) {
    setModeState(next);
    // Re-mode an already-committed filter (preserves its values across include<->exclude). If none
    // is committed yet, the choice lives only in editor state until a value/pattern is entered.
    if (filter) dispatch({ type: "setFilterMode", dim: dim.ref, mode: next });
    if (next === "contains") setPatternDraft(pattern);
  }

  // Trap Tab focus within the panel (a lightweight roving trap over focusable descendants).
  function onKeyDown(event: React.KeyboardEvent) {
    if (event.key !== "Tab") return;
    const focusable = panelRef.current?.querySelectorAll<HTMLElement>(
      'button, input, [href], select, textarea, [tabindex]:not([tabindex="-1"])',
    );
    if (!focusable || focusable.length === 0) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  return (
    <div
      ref={panelRef}
      role="dialog"
      aria-modal="true"
      aria-labelledby={labelId}
      onKeyDown={onKeyDown}
      className="absolute left-0 z-50 mt-1 w-64 border border-line bg-surface p-2 text-2xs shadow-lg"
    >
      <div id={labelId} className="mb-2 flex items-baseline justify-between gap-2">
        <span className="truncate font-semibold text-ink">{dim.label}</span>
        <button
          type="button"
          aria-label="Close filter editor"
          onClick={onClose}
          className="grid size-4 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink"
        >
          ×
        </button>
      </div>

      {/* Mode segmented control */}
      <div role="group" aria-label="Filter mode" className="mb-2 grid grid-cols-3 gap-px border border-line bg-line">
        {MODES.map(({ mode: m, label }) => (
          <button
            key={m}
            type="button"
            aria-pressed={mode === m}
            onClick={() => setMode(m)}
            className={`px-1.5 py-1 text-center ${
              mode === m ? "bg-accent-soft font-medium text-accent" : "bg-surface text-muted hover:bg-surface-soft"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {mode === "contains" ? (
        <input
          ref={searchRef}
          type="text"
          aria-label={`${dim.label} contains`}
          placeholder="Substring…"
          value={patternDraft}
          onChange={(event) => setPatternDraft(event.target.value)}
          className="w-full border border-line bg-surface px-1.5 py-1 text-2xs text-ink placeholder:text-faint"
        />
      ) : (
        <>
          <input
            ref={searchRef}
            type="text"
            aria-label={`Search ${dim.label} values`}
            placeholder="Search values…"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            className="w-full border border-line bg-surface px-1.5 py-1 text-2xs text-ink placeholder:text-faint"
          />
          <div className="mt-2 max-h-56 overflow-y-auto" role="group" aria-label={`${dim.label} values`}>
            {error ? (
              <p className="px-1 py-2 text-danger">{error}</p>
            ) : showSkeleton ? (
              <div className="space-y-1.5 p-1">
                {[0, 1, 2, 3, 4].map((i) => (
                  <div key={i} className="skeleton h-4 w-full" />
                ))}
              </div>
            ) : values.length === 0 ? (
              <p className="px-1 py-2 text-faint">No values</p>
            ) : (
              values.map((value) => {
                const checked = selected.has(value);
                return (
                  <label
                    key={value}
                    className="flex cursor-pointer items-center gap-2 px-1 py-1 hover:bg-surface-soft"
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => dispatch({ type: "toggleFilter", dim: dim.ref, value, mode })}
                      className="size-3 accent-[var(--accent)]"
                    />
                    <span className="min-w-0 truncate text-ink">{displayDimValue(value)}</span>
                  </label>
                );
              })
            )}
          </div>
        </>
      )}

      <div className="mt-2 flex items-center justify-between border-t border-line pt-2">
        <button
          type="button"
          onClick={() => dispatch({ type: "removeFilterDim", dim: dim.ref })}
          className="text-muted underline-offset-2 hover:text-ink hover:underline"
        >
          Clear
        </button>
        <button
          type="button"
          onClick={onClose}
          className="border border-line px-2 py-1 text-muted hover:bg-surface-soft"
        >
          Done
        </button>
      </div>
    </div>
  );
}
