import { useEffect, useMemo, useState } from "react";
import { HttpBackend } from "./data/httpAdapter";
import type { Catalog } from "./data/types";
import { AppShell } from "./components/AppShell";
import { AddFilter } from "./components/AddFilter";
import { Catalog as CatalogRail } from "./components/Catalog";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { DateRangeControl } from "./components/DateRangeControl";
import { FilterPill } from "./components/FilterPill";
import { GrainSelect } from "./components/GrainSelect";
import { EmptyState, ErrorState } from "./components/States";
import { ThemeToggle } from "./components/ThemeToggle";
import { ViewSwitcher } from "./components/ViewSwitcher";
import { grainOptions } from "./lib/time";
import { ExplorerProvider, useExplorer } from "./state/ExplorerContext";
import { useQueryActive } from "./state/queryActivity";
import { ExplorerView } from "./views/ExplorerView";
import { PivotView } from "./views/PivotView";

function QueryStatus() {
  const active = useQueryActive();
  return (
    <span className="flex items-center gap-1.5 text-2xs text-faint" title={active ? "Querying" : "Idle"}>
      {active ? (
        <svg viewBox="0 0 24 24" className="spinner size-3.5 text-accent" fill="none" stroke="currentColor" strokeWidth="3">
          <path d="M12 3a9 9 0 1 0 9 9" strokeLinecap="round" />
        </svg>
      ) : (
        <span aria-hidden="true" className="inline-block size-2 rounded-full bg-accent" />
      )}
    </span>
  );
}

// Allow a bearer token for auth-gated backends: `?token=…` (persisted to localStorage and stripped
// from the URL) or a previously stored token. Resolved once at module load.
function resolveApiToken(): string | undefined {
  const params = new URLSearchParams(window.location.search);
  const fromUrl = params.get("token");
  try {
    if (fromUrl) {
      localStorage.setItem("sidemantic-token", fromUrl);
      params.delete("token");
      const query = params.toString();
      window.history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}`);
      return fromUrl;
    }
    return localStorage.getItem("sidemantic-token") ?? undefined;
  } catch {
    return fromUrl ?? undefined; // storage unavailable (e.g. private mode)
  }
}

const API_TOKEN = resolveApiToken();

function CopyLinkButton() {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      onClick={async () => {
        await navigator.clipboard.writeText(window.location.href);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1200);
      }}
      className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink"
    >
      {copied ? "Copied" : "Copy link"}
    </button>
  );
}

function FullScreen({ children }: { children: React.ReactNode }) {
  return <div className="grid h-screen place-items-center bg-bg p-6">{children}</div>;
}

function Shell() {
  const { state, dispatch, catalog, initial } = useExplorer();
  const model = catalog.models.find((m) => m.name === state.model);
  const dirty = state.dateRange != null || Object.keys(state.filters).length > 0;
  const hasTime = Boolean(model?.timeDimension);
  const grains = grainOptions(model?.timeDimension?.supportedGranularities);
  // Resolve a filtered dimension's ref back to its catalog dimension + owning model, so a pill can
  // open the editor. Filters target the active model's dimensions in practice.
  const dimByRef = useMemo(() => {
    const map = new Map<string, { dim: (typeof catalog.models)[number]["dimensions"][number]; model: (typeof catalog.models)[number] }>();
    for (const m of catalog.models) for (const dim of m.dimensions) map.set(dim.ref, { dim, model: m });
    return map;
  }, [catalog]);

  // One pill per filtered dimension, showing a mode-aware summary; clicking opens the editor.
  const pills = Object.entries(state.filters).flatMap(([dimRef, filter]) => {
    const entry = dimByRef.get(dimRef);
    if (!entry) return [];
    return [
      <FilterPill
        key={dimRef}
        dim={entry.dim}
        model={entry.model}
        filter={filter}
        onRemove={() => dispatch({ type: "removeFilterDim", dim: dimRef })}
      />,
    ];
  });

  const brand = (
    <div className="flex min-w-0 items-baseline gap-2">
      <span className="text-sm font-semibold text-ink">Sidemantic</span>
      <span className="truncate text-2xs text-faint">{model?.label}</span>
    </div>
  );

  const toolbar = (
    <>
      <ViewSwitcher view={state.view} onChange={(view) => dispatch({ type: "setView", view })} />
      <DateRangeControl
        range={state.dateRange}
        disabled={!hasTime}
        onChange={(range) => dispatch({ type: "setDateRange", range })}
        comparison={state.comparison}
        comparisonRange={state.comparisonRange}
        onComparisonChange={(comparison, range) => dispatch({ type: "setComparison", comparison, range })}
      />
      <GrainSelect grain={state.grain} options={grains} disabled={!hasTime} onChange={(grain) => dispatch({ type: "setGrain", grain })} />
      {dirty ? (
        <button
          type="button"
          onClick={() => dispatch({ type: "reset", initial })}
          className="border border-line bg-surface px-2 py-1 text-2xs text-muted hover:border-faint hover:text-ink"
        >
          Reset
        </button>
      ) : null}
      <CopyLinkButton />
      <ThemeToggle />
      <QueryStatus />
    </>
  );

  const filters = (
    <>
      <span className="shrink-0 text-2xs font-semibold uppercase tracking-wide text-faint">Filters</span>
      {pills.length ? pills : <span className="text-2xs text-faint">None</span>}
      {model ? <AddFilter model={model} /> : null}
      {pills.length ? (
        <button
          type="button"
          onClick={() => dispatch({ type: "clearFilters" })}
          className="shrink-0 text-2xs text-muted underline-offset-2 hover:text-ink hover:underline"
        >
          Clear
        </button>
      ) : null}
    </>
  );

  return (
    <AppShell brand={brand} toolbar={toolbar} filters={filters} rail={<CatalogRail />}>
      <ErrorBoundary key={state.view}>
        {state.view === "explore" ? <ExplorerView /> : null}
        {state.view === "pivot" ? <PivotView /> : null}
      </ErrorBoundary>
    </AppShell>
  );
}

export function App() {
  const backend = useMemo(() => new HttpBackend({ transport: "json", token: API_TOKEN }), []);
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    backend
      .getCatalog()
      .then((value) => alive && setCatalog(value))
      .catch((err: unknown) => alive && setError(err instanceof Error ? err.message : String(err)));
    return () => {
      alive = false;
    };
  }, [backend]);

  if (error) return <FullScreen><ErrorState title="Could not load semantic layer" message={error} /></FullScreen>;
  if (!catalog) return <FullScreen><div className="text-sm text-muted">Loading semantic layer…</div></FullScreen>;
  if (!catalog.models.length)
    return <FullScreen><EmptyState title="Empty semantic layer" message="No models were found." /></FullScreen>;

  return (
    <ExplorerProvider catalog={catalog} backend={backend}>
      <Shell />
    </ExplorerProvider>
  );
}
