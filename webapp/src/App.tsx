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
import { TimezoneSelect } from "./components/TimezoneSelect";
import { RowPreviewDrawer } from "./components/RowPreviewDrawer";
import { EmptyState, ErrorState, LoadingState } from "./components/States";
import { ThemeToggle } from "./components/ThemeToggle";
import { ViewSwitcher } from "./components/ViewSwitcher";
import { grainOptions } from "./lib/time";
import { ExplorerProvider, useExplorer } from "./state/ExplorerContext";
import { useQueryActive } from "./state/queryActivity";
import { ExploreIndexView } from "./views/ExploreIndexView";
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

function SessionLogin({ backend, onAuthenticated }: { backend: HttpBackend; onAuthenticated: () => void }) {
  const [token, setToken] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  return (
    <div className="w-full max-w-sm border border-line bg-surface p-6 shadow-sm">
      <h1 className="text-base font-semibold text-ink">Connect to Sidemantic</h1>
      <p className="mt-2 text-xs leading-5 text-muted">
        Enter the API bearer once. It is exchanged for a short-lived, HttpOnly browser session
        where supported, and is never placed in the URL or browser storage.
      </p>
      <form
        className="mt-5 space-y-3"
        onSubmit={async (event) => {
          event.preventDefault();
          setSubmitting(true);
          setError(null);
          try {
            await backend.createBrowserSession(token);
            setToken("");
            onAuthenticated();
          } catch (err: unknown) {
            setError(err instanceof Error ? err.message : String(err));
          } finally {
            setSubmitting(false);
          }
        }}
      >
        <label className="block text-xs font-medium text-ink" htmlFor="api-token">
          API bearer token
        </label>
        <input
          id="api-token"
          type="password"
          autoComplete="off"
          required
          value={token}
          onChange={(event) => setToken(event.target.value)}
          className="w-full border border-line bg-bg px-3 py-2 text-sm text-ink outline-none focus:border-accent"
        />
        {error ? <p className="text-xs text-red-700">{error}</p> : null}
        <button
          type="submit"
          disabled={submitting || !token}
          className="w-full bg-accent px-3 py-2 text-xs font-semibold text-white disabled:opacity-50"
        >
          {submitting ? "Connecting…" : "Connect"}
        </button>
      </form>
    </div>
  );
}

function Shell() {
  const { state, dispatch, catalog, initial } = useExplorer();
  const isHome = state.view === "home";
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
    <button
      type="button"
      onClick={() => dispatch({ type: "setView", view: "home" })}
      aria-label="Home"
      className="flex min-w-0 items-baseline gap-2"
    >
      <span className="text-sm font-semibold text-ink">Sidemantic</span>
      {!isHome && model?.label ? <span className="truncate text-2xs text-faint">{model.label}</span> : null}
    </button>
  );

  // On the home/index view the model-scoped controls (view switcher, date/grain, filters) don't apply.
  const toolbar = isHome ? (
    <>
      <ThemeToggle />
      <QueryStatus />
    </>
  ) : (
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
      <TimezoneSelect timezone={state.timezone} disabled={!hasTime} onChange={(timezone) => dispatch({ type: "setTimezone", timezone })} />
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
    <AppShell
      brand={brand}
      toolbar={toolbar}
      filters={isHome ? undefined : filters}
      rail={<CatalogRail />}
      showRail={!isHome}
      openRailRequest={state.view === "pivot"}
      drawer={isHome ? undefined : <RowPreviewDrawer />}
    >
      <ErrorBoundary key={state.view}>
        {state.view === "home" ? <ExploreIndexView /> : null}
        {state.view === "explore" ? <ExplorerView /> : null}
        {state.view === "pivot" ? <PivotView /> : null}
      </ErrorBoundary>
    </AppShell>
  );
}

export function App() {
  const [authRequired, setAuthRequired] = useState(false);
  const backend = useMemo(
    () => new HttpBackend({ transport: "json", onUnauthorized: () => setAuthRequired(true) }),
    [],
  );
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);

  useEffect(() => {
    let alive = true;
    backend
      .getCatalog()
      .then((value) => alive && setCatalog(value))
      .catch((err: unknown) => alive && setError(err instanceof Error ? err.message : String(err)));
    return () => {
      alive = false;
    };
  }, [backend, reload]);

  if (authRequired) {
    return (
      <FullScreen>
        <SessionLogin
          backend={backend}
          onAuthenticated={() => {
            setAuthRequired(false);
            setError(null);
            setReload((value) => value + 1);
          }}
        />
      </FullScreen>
    );
  }
  if (error) return <FullScreen><ErrorState title="Could not load semantic layer" message={error} /></FullScreen>;
  if (!catalog) return <FullScreen><LoadingState title="Loading semantic layer" message="Reading models and metrics…" /></FullScreen>;
  if (!catalog.models.length)
    return <FullScreen><EmptyState title="Empty semantic layer" message="No models were found." /></FullScreen>;

  return (
    <ExplorerProvider catalog={catalog} backend={backend}>
      <Shell />
    </ExplorerProvider>
  );
}
