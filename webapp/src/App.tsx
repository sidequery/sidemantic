import { useEffect, useMemo, useState } from "react";
import { HttpBackend } from "./data/httpAdapter";
import type { Catalog, DashboardSpec } from "./data/types";
import { AppShell } from "./components/AppShell";
import { AppBrand } from "./components/AppBrand";
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
import { dashboardTabConfig } from "./lib/dashboard";
import { ExplorerProvider, useExplorer } from "./state/ExplorerContext";
import { initialStateFromCatalog } from "./state/explorerState";
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
  const { state, dispatch, catalog, initial, dashboard } = useExplorer();
  const isDashboard = Boolean(dashboard);
  const isHome = state.view === "home" && !isDashboard;
  const model = catalog.models.find((m) => m.name === state.model);
  const configured = useMemo(
    () => dashboardTabConfig(catalog, dashboard, state.dashboardTab),
    [catalog, dashboard, state.dashboardTab],
  );
  const resetInitial = useMemo(
    () => (dashboard ? initialStateFromCatalog(catalog, dashboard, state.dashboardTab) : initial),
    [catalog, dashboard, initial, state.dashboardTab],
  );
  const dirty = state.dateRange != null || Object.keys(state.filters).length > 0;
  const activeTimeDimension = configured?.timeDimension ?? model?.timeDimension;
  const hasTime = Boolean(activeTimeDimension);
  const grains = grainOptions(activeTimeDimension?.supportedGranularities);
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
    <AppBrand
      dashboardTitle={dashboard?.title}
      modelLabel={!isHome && !isDashboard ? model?.label : undefined}
      onHome={() => dispatch({ type: "setView", view: "home" })}
    />
  );

  // On the home/index view the model-scoped controls (view switcher, date/grain, filters) don't apply.
  const toolbar = isHome ? (
    <>
      <ThemeToggle />
      <QueryStatus />
    </>
  ) : (
    <>
      {dashboard ? (
        <div className="flex items-center border border-line bg-surface" role="tablist" aria-label="Dashboard tabs">
          {dashboard.tabs.map((tab) => {
            const selected = tab.id === state.dashboardTab;
            return (
              <button
                key={tab.id}
                type="button"
                role="tab"
                aria-selected={selected}
                className={`px-2 py-1 text-2xs ${selected ? "bg-accent text-white" : "text-muted hover:text-ink"}`}
                onClick={() => {
                  const config = dashboardTabConfig(catalog, dashboard, tab.id);
                  if (config) {
                    dispatch({
                      type: "setDashboardTab",
                      tab: config.id,
                      model: config.model.name,
                      metric: config.selectedMetric,
                      grain: config.grain,
                    });
                  }
                }}
              >
                {tab.label ?? tab.id}
              </button>
            );
          })}
        </div>
      ) : (
        <ViewSwitcher view={state.view} onChange={(view) => dispatch({ type: "setView", view })} />
      )}
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
          onClick={() => dispatch({ type: "reset", initial: resetInitial })}
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
      showRail={!isHome && !isDashboard}
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
  const backend = useMemo(() => new HttpBackend({ transport: "json", token: API_TOKEN }), []);
  const [boot, setBoot] = useState<{ catalog: Catalog; dashboard: DashboardSpec | null } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    Promise.all([backend.getCatalog(), backend.getDashboard()])
      .then(([catalog, dashboard]) => alive && setBoot({ catalog, dashboard }))
      .catch((err: unknown) => alive && setError(err instanceof Error ? err.message : String(err)));
    return () => {
      alive = false;
    };
  }, [backend]);

  if (error) return <FullScreen><ErrorState title="Could not load semantic layer" message={error} /></FullScreen>;
  if (!boot) return <FullScreen><LoadingState title="Loading semantic layer" message="Reading models and metrics…" /></FullScreen>;
  if (!boot.catalog.models.length)
    return <FullScreen><EmptyState title="Empty semantic layer" message="No models were found." /></FullScreen>;

  return (
    <ExplorerProvider catalog={boot.catalog} backend={backend} dashboard={boot.dashboard}>
      <Shell />
    </ExplorerProvider>
  );
}
