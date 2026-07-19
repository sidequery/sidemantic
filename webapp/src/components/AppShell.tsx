import { useEffect, useId, useRef, useState, type ReactNode } from "react";

type AppShellProps = {
  brand: ReactNode;
  toolbar: ReactNode;
  filters?: ReactNode;
  rail: ReactNode;
  children: ReactNode;
  drawer?: ReactNode;
  /** Whether to show the left catalog rail (and its collapse toggle). The home/index view hides it. */
  showRail?: boolean;
  /** Opens the rail when this changes to true; users may still collapse it afterward. */
  openRailRequest?: boolean;
};

/** Collapsible rail | stage frame with a top toolbar and an active-filter strip. */
export function AppShell({ brand, toolbar, filters, rail, children, drawer, showRail = true, openRailRequest }: AppShellProps) {
  const [mobile, setMobile] = useState(() => typeof window !== "undefined" && window.matchMedia("(max-width: 767px)").matches);
  const [railOpen, setRailOpen] = useState(() => typeof window === "undefined" || !window.matchMedia("(max-width: 767px)").matches);
  const railId = useId();
  const railRef = useRef<HTMLElement>(null);
  const toggleRef = useRef<HTMLButtonElement>(null);
  const railVisible = showRail && railOpen;

  useEffect(() => {
    const query = window.matchMedia("(max-width: 767px)");
    const update = () => {
      setMobile(query.matches);
      setRailOpen(!query.matches);
    };
    query.addEventListener("change", update);
    return () => query.removeEventListener("change", update);
  }, []);

  useEffect(() => {
    if (openRailRequest && !mobile) setRailOpen(true);
  }, [openRailRequest, mobile]);

  useEffect(() => {
    if (!mobile || !railOpen) return;
    const opener = toggleRef.current;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    window.requestAnimationFrame(() => railRef.current?.querySelector<HTMLElement>("button")?.focus());

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        setRailOpen(false);
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = railRef.current?.querySelectorAll<HTMLElement>(
        'button:not(:disabled), select:not(:disabled), input:not(:disabled), [href], [tabindex]:not([tabindex="-1"])',
      );
      if (!focusable?.length) return;
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

    document.addEventListener("keydown", onKeyDown, true);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener("keydown", onKeyDown, true);
      opener?.focus();
    };
  }, [mobile, railOpen]);

  return (
    <div className="flex h-[100dvh] min-w-0 flex-col overflow-hidden bg-bg text-ink">
      <header className="relative z-40 flex shrink-0 flex-col gap-3 border-b border-line/80 bg-surface px-4 py-3 shadow-[0_1px_0_rgba(0,0,0,0.02)] md:flex-row md:items-center md:justify-between md:px-5">
        <div className="flex min-w-0 items-center gap-2">
          {showRail ? (
            <button
              ref={toggleRef}
              type="button"
              aria-label={mobile ? "Open catalog" : railOpen ? "Collapse sidebar" : "Expand sidebar"}
              aria-expanded={railOpen}
              aria-controls={railId}
              onClick={() => setRailOpen((open) => !open)}
              className="grid size-9 shrink-0 place-items-center rounded-full bg-surface-soft text-muted transition-colors hover:bg-line hover:text-ink"
            >
              <svg aria-hidden="true" viewBox="0 0 20 20" className="size-4" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round">
                <path d="M3.5 5.5h13M3.5 10h13M3.5 14.5h13" />
              </svg>
            </button>
          ) : null}
          {brand}
        </div>
        <div className="flex w-full min-w-0 flex-wrap items-center gap-2 overflow-visible md:w-auto md:shrink-0 md:flex-nowrap">
          {toolbar}
        </div>
      </header>

      {filters ? (
        // Fixed height so the bar doesn't jump when pills replace the "No filters" text.
        <div className="flex min-h-12 shrink-0 flex-wrap items-center gap-2 overflow-x-auto border-b border-line/80 bg-surface px-4 py-2 md:flex-nowrap md:px-5">
          {filters}
        </div>
      ) : null}

      <div className={`grid min-h-0 min-w-0 flex-1 grid-cols-1 ${railVisible ? "md:grid-cols-[280px_minmax(0,1fr)]" : "md:grid-cols-1"}`}>
        {showRail && railVisible ? (
          <>
            {mobile ? (
              <button
                type="button"
                aria-label="Close catalog"
                className="fixed inset-0 z-40 bg-black/35 backdrop-blur-[2px]"
                onClick={() => setRailOpen(false)}
              />
            ) : null}
            <aside
              id={railId}
              ref={railRef}
              role={mobile ? "dialog" : undefined}
              aria-modal={mobile ? "true" : undefined}
              aria-label={mobile ? "Data catalog" : undefined}
              onClick={(event) => {
                if (mobile && (event.target as HTMLElement).closest("[data-catalog-metric], [data-catalog-dimension]")) setRailOpen(false);
              }}
              onChange={() => mobile && setRailOpen(false)}
              className="fixed inset-y-0 left-0 z-50 min-h-0 w-[min(88vw,320px)] overflow-y-auto rounded-r-2xl bg-surface shadow-floating md:static md:z-auto md:w-auto md:rounded-none md:border-r md:border-line/80 md:shadow-none"
            >
              {mobile ? (
                <div
                  data-testid="catalog-drawer-header"
                  className="sticky top-0 z-10 flex items-center justify-between border-b border-line/80 bg-surface px-4 py-3"
                >
                  <div>
                    <p className="text-sm font-semibold text-ink">Catalog</p>
                    <p className="text-xs text-muted">Choose a model and fields</p>
                  </div>
                  <button
                    type="button"
                    aria-label="Close catalog"
                    onClick={() => setRailOpen(false)}
                    className="grid size-10 place-items-center rounded-full bg-surface-soft text-muted hover:bg-line hover:text-ink"
                  >
                    <span aria-hidden="true" className="text-xl leading-none">×</span>
                  </button>
                </div>
              ) : null}
              {rail}
            </aside>
          </>
        ) : null}
        <main className="relative min-h-0 min-w-0 overflow-hidden">
          <div className={`h-full overflow-y-auto ${drawer ? "pb-8" : ""}`}>{children}</div>
          {drawer}
        </main>
      </div>
    </div>
  );
}
