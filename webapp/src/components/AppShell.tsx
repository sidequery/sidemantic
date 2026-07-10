import { useState, type ReactNode } from "react";

type AppShellProps = {
  brand: ReactNode;
  toolbar: ReactNode;
  filters?: ReactNode;
  rail: ReactNode;
  children: ReactNode;
  /** Whether to show the left catalog rail (and its collapse toggle). The home/index view hides it. */
  showRail?: boolean;
};

/** Collapsible rail | stage frame with a top toolbar and an active-filter strip. */
export function AppShell({ brand, toolbar, filters, rail, children, showRail = true }: AppShellProps) {
  const [railOpen, setRailOpen] = useState(true);
  const railVisible = showRail && railOpen;

  return (
    <div className="flex h-screen min-w-0 flex-col overflow-hidden bg-bg text-ink">
      <header className="flex shrink-0 flex-col gap-2 border-b border-line bg-surface px-3 py-2 md:flex-row md:items-center md:justify-between md:gap-3">
        <div className="flex min-w-0 items-center gap-2">
          {showRail ? (
            <button
              type="button"
              aria-label={railOpen ? "Collapse sidebar" : "Expand sidebar"}
              aria-expanded={railOpen}
              onClick={() => setRailOpen((open) => !open)}
              className="grid size-7 shrink-0 place-items-center border border-line text-faint hover:border-faint hover:text-ink"
            >
              {railOpen ? "‹" : "›"}
            </button>
          ) : null}
          {brand}
        </div>
        <div className="flex w-full min-w-0 flex-wrap items-center gap-2 overflow-x-auto md:w-auto md:shrink-0 md:flex-nowrap">
          {toolbar}
        </div>
      </header>

      {filters ? (
        // Fixed height so the bar doesn't jump when pills replace the "No filters" text.
        <div className="flex min-h-9 shrink-0 flex-wrap items-center gap-2 overflow-x-auto border-b border-line bg-surface px-3 py-2 md:h-9 md:flex-nowrap md:py-0">
          {filters}
        </div>
      ) : null}

      <div className={`grid min-h-0 min-w-0 flex-1 grid-cols-1 ${railVisible ? "md:grid-cols-[260px_minmax(0,1fr)]" : "md:grid-cols-1"}`}>
        {showRail ? (
          <aside className={`max-h-64 min-h-0 overflow-y-auto border-b border-line bg-surface md:max-h-none md:border-b-0 md:border-r ${railVisible ? "" : "hidden"}`}>
            {rail}
          </aside>
        ) : null}
        <main className="min-h-0 min-w-0 overflow-y-auto">{children}</main>
      </div>
    </div>
  );
}
