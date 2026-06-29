import { useState, type ReactNode } from "react";

type AppShellProps = {
  brand: ReactNode;
  toolbar: ReactNode;
  filters?: ReactNode;
  rail: ReactNode;
  children: ReactNode;
};

/** Collapsible rail | stage frame with a top toolbar and an active-filter strip. */
export function AppShell({ brand, toolbar, filters, rail, children }: AppShellProps) {
  const [railOpen, setRailOpen] = useState(true);

  return (
    <div className="flex h-screen flex-col bg-bg text-ink">
      <header className="flex items-center justify-between gap-3 border-b border-line bg-surface px-3 py-2">
        <div className="flex min-w-0 items-center gap-2">
          <button
            type="button"
            aria-label={railOpen ? "Collapse sidebar" : "Expand sidebar"}
            aria-expanded={railOpen}
            onClick={() => setRailOpen((open) => !open)}
            className="grid size-7 shrink-0 place-items-center border border-line text-faint hover:border-faint hover:text-ink"
          >
            {railOpen ? "‹" : "›"}
          </button>
          {brand}
        </div>
        <div className="flex shrink-0 items-center gap-2">{toolbar}</div>
      </header>

      {filters ? (
        // Fixed height so the bar doesn't jump when pills replace the "No filters" text.
        <div className="flex h-9 shrink-0 items-center gap-2 overflow-x-auto border-b border-line bg-surface px-3">
          {filters}
        </div>
      ) : null}

      <div className={`grid min-h-0 flex-1 ${railOpen ? "grid-cols-[260px_1fr]" : "grid-cols-[0_1fr]"}`}>
        <aside className={`min-h-0 overflow-y-auto border-r border-line bg-surface ${railOpen ? "" : "hidden"}`}>
          {rail}
        </aside>
        <main className="min-h-0 overflow-y-auto">{children}</main>
      </div>
    </div>
  );
}
