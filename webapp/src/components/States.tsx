type StateBoxProps = {
  title?: string;
  message: string;
};

/** Centered, fixed-min-height box so loading/empty/error never reflow the layout. */
function StateBox({ tone, title, message }: StateBoxProps & { tone: "muted" | "danger" | "loading" }) {
  const danger = tone === "danger";
  return (
    <div
      className={`grid min-h-[220px] place-items-center rounded-2xl bg-surface p-8 text-center shadow-sm ${danger ? "ring-1 ring-danger/30" : ""}`}
      data-state={tone}
      role={danger ? "alert" : "status"}
      aria-live={danger ? "assertive" : "polite"}
    >
      <div className="max-w-md">
        <span
          aria-hidden="true"
          className={`mx-auto mb-4 grid size-10 place-items-center rounded-full ${danger ? "bg-danger-soft text-danger" : "bg-surface-soft text-muted"}`}
        >
          {tone === "loading" ? <span className="spinner size-4 rounded-full border-2 border-line border-t-accent" /> : danger ? "!" : "—"}
        </span>
        {title ? (
          <h3 className={`text-base font-semibold tracking-[-0.01em] ${danger ? "text-danger" : "text-ink"}`}>{title}</h3>
        ) : null}
        <p className={`mt-1.5 text-sm ${danger ? "text-danger" : "text-muted"}`}>{message}</p>
      </div>
    </div>
  );
}

export function LoadingState({ title = "Loading", message = "Loading metrics…" }: Partial<StateBoxProps>) {
  return <StateBox tone="loading" title={title} message={message} />;
}

export function EmptyState({ title = "No results", message }: StateBoxProps) {
  return <StateBox tone="muted" title={title} message={message} />;
}

export function ErrorState({ title = "Query failed", message }: StateBoxProps) {
  return <StateBox tone="danger" title={title} message={message} />;
}

/** Status dot for the top bar: idle / ok / loading. */
export function StatusDot({ status }: { status: "idle" | "ok" | "loading" }) {
  const color = status === "ok" ? "bg-accent" : status === "loading" ? "bg-faint animate-pulse" : "bg-line";
  return <span aria-hidden="true" className={`inline-block size-2 rounded-full ${color}`} />;
}
