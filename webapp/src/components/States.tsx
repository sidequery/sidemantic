type StateBoxProps = {
  title?: string;
  message: string;
};

/** Centered, fixed-min-height box so loading/empty/error never reflow the layout. */
function StateBox({ tone, title, message }: StateBoxProps & { tone: "muted" | "danger" | "loading" }) {
  const danger = tone === "danger";
  return (
    <div
      className={`grid min-h-[200px] place-items-center border bg-surface p-6 text-center ${
        danger ? "border-danger/40" : "border-line"
      }`}
      data-state={tone}
      role={danger ? "alert" : "status"}
      aria-live={danger ? "assertive" : "polite"}
    >
      <div className="max-w-md">
        {tone === "loading" ? <span aria-hidden="true" className="motion-safe:animate-pulse inline-block size-2 rounded-full bg-accent" /> : null}
        {title ? (
          <h3 className={`text-sm font-semibold ${danger ? "text-danger" : "text-ink"}`}>{title}</h3>
        ) : null}
        <p className={`mt-1 text-xs ${danger ? "text-danger" : "text-muted"}`}>{message}</p>
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
