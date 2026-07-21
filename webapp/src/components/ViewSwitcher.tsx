import type { ViewKind } from "../state/explorerState";

const SEGMENTS: { key: ViewKind; label: string }[] = [
  { key: "explore", label: "Explore" },
  { key: "pivot", label: "Pivot" },
];

export function ViewSwitcher({ view, onChange }: { view: ViewKind; onChange: (view: ViewKind) => void }) {
  return (
    <div role="tablist" aria-label="View" className="inline-flex items-center gap-0.5 rounded-full border border-line bg-surface p-px">
      {SEGMENTS.map((segment) => (
        <button
          key={segment.key}
          role="tab"
          type="button"
          aria-selected={view === segment.key}
          data-view={segment.key}
          data-selected={view === segment.key || undefined}
          onClick={() => onChange(segment.key)}
          className="inline-flex h-6 items-center rounded-full px-2.5 text-xs font-medium text-muted hover:bg-surface-soft hover:text-ink data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent"
        >
          {segment.label}
        </button>
      ))}
    </div>
  );
}
