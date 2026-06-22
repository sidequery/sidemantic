import type { ViewKind } from "../state/explorerState";

const SEGMENTS: { key: ViewKind; label: string }[] = [
  { key: "explore", label: "Explore" },
  { key: "pivot", label: "Pivot" },
];

export function ViewSwitcher({ view, onChange }: { view: ViewKind; onChange: (view: ViewKind) => void }) {
  return (
    <div role="tablist" aria-label="View" className="flex items-center border border-line bg-surface">
      {SEGMENTS.map((segment) => (
        <button
          key={segment.key}
          role="tab"
          type="button"
          aria-selected={view === segment.key}
          data-view={segment.key}
          data-selected={view === segment.key || undefined}
          onClick={() => onChange(segment.key)}
          className="border-r border-line px-2.5 py-1 text-2xs font-medium text-muted last:border-r-0 hover:bg-surface-soft data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent"
        >
          {segment.label}
        </button>
      ))}
    </div>
  );
}
