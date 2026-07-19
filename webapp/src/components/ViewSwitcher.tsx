import { useRef } from "react";
import type { ViewKind } from "../state/explorerState";

const SEGMENTS: { key: ViewKind; label: string }[] = [
  { key: "explore", label: "Explore" },
  { key: "pivot", label: "Pivot" },
];

export function ViewSwitcher({ view, onChange }: { view: ViewKind; onChange: (view: ViewKind) => void }) {
  const list = useRef<HTMLDivElement>(null);

  function move(direction: -1 | 1) {
    const current = Math.max(0, SEGMENTS.findIndex((segment) => segment.key === view));
    const next = SEGMENTS[(current + direction + SEGMENTS.length) % SEGMENTS.length];
    onChange(next.key);
    window.requestAnimationFrame(() => list.current?.querySelector<HTMLElement>(`[data-view="${next.key}"]`)?.focus());
  }

  return (
    <div ref={list} role="tablist" aria-label="View" className="flex items-center rounded-full bg-surface-soft p-1">
      {SEGMENTS.map((segment) => (
        <button
          key={segment.key}
          role="tab"
          type="button"
          aria-selected={view === segment.key}
          tabIndex={view === segment.key ? 0 : -1}
          data-view={segment.key}
          data-selected={view === segment.key || undefined}
          onClick={() => onChange(segment.key)}
          onKeyDown={(event) => {
            if (event.key === "ArrowLeft") {
              event.preventDefault();
              move(-1);
            } else if (event.key === "ArrowRight") {
              event.preventDefault();
              move(1);
            }
          }}
          className="min-h-8 rounded-full px-3 text-xs font-medium text-muted transition-colors hover:text-ink data-[selected=true]:bg-surface data-[selected=true]:text-ink data-[selected=true]:shadow-sm"
        >
          {segment.label}
        </button>
      ))}
    </div>
  );
}
