import type { CatalogMetric } from "../data/types";
import { useExplorer } from "../state/ExplorerContext";

function MetricRow({ metric, selected, onSelect }: { metric: CatalogMetric; selected: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      data-catalog-metric={metric.ref}
      data-selected={selected || undefined}
      title={metric.description}
      onClick={onSelect}
      className="flex w-full items-center justify-between gap-2 border-l-2 px-3 py-1 text-left text-xs hover:bg-surface-soft data-[selected=true]:border-l-accent data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent border-l-transparent text-muted"
    >
      <span className="truncate">{metric.label}</span>
      {metric.agg ? <span className="shrink-0 text-2xs text-faint">{metric.agg}</span> : null}
    </button>
  );
}

/** Left rail: model picker + selectable metrics (ranking) + dimension reference. */
export function Catalog() {
  const { state, dispatch, catalog } = useExplorer();
  const model = catalog.models.find((m) => m.name === state.model);

  return (
    <nav className="flex flex-col gap-4 py-3 text-sm">
      <section className="px-3">
        <h2 className="mb-1 text-2xs font-semibold uppercase tracking-wide text-faint">Model</h2>
        <select
          aria-label="Model"
          value={state.model}
          onChange={(event) => {
            const next = catalog.models.find((m) => m.name === event.target.value);
            const metric = next?.metrics[0]?.ref ?? catalog.graphMetrics[0]?.ref ?? "";
            dispatch({ type: "setModel", model: event.target.value, metric, grain: (next?.defaultGrain as never) ?? "month" });
          }}
          className="w-full border border-line bg-surface px-2 py-1 text-xs text-ink"
        >
          {catalog.models.map((m) => (
            <option key={m.name} value={m.name}>
              {m.label}
            </option>
          ))}
        </select>
      </section>

      <section>
        <h2 className="mb-1 px-3 text-2xs font-semibold uppercase tracking-wide text-faint">Metrics</h2>
        <div>
          {(model?.metrics ?? []).map((metric) => (
            <MetricRow
              key={metric.ref}
              metric={metric}
              selected={state.selectedMetric === metric.ref}
              onSelect={() => dispatch({ type: "setMetric", metric: metric.ref })}
            />
          ))}
        </div>
        {catalog.graphMetrics.length ? (
          <>
            <h3 className="mb-1 mt-3 px-3 text-2xs font-semibold uppercase tracking-wide text-faint">Shared metrics</h3>
            <div>
              {catalog.graphMetrics.map((metric) => (
                <MetricRow
                  key={metric.ref}
                  metric={metric}
                  selected={state.selectedMetric === metric.ref}
                  onSelect={() => dispatch({ type: "setMetric", metric: metric.ref })}
                />
              ))}
            </div>
          </>
        ) : null}
      </section>

      <section>
        <h2 className="mb-1 px-3 text-2xs font-semibold uppercase tracking-wide text-faint">Dimensions</h2>
        <ul className="px-3 text-xs text-muted">
          {(model?.dimensions ?? []).map((dim) => (
            <li key={dim.ref} className="flex items-center justify-between gap-2 py-0.5" title={dim.description}>
              <span className="truncate">{dim.label}</span>
              <span className="shrink-0 text-2xs text-faint">{dim.type === "time" ? "time" : ""}</span>
            </li>
          ))}
        </ul>
      </section>
    </nav>
  );
}
