import type { CatalogDimension, CatalogMetric } from "../data/types";
import { graphMetricsForModel } from "../lib/catalog";
import { useExplorer } from "../state/ExplorerContext";

function toggle(list: string[], ref: string) {
  return list.includes(ref) ? list.filter((item) => item !== ref) : [...list, ref];
}

function MetricRow({ metric, selected, multiple, onSelect }: { metric: CatalogMetric; selected: boolean; multiple?: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      data-catalog-metric={metric.ref}
      data-selected={selected || undefined}
      title={metric.description}
      onClick={onSelect}
      className="flex w-full items-center justify-between gap-2 border-l-2 px-3 py-1 text-left text-xs hover:bg-surface-soft data-[selected=true]:border-l-accent data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent border-l-transparent text-muted"
    >
      <span className="flex min-w-0 items-center gap-2">
        {multiple ? <span aria-hidden="true" className="w-3 shrink-0 text-center text-accent">{selected ? "✓" : ""}</span> : null}
        <span className="truncate">{metric.label}</span>
      </span>
      {metric.agg ? <span className="shrink-0 text-2xs text-faint">{metric.agg}</span> : null}
    </button>
  );
}

function DimensionRow({ dimension, selected, onSelect }: { dimension: CatalogDimension; selected: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      data-catalog-dimension={dimension.ref}
      data-selected={selected || undefined}
      aria-pressed={selected}
      title={dimension.description}
      onClick={onSelect}
      className="flex w-full items-center justify-between gap-2 border-l-2 border-l-transparent px-3 py-1 text-left text-xs text-muted hover:bg-surface-soft data-[selected=true]:border-l-accent data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent"
    >
      <span className="flex min-w-0 items-center gap-2"><span aria-hidden="true" className="w-3 shrink-0 text-center">{selected ? "✓" : ""}</span><span className="truncate">{dimension.label}</span></span>
      <span className="shrink-0 text-2xs text-faint">{dimension.type === "time" ? "time" : ""}</span>
    </button>
  );
}

/** Left rail: model picker + selectable metrics (ranking) + dimension reference. */
export function Catalog() {
  const { state, dispatch, catalog } = useExplorer();
  const model = catalog.models.find((m) => m.name === state.model);
  const graphMetrics = graphMetricsForModel(catalog, state.model);
  const pivot = state.view === "pivot";
  const pivotMetrics = state.pivotMetrics.length ? state.pivotMetrics : state.selectedMetric ? [state.selectedMetric] : [];

  return (
    <nav className="flex flex-col gap-4 py-3 text-sm">
      <section className="px-3">
        <h2 className="mb-1 text-xs font-medium text-muted">Model</h2>
        <select
          aria-label="Model"
          value={state.model}
          onChange={(event) => {
            const next = catalog.models.find((m) => m.name === event.target.value);
            const metric = next?.metrics[0]?.ref ?? graphMetricsForModel(catalog, event.target.value)[0]?.ref ?? "";
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
        <h2 className="mb-1 px-3 text-xs font-medium text-muted">Metrics</h2>
        <div>
          {(model?.metrics ?? []).map((metric) => (
            <MetricRow
              key={metric.ref}
              metric={metric}
              selected={pivot ? pivotMetrics.includes(metric.ref) : state.selectedMetric === metric.ref}
              multiple={pivot}
              onSelect={() => pivot
                ? dispatch({ type: "setPivotMetrics", metrics: toggle(pivotMetrics, metric.ref) })
                : dispatch({ type: "setMetric", metric: metric.ref })}
            />
          ))}
        </div>
        {graphMetrics.length ? (
          <>
            <h3 className="mb-1 mt-3 px-3 text-xs font-medium text-muted">Shared metrics</h3>
            <div>
              {graphMetrics.map((metric) => (
                <MetricRow
                  key={metric.ref}
                  metric={metric}
                  selected={pivot ? pivotMetrics.includes(metric.ref) : state.selectedMetric === metric.ref}
                  multiple={pivot}
                  onSelect={() => pivot
                    ? dispatch({ type: "setPivotMetrics", metrics: toggle(pivotMetrics, metric.ref) })
                    : dispatch({ type: "setMetric", metric: metric.ref })}
                />
              ))}
            </div>
          </>
        ) : null}
      </section>

      <section>
        <h2 className="mb-1 px-3 text-xs font-medium text-muted">Dimensions</h2>
        {pivot ? (
          <div>
            {(model?.dimensions ?? []).map((dimension) => (
              <DimensionRow
                key={dimension.ref}
                dimension={dimension}
                selected={state.pivotDims.includes(dimension.ref)}
                onSelect={() => dispatch({ type: "setPivotDims", dims: toggle(state.pivotDims, dimension.ref) })}
              />
            ))}
          </div>
        ) : (
          <ul className="px-3 text-xs text-muted">
            {(model?.dimensions ?? []).map((dim) => (
              <li key={dim.ref} className="flex items-center justify-between gap-2 py-0.5" title={dim.description}>
                <span className="truncate">{dim.label}</span>
                <span className="shrink-0 text-2xs text-faint">{dim.type === "time" ? "time" : ""}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </nav>
  );
}
