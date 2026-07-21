import type { Grain } from "../data/types";
import { useExplorer } from "../state/ExplorerContext";
import { defaultMetric } from "../state/explorerState";

function plural(count: number, noun: string): string {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

/** Home/landing: a card per model in the catalog. Picking one enters its Explore view. */
export function ExploreIndexView() {
  const { catalog, dispatch } = useExplorer();

  const open = (modelName: string) => {
    const model = catalog.models.find((m) => m.name === modelName);
    const metric = defaultMetric(model, catalog);
    const grain = (model?.defaultGrain as Grain) ?? "month";
    dispatch({ type: "setModel", model: modelName, metric, grain });
    dispatch({ type: "setView", view: "explore" });
  };

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-4">
        <h1 className="text-base font-semibold text-ink">Explore</h1>
        <p className="text-2xs text-muted">{plural(catalog.models.length, "model")} · pick one to explore</p>
      </div>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {catalog.models.map((model) => (
          <button
            key={model.name}
            type="button"
            data-testid="explore-card"
            data-model={model.name}
            onClick={() => open(model.name)}
            className="flex min-h-32 flex-col gap-2 rounded-xl border border-line bg-surface p-4 text-left shadow-[var(--shadow-sm)] transition-colors hover:border-line-strong"
          >
            <div className="flex items-baseline justify-between gap-2">
              <span className="truncate text-sm font-medium text-ink">{model.label}</span>
              {model.timeDimension ? <span className="shrink-0 text-2xs text-faint">time series</span> : null}
            </div>
            {model.description ? <p className="line-clamp-2 text-2xs text-muted">{model.description}</p> : null}
            <div className="mt-auto flex flex-wrap gap-1 pt-1">
              {model.metrics.slice(0, 3).map((metric) => (
                <span key={metric.ref} className="rounded-full bg-surface-soft px-2 py-0.5 text-2xs text-muted">
                  {metric.label}
                </span>
              ))}
            </div>
            <div className="text-2xs text-faint">
              {plural(model.metrics.length, "metric")} · {plural(model.dimensions.length, "dimension")}
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
