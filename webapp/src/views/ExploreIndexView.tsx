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
    <div className="mx-auto w-full max-w-6xl px-5 py-10 sm:px-8 sm:py-14">
      <div className="mb-8 max-w-2xl">
        <p className="mb-2 text-xs font-semibold uppercase tracking-[0.12em] text-accent">Semantic layer</p>
        <h1 className="text-3xl font-semibold tracking-[-0.03em] text-ink sm:text-4xl">Explore your data</h1>
        <p className="mt-2 text-base text-muted">Choose from {plural(catalog.models.length, "model")} to inspect metrics, trends, and dimensions.</p>
      </div>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {catalog.models.map((model) => (
          <button
            key={model.name}
            type="button"
            data-testid="explore-card"
            data-model={model.name}
            onClick={() => open(model.name)}
            className="group flex min-h-48 flex-col gap-3 rounded-2xl bg-surface p-5 text-left shadow-sm transition-[transform,box-shadow] duration-200 ease-out hover:-translate-y-0.5 hover:shadow-floating"
          >
            <div className="flex items-baseline justify-between gap-2">
              <span className="truncate text-lg font-semibold tracking-[-0.015em] text-ink">{model.label}</span>
              {model.timeDimension ? <span className="shrink-0 rounded-full bg-accent-soft px-2.5 py-1 text-2xs font-medium text-accent">Time series</span> : null}
            </div>
            {model.description ? <p className="line-clamp-2 text-sm leading-relaxed text-muted">{model.description}</p> : null}
            <div className="mt-auto flex flex-wrap gap-1.5 pt-2">
              {model.metrics.slice(0, 3).map((metric) => (
                <span key={metric.ref} className="rounded-full bg-surface-soft px-2.5 py-1 text-xs text-muted">
                  {metric.label}
                </span>
              ))}
            </div>
            <div className="flex items-center justify-between border-t border-line/70 pt-3 text-xs text-faint">
              <span>
              {plural(model.metrics.length, "metric")} · {plural(model.dimensions.length, "dimension")}
              </span>
              <span aria-hidden="true" className="text-base text-muted transition-transform group-hover:translate-x-1">→</span>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
