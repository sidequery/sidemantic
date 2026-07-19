import type {
  Catalog,
  CatalogDimension,
  CatalogMetric,
  CatalogModel,
  DashboardSpec,
  Grain,
} from "../data/types";
import { graphMetricsForModel } from "./catalog";

const GRAINS = new Set<Grain>(["hour", "day", "week", "month", "quarter", "year"]);

export type DashboardTabConfig = {
  id: string;
  label: string;
  title: string;
  model: CatalogModel;
  metrics: CatalogMetric[];
  dimensions: CatalogDimension[];
  selectedMetric: string;
  grain: Grain;
  filters: string[];
  segments: string[];
  usePreaggregations?: boolean;
};

function dimensionForRef(model: CatalogModel, ref: string): CatalogDimension | undefined {
  return model.dimensions.find((dimension) => ref === dimension.ref || ref.startsWith(`${dimension.ref}__`));
}

function grainForRef(ref: string, fallback: Grain): Grain {
  const candidate = ref.split("__").at(-1) as Grain | undefined;
  return candidate && GRAINS.has(candidate) ? candidate : fallback;
}

export function dashboardTabConfig(
  catalog: Catalog,
  dashboard: DashboardSpec | null | undefined,
  tabId?: string,
): DashboardTabConfig | null {
  if (!dashboard?.tabs.length) return null;
  const tab = dashboard.tabs.find((candidate) => candidate.id === tabId) ?? dashboard.tabs[0];
  const chart = tab.charts[0];
  if (!chart) return null;

  const dimensionRefs = chart.query.dimensions ?? [];
  const metricRefs = chart.query.metrics ?? [];
  const graphMetricOwner = metricRefs
    .map((ref) => catalog.graphMetrics.find((metric) => metric.ref === ref)?.ownerModel)
    .find((owner): owner is string => Boolean(owner));
  const modelName =
    dimensionRefs.find((ref) => ref.includes("."))?.split(".")[0] ??
    metricRefs.find((ref) => ref.includes("."))?.split(".")[0] ??
    graphMetricOwner;
  const model = catalog.models.find((candidate) => candidate.name === modelName) ?? catalog.models[0];
  if (!model) return null;

  const availableMetrics = [
    ...catalog.models.flatMap((candidate) => candidate.metrics),
    ...graphMetricsForModel(catalog, model.name),
  ];
  const metrics = metricRefs
    .map((ref) => availableMetrics.find((metric) => metric.ref === ref))
    .filter((metric): metric is CatalogMetric => Boolean(metric));
  const dimensions = dimensionRefs
    .map((ref) => dimensionForRef(model, ref))
    .filter((dimension, index, all): dimension is CatalogDimension =>
      Boolean(dimension) && all.findIndex((candidate) => candidate?.ref === dimension?.ref) === index,
    );
  const encodedY = chart.encoding?.y;
  const encodedMetrics = Array.isArray(encodedY) ? encodedY : encodedY ? [encodedY] : [];
  const selectedMetric =
    encodedMetrics.find((ref) => metrics.some((metric) => metric.ref === ref)) ??
    metrics[0]?.ref ??
    model.metrics[0]?.ref ??
    "";
  const encodedTime = chart.encoding?.x;
  const timeRef =
    (encodedTime && dimensionForRef(model, encodedTime)?.type === "time" ? encodedTime : undefined) ??
    dimensionRefs.find((ref) => dimensionForRef(model, ref)?.type === "time");
  const fallbackGrain = (model.defaultGrain as Grain | undefined) ?? "month";

  return {
    id: tab.id,
    label: tab.label ?? tab.id,
    title: chart.title ?? dashboard.title,
    model,
    metrics: metrics.length ? metrics : model.metrics,
    dimensions: dimensions.length ? dimensions : model.dimensions,
    selectedMetric,
    grain: timeRef ? grainForRef(timeRef, fallbackGrain) : fallbackGrain,
    filters: chart.query.filters ?? [],
    segments: chart.query.segments ?? [],
    usePreaggregations: chart.query.use_preaggregations ?? chart.query.usePreaggregations,
  };
}
