import type {
  Catalog,
  CatalogDimension,
  CatalogMetric,
  CatalogModel,
  DashboardSpec,
  Grain,
} from "../data/types";
import { graphMetricsForModel } from "./catalog";

const GRAINS = new Set<Grain>(["second", "minute", "hour", "day", "week", "month", "quarter", "year"]);

function asList<T>(value: T | T[] | undefined): T[] {
  if (value === undefined) return [];
  return Array.isArray(value) ? value : [value];
}

export type DashboardTabConfig = {
  id: string;
  label: string;
  title: string;
  model: CatalogModel;
  metrics: CatalogMetric[];
  dimensions: CatalogDimension[];
  timeDimension?: CatalogDimension;
  selectedMetric: string;
  grain: Grain;
  filters: string[];
  segments: string[];
  usePreaggregations?: boolean;
};

function dimensionForRef(catalog: Catalog, ref: string): CatalogDimension | undefined {
  return catalog.models
    .flatMap((model) => model.dimensions)
    .find((dimension) => ref === dimension.ref || ref.startsWith(`${dimension.ref}__`));
}

function grainForRef(ref: string, fallback: Grain): Grain {
  const candidate = ref.split("__").at(-1) as Grain | undefined;
  return candidate && GRAINS.has(candidate) ? candidate : fallback;
}

function defaultPreaggregations(dashboard: DashboardSpec): boolean | undefined {
  const query = dashboard.defaults?.query;
  if (!query || typeof query !== "object") return undefined;
  const defaults = query as Record<string, unknown>;
  const value = defaults.use_preaggregations ?? defaults.usePreaggregations;
  return typeof value === "boolean" ? value : undefined;
}

function interactionPreaggregations(
  dashboard: DashboardSpec,
  chart: DashboardSpec["tabs"][number]["charts"][number],
): boolean | undefined {
  const defaults = dashboard.defaults?.query;
  const defaultQuery = defaults && typeof defaults === "object" ? (defaults as Record<string, unknown>) : {};
  const value =
    chart.query.interaction_preaggregations ??
    chart.query.interactionPreaggregations ??
    chart.interaction_preaggregations ??
    chart.interactionPreaggregations ??
    defaultQuery.interaction_preaggregations ??
    defaultQuery.interactionPreaggregations;
  return typeof value === "boolean" ? value : undefined;
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

  const dimensionRefs = asList(chart.query.dimensions);
  const metricRefs = asList(chart.query.metrics);
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
    .map((ref) => dimensionForRef(catalog, ref))
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
    (encodedTime && dimensionForRef(catalog, encodedTime)?.type === "time" ? encodedTime : undefined) ??
    dimensionRefs.find((ref) => dimensionForRef(catalog, ref)?.type === "time");
  const fallbackGrain = (model.defaultGrain as Grain | undefined) ?? "month";
  const timeDimension = timeRef ? dimensionForRef(catalog, timeRef) : model.timeDimension;

  return {
    id: tab.id,
    label: tab.label ?? tab.id,
    title: chart.title ?? dashboard.title,
    model,
    metrics: metrics.length ? metrics : model.metrics,
    dimensions: dimensions.length ? dimensions : model.dimensions,
    timeDimension,
    selectedMetric,
    grain: timeRef ? grainForRef(timeRef, fallbackGrain) : fallbackGrain,
    filters: asList(chart.query.filters),
    segments: asList(chart.query.segments),
    usePreaggregations:
      interactionPreaggregations(dashboard, chart) ??
      chart.query.use_preaggregations ??
      chart.query.usePreaggregations ??
      defaultPreaggregations(dashboard),
  };
}
