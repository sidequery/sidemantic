import type { Catalog, CatalogDimension, CatalogMetric, CatalogModel } from "../data/types";
import { labelize } from "./format";

// ---- Raw payload shapes ----

type DescribeDimension = {
  name: string;
  type?: string;
  granularity?: string;
  supported_granularities?: string[];
  label?: string;
  format?: string;
  description?: string;
  public?: boolean;
};

type DescribeMetric = {
  name: string;
  agg?: string;
  type?: string;
  label?: string;
  format?: string;
  description?: string;
  public?: boolean;
  sql?: string;
  base_metric?: string;
  numerator?: string;
  denominator?: string;
};

// Fields marked `public: false` (e.g. imported from Cube/LookML/TMDL) are hidden from the UI.
const isPublic = (field: { public?: boolean }) => field.public !== false;

type DescribeModel = {
  name: string;
  table?: string;
  description?: string;
  default_time_dimension?: string;
  default_grain?: string;
  dimensions?: DescribeDimension[];
  metrics?: DescribeMetric[];
};

type JoinablePair = { from: string; to: string; hops?: number };

type DescribeResponse = { models?: DescribeModel[]; metrics?: DescribeMetric[]; dialect?: string };

type GraphModel = {
  name: string;
  table?: string;
  dimensions?: string[];
  metrics?: string[];
};

type GraphMetric = {
  name: string;
  type?: string;
  description?: string;
  sql?: string;
  base_metric?: string;
  numerator?: string;
  denominator?: string;
};

type GraphResponse = { models?: GraphModel[]; graph_metrics?: GraphMetric[]; joinable_pairs?: JoinablePair[] };

// Heuristic time-dimension detection for the names-only /graph fallback.
const TIME_NAME = /(^|_)(date|time|day|month|week|year|quarter|created|updated|timestamp|_at|_on)($|_)/i;

function pickTimeDimension(dims: CatalogDimension[], defaultName?: string): CatalogDimension | undefined {
  if (defaultName) {
    const match = dims.find((d) => d.name === defaultName);
    if (match) return match;
  }
  return dims.find((d) => d.type === "time") ?? dims.find((d) => TIME_NAME.test(d.name));
}

function ownersFromExpression(expression: string | undefined, models: CatalogModel[]): Set<string> {
  const owners = new Set<string>();
  if (!expression) return owners;
  const modelNames = new Set(models.map((model) => model.name));
  const dottedRefs = expression.match(/[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*/g) ?? [];
  for (const ref of dottedRefs) {
    const [modelName] = ref.split(".");
    if (modelNames.has(modelName)) owners.add(modelName);
  }
  if (owners.size) return owners;

  const bareRef = expression.match(/^[A-Za-z_][A-Za-z0-9_]*$/)?.[0];
  if (!bareRef) return owners;
  for (const model of models) {
    if (model.metrics.some((metric) => metric.name === bareRef)) owners.add(model.name);
  }
  return owners;
}

function ownerModelForMetric(
  metric: Pick<DescribeMetric, "base_metric" | "sql" | "numerator" | "denominator">,
  models: CatalogModel[],
): string | undefined {
  const owners = new Set<string>();
  for (const expression of [metric.base_metric, metric.numerator, metric.denominator, metric.sql]) {
    for (const owner of ownersFromExpression(expression, models)) owners.add(owner);
  }
  return owners.size === 1 ? [...owners][0] : undefined;
}

export function buildCatalogFromDescribe(payload: DescribeResponse): Catalog {
  const models: CatalogModel[] = (payload.models ?? []).map((model) => {
    const dimensions: CatalogDimension[] = (model.dimensions ?? []).filter(isPublic).map((dim) => ({
      ref: `${model.name}.${dim.name}`,
      name: dim.name,
      model: model.name,
      label: dim.label || labelize(dim.name),
      description: dim.description,
      type: dim.type || "categorical",
      granularity: dim.granularity,
      supportedGranularities: dim.supported_granularities,
      format: dim.format,
    }));
    const metrics: CatalogMetric[] = (model.metrics ?? []).filter(isPublic).map((metric) => ({
      ref: `${model.name}.${metric.name}`,
      name: metric.name,
      model: model.name,
      label: metric.label || labelize(metric.name),
      description: metric.description,
      agg: metric.agg,
      type: metric.type,
      format: metric.format,
    }));
    const timeDimension = pickTimeDimension(dimensions, model.default_time_dimension);
    return {
      name: model.name,
      label: labelize(model.name),
      description: model.description,
      table: model.table,
      dimensions,
      metrics,
      timeDimension,
      defaultGrain: model.default_grain || timeDimension?.granularity || "day",
    };
  });

  const graphMetrics: CatalogMetric[] = (payload.metrics ?? []).filter(isPublic).map((metric) => ({
    ref: metric.name,
    name: metric.name,
    ownerModel: ownerModelForMetric(metric, models),
    baseMetric: metric.base_metric,
    label: metric.label || labelize(metric.name),
    description: metric.description,
    type: metric.type,
    format: metric.format,
  }));

  return { models, graphMetrics, dialect: payload.dialect };
}

export function buildCatalogFromGraph(raw: unknown): Catalog {
  const payload = (raw ?? {}) as GraphResponse;
  const models: CatalogModel[] = (payload.models ?? []).map((model) => {
    const dimensions: CatalogDimension[] = (model.dimensions ?? []).map((name) => ({
      ref: `${model.name}.${name}`,
      name,
      model: model.name,
      label: labelize(name),
      type: TIME_NAME.test(name) ? "time" : "categorical",
    }));
    const metrics: CatalogMetric[] = (model.metrics ?? []).map((name) => ({
      ref: `${model.name}.${name}`,
      name,
      model: model.name,
      label: labelize(name),
    }));
    const timeDimension = pickTimeDimension(dimensions);
    return {
      name: model.name,
      label: labelize(model.name),
      table: model.table,
      dimensions,
      metrics,
      timeDimension,
      defaultGrain: timeDimension?.granularity || "day",
    };
  });

  const graphMetrics: CatalogMetric[] = (payload.graph_metrics ?? []).map((metric) => ({
    ref: metric.name,
    name: metric.name,
    ownerModel: ownerModelForMetric(metric, models),
    baseMetric: metric.base_metric,
    label: labelize(metric.name),
    type: metric.type,
    description: metric.description,
  }));

  return { models, graphMetrics, joinablePairs: payload.joinable_pairs ?? [] };
}

export function withJoinablePairs(catalog: Catalog, raw: unknown): Catalog {
  const payload = (raw ?? {}) as Pick<GraphResponse, "joinable_pairs">;
  return { ...catalog, joinablePairs: payload.joinable_pairs ?? catalog.joinablePairs ?? [] };
}

export function graphMetricsForModel(catalog: Catalog, modelName: string): CatalogMetric[] {
  const joinable = new Set<string>();
  for (const pair of catalog.joinablePairs ?? []) {
    if (pair.from === modelName) joinable.add(pair.to);
    if (pair.to === modelName) joinable.add(pair.from);
  }
  return catalog.graphMetrics.filter((metric) => {
    if (!metric.ownerModel) return true;
    return metric.ownerModel === modelName || joinable.has(metric.ownerModel);
  });
}
