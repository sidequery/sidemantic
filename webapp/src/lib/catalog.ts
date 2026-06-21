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

type DescribeResponse = { models?: DescribeModel[]; metrics?: DescribeMetric[]; dialect?: string };

type GraphModel = {
  name: string;
  table?: string;
  dimensions?: string[];
  metrics?: string[];
};

type GraphResponse = { models?: GraphModel[]; graph_metrics?: { name: string; type?: string; description?: string }[] };

// Heuristic time-dimension detection for the names-only /graph fallback.
const TIME_NAME = /(^|_)(date|time|day|month|week|year|quarter|created|updated|timestamp|_at|_on)($|_)/i;

function pickTimeDimension(dims: CatalogDimension[], defaultName?: string): CatalogDimension | undefined {
  if (defaultName) {
    const match = dims.find((d) => d.name === defaultName);
    if (match) return match;
  }
  return dims.find((d) => d.type === "time") ?? dims.find((d) => TIME_NAME.test(d.name));
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
    label: labelize(metric.name),
    type: metric.type,
    description: metric.description,
  }));

  return { models, graphMetrics };
}
