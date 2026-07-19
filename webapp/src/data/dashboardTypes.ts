import type { FieldRef } from "./types";

// Declarative dashboard contract returned by GET /dashboard. Both snake_case (YAML/Python) and
// camelCase (generated TypeScript authoring) spellings are accepted at the HTTP boundary.
export type DashboardDocument = {
  schema?: "sidemantic.dashboard.v1";
  title: string;
  defaults?: {
    query?: {
      use_preaggregations?: boolean;
      usePreaggregations?: boolean;
    };
  };
  tabs: DashboardTab[];
};

export type DashboardTab = {
  id: string;
  label?: string;
  charts: DashboardChart[];
};

export type DashboardChart = {
  id: string;
  title?: string;
  type?: "auto" | "bar" | "line" | "area";
  query: {
    metrics: FieldRef[];
    dimensions?: FieldRef[];
    filters?: string[];
    segments?: string[];
    order_by?: string[];
    orderBy?: string[];
    limit?: number;
    use_preaggregations?: boolean;
    usePreaggregations?: boolean;
  };
  encoding?: {
    x?: FieldRef;
    y?: FieldRef | FieldRef[];
    color?: FieldRef;
    size?: FieldRef;
    facet?: FieldRef;
  };
  interactions?: {
    crossfilter?: boolean;
    brush?: boolean | { fields?: FieldRef[]; channel?: "x" | "y" | "xy" };
    select?: boolean | { fields?: FieldRef[] };
  };
};
