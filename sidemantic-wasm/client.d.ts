/**
 * Typed client runtime for Sidemantic.
 *
 * Pair this with a generated schema module (`sidemantic gen types`, or the
 * JS generator) and one of the transports under `sidemantic-wasm/adapters/*`
 * (or your own `run` executor). The schema is consumed purely at the type
 * level: `client.query(...)` autocompletes your metrics/dimensions and the
 * returned rows are typed from the field types in the generated schema.
 */

export type ScalarTag = "string" | "number" | "boolean";

type TsOf<T> = T extends "string" ? string : T extends "number" ? number : T extends "boolean" ? boolean : never;

export interface DimensionDef {
  readonly kind: string;
  readonly ts: ScalarTag;
  /** Time dimensions list their selectable grains; `name__grain` refs are derived from these. */
  readonly grains?: readonly string[];
}

export interface MetricDef {
  readonly agg: string | null;
  readonly ts: ScalarTag;
}

export interface ModelDef {
  readonly dimensions: Readonly<Record<string, DimensionDef>>;
  readonly metrics: Readonly<Record<string, MetricDef>>;
}

export interface SchemaShape {
  readonly models: Readonly<Record<string, ModelDef>>;
  readonly topMetrics: readonly string[];
}

type ModelName<S extends SchemaShape> = keyof S["models"] & string;

/** Every valid `model.metric` reference, plus bare top-level metric names. */
export type MetricRef<S extends SchemaShape> =
  | {
      [M in ModelName<S>]: `${M}.${keyof S["models"][M]["metrics"] & string}`;
    }[ModelName<S>]
  | S["topMetrics"][number];

/** Every valid `model.dimension` reference, including `__grain` suffixes for time dims. */
export type DimRef<S extends SchemaShape> = {
  [M in ModelName<S>]: {
    [D in keyof S["models"][M]["dimensions"] & string]: S["models"][M]["dimensions"][D] extends {
      grains: readonly (infer G extends string)[];
    }
      ? `${M}.${D}` | `${M}.${D}__${G}`
      : `${M}.${D}`;
  }[keyof S["models"][M]["dimensions"] & string];
}[ModelName<S>];

export interface SidemanticQuery<S extends SchemaShape> {
  readonly metrics: readonly MetricRef<S>[];
  readonly dimensions?: readonly DimRef<S>[];
  /** Raw SQL filter expressions, e.g. `"orders.status = 'completed'"`. Not type-checked. */
  readonly filters?: readonly string[];
  readonly order_by?: readonly string[];
  readonly limit?: number;
  readonly ungrouped?: boolean;
}

type Leaf<R extends string> = R extends `${string}.${infer Rest}` ? Rest : R;

type DimCell<S extends SchemaShape, M extends string, D extends string> = M extends ModelName<S>
  ? D extends keyof S["models"][M]["dimensions"]
    ? TsOf<S["models"][M]["dimensions"][D]["ts"]>
    : never
  : never;

type CellOf<S extends SchemaShape, R extends string> = R extends `${infer M}.${infer D}__${string}`
  ? DimCell<S, M, D>
  : R extends `${infer M}.${infer N}`
    ? M extends ModelName<S>
      ? N extends keyof S["models"][M]["metrics"]
        ? TsOf<S["models"][M]["metrics"][N]["ts"]>
        : N extends keyof S["models"][M]["dimensions"]
          ? TsOf<S["models"][M]["dimensions"][N]["ts"]>
          : never
      : never
    : number;

/**
 * Row shape inferred from a query: output columns are aliased to the bare last
 * segment (`orders.total_revenue` -> `total_revenue`). NOTE: when two selected
 * models share a leaf name the engine renames to `{model}_{leaf}`; disambiguate
 * those queries with explicit aliases.
 */
// Extract the selected dimension union only when `dimensions` is present, so a metrics-only
// query (with no `dimensions` key) still types its metric columns instead of collapsing.
type SelectedDimensions<Q> = Q extends { dimensions: readonly (infer D)[] } ? D : never;

export type Row<S extends SchemaShape, Q extends SidemanticQuery<S>> = {
  [R in (Q["metrics"][number] | SelectedDimensions<Q>) as Leaf<R & string>]: CellOf<S, R & string>;
};

/** Plain query payload handed to a transport's `run`. */
export interface QueryPayload {
  metrics: readonly string[];
  dimensions?: readonly string[];
  filters?: readonly string[];
  order_by?: readonly string[];
  limit?: number;
  ungrouped?: boolean;
}

export type RunFn = (query: QueryPayload) => Promise<Record<string, unknown>[]>;

export interface CreateClientOptions {
  run: RunFn;
  /** Validate metric/dimension refs against the schema before calling `run` (default true). */
  validate?: boolean;
}

export interface SidemanticClient<S extends SchemaShape> {
  readonly schema: S;
  // `const Q` preserves the metric/dimension array literals so result rows are typed
  // from exactly the selected fields (no `as const` needed at the call site).
  query<const Q extends SidemanticQuery<S>>(query: Q): Promise<Array<Row<S, Q>>>;
}

export function createClient<S extends SchemaShape>(schema: S, options: CreateClientOptions): SidemanticClient<S>;

// ---------------------------------------------------------------------------
// sqlx-style typed semantic SQL
// ---------------------------------------------------------------------------

export interface QueryTypeDef {
  row: Record<string, unknown>;
  params: Record<string, unknown>;
}

/** Map of exact semantic-SQL string -> its generated row/params types. */
export type QueryMap = Record<string, QueryTypeDef>;

export type SqlRunFn = (sql: string, params?: Record<string, unknown>) => Promise<Record<string, unknown>[]>;

export interface CreateSqlClientOptions {
  run: SqlRunFn;
}

type RowOf<T> = T extends { row: infer R } ? R : never;
type ParamsOf<T> = T extends { params: infer P } ? P : Record<string, never>;
type ParamsArg<P> = [keyof P] extends [never] ? [params?: Record<string, never>] : [params: P];

// `G` is intentionally unconstrained: a generated `interface GeneratedQueries` has no
// implicit index signature and so would not satisfy a `Record<string, ...>` bound.
export interface SidemanticSqlClient<G> {
  /**
   * Run a generated semantic-SQL query. `sql` must be one of the exact string
   * literals captured by `sidemantic gen sql`; an unknown string is a compile
   * error. Rows and params are typed from the generated query map.
   */
  query<K extends keyof G & string>(sql: K, ...args: ParamsArg<ParamsOf<G[K]>>): Promise<Array<RowOf<G[K]>>>;
}

export function createSqlClient<G>(options: CreateSqlClientOptions): SidemanticSqlClient<G>;
