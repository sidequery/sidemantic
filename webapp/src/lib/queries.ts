import { NULL_TOKEN, type CatalogDimension, type FieldRef, type Grain, type StructuredQuery } from "../data/types";
import { sqlLiteral } from "./format";
import { timeFilters, type DateRange } from "./time";

// A dimension's filter mode:
//   include  — keep rows whose value is in `values` (the crossfilter default; `=`/`IN`)
//   exclude  — drop rows whose value is in `values` (`!=`/`NOT IN`)
//   contains — keep rows whose value matches `pattern` (`ILIKE '%…%'`; `values` ignored)
export type FilterMode = "include" | "exclude" | "contains";

// One dimension's filter. `values` OR within include/exclude and AND across dimensions (the
// approved crossfilter rules). A value may be NULL_TOKEN. `pattern` is used only by contains mode.
export type DimFilter = { mode: FilterMode; values: string[]; pattern?: string };

// Crossfilter + editor selections: dimension ref -> its filter.
export type FilterState = Record<FieldRef, DimFilter>;

/** An include-mode filter over `values` — the shape leaderboard crossfilter clicks write. */
export function includeFilter(values: string[]): DimFilter {
  return { mode: "include", values };
}

/** True when a dimension's filter would emit no SQL (so it should be dropped from state). */
export function isEmptyFilter(filter: DimFilter): boolean {
  return filter.mode === "contains" ? !filter.pattern : filter.values.length === 0;
}

/** dimension ref -> semantic type, used to emit correctly-typed filter literals. */
export type DimTypes = Record<FieldRef, string>;

export function dimTypes(dimensions: CatalogDimension[]): DimTypes {
  return Object.fromEntries(dimensions.map((dim) => [dim.ref, dim.type]));
}

/** Render a single filter value as a SQL literal, honoring the dimension's type so numeric and
 *  boolean dimensions aren't quoted (which type-strict backends reject). */
function filterLiteral(value: string, type?: string): string {
  if ((type === "numeric" || type === "number") && value.trim() !== "" && Number.isFinite(Number(value))) {
    return value;
  }
  if (type === "boolean") {
    const lower = value.toLowerCase();
    if (lower === "true" || lower === "false") return lower;
  }
  return sqlLiteral(value);
}

/**
 * Escape a user-typed substring for a SQL LIKE/ILIKE pattern wrapped as `%…%`. The LIKE
 * metacharacters `%` and `_` are neutralized with a backslash (paired with `ESCAPE '\'` at the
 * call site) so a literal `%` in the pattern matches a literal `%` rather than "anything".
 */
export function likeEscape(pattern: string): string {
  return pattern.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_");
}

/** SQL for one dimension's include/exclude selection (values only — the caller handles contains). */
function membershipExpr(dimRef: FieldRef, filter: DimFilter, type?: string): string | null {
  const negate = filter.mode === "exclude";
  const hasNull = filter.values.includes(NULL_TOKEN);
  const present = filter.values.filter((value) => value !== NULL_TOKEN);

  let presentExpr: string | null = null;
  if (present.length === 1) {
    presentExpr = `${dimRef} ${negate ? "!=" : "="} ${filterLiteral(present[0], type)}`;
  } else if (present.length > 1) {
    const list = present.map((v) => filterLiteral(v, type)).join(", ");
    presentExpr = `${dimRef} ${negate ? "NOT IN" : "IN"} (${list})`;
  }

  if (!negate) {
    // Include: match any selected value (OR), plus IS NULL if the null token was selected.
    const parts: string[] = [];
    if (presentExpr) parts.push(presentExpr);
    if (hasNull) parts.push(`${dimRef} IS NULL`);
    if (parts.length === 0) return null;
    return parts.length === 1 ? parts[0] : `(${parts.join(" OR ")})`;
  }

  // Exclude. `dim != v` / `dim NOT IN (...)` is UNKNOWN for NULL rows, which would silently
  // drop them. Only exclude NULLs when the null token was explicitly selected for exclusion;
  // otherwise keep them with an `OR dim IS NULL` branch.
  if (hasNull) {
    const parts: string[] = [];
    if (presentExpr) parts.push(presentExpr);
    parts.push(`${dimRef} IS NOT NULL`);
    return parts.length === 1 ? parts[0] : `(${parts.join(" AND ")})`;
  }
  if (!presentExpr) return null;
  return `(${presentExpr} OR ${dimRef} IS NULL)`;
}

/**
 * Turn per-dimension filters into SQL filter expressions.
 * `excludeDim` drops a single dimension's own filter — used when ranking that very dimension's
 * leaderboard, so a leaderboard never filters itself out. `types` gives each dimension's semantic
 * type so values are quoted/cast correctly. include -> `=`/`IN` (+ `IS NULL`), exclude -> `!=`/`NOT
 * IN` (+ `IS NOT NULL`), contains -> `ILIKE '%<escaped>%'`.
 */
export function filterExprs(filters: FilterState, opts: { types?: DimTypes; excludeDim?: FieldRef } = {}): string[] {
  const out: string[] = [];
  for (const [dimRef, filter] of Object.entries(filters)) {
    if (dimRef === opts.excludeDim || isEmptyFilter(filter)) continue;
    const type = opts.types?.[dimRef];
    if (filter.mode === "contains") {
      // Cast to text so contains works on numeric/boolean dimensions: DuckDB and Postgres
      // reject ILIKE on non-text operands.
      const pat = sqlLiteral(`%${likeEscape(filter.pattern ?? "")}%`);
      out.push(`CAST(${dimRef} AS VARCHAR) ILIKE ${pat} ESCAPE '\\'`);
      continue;
    }
    const expr = membershipExpr(dimRef, filter, type);
    if (expr) out.push(expr);
  }
  return out;
}

/**
 * Assemble the active filter expressions for a panel: crossfilter selections plus the time-range
 * bound. `excludeDim` drops one dimension's own selection (so a leaderboard never filters itself).
 */
export function composeFilters(
  filters: FilterState,
  opts: { timeRef?: FieldRef; range?: DateRange; excludeDim?: FieldRef; types?: DimTypes } = {},
): string[] {
  const base = filterExprs(filters, { types: opts.types, excludeDim: opts.excludeDim });
  if (opts.timeRef && opts.range) base.push(...timeFilters(opts.timeRef, opts.range));
  return base;
}

/** Aggregate totals for one or more metrics (no group-by). */
export function metricTotals(metrics: FieldRef[], filters: string[]): StructuredQuery {
  return { metrics, filters };
}

// Upper bound on buckets per grain — generous enough to cover any realistic range (so the series
// is never truncated to its oldest 500 buckets) while still bounding the points sent to the chart.
const GRAIN_BUCKET_CAP: Record<Grain, number> = {
  hour: 9600, // ~13 months hourly
  day: 4000, // ~11 years daily
  week: 800,
  month: 240,
  quarter: 120,
  year: 60,
};

/** A metric time series bucketed by `grain` on `timeRef`, ordered ascending. */
export function metricSeries(metrics: FieldRef[], timeRef: FieldRef, grain: Grain, filters: string[]): StructuredQuery {
  const timeDim = `${timeRef}__${grain}`;
  return { metrics, dimensions: [timeDim], filters, orderBy: [`${timeDim} ASC`], limit: GRAIN_BUCKET_CAP[grain] ?? 2000 };
}

/** Top-N values of one dimension ranked by a metric (a leaderboard panel). */
export function dimensionLeaderboard(
  metricRef: FieldRef,
  dimRef: FieldRef,
  filters: string[],
  limit = 6,
): StructuredQuery {
  return { metrics: [metricRef], dimensions: [dimRef], filters, orderBy: [`${metricRef} DESC`], limit };
}

/**
 * Distinct values of one dimension for the filter editor's checkbox list. Grouping by the single
 * dimension with no metric yields `SELECT DISTINCT <dim> … GROUP BY <dim>` at the backend; the
 * `filters` already fold in the search text (an ILIKE) and the surrounding crossfilter context.
 */
export function distinctValues(dimRef: FieldRef, filters: string[], limit = 50): StructuredQuery {
  return { dimensions: [dimRef], filters, orderBy: [`${dimRef} ASC`], limit };
}

/** Grouped pivot table: N dimensions x M metrics. */
export function pivotGroup(
  metrics: FieldRef[],
  dimensions: FieldRef[],
  filters: string[],
  orderBy?: string[],
  limit = 500,
): StructuredQuery {
  return { metrics, dimensions, filters, orderBy, limit };
}

/** Ungrouped raw rows for the inspect/preview view. */
export function previewRows(
  fields: { dimensions: FieldRef[]; metrics: FieldRef[] },
  filters: string[],
  limit = 50,
): StructuredQuery {
  return { dimensions: fields.dimensions, metrics: fields.metrics, filters, ungrouped: true, limit };
}
