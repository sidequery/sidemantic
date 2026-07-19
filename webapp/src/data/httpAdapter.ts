import { buildCatalogFromDescribe, buildCatalogFromGraph, withJoinablePairs } from "../lib/catalog";
import type { SidemanticBackend } from "./backend";
import { decodeArrow } from "./arrow";
import { normalizeDashboardDocument, type DashboardDocument } from "./dashboardTypes";
import type { Catalog, QueryResult, ResultRow, StructuredQuery } from "./types";

const ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream";

export type HttpAdapterOptions = {
  /** Base URL for the backend. Empty = same-origin (the embedded/dev-proxy default). */
  baseUrl?: string;
  /** Bearer token when the backend was started with --auth-token. */
  token?: string;
  /** Wire format for query results. JSON is correctness-first; arrow is the wide/large fast path. */
  transport?: "json" | "arrow";
  /** Default pre-aggregation routing for every query (graceful base-table fallback). Default true —
   *  the lever that keeps the dashboard fast on million/billion-row tables. */
  usePreaggregations?: boolean;
  /** Max concurrent /query requests. The dashboard fan-out runs at most this many in parallel;
   *  the rest queue. Default 6. */
  maxConcurrency?: number;
};

/** Minimal bounded-concurrency gate: runs at most `max` tasks at once, queues the rest. */
class Semaphore {
  private active = 0;
  private readonly waiters: Array<() => void> = [];

  constructor(private readonly max: number) {}

  async run<T>(task: () => Promise<T>): Promise<T> {
    if (this.active >= this.max) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    this.active += 1;
    try {
      return await task();
    } finally {
      this.active -= 1;
      this.waiters.shift()?.();
    }
  }
}

function toRequestBody(query: StructuredQuery): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  if (query.metrics?.length) body.metrics = query.metrics;
  if (query.dimensions?.length) body.dimensions = query.dimensions;
  if (query.filters?.length) body.filters = query.filters;
  if (query.segments?.length) body.segments = query.segments;
  if (query.orderBy?.length) body.order_by = query.orderBy;
  if (query.limit != null) body.limit = query.limit;
  if (query.offset != null) body.offset = query.offset;
  if (query.ungrouped) body.ungrouped = true;
  if (query.parameters) body.parameters = query.parameters;
  if (query.usePreaggregations != null) body.use_preaggregations = query.usePreaggregations;
  // Omit UTC so requests stay identical to the pre-timezone wire format (backend defaults to UTC).
  if (query.timezone && query.timezone !== "UTC") body.timezone = query.timezone;
  return body;
}

export class HttpBackend implements SidemanticBackend {
  private readonly baseUrl: string;
  private readonly token?: string;
  private readonly transport: "json" | "arrow";
  private readonly usePreaggregations: boolean;
  private readonly gate: Semaphore;

  constructor(options: HttpAdapterOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "").replace(/\/$/, "");
    this.token = options.token;
    this.transport = options.transport ?? "json";
    this.usePreaggregations = options.usePreaggregations ?? true;
    this.gate = new Semaphore(options.maxConcurrency ?? 6);
  }

  /** Apply the adapter-wide pre-agg default unless the query overrides it. */
  private body(query: StructuredQuery): Record<string, unknown> {
    return toRequestBody({ usePreaggregations: this.usePreaggregations, ...query });
  }

  private headers(extra?: Record<string, string>): Record<string, string> {
    const headers: Record<string, string> = { ...extra };
    if (this.token) headers.Authorization = `Bearer ${this.token}`;
    return headers;
  }

  private url(path: string): string {
    return `${this.baseUrl}${path}`;
  }

  private async getJson<T>(path: string): Promise<T> {
    const res = await fetch(this.url(path), { headers: this.headers() });
    if (!res.ok) throw new Error(await this.errorText(res, path));
    return (await res.json()) as T;
  }

  private async errorText(res: Response, path: string): Promise<string> {
    let detail = "";
    try {
      const body = await res.json();
      detail = (body as { error?: string; detail?: string }).error ?? (body as { detail?: string }).detail ?? "";
    } catch {
      // non-JSON error body
    }
    return `${path} failed (${res.status})${detail ? `: ${detail}` : ""}`;
  }

  async health(): Promise<boolean> {
    try {
      const res = await fetch(this.url("/health"), { headers: this.headers() });
      return res.ok;
    } catch {
      return false;
    }
  }

  async getCatalog(): Promise<Catalog> {
    // Prefer the rich /describe payload; gracefully fall back to /graph (names only) so the UI
    // still works against a backend that hasn't exposed /describe yet.
    try {
      const res = await fetch(this.url("/describe"), { headers: this.headers() });
      if (res.ok) {
        const catalog = buildCatalogFromDescribe(await res.json());
        try {
          return withJoinablePairs(catalog, await this.getJson<unknown>("/graph"));
        } catch {
          return catalog;
        }
      }
    } catch {
      // fall through to /graph
    }
    const graph = await this.getJson<unknown>("/graph");
    return buildCatalogFromGraph(graph);
  }

  async getDashboard(): Promise<DashboardDocument | null> {
    const res = await fetch(this.url("/dashboard"), { headers: this.headers() });
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(await this.errorText(res, "/dashboard"));
    // Some embedded SPA hosts (notably the Rust server) serve index.html for unknown routes.
    // Treat that fallback like an absent dashboard instead of trying to parse HTML as JSON and
    // breaking the generic explorer.
    const contentType = res.headers.get("Content-Type")?.toLowerCase() ?? "";
    if (!contentType.includes("json")) return null;
    return normalizeDashboardDocument((await res.json()) as DashboardDocument);
  }

  async compile(query: StructuredQuery): Promise<string> {
    const res = await fetch(this.url("/compile"), {
      method: "POST",
      headers: this.headers({ "Content-Type": "application/json" }),
      body: JSON.stringify(this.body(query)),
    });
    if (!res.ok) throw new Error(await this.errorText(res, "/compile"));
    return ((await res.json()) as { sql: string }).sql;
  }

  async runQuery(query: StructuredQuery): Promise<QueryResult> {
    // Bound the dashboard fan-out: at most `maxConcurrency` requests in flight at once.
    return this.gate.run(() => (this.transport === "arrow" ? this.runQueryArrow(query) : this.runQueryJson(query)));
  }

  private async runQueryJson(query: StructuredQuery): Promise<QueryResult> {
    const res = await fetch(this.url("/query"), {
      method: "POST",
      headers: this.headers({ "Content-Type": "application/json" }),
      body: JSON.stringify(this.body(query)),
    });
    if (!res.ok) throw new Error(await this.errorText(res, "/query"));
    const payload = (await res.json()) as { sql: string; rows: ResultRow[]; row_count: number };
    const rows = payload.rows ?? [];
    const columns = rows.length ? Object.keys(rows[0]) : [];
    return { columns, rows, rowCount: payload.row_count ?? rows.length, sql: payload.sql ?? "" };
  }

  private async runQueryArrow(query: StructuredQuery): Promise<QueryResult> {
    const res = await fetch(this.url("/query?format=arrow"), {
      method: "POST",
      headers: this.headers({ "Content-Type": "application/json", Accept: ARROW_MEDIA_TYPE }),
      body: JSON.stringify(this.body(query)),
    });
    if (!res.ok) throw new Error(await this.errorText(res, "/query"));
    const bytes = new Uint8Array(await res.arrayBuffer());
    const { columns, rows } = decodeArrow(bytes);
    const sql = res.headers.get("X-Sidemantic-Sql") ?? "";
    return { columns, rows, rowCount: rows.length, sql };
  }
}
