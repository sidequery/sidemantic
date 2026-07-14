import type { ReactNode } from "react";

type QueryDebugPanelProps = {
  queries: Record<string, string | undefined>;
  inputs?: Record<string, QueryDebugInput | undefined>;
};

export type QueryDebugInput = {
  metrics?: string[];
  dimensions?: string[];
  filters?: string[];
};

const SQL_KEYWORDS = new Set([
  "and", "as", "asc", "by", "case", "cast", "count", "date_trunc", "desc", "else", "end", "from",
  "group", "in", "is", "join", "left", "limit", "not", "null", "on", "or", "order", "over",
  "partition", "select", "sum", "then", "when", "where", "with",
]);

export type SqlToken = { kind: "comment" | "string" | "number" | "keyword" | "plain"; value: string };

/** Small dependency-free tokenizer shared conceptually with the copyable skill component. */
export function tokenizeSql(source: string): SqlToken[] {
  const tokens: SqlToken[] = [];
  let index = 0;
  while (index < source.length) {
    const rest = source.slice(index);
    const comment = rest.match(/^--[^\n]*/);
    const string = rest.match(/^'(?:''|[^'])*'/);
    const number = rest.match(/^\b\d+(?:\.\d+)?\b/);
    const word = rest.match(/^[A-Za-z_][A-Za-z0-9_]*/);
    const match = comment ?? string ?? number ?? word;
    if (!match) {
      tokens.push({ kind: "plain", value: source[index] });
      index += 1;
      continue;
    }
    const value = match[0];
    const kind = comment
      ? "comment"
      : string
        ? "string"
        : number
          ? "number"
          : SQL_KEYWORDS.has(value.toLowerCase())
            ? "keyword"
            : "plain";
    tokens.push({ kind, value });
    index += value.length;
  }
  return tokens;
}

const TOKEN_CLASS: Record<SqlToken["kind"], string> = {
  comment: "italic text-faint",
  string: "text-accent",
  number: "text-danger",
  keyword: "font-semibold text-accent",
  plain: "",
};

/** Collapsible generated-SQL inspector. Keeps the data-testid contract for verification. */
export function QueryDebugPanel({ queries, inputs = {} }: QueryDebugPanelProps) {
  const text = Object.entries(queries)
    .filter(([, sql]) => sql)
    .map(([name, sql]) => `-- ${name}\n${sql}`)
    .join("\n\n");
  const tokens = tokenizeSql(text || "No queries yet.");

  return (
    <details className="border border-line bg-surface">
      <summary className="cursor-pointer px-3 py-2 text-2xs font-semibold uppercase tracking-wide text-faint">
        Generated SQL
      </summary>
      {Object.keys(inputs).length > 0 ? (
        <div data-testid="query-inputs" className="grid gap-px border-t border-line bg-line sm:grid-cols-2">
          {Object.entries(inputs).map(([name, input]) =>
            input ? (
              <section key={name} className="min-w-0 bg-surface px-3 py-2 text-2xs">
                <h3 className="mb-1 font-semibold text-ink">{name}</h3>
                {([
                  ["Metrics", input.metrics],
                  ["Dimensions", input.dimensions],
                  ["Filters", input.filters],
                ] as const).map(([label, values]) =>
                  values?.length ? (
                    <p key={label} className="truncate text-muted" title={values.join(", ")}>
                      <strong className="font-medium text-faint">{label}:</strong> {values.join(", ")}
                    </p>
                  ) : null,
                )}
              </section>
            ) : null,
          )}
        </div>
      ) : null}
      <pre
        data-testid="query-debug"
        className="max-h-72 overflow-auto whitespace-pre-wrap border-t border-line px-3 py-2 font-mono text-2xs text-muted"
      >
        {tokens.map((token, index): ReactNode =>
          TOKEN_CLASS[token.kind] ? (
            <span key={index} className={TOKEN_CLASS[token.kind]} data-token={token.kind}>
              {token.value}
            </span>
          ) : (
            token.value
          ),
        )}
      </pre>
    </details>
  );
}
