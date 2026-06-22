import type { ReactNode } from "react";
import type { SidemanticQuerySpec } from "./types";

type QueryDebugPanelProps = {
  queries: Record<string, SidemanticQuerySpec | undefined>;
};

// Keep this keyword set in sync with the static `SQL_KEYWORDS` so highlighting matches across
// both implementations.
const SQL_KEYWORDS = new Set([
  "and", "as", "asc", "by", "case", "cast", "count", "cte", "date_trunc", "desc", "else", "end",
  "from", "group", "in", "is", "join", "left", "limit", "not", "null", "on", "or", "order", "over",
  "partition", "select", "sum", "then", "when", "where", "with",
]);

type SqlToken = { className: string; value: string };

function tokenizeSql(source: string): SqlToken[] {
  const text = String(source || "");
  const tokens: SqlToken[] = [];
  let index = 0;

  while (index < text.length) {
    const rest = text.slice(index);

    const comment = rest.match(/^--[^\n]*/);
    if (comment) {
      tokens.push({ className: "italic text-slate-400", value: comment[0] });
      index += comment[0].length;
      continue;
    }

    const string = rest.match(/^'(?:''|[^'])*'/);
    if (string) {
      tokens.push({ className: "text-emerald-700", value: string[0] });
      index += string[0].length;
      continue;
    }

    const number = rest.match(/^\b\d+(?:\.\d+)?\b/);
    if (number) {
      tokens.push({ className: "text-amber-700", value: number[0] });
      index += number[0].length;
      continue;
    }

    const word = rest.match(/^[A-Za-z_][A-Za-z0-9_]*/);
    if (word) {
      const value = word[0];
      tokens.push({
        className: SQL_KEYWORDS.has(value.toLowerCase()) ? "font-medium text-indigo-700" : "",
        value,
      });
      index += value.length;
      continue;
    }

    tokens.push({ className: "", value: text[index] });
    index += 1;
  }

  return tokens;
}

export function QueryDebugPanel({ queries }: QueryDebugPanelProps) {
  const text = Object.entries(queries)
    .map(([name, query]) => `-- ${name}\n${query?.sql || ""}`)
    .join("\n\n");
  const tokens = tokenizeSql(text);

  return (
    <details className="rounded-lg border border-slate-200 bg-white p-3 text-sm shadow-sm">
      <summary className="cursor-pointer text-xs font-medium text-slate-600">Generated SQL</summary>
      <pre data-testid="query-debug" className="mt-3 overflow-auto whitespace-pre-wrap text-xs text-slate-700">
        {tokens.map((token, index): ReactNode =>
          token.className ? (
            <span key={index} className={token.className}>
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
