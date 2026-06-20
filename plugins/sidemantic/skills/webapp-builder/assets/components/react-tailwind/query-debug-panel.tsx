import type { SidemanticQuerySpec } from "./types";

type QueryDebugPanelProps = {
  queries: Record<string, SidemanticQuerySpec | undefined>;
};

export function QueryDebugPanel({ queries }: QueryDebugPanelProps) {
  const text = Object.entries(queries)
    .map(([name, query]) => `-- ${name}\n${query?.sql || ""}`)
    .join("\n\n");

  return (
    <details className="rounded-lg border border-slate-200 bg-white p-3 text-sm shadow-sm">
      <summary className="cursor-pointer text-xs font-medium text-slate-600">Generated SQL</summary>
      <pre data-testid="query-debug" className="mt-3 overflow-auto whitespace-pre-wrap text-xs text-slate-700">
        {text}
      </pre>
    </details>
  );
}
