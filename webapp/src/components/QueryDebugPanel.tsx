type QueryDebugPanelProps = {
  queries: Record<string, string | undefined>;
};

/** Collapsible generated-SQL inspector. Keeps the data-testid contract for verification. */
export function QueryDebugPanel({ queries }: QueryDebugPanelProps) {
  const text = Object.entries(queries)
    .filter(([, sql]) => sql)
    .map(([name, sql]) => `-- ${name}\n${sql}`)
    .join("\n\n");

  return (
    <details className="border border-line bg-surface">
      <summary className="cursor-pointer px-3 py-2 text-2xs font-semibold uppercase tracking-wide text-faint">
        Generated SQL
      </summary>
      <pre
        data-testid="query-debug"
        className="max-h-72 overflow-auto whitespace-pre-wrap border-t border-line px-3 py-2 font-mono text-2xs text-muted"
      >
        {text || "No queries yet."}
      </pre>
    </details>
  );
}
