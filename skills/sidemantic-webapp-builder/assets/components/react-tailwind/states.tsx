type StateBoxProps = {
  title?: string;
  message: string;
};

export function LoadingState({ message = "Loading metrics..." }: Partial<StateBoxProps>) {
  return <div className="rounded-lg border border-slate-200 bg-white p-4 text-sm text-slate-500">{message}</div>;
}

export function EmptyState({ title = "No results", message }: StateBoxProps) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <h3 className="text-sm font-semibold text-slate-950">{title}</h3>
      <p className="mt-1 text-sm text-slate-500">{message}</p>
    </div>
  );
}

export function ErrorState({ title = "Query failed", message }: StateBoxProps) {
  return (
    <div className="rounded-lg border border-red-200 bg-white p-4">
      <h3 className="text-sm font-semibold text-red-800">{title}</h3>
      <p className="mt-1 text-sm text-red-700">{message}</p>
    </div>
  );
}
