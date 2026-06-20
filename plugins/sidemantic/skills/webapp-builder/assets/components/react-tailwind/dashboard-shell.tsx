import type { ReactNode } from "react";

type DashboardShellProps = {
  title: string;
  eyebrow?: string;
  status?: ReactNode;
  toolbar?: ReactNode;
  children: ReactNode;
};

export function DashboardShell({ title, eyebrow = "Sidemantic", status, toolbar, children }: DashboardShellProps) {
  return (
    <main className="mx-auto max-w-6xl px-4 py-5 text-slate-950 sm:px-6">
      <header className="flex flex-wrap items-end justify-between gap-4 border-b border-slate-200 pb-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-normal text-slate-500">{eyebrow}</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal">{title}</h1>
        </div>
        {status ? <div className="text-sm text-slate-500">{status}</div> : null}
      </header>
      {toolbar ? <section className="flex flex-wrap gap-2 py-3">{toolbar}</section> : null}
      <section className="grid gap-4 py-4">{children}</section>
    </main>
  );
}
