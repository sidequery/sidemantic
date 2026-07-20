export function AppBrand({
  dashboardTitle,
  modelLabel,
  onHome,
}: {
  dashboardTitle?: string;
  modelLabel?: string;
  onHome: () => void;
}) {
  const content = (
    <>
      <span className="text-sm font-semibold text-ink">{dashboardTitle ?? "Sidemantic"}</span>
      {dashboardTitle === undefined && modelLabel ? (
        <span className="truncate text-2xs text-faint">{modelLabel}</span>
      ) : null}
    </>
  );

  if (dashboardTitle !== undefined) {
    return <span className="flex min-w-0 items-baseline gap-2">{content}</span>;
  }

  return (
    <button
      type="button"
      onClick={onHome}
      aria-label="Home"
      className="flex min-w-0 items-baseline gap-2"
    >
      {content}
    </button>
  );
}
