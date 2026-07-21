export type TabItem = { key: string; label: string };

type TabsProps = {
  tabs: TabItem[];
  active: string;
  onChange: (key: string) => void;
  ariaLabel?: string;
};

// Generic segmented tab switcher (the ViewSwitcher idiom with caller-supplied segments). Renders
// the tablist only — associate panels via aria-controls/ids in the caller if needed.
export function Tabs({ tabs, active, onChange, ariaLabel = "Tabs" }: TabsProps) {
  return (
    <div role="tablist" aria-label={ariaLabel} className="flex items-center overflow-hidden rounded border border-line bg-surface">
      {tabs.map((tab) => (
        <button
          key={tab.key}
          role="tab"
          type="button"
          aria-selected={active === tab.key}
          data-tab={tab.key}
          data-selected={active === tab.key || undefined}
          onClick={() => onChange(tab.key)}
          className="border-r border-line px-2.5 py-1 text-2xs font-medium text-muted last:border-r-0 hover:bg-surface-soft data-[selected=true]:bg-accent-soft data-[selected=true]:text-accent"
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
