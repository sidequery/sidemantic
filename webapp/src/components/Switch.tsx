type SwitchProps = {
  checked: boolean;
  onChange: (checked: boolean) => void;
  /** Visible inline label; also the accessible name unless ariaLabel is set. */
  label?: string;
  ariaLabel?: string;
  disabled?: boolean;
};

// Toggle in the WAI-ARIA switch pattern: a real button with role="switch", so Space/Enter and
// focus handling come for free.
export function Switch({ checked, onChange, label, ariaLabel, disabled }: SwitchProps) {
  return (
    <label className={`flex items-center gap-1.5 text-xs ${disabled ? "opacity-50" : ""}`}>
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        aria-label={ariaLabel ?? label}
        disabled={disabled}
        onClick={() => onChange(!checked)}
        data-checked={checked || undefined}
        className={`relative h-4 w-7 shrink-0 rounded-full border transition-colors ${
          checked ? "border-accent bg-accent" : "border-line bg-surface-soft"
        }`}
      >
        <span
          aria-hidden="true"
          className={`absolute top-1/2 size-3 -translate-y-1/2 rounded-full bg-surface shadow transition-[left] ${
            checked ? "left-[14px]" : "left-[2px]"
          }`}
        />
      </button>
      {label ? <span className="text-muted">{label}</span> : null}
    </label>
  );
}
