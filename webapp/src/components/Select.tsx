export type SelectOption = { value: string; label?: string };

type SelectProps = {
  value: string;
  options: SelectOption[];
  onChange: (value: string) => void;
  /** Visible inline label to the left of the control (hidden on narrow screens). */
  label?: string;
  ariaLabel?: string;
  placeholder?: string;
  disabled?: boolean;
};

// Generic single-value dropdown in the GrainSelect idiom: a styled native <select>, so keyboard,
// mobile, and screen-reader behavior come from the platform.
export function Select({ value, options, onChange, label, ariaLabel, placeholder, disabled }: SelectProps) {
  return (
    <label className="flex items-center gap-1.5 text-xs text-faint">
      {label ? <span className="hidden sm:inline">{label}</span> : null}
      <select
        aria-label={ariaLabel ?? label}
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        className="h-7 rounded-full border border-line bg-surface px-2.5 text-xs text-ink disabled:opacity-50"
      >
        {placeholder ? (
          <option value="" disabled>
            {placeholder}
          </option>
        ) : null}
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label ?? option.value}
          </option>
        ))}
      </select>
    </label>
  );
}
