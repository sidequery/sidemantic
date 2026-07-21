import type { ButtonHTMLAttributes } from "react";

export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
  size?: "sm" | "md";
};

const VARIANT_CLASSES: Record<ButtonVariant, string> = {
  primary: "border-accent bg-accent-soft font-medium text-accent hover:bg-accent hover:text-surface",
  secondary: "border-line bg-surface text-ink hover:bg-surface-soft",
  ghost: "border-transparent bg-transparent text-muted hover:bg-surface-soft hover:text-ink",
  danger: "border-danger bg-danger-soft font-medium text-danger hover:bg-danger hover:text-surface",
};

// Flat, hairline-bordered button in the data-tool idiom. Defaults to type="button" so toolbar
// buttons inside forms never submit accidentally.
export function Button({ variant = "secondary", size = "md", type = "button", className, ...rest }: ButtonProps) {
  const sizing = size === "sm" ? "h-6 px-2.5 text-2xs" : "h-7 px-3 text-xs";
  return (
    <button
      type={type}
      data-variant={variant}
      className={`inline-flex items-center justify-center rounded-full border ${sizing} ${VARIANT_CLASSES[variant]} disabled:pointer-events-none disabled:opacity-50 ${className ?? ""}`}
      {...rest}
    />
  );
}
