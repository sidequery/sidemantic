export type Theme = "light" | "dark";

export type ThemeTokens = Partial<{
  background: string;
  surface: string;
  surfaceSoft: string;
  ink: string;
  muted: string;
  faint: string;
  line: string;
  action: string;
  actionSoft: string;
  chartPrimary: string;
  chartPrimarySoft: string;
  chartPrimarySelected: string;
  danger: string;
  dangerSoft: string;
}>;

const TOKEN_PROPERTIES: Record<keyof ThemeTokens, `--${string}`> = {
  background: "--bg",
  surface: "--surface",
  surfaceSoft: "--surface-soft",
  ink: "--ink",
  muted: "--muted",
  faint: "--faint",
  line: "--line",
  action: "--accent",
  actionSoft: "--accent-soft",
  chartPrimary: "--chart-primary",
  chartPrimarySoft: "--chart-primary-soft",
  chartPrimarySelected: "--chart-primary-selected",
  danger: "--danger",
  dangerSoft: "--danger-soft",
};

const KEY = "sidemantic-theme";

export function getTheme(): Theme {
  const stored = localStorage.getItem(KEY);
  if (stored === "light" || stored === "dark") return stored;
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export function applyTheme(theme: Theme): void {
  document.documentElement.dataset.theme = theme;
}

/** Set the initial theme before first paint to avoid a flash. */
export function initTheme(): void {
  applyTheme(getTheme());
}

export function toggleTheme(): Theme {
  const next: Theme = getTheme() === "dark" ? "light" : "dark";
  localStorage.setItem(KEY, next);
  applyTheme(next);
  return next;
}

/** Override semantic UI tokens on the document or on a scoped component container. */
export function applyThemeTokens(tokens: ThemeTokens, target: HTMLElement = document.documentElement): void {
  for (const [name, value] of Object.entries(tokens) as [keyof ThemeTokens, string | undefined][]) {
    if (value) target.style.setProperty(TOKEN_PROPERTIES[name], value);
    else target.style.removeProperty(TOKEN_PROPERTIES[name]);
  }
}
