export type Theme = "light" | "dark";

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
