import type { Config } from "tailwindcss";

// Semantic color tokens are driven by CSS variables (see src/index.css) so a dark theme
// can be layered later without touching component classes. Palette seeded from hogflare's
// "data tool" tokens: hairline borders, one primary accent, mono numerals.
const config: Config = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--bg)",
        surface: "var(--surface)",
        "surface-soft": "var(--surface-soft)",
        ink: "var(--ink)",
        muted: "var(--muted)",
        faint: "var(--faint)",
        line: "var(--line)",
        accent: "var(--accent)",
        "accent-soft": "var(--accent-soft)",
        danger: "var(--danger)",
        "danger-soft": "var(--danger-soft)",
        success: "var(--success)",
        "success-soft": "var(--success-soft)",
        "line-strong": "var(--line-strong)",
        "chart-primary": "var(--chart-primary)",
        "chart-primary-soft": "var(--chart-primary-soft)",
        "chart-primary-selected": "var(--chart-primary-selected)",
      },
      fontFamily: {
        // Resolved through CSS variables (src/index.css) so hosts and the typography
        // gallery can swap faces without touching component classes.
        sans: "var(--font-sans, sans-serif)",
        mono: "var(--font-mono, monospace)",
      },
      fontSize: {
        "2xs": ["11px", { lineHeight: "1.3" }],
        xs: ["12px", { lineHeight: "1.35" }],
        sm: ["13px", { lineHeight: "1.4" }],
        base: ["14px", { lineHeight: "1.45" }],
      },
      borderColor: {
        DEFAULT: "var(--line)",
      },
    },
  },
  plugins: [],
};

export default config;
