import type { Config } from "tailwindcss";

// Semantic color tokens are driven by CSS variables (see src/index.css) so themes stay
// centralized. The palette and type stack follow Sidequery's restrained product system.
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
        "chart-primary": "var(--chart-primary)",
        "chart-primary-soft": "var(--chart-primary-soft)",
        "chart-primary-selected": "var(--chart-primary-selected)",
      },
      fontFamily: {
        sans: ["Geist Sans", "-apple-system", "BlinkMacSystemFont", "SF Pro Text", "Segoe UI", "sans-serif"],
        mono: ["Geist Mono", "SFMono-Regular", "ui-monospace", "Menlo", "Consolas", "monospace"],
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
      boxShadow: {
        sm: "var(--shadow-sm)",
        floating: "var(--shadow)",
      },
    },
  },
  plugins: [],
};

export default config;
