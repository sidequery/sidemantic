import { useEffect, useState, type CSSProperties } from "react";
import { MetricCard } from "../components/MetricCard";
import { Select } from "../components/Select";
import { ThemeToggle } from "../components/ThemeToggle";
import { TimeSeriesChart } from "../components/TimeSeriesChart";

// Live typography workbench at /typography: pick a sans, mono, and display face (Google Fonts
// load on demand), see them applied to real components, and compare candidate size ramps.
// This view is a decision tool — it is not exported from the ui bundle.

type FontOption = {
  value: string;
  label: string;
  /** Google Fonts css2 family query, e.g. "Inter:wght@400;500;600". Absent = system, no load. */
  gf?: string;
  /** Full stylesheet URL for non-Google hosts (Fontshare). Wins over gf. */
  url?: string;
  /** font-family value to apply. */
  stack: string;
};

function fontshare(slug: string): string {
  return `https://api.fontshare.com/v2/css?f[]=${slug}&display=swap`;
}

const SANS_FONTS: FontOption[] = [
  { value: "system", label: "System (SF Pro)", stack: "system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif" },
  { value: "geist", label: "Geist", gf: "Geist:wght@400;500;600", stack: "'Geist', system-ui, sans-serif" },
  { value: "inter", label: "Inter", gf: "Inter:wght@400;500;600", stack: "'Inter', system-ui, sans-serif" },
  { value: "instrument", label: "Instrument Sans", gf: "Instrument+Sans:wght@400;500;600", stack: "'Instrument Sans', system-ui, sans-serif" },
  { value: "plex", label: "IBM Plex Sans", gf: "IBM+Plex+Sans:wght@400;500;600", stack: "'IBM Plex Sans', system-ui, sans-serif" },
  { value: "schibsted", label: "Schibsted Grotesk", gf: "Schibsted+Grotesk:wght@400;500;600", stack: "'Schibsted Grotesk', system-ui, sans-serif" },
  { value: "figtree", label: "Figtree", gf: "Figtree:wght@400;500;600", stack: "'Figtree', system-ui, sans-serif" },
  { value: "manrope", label: "Manrope", gf: "Manrope:wght@400;500;600", stack: "'Manrope', system-ui, sans-serif" },
  { value: "publicsans", label: "Public Sans", gf: "Public+Sans:wght@400;500;600", stack: "'Public Sans', system-ui, sans-serif" },
  { value: "spacegrotesk", label: "Space Grotesk", gf: "Space+Grotesk:wght@400;500;600", stack: "'Space Grotesk', system-ui, sans-serif" },
  // ---- less-traveled picks ----
  { value: "satoshi", label: "Satoshi ✦", url: fontshare("satoshi@400,500,700"), stack: "'Satoshi', system-ui, sans-serif" },
  { value: "generalsans", label: "General Sans ✦", url: fontshare("general-sans@400,500,600"), stack: "'General Sans', system-ui, sans-serif" },
  { value: "switzer", label: "Switzer ✦", url: fontshare("switzer@400,500,600"), stack: "'Switzer', system-ui, sans-serif" },
  { value: "cabinet", label: "Cabinet Grotesk ✦", url: fontshare("cabinet-grotesk@400,500,700"), stack: "'Cabinet Grotesk', system-ui, sans-serif" },
  { value: "bricolage", label: "Bricolage Grotesque ✦", gf: "Bricolage+Grotesque:wght@400;500;600", stack: "'Bricolage Grotesque', system-ui, sans-serif" },
  { value: "familjen", label: "Familjen Grotesk ✦", gf: "Familjen+Grotesk:wght@400;500;600", stack: "'Familjen Grotesk', system-ui, sans-serif" },
  { value: "hanken", label: "Hanken Grotesk ✦", gf: "Hanken+Grotesk:wght@400;500;600", stack: "'Hanken Grotesk', system-ui, sans-serif" },
  { value: "reddit", label: "Reddit Sans ✦", gf: "Reddit+Sans:wght@400;500;600", stack: "'Reddit Sans', system-ui, sans-serif" },
  { value: "chivo", label: "Chivo ✦", gf: "Chivo:wght@400;500;600", stack: "'Chivo', system-ui, sans-serif" },
  { value: "hostgrotesk", label: "Host Grotesk ✦", gf: "Host+Grotesk:wght@400;500;600", stack: "'Host Grotesk', system-ui, sans-serif" },
  { value: "sora", label: "Sora ✦", gf: "Sora:wght@400;500;600", stack: "'Sora', system-ui, sans-serif" },
];

const MONO_FONTS: FontOption[] = [
  { value: "system", label: "System (SF Mono)", stack: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" },
  { value: "geistmono", label: "Geist Mono", gf: "Geist+Mono:wght@400;500", stack: "'Geist Mono', ui-monospace, monospace" },
  { value: "jetbrains", label: "JetBrains Mono", gf: "JetBrains+Mono:wght@400;500", stack: "'JetBrains Mono', ui-monospace, monospace" },
  { value: "plexmono", label: "IBM Plex Mono", gf: "IBM+Plex+Mono:wght@400;500", stack: "'IBM Plex Mono', ui-monospace, monospace" },
  { value: "firacode", label: "Fira Code", gf: "Fira+Code:wght@400;500", stack: "'Fira Code', ui-monospace, monospace" },
  { value: "splinemono", label: "Spline Sans Mono", gf: "Spline+Sans+Mono:wght@400;500", stack: "'Spline Sans Mono', ui-monospace, monospace" },
  { value: "dmmono", label: "DM Mono", gf: "DM+Mono:wght@400;500", stack: "'DM Mono', ui-monospace, monospace" },
  // ---- less-traveled picks ----
  { value: "martianmono", label: "Martian Mono ✦", gf: "Martian+Mono:wght@400;500", stack: "'Martian Mono', ui-monospace, monospace" },
  { value: "fragment", label: "Fragment Mono ✦", gf: "Fragment+Mono", stack: "'Fragment Mono', ui-monospace, monospace" },
  { value: "redhatmono", label: "Red Hat Mono ✦", gf: "Red+Hat+Mono:wght@400;500", stack: "'Red Hat Mono', ui-monospace, monospace" },
  { value: "chivomono", label: "Chivo Mono ✦", gf: "Chivo+Mono:wght@400;500", stack: "'Chivo Mono', ui-monospace, monospace" },
  { value: "azeret", label: "Azeret Mono ✦", gf: "Azeret+Mono:wght@400;500", stack: "'Azeret Mono', ui-monospace, monospace" },
  { value: "spacemono", label: "Space Mono ✦", gf: "Space+Mono:wght@400;700", stack: "'Space Mono', ui-monospace, monospace" },
];

const DISPLAY_FONTS: FontOption[] = [
  { value: "sans", label: "Same as sans", stack: "" },
  { value: "newsreader", label: "Newsreader (serif)", gf: "Newsreader:opsz,wght@6..72,400..700", stack: "'Newsreader', Georgia, serif" },
  { value: "sourceserif", label: "Source Serif 4", gf: "Source+Serif+4:opsz,wght@8..60,400..700", stack: "'Source Serif 4', Georgia, serif" },
  { value: "fraunces", label: "Fraunces", gf: "Fraunces:opsz,wght@9..144,400..700", stack: "'Fraunces', Georgia, serif" },
  { value: "instrumentserif", label: "Instrument Serif", gf: "Instrument+Serif", stack: "'Instrument Serif', Georgia, serif" },
  { value: "spacegrotesk", label: "Space Grotesk", gf: "Space+Grotesk:wght@400;500;600", stack: "'Space Grotesk', system-ui, sans-serif" },
  // ---- less-traveled picks ----
  { value: "clash", label: "Clash Display ✦", url: fontshare("clash-display@400,500,600"), stack: "'Clash Display', system-ui, sans-serif" },
  { value: "cabinetdisplay", label: "Cabinet Grotesk ✦", url: fontshare("cabinet-grotesk@500,700,800"), stack: "'Cabinet Grotesk', system-ui, sans-serif" },
  { value: "sentient", label: "Sentient (serif) ✦", url: fontshare("sentient@400,500,700"), stack: "'Sentient', Georgia, serif" },
  { value: "gambetta", label: "Gambetta (serif) ✦", url: fontshare("gambetta@400,500,700"), stack: "'Gambetta', Georgia, serif" },
  { value: "bricolagedisplay", label: "Bricolage Grotesque ✦", gf: "Bricolage+Grotesque:opsz,wght@12..96,400..700", stack: "'Bricolage Grotesque', system-ui, sans-serif" },
  { value: "youngserif", label: "Young Serif ✦", gf: "Young+Serif", stack: "'Young Serif', Georgia, serif" },
  { value: "unbounded", label: "Unbounded ✦", gf: "Unbounded:wght@400;500;600", stack: "'Unbounded', system-ui, sans-serif" },
];

// One-click sans + mono (+ display) combinations worth judging as a set.
const PAIRINGS: Array<{ label: string; sans: string; mono: string; display: string }> = [
  { label: "Geist / Geist Mono", sans: "geist", mono: "geistmono", display: "sans" },
  { label: "Inter / JetBrains", sans: "inter", mono: "jetbrains", display: "sans" },
  { label: "IBM Plex", sans: "plex", mono: "plexmono", display: "sans" },
  { label: "Instrument / Spline", sans: "instrument", mono: "splinemono", display: "sans" },
  { label: "Schibsted / Fira", sans: "schibsted", mono: "firacode", display: "sans" },
  { label: "Editorial: Geist + Newsreader", sans: "geist", mono: "geistmono", display: "newsreader" },
  { label: "Satoshi / Martian ✦", sans: "satoshi", mono: "martianmono", display: "sans" },
  { label: "General Sans / Fragment ✦", sans: "generalsans", mono: "fragment", display: "sans" },
  { label: "Switzer + Clash headings ✦", sans: "switzer", mono: "redhatmono", display: "clash" },
  { label: "Cabinet + Gambetta ✦", sans: "cabinet", mono: "chivomono", display: "gambetta" },
  { label: "Bricolage all the way ✦", sans: "bricolage", mono: "spacemono", display: "bricolagedisplay" },
];

const loadedFamilies = new Set<string>();

function ensureFontLoaded(option: FontOption | undefined) {
  if (!option) return;
  const href = option.url ?? (option.gf ? `https://fonts.googleapis.com/css2?family=${option.gf}&display=swap` : undefined);
  if (!href || loadedFamilies.has(href)) return;
  loadedFamilies.add(href);
  const link = document.createElement("link");
  link.rel = "stylesheet";
  link.href = href;
  document.head.appendChild(link);
}

function byValue(options: FontOption[], value: string): FontOption {
  return options.find((option) => option.value === value) ?? options[0];
}

// The size ramp as shipped today, with every place each size appears.
const CURRENT_RAMP: Array<{ px: string; cls: string; usage: string; sample: string }> = [
  { px: "8", cls: "text-[8px]", usage: "MetricCard delta arrow glyph", sample: "▲" },
  { px: "9", cls: "text-[9px]", usage: "DataTable sort direction glyph", sample: "▲▼" },
  { px: "10", cls: "text-[10px]", usage: "Chart axis labels (all 9 charts)", sample: "Jan  Feb  Mar  12.4k  18.2k" },
  { px: "11", cls: "text-2xs", usage: "Captions, chips, table body, helper text (58 uses)", sample: "Canonical React primitives · updated 2h ago" },
  { px: "12", cls: "text-xs", usage: "Controls, labels, leaderboard rows — the workhorse (70 uses)", sample: "Revenue by region, last 28 days" },
  { px: "13", cls: "text-sm", usage: "Section titles, panel headers (9 uses); also the body default", sample: "Customer Region" },
  { px: "14", cls: "text-base", usage: "Explore index page title (1 use)", sample: "Explore" },
  { px: "19", cls: "text-[19px]", usage: "MetricCard value", sample: "$288,291" },
  { px: "20", cls: "text-xl", usage: "MetricTimeSeries headline total", sample: "$1,282,392" },
  { px: "24", cls: "text-2xl", usage: "DashboardShell page title", sample: "Revenue overview" },
];

// Candidate ramps to choose between. Values are px for: caption / label / body / section title /
// metric value / headline / page title.
const RAMP_CANDIDATES: Array<{ name: string; note: string; caption: number; label: number; body: number; section: number; value: number; headline: number; title: number }> = [
  { name: "A — Current", note: "11 / 12 / 13 base. Dense, reads small on 27″ displays.", caption: 11, label: 12, body: 13, section: 13, value: 19, headline: 20, title: 24 },
  { name: "B — One up", note: "12 / 13 / 14 base. Same hierarchy, comfortably legible.", caption: 12, label: 13, body: 14, section: 14, value: 22, headline: 24, title: 28 },
  { name: "C — Quiet UI, loud numbers", note: "Keeps the dense chrome, doubles down on data: bigger values and titles.", caption: 11, label: 12, body: 13, section: 13, value: 24, headline: 28, title: 30 },
];

const SPARK = [31, 42, 38, 55, 60, 74];
const TREND = Array.from({ length: 12 }, (_, index) => ({
  x: `2025-${String(index + 1).padStart(2, "0")}-01`,
  y: 96000 + index * 14000 + (index % 3) * 9000,
}));

const SQL_SAMPLE = `select region, sum(revenue) as revenue
from orders
where created_at >= date '2025-01-01'
group by all
order by revenue desc`;

function WeightRow({ family, size }: { family: string; size: string }) {
  return (
    <div className="flex flex-wrap items-baseline gap-x-6 gap-y-1" style={{ fontFamily: family }}>
      <span className={`${size} font-normal`}>Regular 400 — Revenue by payment method</span>
      <span className={`${size} font-medium`}>Medium 500 — Revenue by payment method</span>
      <span className={`${size} font-semibold`}>Semibold 600 — Revenue by payment method</span>
    </div>
  );
}

export function TypographyGallery() {
  const [sans, setSans] = useState("geist");
  const [mono, setMono] = useState("geistmono");
  const [display, setDisplay] = useState("sans");

  const sansOption = byValue(SANS_FONTS, sans);
  const monoOption = byValue(MONO_FONTS, mono);
  const displayOption = byValue(DISPLAY_FONTS, display);
  const displayStack = displayOption.stack || sansOption.stack;

  useEffect(() => {
    ensureFontLoaded(sansOption);
    ensureFontLoaded(monoOption);
    ensureFontLoaded(displayOption);
  }, [sansOption, monoOption, displayOption]);

  // Everything inside this wrapper resolves the font tokens to the current picks.
  const previewStyle = {
    "--font-sans": sansOption.stack,
    "--font-mono": monoOption.stack,
    "--font-display": displayStack,
    fontFamily: "var(--font-sans)",
  } as CSSProperties;

  return (
    <main className="min-h-screen bg-bg text-ink">
      <header className="sticky top-0 z-20 flex items-center justify-between gap-3 border-b border-line bg-surface px-4 py-3">
        <div>
          <h1 className="text-sm font-semibold">Typography workbench</h1>
          <p className="text-2xs text-faint">
            Pick faces, judge them on real components · <a className="text-accent hover:underline" href="/components">component gallery →</a>
          </p>
        </div>
        <ThemeToggle />
      </header>

      <div className="space-y-10 p-4">
        <section>
          <div className="flex flex-wrap items-center gap-3">
            <Select label="Sans" value={sans} options={SANS_FONTS.map(({ value, label }) => ({ value, label }))} onChange={setSans} />
            <Select label="Mono" value={mono} options={MONO_FONTS.map(({ value, label }) => ({ value, label }))} onChange={setMono} />
            <Select label="Headings" value={display} options={DISPLAY_FONTS.map(({ value, label }) => ({ value, label }))} onChange={setDisplay} />
            <div className="flex flex-wrap items-center gap-1.5">
              {PAIRINGS.map((pairing) => {
                const active = pairing.sans === sans && pairing.mono === mono && pairing.display === display;
                return (
                  <button
                    key={pairing.label}
                    type="button"
                    data-selected={active || undefined}
                    onClick={() => {
                      setSans(pairing.sans);
                      setMono(pairing.mono);
                      setDisplay(pairing.display);
                    }}
                    className={`inline-flex h-7 items-center rounded-full border px-3 text-xs transition-colors ${
                      active ? "border-accent text-accent" : "border-line bg-surface text-muted hover:border-line-strong"
                    }`}
                  >
                    {pairing.label}
                  </button>
                );
              })}
            </div>
          </div>
        </section>

        <div style={previewStyle} className="space-y-10">
          <section>
            <h2 className="mb-3 text-xs font-medium text-muted">In context</h2>
            <div className="space-y-6">
              <div>
                <p className="text-2xs text-faint">Acme Analytics · production</p>
                <h1 className="mt-1 text-2xl font-semibold text-ink">Revenue overview</h1>
                <div className="text-sm text-muted">Updated 2 minutes ago · 448,214 rows scanned</div>
              </div>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                <MetricCard metric="t.revenue" label="Revenue" value={288291} format={{ format: "currency" }} delta={{ label: "+12.4%", tone: "positive" }} comparison="vs previous month" sparkValues={SPARK} selected onSelect={() => {}} />
                <MetricCard metric="t.orders" label="Order Count" value={447} delta={{ label: "-2.1%", tone: "negative" }} comparison="vs previous month" sparkValues={[21, 34, 28, 41, 44, 38]} onSelect={() => {}} />
                <MetricCard metric="t.margin" label="Gross Margin" value={0.324} format={{ format: "percent" }} delta={{ label: "+1.1pt", tone: "positive" }} comparison="vs previous month" sparkValues={[24, 26, 29, 27, 31, 32]} onSelect={() => {}} />
              </div>
              <TimeSeriesChart
                points={TREND}
                formatValue={(value) => `$${Math.round(value).toLocaleString("en-US")}`}
                formatAxis={(value) => `$${Math.round(value / 1000)}k`}
                formatLabel={(label) => label.slice(0, 7)}
                ariaLabel="Twelve month revenue trend"
              />
              <div className="max-w-xl">
                <h3 className="text-sm font-semibold text-ink">How the comparison works</h3>
                <p className="mt-1 text-xs leading-5 text-muted">
                  The overlay shifts the previous period onto the current axis so both lines share a scale. Deltas
                  are computed on the visible range only — zooming recomputes them. Hover any point for the exact
                  pair of values; the quiet gray line is always the older period.
                </p>
              </div>
              <pre className="overflow-x-auto font-mono text-xs leading-5 text-muted">{SQL_SAMPLE}</pre>
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-xs font-medium text-muted">Numerals — mono, tabular</h2>
            <div className="flex flex-wrap gap-12">
              <div className="font-mono tnum text-sm text-ink">
                {["$1,204,918.24", "$984,003.10", "$88,214.00", "$1,111,111.11"].map((amount) => (
                  <div key={amount} className="text-right">{amount}</div>
                ))}
              </div>
              <div className="font-mono tnum text-sm text-ink">
                {["2025-06-30 23:59:59", "2025-07-01 00:00:00", "2025-12-31 08:15:42"].map((stamp) => (
                  <div key={stamp}>{stamp}</div>
                ))}
              </div>
              <div className="font-mono text-sm text-ink">0O · 1lI · 5S · 8B · {"->"} {"=>"} {"!="} {">="}</div>
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-xs font-medium text-muted">Weights in use — 400 / 500 / 600 (no bold anywhere)</h2>
            <div className="space-y-2 text-ink">
              <WeightRow family="var(--font-sans)" size="text-sm" />
              <WeightRow family="var(--font-sans)" size="text-xs" />
              <div className="text-xl font-semibold" style={{ fontFamily: "var(--font-display)" }}>
                Display 600 — Quarterly revenue, all regions
              </div>
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-xs font-medium text-muted">Current ramp — every size in the app and where it appears</h2>
            <div className="space-y-1.5">
              {CURRENT_RAMP.map((step) => (
                <div key={step.px} className="grid grid-cols-[3rem_7rem_1fr_1.2fr] items-baseline gap-3">
                  <span className="font-mono tnum text-2xs text-faint">{step.px}px</span>
                  <span className="font-mono text-2xs text-faint">{step.cls}</span>
                  <span className="truncate text-ink" style={{ fontSize: `${step.px}px`, lineHeight: 1.3 }}>{step.sample}</span>
                  <span className="text-2xs text-faint">{step.usage}</span>
                </div>
              ))}
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-xs font-medium text-muted">Size ramp candidates</h2>
            <div className="grid grid-cols-1 gap-8 lg:grid-cols-3">
              {RAMP_CANDIDATES.map((ramp) => (
                <article key={ramp.name} className="min-w-0">
                  <h3 className="text-sm font-semibold text-ink">{ramp.name}</h3>
                  <p className="mt-0.5 text-2xs text-faint">{ramp.note}</p>
                  <div className="mt-3 space-y-2">
                    <div className="text-faint" style={{ fontSize: ramp.caption, lineHeight: 1.3 }}>Caption {ramp.caption}px · vs previous month</div>
                    <div className="text-muted" style={{ fontSize: ramp.label, lineHeight: 1.35 }}>Label {ramp.label}px · Revenue by region</div>
                    <div className="text-ink" style={{ fontSize: ramp.body, lineHeight: 1.45 }}>Body {ramp.body}px · The overlay shifts the previous period onto the current axis so both lines share a scale.</div>
                    <div className="font-semibold text-ink" style={{ fontSize: ramp.section, lineHeight: 1.35 }}>Section title {ramp.section}px</div>
                    <div className="font-mono tnum font-semibold text-ink" style={{ fontSize: ramp.value, lineHeight: 1.15 }}>$288,291</div>
                    <div className="font-mono tnum font-semibold text-ink" style={{ fontSize: ramp.headline, lineHeight: 1.15 }}>$1,282,392</div>
                    <div className="font-semibold text-ink" style={{ fontFamily: "var(--font-display)", fontSize: ramp.title, lineHeight: 1.2, letterSpacing: "-0.025em" }}>Revenue overview</div>
                  </div>
                </article>
              ))}
            </div>
          </section>
        </div>
      </div>
    </main>
  );
}
