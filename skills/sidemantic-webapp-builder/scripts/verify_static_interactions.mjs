#!/usr/bin/env node
// Browser smoke tests for Sidemantic static dashboards.
//
// Run with Playwright available, for example:
// bunx --bun -p playwright node skills/sidemantic-webapp-builder/scripts/verify_static_interactions.mjs --url http://127.0.0.1:4519/

import process from "node:process";

function parseArgs(argv) {
  const args = {
    headless: true,
    timeout: 10000,
  };
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--url") {
      args.url = argv[++index];
    } else if (arg === "--headed") {
      args.headless = false;
    } else if (arg === "--timeout") {
      args.timeout = Number(argv[++index]);
    } else if (arg === "--help" || arg === "-h") {
      args.help = true;
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  return args;
}

function usage() {
  return `Usage:
  bunx --bun -p playwright node skills/sidemantic-webapp-builder/scripts/verify_static_interactions.mjs --url http://127.0.0.1:4519/

Options:
  --url <url>       App URL to test.
  --headed          Run a headed browser.
  --timeout <ms>    Per-action timeout. Default: 10000.
`;
}

async function safeText(locator) {
  if ((await locator.count()) === 0) return "";
  return locator.first().innerText();
}

async function snapshot(page) {
  return {
    metricText: await safeText(page.locator('[data-testid="metric-totals"]')),
    leaderboardText: await safeText(page.locator('[data-testid="dimension-leaderboard"]')),
    previewText: await safeText(page.locator('[data-testid="data-preview"]')),
    filterCount: await page.locator('[data-testid="filter-pills"] [data-dimension]').count(),
    selectedMetricCount: await page.locator('[data-testid="metric-totals"] [data-selected="true"]').count(),
    selectedRowCount: await page.locator('[data-testid="leaderboard-rows"] [data-selected="true"]').count(),
    columnChartCount: await page.locator("svg.sdm-column-chart").count(),
    sparklineCount: await page.locator("svg.sdm-sparkline").count(),
  };
}

function changed(before, after, fields) {
  return fields.some((field) => before[field] !== after[field]);
}

async function expectChange(name, before, after, fields) {
  if (!changed(before, after, fields)) {
    throw new Error(`${name} did not change any of: ${fields.join(", ")}`);
  }
}

async function assertNoPersistentStateGallery(page) {
  const stateTexts = [
    "Loading: metrics are refreshing with stable layout.",
    "Empty: no rows for the current filter set.",
    "Error: query failed or returned an invalid result shape.",
  ];
  const counts = await Promise.all(stateTexts.map((text) => page.getByText(text, { exact: true }).count()));
  if (counts.every((count) => count > 0)) {
    throw new Error("Loading, empty, and error states are all rendered as persistent dashboard content.");
  }
}

async function assertChartsBounded(page) {
  const chartState = await page.locator("svg.sdm-sparkline, svg.sdm-column-chart").evaluateAll((charts) =>
    charts.map((chart) => {
      const style = globalThis.getComputedStyle(chart);
      const rect = chart.getBoundingClientRect();
      return {
        className: chart.getAttribute("class") || "",
        overflow: style.overflow,
        width: rect.width,
        height: rect.height,
        hasViewBox: chart.hasAttribute("viewBox"),
      };
    }),
  );
  for (const chart of chartState) {
    if (!chart.hasViewBox || chart.overflow !== "hidden" || chart.width <= 0 || chart.height <= 0) {
      throw new Error(`Chart is not bounded: ${JSON.stringify(chart)}`);
    }
  }
}

async function clickFirstFilterRemove(page, timeout) {
  const removeButtons = page.locator('[data-testid="filter-pills"] button');
  if ((await removeButtons.count()) === 0) return { skipped: true, reason: "no removable filter pills" };
  const before = await snapshot(page);
  await removeButtons.first().click({ timeout });
  const after = await snapshot(page);
  await expectChange("Removing a filter pill", before, after, [
    "metricText",
    "leaderboardText",
    "previewText",
    "filterCount",
  ]);
  return { skipped: false, before, after };
}

async function clickLeaderboardRow(page, timeout) {
  const rows = page.locator('[data-testid="leaderboard-rows"] button[data-dimension]');
  if ((await rows.count()) === 0) return { skipped: true, reason: "no interactive leaderboard rows" };
  const before = await snapshot(page);
  await rows.first().click({ timeout });
  const after = await snapshot(page);
  if (after.selectedRowCount < 1) {
    throw new Error("Clicking a leaderboard row did not mark any row selected.");
  }
  await expectChange("Clicking a leaderboard row", before, after, [
    "metricText",
    "previewText",
    "filterCount",
    "selectedRowCount",
  ]);
  return { skipped: false, before, after };
}

async function clickMetricCard(page, timeout) {
  const metrics = page.locator('[data-testid="metric-totals"] button[data-metric]');
  if ((await metrics.count()) < 2) return { skipped: true, reason: "fewer than two interactive metric cards" };
  const before = await snapshot(page);
  await metrics.nth(1).click({ timeout });
  const after = await snapshot(page);
  if (after.selectedMetricCount < 1) {
    throw new Error("Clicking a metric card did not mark any metric selected.");
  }
  await expectChange("Clicking a metric card", before, after, ["leaderboardText", "selectedMetricCount"]);
  return { skipped: false, before, after };
}

async function clickReset(page, timeout) {
  const reset = page.locator('[data-action="reset"], button:has-text("Reset filters")');
  if ((await reset.count()) === 0) return { skipped: true, reason: "no reset control" };
  const before = await snapshot(page);
  await reset.first().click({ timeout });
  const after = await snapshot(page);
  await expectChange("Clicking reset", before, after, ["metricText", "previewText", "filterCount", "selectedMetricCount"]);
  return { skipped: false, before, after };
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    console.log(usage());
    return 0;
  }
  if (!args.url) {
    throw new Error("--url is required.\n" + usage());
  }

  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: args.headless });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  const consoleErrors = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => {
    consoleErrors.push(error.message);
  });

  try {
    await page.goto(args.url, { waitUntil: "load", timeout: args.timeout });
    await page.locator('[data-testid="metric-totals"]').waitFor({ timeout: args.timeout });

    const initial = await snapshot(page);
    await assertNoPersistentStateGallery(page);
    await assertChartsBounded(page);
    const filter = await clickFirstFilterRemove(page, args.timeout);
    const leaderboard = await clickLeaderboardRow(page, args.timeout);
    const metric = await clickMetricCard(page, args.timeout);
    const reset = await clickReset(page, args.timeout);
    const final = await snapshot(page);

    if (consoleErrors.length > 0) {
      throw new Error(`Console errors: ${consoleErrors.join(" | ")}`);
    }

    console.log(
      JSON.stringify(
        {
          ok: true,
          url: args.url,
          initial,
          checks: {
            boundedCharts: true,
            noPersistentStateGallery: true,
            filter,
            leaderboard,
            metric,
            reset,
          },
          final,
        },
        null,
        2,
      ),
    );
    return 0;
  } finally {
    await browser.close();
  }
}

main()
  .then((code) => {
    process.exitCode = code;
  })
  .catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
