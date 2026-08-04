/**
 * Fetches weekly npm download counts for @lix-js/sdk and writes them to
 * src/npm-downloads.gen.json.
 *
 * The website renders the download chart from this static file. A scheduled
 * GitHub Actions workflow (.github/workflows/update-download-stats.yml) runs
 * this script weekly and commits the result, which triggers a redeploy.
 *
 * Values are intentionally coarse: full Monday-Sunday weeks only, rounded to
 * the nearest thousand.
 *
 * Usage: node website/scripts/update-npm-downloads.mjs
 */
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";

const PACKAGE = "@lix-js/sdk";
const WEEKS = 53;
const OUTPUT_PATH = fileURLToPath(
  new URL("../src/npm-downloads.gen.json", import.meta.url),
);

function toIsoDate(date) {
  return date.toISOString().slice(0, 10);
}

/** Most recent completed Sunday (UTC), exclusive of today. */
function lastCompletedSunday(now) {
  const date = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()),
  );
  // getUTCDay(): 0 = Sunday. Step back to the previous Sunday, never today.
  const daysBack = date.getUTCDay() === 0 ? 7 : date.getUTCDay();
  date.setUTCDate(date.getUTCDate() - daysBack);
  return date;
}

const now = new Date();
const end = lastCompletedSunday(now);
const start = new Date(end);
start.setUTCDate(start.getUTCDate() - (WEEKS * 7 - 1));

const url = `https://api.npmjs.org/downloads/range/${toIsoDate(start)}:${toIsoDate(end)}/${PACKAGE}`;
const res = await fetch(url);
if (!res.ok) {
  throw new Error(`npm downloads fetch failed: ${res.status} ${url}`);
}
const body = await res.json();
const byDay = new Map(
  (body.downloads ?? []).map((entry) => [entry.day, entry.downloads]),
);

const weeks = [];
for (let i = 0; i < WEEKS; i++) {
  const weekStart = new Date(start);
  weekStart.setUTCDate(weekStart.getUTCDate() + i * 7);
  let total = 0;
  let hasData = false;
  for (let d = 0; d < 7; d++) {
    const day = new Date(weekStart);
    day.setUTCDate(day.getUTCDate() + d);
    const value = byDay.get(toIsoDate(day));
    if (value !== undefined) hasData = true;
    total += value ?? 0;
  }
  const weekEnding = new Date(weekStart);
  weekEnding.setUTCDate(weekEnding.getUTCDate() + 6);
  // Skip leading weeks from before the package existed.
  if (!hasData && weeks.length === 0) continue;
  weeks.push({
    weekEnding: toIsoDate(weekEnding),
    downloads: Math.round(total / 1000) * 1000,
  });
}

const latest = weeks[weeks.length - 1]?.downloads ?? 0;

const payload = {
  generatedAt: now.toISOString(),
  package: PACKAGE,
  latestWeeklyDownloads: latest,
  weeks,
};

await fs.writeFile(OUTPUT_PATH, JSON.stringify(payload, null, 2) + "\n");
console.log(
  `Wrote ${weeks.length} weeks for ${PACKAGE} (latest full week: ${latest.toLocaleString("en-US")}) to ${OUTPUT_PATH}`,
);
