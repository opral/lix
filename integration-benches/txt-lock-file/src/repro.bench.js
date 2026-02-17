import { access, mkdir, readFile, writeFile } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { performance } from "node:perf_hooks";
import { openLix, createWasmSqliteBackend } from "js-sdk";

const execFileAsync = promisify(execFile);

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..", "..");
const RESULTS_DIR = join(__dirname, "..", "results");

const DEFAULT_REPO_PATH = join(
  REPO_ROOT,
  "packages",
  "nextjs-replay-bench",
  ".cache",
  "nextjs-replay",
  "next.js",
);
const DEFAULT_TARGET_COMMIT = "29c226771ce8b5b26632c8e7753e69f7407933b4";
const DEFAULT_FILE_PATH = "yarn.lock";
const DEFAULT_OUTPUT_PATH = join(RESULTS_DIR, "text-lines-repro.bench.json");

const TEXT_LINES_MANIFEST = {
  key: "plugin_text_lines",
  runtime: "wasm-component-v1",
  api_version: "0.1.0",
  detect_changes_glob: "**/*",
  entry: "plugin.wasm",
};

async function main() {
  const config = {
    repoPath: process.env.BENCH_REPO_PATH ?? DEFAULT_REPO_PATH,
    targetCommit: process.env.BENCH_TARGET_COMMIT ?? DEFAULT_TARGET_COMMIT,
    filePath: process.env.BENCH_FILE_PATH ?? DEFAULT_FILE_PATH,
    iterations: parsePositiveInt("BENCH_ITERATIONS", 20),
    warmup: parseNonNegativeInt("BENCH_WARMUP", 4),
    pluginMode: parsePluginMode(process.env.BENCH_PLUGIN_MODE ?? "both"),
    outputPath: process.env.BENCH_OUTPUT_PATH ?? DEFAULT_OUTPUT_PATH,
  };

  await assertRepoExists(config.repoPath);

  const parentCommit = (
    await runGit(config.repoPath, ["rev-parse", `${config.targetCommit}^`], "utf8")
  ).trim();
  const beforeBytes = await runGit(
    config.repoPath,
    ["show", `${parentCommit}:${config.filePath}`],
    "buffer",
  );
  const afterBytes = await runGit(
    config.repoPath,
    ["show", `${config.targetCommit}:${config.filePath}`],
    "buffer",
  );

  const scenarios = buildScenarios(config.pluginMode);
  const results = [];
  for (const scenario of scenarios) {
    results.push(
      await runScenario({
        scenario,
        beforeBytes: new Uint8Array(beforeBytes),
        afterBytes: new Uint8Array(afterBytes),
        config,
      }),
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    config: {
      ...config,
      parentCommit,
      beforeBytes: beforeBytes.byteLength,
      afterBytes: afterBytes.byteLength,
      beforeLineCount: countLines(beforeBytes),
      afterLineCount: countLines(afterBytes),
    },
    scenarios: results,
  };

  await mkdir(dirname(config.outputPath), { recursive: true });
  await writeFile(config.outputPath, JSON.stringify(report, null, 2));

  printSummary(report);
  console.log(`Wrote benchmark report: ${config.outputPath}`);
}

async function runScenario({ scenario, beforeBytes, afterBytes, config }) {
  const backend = await createWasmSqliteBackend();
  const lix = await openLix({
    backend,
    keyValues: [
      {
        key: "lix_deterministic_mode",
        value: { enabled: true },
        lixcol_version_id: "global",
      },
    ],
  });

  try {
    if (scenario.installTextLinesPlugin) {
      const wasmBytes = await loadTextLinesPluginWasmBytes();
      await lix.installPlugin({
        manifestJson: TEXT_LINES_MANIFEST,
        wasmBytes,
      });
    }

    const fileId = "bench-yarn-lock";
    await lix.execute(
      "INSERT INTO lix_file (id, path, data) VALUES ('bench-yarn-lock', '/yarn.lock', ?)",
      [beforeBytes],
    );

    const warmupSamples = [];
    let current = "before";
    for (let i = 0; i < config.warmup; i++) {
      const target = current === "before" ? afterBytes : beforeBytes;
      warmupSamples.push(await timeUpdate(lix, target, fileId));
      current = current === "before" ? "after" : "before";
    }

    const measuredSamples = [];
    for (let i = 0; i < config.iterations; i++) {
      const target = current === "before" ? afterBytes : beforeBytes;
      measuredSamples.push(await timeUpdate(lix, target, fileId));
      current = current === "before" ? "after" : "before";
    }

    const textLineCount = scenario.installTextLinesPlugin
      ? await queryScalarNumber(
          lix,
          "SELECT COUNT(*) FROM lix_state_by_version WHERE file_id = 'bench-yarn-lock' AND schema_key = 'text_line'",
        )
      : null;
    const textDocumentCount = scenario.installTextLinesPlugin
      ? await queryScalarNumber(
          lix,
          "SELECT COUNT(*) FROM lix_state_by_version WHERE file_id = 'bench-yarn-lock' AND schema_key = 'text_document'",
        )
      : null;

    return {
      scenario: scenario.name,
      installTextLinesPlugin: scenario.installTextLinesPlugin,
      warmup: summarizeSamples(warmupSamples),
      measured: summarizeSamples(measuredSamples),
      textLineCount,
      textDocumentCount,
    };
  } finally {
    await lix.close();
  }
}

async function timeUpdate(lix, bytes, fileId) {
  const started = performance.now();
  await lix.execute(
    "UPDATE lix_file SET path = '/yarn.lock', data = ? WHERE id = 'bench-yarn-lock'",
    [bytes],
  );
  const elapsedMs = performance.now() - started;

  const idCheck = await lix.execute(
    "SELECT id FROM lix_file WHERE id = ? LIMIT 1",
    [fileId],
  );
  if ((idCheck.rows?.length ?? 0) !== 1) {
    throw new Error("invariant failed: benchmark file missing after update");
  }
  return elapsedMs;
}

function summarizeSamples(samples) {
  if (!samples.length) {
    return { count: 0, meanMs: 0, minMs: 0, maxMs: 0, p50Ms: 0, p95Ms: 0 };
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const total = sorted.reduce((sum, value) => sum + value, 0);
  return {
    count: sorted.length,
    meanMs: total / sorted.length,
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
  };
}

function percentile(sorted, p) {
  const index = Math.floor((sorted.length - 1) * p);
  return sorted[index];
}

function buildScenarios(mode) {
  if (mode === "on") {
    return [{ name: "plugin:on", installTextLinesPlugin: true }];
  }
  if (mode === "off") {
    return [{ name: "plugin:off", installTextLinesPlugin: false }];
  }
  return [
    { name: "plugin:on", installTextLinesPlugin: true },
    { name: "plugin:off", installTextLinesPlugin: false },
  ];
}

function parsePositiveInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got '${raw}'`);
  }
  return parsed;
}

function parseNonNegativeInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer, got '${raw}'`);
  }
  return parsed;
}

function parsePluginMode(raw) {
  const normalized = String(raw).toLowerCase();
  if (normalized === "on" || normalized === "off" || normalized === "both") {
    return normalized;
  }
  throw new Error(`BENCH_PLUGIN_MODE must be one of on|off|both, got '${raw}'`);
}

async function assertRepoExists(repoPath) {
  try {
    await access(join(repoPath, ".git"), fsConstants.F_OK);
  } catch {
    throw new Error(
      `repository path does not exist or is not a git repo: ${repoPath}`,
    );
  }
}

async function runGit(repoPath, args, output = "utf8") {
  const { stdout } = await execFileAsync("git", args, {
    cwd: repoPath,
    maxBuffer: 64 * 1024 * 1024,
    encoding: output === "buffer" ? "buffer" : "utf8",
  });
  return stdout;
}

async function loadTextLinesPluginWasmBytes() {
  const packageReleasePath = join(
    REPO_ROOT,
    "packages",
    "plugin-text-lines",
    "target",
    "wasm32-wasip2",
    "release",
    "plugin_text_lines.wasm",
  );
  const workspaceReleasePath = join(
    REPO_ROOT,
    "target",
    "wasm32-wasip2",
    "release",
    "plugin_text_lines.wasm",
  );
  const packageDebugPath = join(
    REPO_ROOT,
    "packages",
    "plugin-text-lines",
    "target",
    "wasm32-wasip2",
    "debug",
    "plugin_text_lines.wasm",
  );
  const workspaceDebugPath = join(
    REPO_ROOT,
    "target",
    "wasm32-wasip2",
    "debug",
    "plugin_text_lines.wasm",
  );

  for (const path of [
    packageReleasePath,
    workspaceReleasePath,
    packageDebugPath,
    workspaceDebugPath,
  ]) {
    try {
      return new Uint8Array(await readFile(path));
    } catch {
      // continue
    }
  }

  throw new Error(
    "plugin_text_lines.wasm not found. Build it first (cargo build --release --manifest-path packages/plugin-text-lines/Cargo.toml --target wasm32-wasip2).",
  );
}

async function queryScalarNumber(lix, sql) {
  const result = await lix.execute(sql, []);
  const value = result.rows?.[0]?.[0];
  return scalarToNumber(value, sql);
}

function scalarToNumber(value, context) {
  if (value === null || value === undefined) {
    throw new Error(`missing scalar for query: ${context}`);
  }
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string") return Number(value);
  if (typeof value === "object") {
    if (value.kind === "Integer" || value.kind === "Real" || value.kind === "Text") {
      return Number(value.value);
    }
  }
  throw new Error(`unsupported scalar value: ${JSON.stringify(value)}`);
}

function countLines(bytes) {
  let lines = 0;
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] === 10) lines += 1;
  }
  if (bytes.length > 0 && bytes[bytes.length - 1] !== 10) {
    lines += 1;
  }
  return lines;
}

function printSummary(report) {
  console.log("");
  console.log("Text-Lines Repro Benchmark");
  console.log(`target: ${report.config.targetCommit.slice(0, 12)} ${report.config.filePath}`);
  console.log(
    `blob sizes: before=${report.config.beforeBytes}B after=${report.config.afterBytes}B`,
  );
  console.log(
    `line counts: before=${report.config.beforeLineCount} after=${report.config.afterLineCount}`,
  );
  console.log(
    `iterations: warmup=${report.config.warmup} measured=${report.config.iterations}`,
  );
  for (const scenario of report.scenarios) {
    console.log("");
    console.log(`Scenario ${scenario.scenario}`);
    console.log(
      `  mean=${scenario.measured.meanMs.toFixed(3)}ms p50=${scenario.measured.p50Ms.toFixed(3)}ms p95=${scenario.measured.p95Ms.toFixed(3)}ms max=${scenario.measured.maxMs.toFixed(3)}ms`,
    );
    console.log(
      `  text_line=${scenario.textLineCount ?? "n/a"} text_document=${scenario.textDocumentCount ?? "n/a"}`,
    );
  }
}

main().catch((error) => {
  console.error("text-lines repro benchmark failed:");
  console.error(error);
  process.exitCode = 1;
});
