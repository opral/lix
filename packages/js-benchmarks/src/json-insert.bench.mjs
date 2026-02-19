import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { openLix as openOldLix } from "@lix-js/sdk";
import { plugin as legacyJsonPlugin } from "@lix-js/plugin-json";
import { openLix as openNewLix } from "js-sdk";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = join(__dirname, "..", "results");
const OUTPUT_PATH = join(OUTPUT_DIR, "json-insert.bench.json");
const REPO_ROOT = join(__dirname, "..", "..", "..");

const JSON_LEAF_COUNT = parseEnvInt("BENCH_JSON_LEAF_COUNT", 120);
const ITERATIONS = parseEnvInt("BENCH_JSON_ITER", 30);
const WARMUP_ITERATIONS = parseEnvInt("BENCH_WARMUP", 2);
const REQUIRE_PLUGIN_EXEC = process.env.BENCH_REQUIRE_PLUGIN_EXEC === "1";
const SHOW_PROGRESS = process.env.BENCH_PROGRESS !== "0";

const OPERATION = {
  key: "json_file_insert",
  label: `JSON file insert (${JSON_LEAF_COUNT} leaves)`,
};

const MANIFEST = {
  key: "plugin_json",
  runtime: "wasm-component-v1",
  api_version: "0.1.0",
  match: { path_glob: "*.json" },
  entry: "plugin.wasm",
};

async function main() {
  const startedAt = new Date().toISOString();
  const oldAdapter = await setupOldAdapter();
  const newAdapter = await setupNewAdapter();

  try {
    const oldResult = await runAdapter(oldAdapter);
    const newResult = await runAdapter(newAdapter);

    const speedupNewOverOld = oldResult.timing.meanMs / newResult.timing.meanMs;
    const comparablePluginExecution =
      oldResult.pluginRows.meanPerFile > 0 && newResult.pluginRows.meanPerFile > 0;

    const warnings = [];
    if (oldResult.pluginRows.meanPerFile <= 0) {
      warnings.push(
        "Legacy adapter produced 0 plugin rows/file. JSON plugin did not run as expected.",
      );
    }
    if (newResult.pluginRows.meanPerFile <= 0) {
      warnings.push(
        "New js-sdk adapter produced 0 plugin rows/file. Plugin execution did not produce state rows.",
      );
    }

    if (REQUIRE_PLUGIN_EXEC && warnings.length > 0) {
      throw new Error(
        [
          "BENCH_REQUIRE_PLUGIN_EXEC=1 was set, but plugin execution check failed.",
          ...warnings,
        ].join("\n"),
      );
    }

    const report = {
      generatedAt: new Date().toISOString(),
      startedAt,
      operation: OPERATION,
      config: {
        warmupIterations: WARMUP_ITERATIONS,
        iterations: ITERATIONS,
        jsonLeafCount: JSON_LEAF_COUNT,
        requirePluginExecution: REQUIRE_PLUGIN_EXEC,
      },
      oldSdk: oldResult,
      newJsSdk: newResult,
      speedupNewOverOld,
      comparablePluginExecution,
      warnings,
    };

    await mkdir(OUTPUT_DIR, { recursive: true });
    await writeFile(OUTPUT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

    printSummary(report);
    console.log(`\nWrote benchmark report: ${OUTPUT_PATH}`);
  } finally {
    await oldAdapter.close();
    await newAdapter.close();
  }
}

async function setupOldAdapter() {
  const lix = await openOldLix({
    providePlugins: [legacyJsonPlugin],
    keyValues: [
      {
        key: "lix_deterministic_mode",
        value: { enabled: true },
        lixcol_version_id: "global",
        lixcol_untracked: true,
      },
    ],
  });

  return {
    name: "old @lix-js/sdk",
    async insertOne(sequence) {
      const fileId = `legacy-json-${sequence}`;
      const path = `/bench/legacy-${sequence}.json`;
      const data = jsonBytes(sequence, JSON_LEAF_COUNT);

      await lix.db.executeQuery({
        sql: "INSERT INTO file (id, path, data) VALUES (?, ?, ?)",
        parameters: [fileId, path, data],
      });

      const countResult = await lix.db.executeQuery({
        sql: "SELECT COUNT(*) AS c FROM state WHERE file_id = ? AND plugin_key = 'plugin_json'",
        parameters: [fileId],
      });

      return Number(countResult.rows?.[0]?.c ?? 0);
    },
    async close() {
      await lix.close();
    },
  };
}

async function setupNewAdapter() {
  const lix = await openNewLix({
    keyValues: [
      {
        key: "lix_deterministic_mode",
        value: { enabled: true },
        lixcol_version_id: "global",
      },
    ],
  });
  const wasmBytes = await loadPluginJsonV2WasmBytes();

  await lix.installPlugin({
    manifestJson: MANIFEST,
    wasmBytes,
  });

  return {
    name: "new js-sdk",
    async insertOne(sequence) {
      const fileId = `new-json-${sequence}`;
      const path = `/bench/new-${sequence}.json`;
      const data = jsonBytes(sequence, JSON_LEAF_COUNT);

      await lix.execute("INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)", [
        fileId,
        path,
        data,
      ]);

      const countResult = await lix.execute(
        "SELECT COUNT(*) FROM lix_state WHERE file_id = ? AND plugin_key = 'plugin_json'",
        [fileId],
      );

      return scalarToNumber(countResult.rows?.[0]?.[0], "lix_state plugin row count");
    },
    async close() {
      await lix.close();
    },
  };
}

async function runAdapter(adapter) {
  if (SHOW_PROGRESS) {
    console.log(
      color(
        `[progress] ${adapter.name}: warmup ${WARMUP_ITERATIONS}, benchmark ${ITERATIONS}`,
        "dim",
      ),
    );
  }

  let sequence = 0;
  for (let i = 0; i < WARMUP_ITERATIONS; i++) {
    await adapter.insertOne(sequence++);
  }

  const durations = [];
  const pluginRows = [];
  const every = Math.max(1, Math.floor(ITERATIONS / 6));
  for (let i = 0; i < ITERATIONS; i++) {
    const start = performance.now();
    const rowCount = await adapter.insertOne(sequence++);
    durations.push(performance.now() - start);
    pluginRows.push(rowCount);
    if (SHOW_PROGRESS && (i + 1 === ITERATIONS || (i + 1) % every === 0)) {
      const pct = (((i + 1) / ITERATIONS) * 100).toFixed(0);
      console.log(color(`[progress] ${adapter.name}: ${i + 1}/${ITERATIONS} (${pct}%)`, "dim"));
    }
  }

  return {
    adapter: adapter.name,
    timing: summarizeTiming(durations),
    pluginRows: summarizePluginRows(pluginRows),
  };
}

function summarizeTiming(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const meanMs = samples.reduce((sum, value) => sum + value, 0) / samples.length;
  return {
    meanMs,
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    sampleCount: samples.length,
  };
}

function summarizePluginRows(rows) {
  const sorted = [...rows].sort((a, b) => a - b);
  const meanPerFile = rows.reduce((sum, value) => sum + value, 0) / rows.length;
  return {
    meanPerFile,
    minPerFile: sorted[0],
    maxPerFile: sorted[sorted.length - 1],
  };
}

async function loadPluginJsonV2WasmBytes() {
  const debugPath = join(
    REPO_ROOT,
    "packages",
    "plugin-json-v2",
    "target",
    "wasm32-wasip2",
    "debug",
    "plugin_json_v2.wasm",
  );

  try {
    return await readFile(debugPath);
  } catch {
    await ensurePluginJsonV2WasmBuilt();
    return await readFile(debugPath);
  }
}

async function ensurePluginJsonV2WasmBuilt() {
  const manifestPath = join(REPO_ROOT, "packages", "plugin-json-v2", "Cargo.toml");
  if (SHOW_PROGRESS) {
    console.log(color("[progress] building plugin-json-v2 wasm (wasm32-wasip2)", "dim"));
  }

  try {
    await runCommand("cargo", [
      "build",
      "--manifest-path",
      manifestPath,
      "--target",
      "wasm32-wasip2",
    ]);
  } catch (error) {
    const message = String(error?.message ?? error);
    if (
      message.includes("wasm32-wasip2") &&
      (message.includes("can't find crate for `core`") ||
        message.includes("target may not be installed"))
    ) {
      await runCommand("rustup", ["target", "add", "wasm32-wasip2"]);
      await runCommand("cargo", [
        "build",
        "--manifest-path",
        manifestPath,
        "--target",
        "wasm32-wasip2",
      ]);
      return;
    }
    throw error;
  }

  if (SHOW_PROGRESS) {
    console.log(color("[progress] plugin-json-v2 wasm build done", "dim"));
  }
}

async function runCommand(cmd, args) {
  await new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${cmd} ${args.join(" ")} failed:\n${stderr}`));
      }
    });
  });
}

function jsonBytes(sequence, leafCount) {
  const payload = createPayload(sequence, leafCount);
  return new TextEncoder().encode(JSON.stringify(payload));
}

function createPayload(sequence, leafCount) {
  const values = {};
  for (let i = 0; i < leafCount; i++) {
    const key = `k_${String(i).padStart(4, "0")}`;
    values[key] = (sequence + i) % 2 === 0 ? `value_${sequence}_${i}` : sequence + i;
  }

  return {
    meta: {
      id: sequence,
      version: 1,
    },
    values,
  };
}

function scalarToNumber(value, context) {
  if (value === null || value === undefined) {
    throw new Error(`Missing scalar value for ${context}`);
  }
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "string") {
    return Number(value);
  }
  if (typeof value === "object") {
    if (
      value.kind === "Integer" ||
      value.kind === "Real" ||
      value.kind === "Text"
    ) {
      return Number(value.value);
    }
  }
  throw new Error(`Unsupported scalar value for ${context}: ${JSON.stringify(value)}`);
}

function parseEnvInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got: ${raw}`);
  }
  return parsed;
}

function formatMs(value) {
  return `${value.toFixed(2)}ms`;
}

function printSummary(report) {
  const width = 70;
  const title = "LIX JSON Plugin Insert Bench";
  const border = `+${"-".repeat(width - 2)}+`;
  console.log("");
  console.log(color(border, "dim"));
  console.log(color(`| ${title.padEnd(width - 4)} |`, "dim"));
  console.log(color(border, "dim"));
  console.log(color("Lower is faster.", "dim"));
  console.log(color(`Payload size: ${report.config.jsonLeafCount} JSON leaves`, "dim"));
  console.log(color(`Warmup iterations: ${report.config.warmupIterations}`, "dim"));

  console.log("");
  console.log(color(report.operation.label, "bold"));
  console.log(`  old @lix-js/sdk: ${color(formatMs(report.oldSdk.timing.meanMs), "red")}`);
  console.log(`  new js-sdk:      ${color(formatMs(report.newJsSdk.timing.meanMs), "green")}`);
  console.log(color("Summary", "bold"));
  console.log(
    `  new js-sdk ran ${color(`${report.speedupNewOverOld.toFixed(2)}x`, "green")} faster than old @lix-js/sdk`,
  );
  console.log(
    `  plugin rows/file (old): ${report.oldSdk.pluginRows.meanPerFile.toFixed(1)}`,
  );
  console.log(
    `  plugin rows/file (new): ${report.newJsSdk.pluginRows.meanPerFile.toFixed(1)}`,
  );

  if (report.warnings.length > 0) {
    console.log("");
    console.log(color("Warnings", "bold"));
    for (const warning of report.warnings) {
      console.log(`  ${color("-", "yellow")} ${color(warning, "yellow")}`);
    }
  }
}

const ANSI = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
};

function color(text, style) {
  const forceColor = process.env.BENCH_FORCE_COLOR === "1";
  const supportsColor = forceColor || (!process.env.NO_COLOR && Boolean(process.stdout?.isTTY));
  if (!supportsColor) {
    return text;
  }
  const code = ANSI[style];
  if (!code) {
    return text;
  }
  return `${code}${text}${ANSI.reset}`;
}

main().catch((error) => {
  console.error("Benchmark run failed:");
  console.error(error);
  process.exitCode = 1;
});
