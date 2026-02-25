import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { openLix as openNewLix } from "@lix-js/sdk";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = join(__dirname, "..", "results");
const OUTPUT_PATH = join(OUTPUT_DIR, "json-insert-breakdown.bench.json");
const REPO_ROOT = join(__dirname, "..", "..", "..");

const JSON_LEAF_COUNT = parseEnvInt("BENCH_JSON_LEAF_COUNT", 120);
const ITERATIONS = parseEnvInt("BENCH_JSON_ITER", 24);
const WARMUP_ITERATIONS = parseEnvInt("BENCH_WARMUP", 1);
const SHOW_PROGRESS = process.env.BENCH_PROGRESS !== "0";

const MANIFEST = {
  key: "plugin_json",
  runtime: "wasm-component-v1",
  api_version: "0.1.0",
  match: { path_glob: "*.json" },
  entry: "plugin.wasm",
};

const SCENARIOS = [
  {
    key: "no_plugin_json_path",
    label: "No plugin installed (.json path)",
    installPlugin: false,
    pathExt: "json",
  },
  {
    key: "plugin_installed_non_matching_path",
    label: "Plugin installed (.txt path)",
    installPlugin: true,
    pathExt: "txt",
  },
  {
    key: "plugin_installed_matching_path",
    label: "Plugin installed (.json path)",
    installPlugin: true,
    pathExt: "json",
  },
];

async function main() {
  const startedAt = new Date().toISOString();
  const wasmBytes = await loadPluginJsonV2WasmBytes();

  const scenarioResults = [];
  for (const scenario of SCENARIOS) {
    const result = await runScenario(scenario, wasmBytes);
    scenarioResults.push(result);
  }

  const matching = scenarioResults.find((entry) => entry.key === "plugin_installed_matching_path");
  const noPlugin = scenarioResults.find((entry) => entry.key === "no_plugin_json_path");
  const nonMatching = scenarioResults.find(
    (entry) => entry.key === "plugin_installed_non_matching_path",
  );

  const report = {
    generatedAt: new Date().toISOString(),
    startedAt,
    config: {
      warmupIterations: WARMUP_ITERATIONS,
      iterations: ITERATIONS,
      jsonLeafCount: JSON_LEAF_COUNT,
    },
    scenarios: scenarioResults,
    derived: {
      matchingVsNoPlugin: safeRatio(noPlugin?.timing?.meanMs, matching?.timing?.meanMs),
      matchingVsInstalledNonMatching: safeRatio(nonMatching?.timing?.meanMs, matching?.timing?.meanMs),
    },
  };

  await mkdir(OUTPUT_DIR, { recursive: true });
  await writeFile(OUTPUT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

  printSummary(report);
  console.log(`\nWrote benchmark report: ${OUTPUT_PATH}`);
}

async function runScenario(scenario, wasmBytes) {
  const lix = await openNewLix({
    keyValues: [
      {
        key: "lix_deterministic_mode",
        value: { enabled: true },
        lixcol_version_id: "global",
      },
    ],
  });
  try {
    if (scenario.installPlugin) {
      await lix.installPlugin({ manifestJson: MANIFEST, wasmBytes });
    }

    let sequence = 0;
    for (let i = 0; i < WARMUP_ITERATIONS; i++) {
      await insertOne(lix, scenario.pathExt, sequence++);
    }

    if (SHOW_PROGRESS) {
      console.log(
        color(
          `[progress] ${scenario.label}: warmup ${WARMUP_ITERATIONS}, benchmark ${ITERATIONS}`,
          "dim",
        ),
      );
    }

    const durations = [];
    const every = Math.max(1, Math.floor(ITERATIONS / 6));
    for (let i = 0; i < ITERATIONS; i++) {
      const start = performance.now();
      await insertOne(lix, scenario.pathExt, sequence++);
      durations.push(performance.now() - start);

      if (SHOW_PROGRESS && (i + 1 === ITERATIONS || (i + 1) % every === 0)) {
        const pct = (((i + 1) / ITERATIONS) * 100).toFixed(0);
        console.log(color(`[progress] ${scenario.label}: ${i + 1}/${ITERATIONS} (${pct}%)`, "dim"));
      }
    }

    const pluginRowsResult = await lix.execute(
      "SELECT COUNT(*) FROM lix_state WHERE plugin_key = 'plugin_json'",
      [],
    );

    const fileRowsResult = await lix.execute("SELECT COUNT(*) FROM lix_file", []);

    return {
      key: scenario.key,
      label: scenario.label,
      installPlugin: scenario.installPlugin,
      pathExt: scenario.pathExt,
      timing: summarizeTiming(durations),
      pluginRowsTotal: scalarToNumber(pluginRowsResult.rows?.[0]?.[0], "plugin rows total"),
      fileRowsTotal: scalarToNumber(fileRowsResult.rows?.[0]?.[0], "file rows total"),
    };
  } finally {
    await lix.close();
  }
}

async function insertOne(lix, pathExt, sequence) {
  const fileId = `breakdown-${pathExt}-${sequence}`;
  const path = `/bench/${fileId}.${pathExt}`;
  const data = jsonBytes(sequence, JSON_LEAF_COUNT);
  await lix.execute("INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)", [fileId, path, data]);
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
    await runCommand("cargo", ["build", "--manifest-path", manifestPath, "--target", "wasm32-wasip2"]);
  } catch (error) {
    const message = String(error?.message ?? error);
    throw new Error(
      [
        "Failed to build plugin-json-v2 wasm.",
        "Install the target and rerun:",
        "  rustup target add wasm32-wasip2",
        `Original error: ${message}`,
      ].join("\n"),
    );
  }

  if (SHOW_PROGRESS) {
    console.log(color("[progress] plugin-json-v2 wasm build done", "dim"));
  }
}

function runCommand(command, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: REPO_ROOT,
      stdio: ["ignore", SHOW_PROGRESS ? "inherit" : "pipe", SHOW_PROGRESS ? "inherit" : "pipe"],
      env: process.env,
    });

    let stderr = "";
    if (!SHOW_PROGRESS && child.stderr) {
      child.stderr.on("data", (chunk) => {
        stderr += chunk.toString();
      });
    }

    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(stderr || `${command} ${args.join(" ")} failed with code ${code}`));
      }
    });
  });
}

function jsonBytes(seed, leafCount) {
  const payload = {};
  for (let i = 0; i < leafCount; i++) {
    payload[`k_${i}`] = `${seed}_${i}_${"x".repeat((i % 9) + 1)}`;
  }
  return Buffer.from(JSON.stringify(payload));
}

function parseEnvInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function scalarToNumber(value, label) {
  if (!value) {
    throw new Error(`Missing scalar for ${label}`);
  }

  if (typeof value === "number") {
    return value;
  }

  const kind = value.kind;
  const raw = value.value;

  if (kind === "Integer" || kind === "Real") {
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) {
      throw new Error(`Non-finite numeric scalar for ${label}: ${raw}`);
    }
    return parsed;
  }

  if (kind === "Text") {
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) {
      throw new Error(`Text scalar is not numeric for ${label}: ${raw}`);
    }
    return parsed;
  }

  throw new Error(`Unexpected scalar kind for ${label}: ${kind}`);
}

function safeRatio(base, target) {
  if (!Number.isFinite(base) || !Number.isFinite(target) || target <= 0) {
    return null;
  }
  return base / target;
}

function printSummary(report) {
  const lines = [
    "",
    "+--------------------------------------------------------------------+",
    "| LIX JSON Insert Breakdown (new @lix-js/sdk)                        |",
    "+--------------------------------------------------------------------+",
    "Lower is faster.",
    `Payload size: ${report.config.jsonLeafCount} JSON leaves`,
    `Warmup iterations: ${report.config.warmupIterations}`,
    "",
  ];

  for (const scenario of report.scenarios) {
    lines.push(`${scenario.label}`);
    lines.push(`  mean: ${scenario.timing.meanMs.toFixed(2)}ms`);
    lines.push(`  min/max: ${scenario.timing.minMs.toFixed(2)}ms / ${scenario.timing.maxMs.toFixed(2)}ms`);
    lines.push(`  plugin rows total: ${scenario.pluginRowsTotal}`);
    lines.push(`  file rows total:   ${scenario.fileRowsTotal}`);
    lines.push("");
  }

  if (report.derived.matchingVsNoPlugin !== null) {
    lines.push(
      `matching(.json) vs no-plugin speedup: ${report.derived.matchingVsNoPlugin.toFixed(2)}x`,
    );
  }
  if (report.derived.matchingVsInstalledNonMatching !== null) {
    lines.push(
      `matching(.json) vs installed(.txt) speedup: ${report.derived.matchingVsInstalledNonMatching.toFixed(2)}x`,
    );
  }

  console.log(lines.join("\n"));
}

const ANSI = {
  reset: "\u001b[0m",
  dim: "\u001b[2m",
};

function color(text, type) {
  const forceColor = process.env.BENCH_FORCE_COLOR === "1";
  const useColor = forceColor || process.stdout.isTTY;
  if (!useColor) {
    return text;
  }
  const prefix = ANSI[type] || "";
  return `${prefix}${text}${ANSI.reset}`;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
