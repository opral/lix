import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { openLix as openOldLix } from "@lix-js/sdk";
import { openLix as openNewLix } from "js-sdk/dist/open-lix.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = join(__dirname, "..", "results");
const OUTPUT_PATH = join(OUTPUT_DIR, "state-insert.bench.json");

const FILE_ID = "bench_insert_file";
const PLUGIN_KEY = "bench_insert_plugin";
const BENCH_SCHEMA_KEY = "bench_vtable_insert_schema";

const BENCH_OPERATIONS = [
  {
    key: "single_row_insert",
    label: "Single row insert",
    chunkSize: 1,
    iterations: parseEnvInt("BENCH_ITER_SINGLE", 300),
  },
  {
    key: "chunk_10_insert",
    label: "10-row chunk insert",
    chunkSize: 10,
    iterations: parseEnvInt("BENCH_ITER_CHUNK_10", 120),
  },
  {
    key: "chunk_100_insert",
    label: "100-row chunk insert",
    chunkSize: 100,
    iterations: parseEnvInt("BENCH_ITER_CHUNK_100", 30),
  },
];

const WARMUP_ITERATIONS = parseEnvInt("BENCH_WARMUP", 3);

const oldSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    source: { type: "string" },
    version: { type: "string" },
    idx: { type: "integer" },
  },
  required: ["source"],
  "x-lix-key": BENCH_SCHEMA_KEY,
  "x-lix-version": "1.0",
  "x-lix-primary-key": ["/source"],
};

const newSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    source: { type: "string" },
    version: { type: "string" },
    idx: { type: "integer" },
  },
  required: ["source"],
  "x-lix-key": BENCH_SCHEMA_KEY,
  "x-lix-version": "1",
  "x-lix-primary-key": ["/source"],
};

async function main() {
  const startedAt = new Date().toISOString();

  const oldAdapter = await setupOldAdapter();
  const newAdapter = await setupNewAdapter();

  try {
    const results = [];
    for (const operation of BENCH_OPERATIONS) {
      const oldResult = await runOperation(oldAdapter, operation);
      const newResult = await runOperation(newAdapter, operation);
      const speedup = oldResult.meanMs / newResult.meanMs;
      results.push({
        operation,
        oldSdk: oldResult,
        newJsSdk: newResult,
        newBaseline: {
          meanMs: newResult.meanMs,
        },
        speedupNewOverOld: speedup,
      });
    }

    const report = {
      generatedAt: new Date().toISOString(),
      startedAt,
      config: {
        warmupIterations: WARMUP_ITERATIONS,
        operations: BENCH_OPERATIONS,
      },
      results,
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
    keyValues: [
      {
        key: "lix_deterministic_mode",
        value: { enabled: true },
        lixcol_version_id: "global",
        lixcol_untracked: true,
      },
    ],
  });

  await oldExecute(
    lix,
    "INSERT INTO stored_schema_by_version (value, lixcol_version_id) VALUES (?, ?)",
    [JSON.stringify(oldSchema), "global"],
  );

  const versionRows = await oldExecute(
    lix,
    "SELECT version_id FROM active_version ORDER BY version_id LIMIT 1",
    [],
  );
  const versionId = String(versionRows[0].version_id);

  return {
    name: "@lix-js/sdk",
    schemaVersion: "1.0",
    tableName: "state_by_version",
    versionId,
    seq: 0,
    async insertChunk(chunkSize) {
      await insertRows({
        execute: (sql, params) => oldExecute(lix, sql, params),
        tableName: this.tableName,
        schemaVersion: this.schemaVersion,
        versionId: this.versionId,
        chunkSize,
        nextSeq: () => this.seq++,
      });
    },
    async close() {
      await lix.close();
    },
  };
}

async function setupNewAdapter() {
  const lix = await openNewLix();

  await lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?, ?)",
    ["lix_deterministic_mode", "{\"enabled\":true}"],
  );

  await lix.execute(
    "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('lix_stored_schema', ?)",
    [JSON.stringify({ value: newSchema })],
  );

  const versionResult = await lix.execute(
    "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
    [],
  );
  const versionId = scalarToString(versionResult.rows?.[0]?.[0], "lix_active_version.version_id");

  return {
    name: "js-sdk",
    schemaVersion: "1",
    tableName: "lix_state_by_version",
    versionId,
    seq: 0,
    async insertChunk(chunkSize) {
      await insertRows({
        execute: (sql, params) => lix.execute(sql, params),
        tableName: this.tableName,
        schemaVersion: this.schemaVersion,
        versionId: this.versionId,
        chunkSize,
        nextSeq: () => this.seq++,
      });
    },
    async close() {
      // js-sdk currently does not expose close().
    },
  };
}

async function runOperation(adapter, operation) {
  for (let i = 0; i < WARMUP_ITERATIONS; i++) {
    await adapter.insertChunk(operation.chunkSize);
  }

  const samples = [];
  for (let i = 0; i < operation.iterations; i++) {
    const start = performance.now();
    await adapter.insertChunk(operation.chunkSize);
    samples.push(performance.now() - start);
  }

  return summarizeSamples(samples, operation.chunkSize);
}

async function insertRows(args) {
  const {
    execute,
    tableName,
    schemaVersion,
    versionId,
    chunkSize,
    nextSeq,
  } = args;

  const placeholders = [];
  const params = [];

  for (let i = 0; i < chunkSize; i++) {
    const seq = nextSeq();
    const source = `${FILE_ID}_${versionId}_${seq}`;
    const entityId = source;
    const snapshotContent = JSON.stringify({
      source,
      version: versionId,
      idx: seq,
    });

    placeholders.push("(?, ?, ?, ?, ?, ?, ?)");
    params.push(
      entityId,
      BENCH_SCHEMA_KEY,
      FILE_ID,
      versionId,
      PLUGIN_KEY,
      snapshotContent,
      schemaVersion,
    );
  }

  const sql = `INSERT INTO ${tableName} (
    entity_id,
    schema_key,
    file_id,
    version_id,
    plugin_key,
    snapshot_content,
    schema_version
  ) VALUES ${placeholders.join(",")}`;

  await execute(sql, params);
}

async function oldExecute(lix, sql, parameters) {
  const result = await lix.db.executeQuery({
    sql,
    parameters,
  });
  return result.rows ?? [];
}

function summarizeSamples(samples, chunkSize) {
  const sorted = [...samples].sort((a, b) => a - b);
  const meanMs = samples.reduce((sum, value) => sum + value, 0) / samples.length;
  const variance =
    samples.reduce((sum, value) => sum + (value - meanMs) ** 2, 0) / samples.length;
  const stdDevMs = Math.sqrt(variance);
  const p50Ms = percentile(sorted, 0.5);
  const p95Ms = percentile(sorted, 0.95);
  const minMs = sorted[0];
  const maxMs = sorted[sorted.length - 1];

  return {
    meanMs,
    stdDevMs,
    minMs,
    maxMs,
    p50Ms,
    p95Ms,
    perRowMeanMs: meanMs / chunkSize,
    rowsPerSecond: chunkSize / (meanMs / 1000),
    sampleCount: samples.length,
  };
}

function percentile(sorted, p) {
  if (sorted.length === 0) {
    return 0;
  }
  const index = Math.max(0, Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p)));
  return sorted[index];
}

function percentDelta(current, baseline) {
  if (baseline === 0) {
    return null;
  }
  return ((current - baseline) / baseline) * 100;
}

function scalarToString(value, context) {
  if (value === null || value === undefined) {
    throw new Error(`Missing scalar value for ${context}`);
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  if (typeof value === "object") {
    const kind = value.kind;
    if (kind === "Text" || kind === "Integer" || kind === "Real") {
      return String(value.value);
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
  const title = "LIX State Insert Bench (new js-sdk vs old @lix-js/sdk)";
  const border = `+${"-".repeat(width - 2)}+`;
  console.log("");
  console.log(color(border, "dim"));
  console.log(color(`| ${title.padEnd(width - 4)} |`, "dim"));
  console.log(color(border, "dim"));
  console.log(color("Lower is faster.", "dim"));
  console.log(color(`Warmup iterations: ${report.config.warmupIterations}`, "dim"));

  for (const item of report.results) {
    const old = item.oldSdk;
    const nu = item.newJsSdk;
    const speedupText = `${item.speedupNewOverOld.toFixed(2)}x`;
    console.log("");
    console.log(color(item.operation.label, "bold"));
    console.log(`  old @lix-js/sdk: ${color(formatMs(old.meanMs), "red")}`);
    console.log(`  new js-sdk:      ${color(formatMs(nu.meanMs), "green")}`);

    console.log(color("Summary", "bold"));
    console.log(`  new js-sdk ran ${color(speedupText, "green")} faster than old @lix-js/sdk`);
  }
}

const ANSI = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  red: "\x1b[31m",
  green: "\x1b[32m",
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
