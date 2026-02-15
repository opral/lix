import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { openLix } from "js-sdk";
import {
  ensureGitRepo,
  listLinearCommits,
  readCommitPatchSet,
} from "./git-history.js";
import {
  createReplayState,
  prepareCommitChanges,
  buildReplayCommitStatements,
} from "./apply-to-lix.js";
import { printProgress, printSummary, summarizeSamples } from "./report.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..", "..");
const OUTPUT_DIR = join(__dirname, "..", "results");
const OUTPUT_PATH = join(OUTPUT_DIR, "nextjs-replay.bench.json");
const DEFAULT_CACHE_DIR = join(__dirname, "..", ".cache", "nextjs-replay");

const CONFIG = {
  repoUrl: process.env.BENCH_REPLAY_REPO_URL ?? "https://github.com/vercel/next.js.git",
  repoPath: process.env.BENCH_REPLAY_REPO_PATH ?? "",
  repoRef: process.env.BENCH_REPLAY_REF ?? "HEAD",
  cacheDir: process.env.BENCH_REPLAY_CACHE_DIR ?? DEFAULT_CACHE_DIR,
  commitLimit: parseEnvInt("BENCH_REPLAY_COMMITS", 1000),
  firstParent: parseEnvBool("BENCH_REPLAY_FIRST_PARENT", true),
  syncRemote: parseEnvBool("BENCH_REPLAY_FETCH", true),
  progressEvery: parseEnvInt("BENCH_REPLAY_PROGRESS_EVERY", 25),
  showProgress: parseEnvBool("BENCH_REPLAY_PROGRESS", true),
  installTextLinesPlugin: parseEnvBool("BENCH_REPLAY_INSTALL_TEXT_LINES_PLUGIN", true),
  exportSnapshot: parseEnvBool("BENCH_REPLAY_EXPORT_SNAPSHOT", false),
  exportSnapshotPath: process.env.BENCH_REPLAY_SNAPSHOT_PATH ?? "",
  maxInsertRows: parseEnvInt("BENCH_REPLAY_MAX_INSERT_ROWS", 200),
  maxInsertSqlChars: parseEnvInt("BENCH_REPLAY_MAX_INSERT_SQL_CHARS", 1_500_000),
};

const TEXT_LINES_MANIFEST = {
  key: "plugin_text_lines",
  runtime: "wasm-component-v1",
  api_version: "0.1.0",
  detect_changes_glob: "**/*",
  entry: "plugin.wasm",
};

async function main() {
  const startedAt = new Date().toISOString();
  const replayStarted = performance.now();

  const repo = await ensureGitRepo({
    repoPath: CONFIG.repoPath || undefined,
    repoUrl: CONFIG.repoUrl,
    cacheDir: CONFIG.cacheDir,
    defaultDirName: "next.js",
    syncRemote: CONFIG.syncRemote,
    ref: CONFIG.repoRef,
  });

  const commits = await listLinearCommits(repo.repoPath, {
    ref: CONFIG.repoRef,
    maxCount: CONFIG.commitLimit,
    firstParent: CONFIG.firstParent,
  });

  if (commits.length === 0) {
    throw new Error(`no commits found at ${repo.repoPath} (${CONFIG.repoRef})`);
  }

  if (CONFIG.showProgress) {
    console.log(
      `[progress] replaying ${commits.length} commits from ${repo.repoPath} (${repo.source})`,
    );
  }

  const lix = await openLix();

  try {
    if (CONFIG.installTextLinesPlugin) {
      const pluginWasmBytes = await loadTextLinesPluginWasmBytes(CONFIG.showProgress);
      await lix.installPlugin({
        manifestJson: TEXT_LINES_MANIFEST,
        wasmBytes: pluginWasmBytes,
      });
    }

    const state = createReplayState();
    const commitDurations = [];
    const slowCommits = [];

    let commitsApplied = 0;
    let commitsNoop = 0;
    let totalChangedPaths = 0;
    let totalBlobBytes = 0;
    let totalSqlChars = 0;
    let totalEngineStatements = 0;
    let totalInserts = 0;
    let totalUpdates = 0;
    let totalDeletes = 0;

    for (let index = 0; index < commits.length; index++) {
      const commitSha = commits[index];
      const patchSet = await readCommitPatchSet(repo.repoPath, commitSha);
      const prepared = prepareCommitChanges(state, patchSet.changes, patchSet.blobByOid);

      totalChangedPaths += patchSet.changes.length;
      totalBlobBytes += prepared.blobBytes;
      totalInserts += prepared.inserts.length;
      totalUpdates += prepared.updates.length;
      totalDeletes += prepared.deletes.length;

      const statements = buildReplayCommitStatements(prepared, {
        maxInsertRows: CONFIG.maxInsertRows,
        maxInsertSqlChars: CONFIG.maxInsertSqlChars,
      });

      if (statements.length === 0) {
        commitsNoop += 1;
      } else {
        const commitStarted = performance.now();
        try {
          await executeStatements(lix, statements);
        } catch (error) {
          throw new Error(
            `failed while replaying commit ${commitSha}: ${String(error?.message ?? error)}`,
          );
        }
        const commitMs = performance.now() - commitStarted;

        commitsApplied += 1;
        commitDurations.push(commitMs);
        totalSqlChars += statementChars(statements);
        totalEngineStatements += statements.length;
        pushSlowCommit(slowCommits, {
          commitSha,
          durationMs: commitMs,
          changedPaths: patchSet.changes.length,
          inserts: prepared.inserts.length,
          updates: prepared.updates.length,
          deletes: prepared.deletes.length,
        });
      }

      if (
        CONFIG.showProgress &&
        (index === 0 || (index + 1) % CONFIG.progressEvery === 0 || index + 1 === commits.length)
      ) {
        printProgress({
          index: index + 1,
          total: commits.length,
          commitSha,
          elapsedMs: performance.now() - replayStarted,
          changedPaths: totalChangedPaths,
          commitsApplied,
          commitsNoop,
        });
      }
    }

    const replayMs = performance.now() - replayStarted;
    const storage = await collectStorageCounters(lix);
    const snapshotArtifact = await maybeWriteSnapshotArtifact(lix);

    const report = {
      generatedAt: new Date().toISOString(),
      startedAt,
      config: {
        ...CONFIG,
      },
      repo: {
        path: repo.repoPath,
        source: repo.source,
        ref: CONFIG.repoRef,
      },
      commitTotals: {
        discovered: commits.length,
        applied: commitsApplied,
        noop: commitsNoop,
      },
      io: {
        changedPaths: totalChangedPaths,
        blobBytes: totalBlobBytes,
        sqlChars: totalSqlChars,
        engineStatements: totalEngineStatements,
        inserts: totalInserts,
        updates: totalUpdates,
        deletes: totalDeletes,
      },
      timings: {
        replayMs,
        commit: summarizeSamples(commitDurations),
      },
      throughput: {
        commitsPerSecond: commitsApplied / Math.max(replayMs / 1000, 0.001),
        changedPathsPerSecond: totalChangedPaths / Math.max(replayMs / 1000, 0.001),
        blobMegabytesPerSecond: totalBlobBytes / 1024 / 1024 / Math.max(replayMs / 1000, 0.001),
      },
      storage,
      snapshotArtifact,
      slowestCommits: slowCommits,
    };

    await mkdir(OUTPUT_DIR, { recursive: true });
    await writeFile(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`, "utf8");

    printSummary(report);
    console.log(`\nWrote benchmark report: ${OUTPUT_PATH}`);
  } finally {
    await lix.close();
  }
}

function pushSlowCommit(store, candidate) {
  store.push(candidate);
  store.sort((left, right) => right.durationMs - left.durationMs);
  if (store.length > 20) {
    store.length = 20;
  }
}

async function collectStorageCounters(lix) {
  const fileRows = await queryScalarNumber(lix, "SELECT COUNT(*) FROM lix_file", "lix_file count");
  const internalChangeRows = await queryScalarNumber(
    lix,
    "SELECT COUNT(*) FROM lix_internal_change",
    "lix_internal_change count",
  );
  const internalSnapshotRows = await queryScalarNumber(
    lix,
    "SELECT COUNT(*) FROM lix_internal_snapshot",
    "lix_internal_snapshot count",
  );

  const schemaCountResult = await lix.execute(
    "SELECT schema_key, COUNT(*) AS row_count FROM lix_internal_change GROUP BY schema_key ORDER BY row_count DESC LIMIT 20",
    [],
  );

  const topSchemaCounts = (schemaCountResult.rows ?? []).map((row) => ({
    schemaKey: scalarToString(row?.[0], "schema_key"),
    rowCount: scalarToNumber(row?.[1], "row_count"),
  }));

  return {
    fileRows,
    internalChangeRows,
    internalSnapshotRows,
    topSchemaCounts,
  };
}

async function executeStatements(lix, statements) {
  for (const sql of statements) {
    await lix.execute(sql, []);
  }
}

function statementChars(statements) {
  return statements.reduce((total, sql) => total + sql.length, 0);
}

async function maybeWriteSnapshotArtifact(lix) {
  if (!CONFIG.exportSnapshot && !CONFIG.exportSnapshotPath) {
    return null;
  }

  const outputPath = CONFIG.exportSnapshotPath || join(OUTPUT_DIR, "nextjs-replay.snapshot.lix");
  const snapshotBytes = await lix.exportSnapshot();
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, snapshotBytes);
  return {
    path: outputPath,
    bytes: snapshotBytes.byteLength,
  };
}

async function loadTextLinesPluginWasmBytes(showProgress) {
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

  try {
    return await readFile(packageDebugPath);
  } catch {
    try {
      return await readFile(workspaceDebugPath);
    } catch {
      await ensureTextLinesPluginWasmBuilt(showProgress);
      try {
        return await readFile(packageDebugPath);
      } catch {
        return await readFile(workspaceDebugPath);
      }
    }
  }
}

async function ensureTextLinesPluginWasmBuilt(showProgress) {
  const manifestPath = join(REPO_ROOT, "packages", "plugin-text-lines", "Cargo.toml");

  if (showProgress) {
    console.log("[progress] building plugin-text-lines wasm (wasm32-wasip2)");
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

  if (showProgress) {
    console.log("[progress] plugin-text-lines wasm build done");
  }
}

async function queryScalarNumber(lix, sql, context) {
  const result = await lix.execute(sql, []);
  return scalarToNumber(result.rows?.[0]?.[0], context);
}

function scalarToNumber(value, context) {
  if (value === null || value === undefined) {
    throw new Error(`missing scalar for ${context}`);
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
    if (value.kind === "Integer" || value.kind === "Real" || value.kind === "Text") {
      return Number(value.value);
    }
  }
  throw new Error(`unsupported scalar value for ${context}: ${JSON.stringify(value)}`);
}

function scalarToString(value, context) {
  if (value === null || value === undefined) {
    throw new Error(`missing scalar for ${context}`);
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  if (typeof value === "object") {
    if (value.kind === "Text" || value.kind === "Integer" || value.kind === "Real") {
      return String(value.value);
    }
  }
  throw new Error(`unsupported text scalar for ${context}: ${JSON.stringify(value)}`);
}

function parseEnvInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got '${raw}'`);
  }
  return parsed;
}

function parseEnvBool(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  if (["1", "true", "yes", "on"].includes(raw.toLowerCase())) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(raw.toLowerCase())) {
    return false;
  }
  throw new Error(`${name} must be boolean-like (0/1/true/false), got '${raw}'`);
}

async function runCommand(command, args) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
    const stderr = [];

    child.stderr.on("data", (chunk) => {
      stderr.push(chunk);
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(
          new Error(
            `${command} ${args.join(" ")} failed with exit code ${code}:\n${Buffer.concat(stderr).toString("utf8")}`,
          ),
        );
      }
    });
  });
}

main().catch((error) => {
  console.error("Benchmark run failed:");
  console.error(error);
  process.exitCode = 1;
});
