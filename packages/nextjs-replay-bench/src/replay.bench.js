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
  warmupCommitCount: parseEnvNonNegativeInt("BENCH_REPLAY_WARMUP_COMMITS", 5),
  firstParent: parseEnvBool("BENCH_REPLAY_FIRST_PARENT", true),
  syncRemote: parseEnvBool("BENCH_REPLAY_FETCH", false),
  progressEvery: parseEnvInt("BENCH_REPLAY_PROGRESS_EVERY", 25),
  showProgress: parseEnvBool("BENCH_REPLAY_PROGRESS", true),
  installTextLinesPlugin: parseEnvBool("BENCH_REPLAY_INSTALL_TEXT_LINES_PLUGIN", true),
  exportSnapshot: parseEnvBool("BENCH_REPLAY_EXPORT_SNAPSHOT", false),
  exportSnapshotPath: process.env.BENCH_REPLAY_SNAPSHOT_PATH ?? "",
  collectStorageCounters: parseEnvBool("BENCH_REPLAY_STORAGE_COUNTERS", true),
  maxInsertRows: parseEnvInt("BENCH_REPLAY_MAX_INSERT_ROWS", 200),
  maxInsertSqlChars: parseEnvInt("BENCH_REPLAY_MAX_INSERT_SQL_CHARS", 1_500_000),
  executeMode: parseExecuteMode(process.env.BENCH_REPLAY_EXECUTE_MODE),
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

  const repoSetupStarted = performance.now();
  const repo = await ensureGitRepo({
    repoPath: CONFIG.repoPath || undefined,
    repoUrl: CONFIG.repoUrl,
    cacheDir: CONFIG.cacheDir,
    defaultDirName: "next.js",
    syncRemote: CONFIG.syncRemote,
    ref: CONFIG.repoRef,
  });
  const repoSetupMs = performance.now() - repoSetupStarted;

  const commitDiscoveryStarted = performance.now();
  const totalRequestedCommits = CONFIG.commitLimit + CONFIG.warmupCommitCount;
  const commits = await listLinearCommits(repo.repoPath, {
    ref: CONFIG.repoRef,
    maxCount: totalRequestedCommits,
    firstParent: CONFIG.firstParent,
  });
  const commitDiscoveryMs = performance.now() - commitDiscoveryStarted;

  if (commits.length === 0) {
    throw new Error(`no commits found at ${repo.repoPath} (${CONFIG.repoRef})`);
  }

  const warmupCommitCount = Math.min(CONFIG.warmupCommitCount, commits.length);
  const measuredCommits = commits.slice(warmupCommitCount);
  if (measuredCommits.length === 0) {
    throw new Error(
      `warmup consumed all discovered commits (warmup=${warmupCommitCount}, discovered=${commits.length})`,
    );
  }

  if (CONFIG.showProgress) {
    console.log(
      `[progress] replaying ${commits.length} commits (warmup=${warmupCommitCount}, measured=${measuredCommits.length}) from ${repo.repoPath} (${repo.source})`,
    );
  }

  const lixOpenStarted = performance.now();
  const lix = await openLix({
    keyValues: [{
      key: "lix_deterministic_mode",
      value: { enabled: true },
      lixcol_version_id: "global",
    }],
  });
  const lixOpenMs = performance.now() - lixOpenStarted;

  try {
    let pluginInstallMs = 0;
    if (CONFIG.installTextLinesPlugin) {
      const pluginWasmBytes = await loadTextLinesPluginWasmBytes(CONFIG.showProgress);
      const pluginInstallStarted = performance.now();
      await lix.installPlugin({
        manifestJson: TEXT_LINES_MANIFEST,
        wasmBytes: pluginWasmBytes,
      });
      pluginInstallMs = performance.now() - pluginInstallStarted;
    }

    const state = createReplayState();
    const commitDurations = [];
    const phaseDurations = {
      readPatchSetMs: [],
      prepareMs: [],
      buildStatementsMs: [],
      executeStatementsMs: [],
      commitTotalMs: [],
    };
    const phaseTotalsMs = {
      readPatchSetMs: 0,
      prepareMs: 0,
      buildStatementsMs: 0,
      executeStatementsMs: 0,
      commitTotalMs: 0,
    };
    const slowCommits = [];
    const slowStatements = [];

    let commitsApplied = 0;
    let commitsNoop = 0;
    let totalChangedPaths = 0;
    let totalBlobBytes = 0;
    let totalSqlChars = 0;
    let totalEngineStatements = 0;
    let totalInserts = 0;
    let totalUpdates = 0;
    let totalDeletes = 0;

    let warmupCommitsApplied = 0;
    let warmupCommitsNoop = 0;
    let warmupChangedPaths = 0;

    const allCommitLoopStarted = performance.now();
    let measuredReplayStarted = warmupCommitCount === 0 ? allCommitLoopStarted : null;
    let warmupMs = 0;
    for (let index = 0; index < commits.length; index++) {
      if (index === warmupCommitCount && measuredReplayStarted === null) {
        warmupMs = performance.now() - allCommitLoopStarted;
        measuredReplayStarted = performance.now();
      }
      const isWarmup = index < warmupCommitCount;
      const commitSha = commits[index];
      const commitStarted = performance.now();

      const patchSetStarted = performance.now();
      const patchSet = await readCommitPatchSet(repo.repoPath, commitSha);
      const readPatchSetMs = performance.now() - patchSetStarted;
      if (!isWarmup) {
        phaseDurations.readPatchSetMs.push(readPatchSetMs);
        phaseTotalsMs.readPatchSetMs += readPatchSetMs;
      }

      const prepareStarted = performance.now();
      const prepared = prepareCommitChanges(state, patchSet.changes, patchSet.blobByOid);
      const prepareMs = performance.now() - prepareStarted;
      if (!isWarmup) {
        phaseDurations.prepareMs.push(prepareMs);
        phaseTotalsMs.prepareMs += prepareMs;
        totalChangedPaths += patchSet.changes.length;
        totalBlobBytes += prepared.blobBytes;
        totalInserts += prepared.inserts.length;
        totalUpdates += prepared.updates.length;
        totalDeletes += prepared.deletes.length;
      } else {
        warmupChangedPaths += patchSet.changes.length;
      }

      const buildStatementsStarted = performance.now();
      const statements = buildReplayCommitStatements(prepared, {
        maxInsertRows: CONFIG.maxInsertRows,
        maxInsertSqlChars: CONFIG.maxInsertSqlChars,
      });
      const buildStatementsMs = performance.now() - buildStatementsStarted;
      if (!isWarmup) {
        phaseDurations.buildStatementsMs.push(buildStatementsMs);
        phaseTotalsMs.buildStatementsMs += buildStatementsMs;
      }

      if (statements.length === 0) {
        if (isWarmup) {
          warmupCommitsNoop += 1;
        } else {
          commitsNoop += 1;
        }
      } else {
        let executeResult;
        try {
          executeResult =
            CONFIG.executeMode === "transaction-script"
              ? await executeStatementsAsTransactionScript(lix, statements)
              : await executeStatements(lix, statements);
        } catch (error) {
          throw new Error(
            `failed while replaying commit ${commitSha}: ${String(error?.message ?? error)}`,
          );
        }
        const commitMs = executeResult.totalMs;

        if (isWarmup) {
          warmupCommitsApplied += 1;
        } else {
          commitsApplied += 1;
          commitDurations.push(commitMs);
          phaseDurations.executeStatementsMs.push(commitMs);
          phaseTotalsMs.executeStatementsMs += commitMs;
          totalSqlChars += statementChars(statements);
          totalEngineStatements += statements.length;
          for (const statement of executeResult.slowestStatements) {
            pushSlowStatement(slowStatements, {
              commitSha,
              ...statement,
            });
          }
          pushSlowCommit(slowCommits, {
            commitSha,
            durationMs: commitMs,
            changedPaths: patchSet.changes.length,
            inserts: prepared.inserts.length,
            updates: prepared.updates.length,
            deletes: prepared.deletes.length,
          });
        }
      }

      const commitTotalMs = performance.now() - commitStarted;
      if (!isWarmup) {
        phaseDurations.commitTotalMs.push(commitTotalMs);
        phaseTotalsMs.commitTotalMs += commitTotalMs;
      }

      const phaseIndex = isWarmup ? index + 1 : index - warmupCommitCount + 1;
      const phaseTotal = isWarmup ? warmupCommitCount : measuredCommits.length;
      if (
        CONFIG.showProgress &&
        phaseTotal > 0 &&
        (phaseIndex === 1 || phaseIndex % CONFIG.progressEvery === 0 || phaseIndex === phaseTotal)
      ) {
        printProgress({
          label: isWarmup ? "warmup" : "measure",
          index: phaseIndex,
          total: phaseTotal,
          commitSha,
          elapsedMs: performance.now() - replayStarted,
          changedPaths: isWarmup ? warmupChangedPaths : totalChangedPaths,
          commitsApplied: isWarmup ? warmupCommitsApplied : commitsApplied,
          commitsNoop: isWarmup ? warmupCommitsNoop : commitsNoop,
        });
      }
    }
    if (measuredReplayStarted === null) {
      throw new Error("internal error: measured replay did not start");
    }
    const commitLoopMs = performance.now() - measuredReplayStarted;
    if (warmupCommitCount === 0) {
      warmupMs = 0;
    } else if (warmupMs === 0) {
      warmupMs = performance.now() - allCommitLoopStarted - commitLoopMs;
    }

    const replayMs = commitLoopMs;
    const overallReplayMs = performance.now() - replayStarted;

    let storage = {
      fileRows: 0,
      internalChangeRows: 0,
      internalSnapshotRows: 0,
      topSchemaCounts: [],
    };
    const storageQueryStarted = performance.now();
    let storageQueryMs = 0;
    if (CONFIG.collectStorageCounters) {
      if (CONFIG.showProgress) {
        console.log("[progress:post-replay] collecting storage counters");
      }
      storage = await collectStorageCounters(lix);
      storageQueryMs = performance.now() - storageQueryStarted;
    } else if (CONFIG.showProgress) {
      console.log("[progress:post-replay] skipping storage counters (set BENCH_REPLAY_STORAGE_COUNTERS=1 to enable)");
    }
    const snapshotExportStarted = performance.now();
    const snapshotArtifact = await maybeWriteSnapshotArtifact(lix);
    const snapshotExportMs = performance.now() - snapshotExportStarted;

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
        requested: totalRequestedCommits,
        discovered: commits.length,
        measuredDiscovered: measuredCommits.length,
        warmupRequested: CONFIG.warmupCommitCount,
        warmupUsed: warmupCommitCount,
        warmupApplied: warmupCommitsApplied,
        warmupNoop: warmupCommitsNoop,
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
        overallReplayMs,
        commitLoopMs,
        warmupMs,
        setup: {
          repoSetupMs,
          commitDiscoveryMs,
          lixOpenMs,
          pluginInstallMs,
        },
        postReplay: {
          storageQueryMs,
          snapshotExportMs,
        },
        commit: summarizeSamples(commitDurations),
        phaseTotalsMs,
        phase: {
          readPatchSet: summarizeSamples(phaseDurations.readPatchSetMs),
          prepare: summarizeSamples(phaseDurations.prepareMs),
          buildStatements: summarizeSamples(phaseDurations.buildStatementsMs),
          executeStatements: summarizeSamples(phaseDurations.executeStatementsMs),
          commitTotal: summarizeSamples(phaseDurations.commitTotalMs),
        },
        phaseBreakdown: buildPhaseBreakdown(phaseTotalsMs, replayMs),
      },
      throughput: {
        commitsPerSecond: commitsApplied / Math.max(replayMs / 1000, 0.001),
        commitsPerSecondCommitLoop: commitsApplied / Math.max(commitLoopMs / 1000, 0.001),
        changedPathsPerSecond: totalChangedPaths / Math.max(replayMs / 1000, 0.001),
        blobMegabytesPerSecond: totalBlobBytes / 1024 / 1024 / Math.max(replayMs / 1000, 0.001),
        executeStatementsPerSecond:
          totalEngineStatements / Math.max(phaseTotalsMs.executeStatementsMs / 1000, 0.001),
      },
      storage,
      snapshotArtifact,
      slowestCommits: slowCommits,
      slowestStatements: slowStatements,
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

function pushSlowStatement(store, candidate) {
  store.push(candidate);
  store.sort((left, right) => right.durationMs - left.durationMs);
  if (store.length > 20) {
    store.length = 20;
  }
}

function buildPhaseBreakdown(phaseTotalsMs, replayMs) {
  const phases = [
    ["readPatchSetMs", phaseTotalsMs.readPatchSetMs],
    ["prepareMs", phaseTotalsMs.prepareMs],
    ["buildStatementsMs", phaseTotalsMs.buildStatementsMs],
    ["executeStatementsMs", phaseTotalsMs.executeStatementsMs],
  ];
  const accountedMs = phases.reduce((sum, [, value]) => sum + value, 0);
  const breakdown = phases
    .map(([phase, totalMs]) => ({
      phase,
      totalMs,
      sharePct: replayMs > 0 ? (totalMs / replayMs) * 100 : 0,
    }))
    .sort((left, right) => right.totalMs - left.totalMs);

  breakdown.push({
    phase: "setupAndOverheadMs",
    totalMs: Math.max(0, replayMs - accountedMs),
    sharePct: replayMs > 0 ? Math.max(0, ((replayMs - accountedMs) / replayMs) * 100) : 0,
  });

  return breakdown.sort((left, right) => right.totalMs - left.totalMs);
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
  const slowestStatements = [];
  let totalMs = 0;

  for (let index = 0; index < statements.length; index++) {
    const statement = statements[index];
    const sql = statement.sql;
    const params = statement.params ?? [];
    const statementStarted = performance.now();
    await lix.execute(sql, params);
    const durationMs = performance.now() - statementStarted;
    totalMs += durationMs;
    pushSlowStatement(slowestStatements, {
      statementIndex: index,
      durationMs,
      sqlChars: sql.length,
      sqlPreview: summarizeSql(sql),
    });
  }

  return {
    totalMs,
    slowestStatements,
  };
}

async function executeStatementsAsTransactionScript(lix, statements) {
  if (statements.some((statement) => Array.isArray(statement.params) && statement.params.length > 0)) {
    throw new Error(
      "transaction-script mode is incompatible with parameterized replay statements; use BENCH_REPLAY_EXECUTE_MODE=per-statement",
    );
  }
  const script = ["BEGIN", ...statements.map((statement) => statement.sql), "COMMIT"].join(";\n");
  const started = performance.now();
  await lix.execute(script, []);
  const durationMs = performance.now() - started;
  return {
    totalMs: durationMs,
    slowestStatements: [{
      statementIndex: -1,
      durationMs,
      sqlChars: script.length,
      sqlPreview: summarizeSql(script),
    }],
  };
}

function summarizeSql(sql) {
  const flattened = String(sql).replace(/\s+/g, " ").trim();
  if (flattened.length <= 140) {
    return flattened;
  }
  return `${flattened.slice(0, 140)}…`;
}

function statementChars(statements) {
  return statements.reduce((total, statement) => total + statement.sql.length, 0);
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

  try {
    return await readFile(packageReleasePath);
  } catch {
    try {
      return await readFile(workspaceReleasePath);
    } catch {
      try {
        return await readFile(packageDebugPath);
      } catch {
        try {
          return await readFile(workspaceDebugPath);
        } catch {
          await ensureTextLinesPluginWasmBuilt(showProgress);
          try {
            return await readFile(packageReleasePath);
          } catch {
            try {
              return await readFile(workspaceReleasePath);
            } catch {
              try {
                return await readFile(packageDebugPath);
              } catch {
                return await readFile(workspaceDebugPath);
              }
            }
          }
        }
      }
    }
  }
}

async function ensureTextLinesPluginWasmBuilt(showProgress) {
  const manifestPath = join(REPO_ROOT, "packages", "plugin-text-lines", "Cargo.toml");

  if (showProgress) {
    console.log("[progress] building plugin-text-lines wasm (wasm32-wasip2, release)");
  }

  try {
    await runCommand("cargo", [
      "build",
      "--release",
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
        "--release",
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

function parseEnvNonNegativeInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer, got '${raw}'`);
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

function parseExecuteMode(raw) {
  if (!raw) {
    return "per-statement";
  }
  if (raw === "per-statement" || raw === "transaction-script") {
    return raw;
  }
  throw new Error(
    `BENCH_REPLAY_EXECUTE_MODE must be 'per-statement' or 'transaction-script', got '${raw}'`,
  );
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
