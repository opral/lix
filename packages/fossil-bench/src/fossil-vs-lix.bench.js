import { statSync } from "node:fs";
import { mkdir, rm, stat, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";

import Database from "better-sqlite3";
import { createBetterSqlite3Backend } from "@lix-js/better-sqlite3-backend";
import { openLix } from "js-sdk";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PACKAGE_DIR = join(__dirname, "..");
const OUTPUT_DIR = join(PACKAGE_DIR, "results");
const DEFAULT_OUTPUT_PATH = join(OUTPUT_DIR, "fossil-vs-lix.bench.json");
const DEFAULT_COMMIT_OUTPUT_PATH = join(OUTPUT_DIR, "fossil-vs-lix.commit.bench.json");
const CACHE_ROOT = join(PACKAGE_DIR, ".cache");

const DATASET_CLASSES = [
  {
    key: "random",
    ext: "bin",
    initialBytes: 128 * 1024,
  },
  {
    key: "media_like",
    ext: "dat",
    initialBytes: 256 * 1024,
  },
  {
    key: "append_friendly",
    ext: "dat",
    initialBytes: 64 * 1024,
  },
];

const LIX_STORAGE_TABLES = [
  "lix_internal_change",
  "lix_internal_snapshot",
  "lix_internal_binary_blob_store",
  "lix_internal_binary_chunk_store",
  "lix_internal_binary_blob_manifest",
  "lix_internal_binary_blob_manifest_chunk",
  "lix_internal_binary_file_version_ref",
];

const LIX_COMMIT_TRACE_TABLES = [
  "lix_internal_change",
  "lix_internal_snapshot",
  "lix_internal_binary_blob_manifest",
  "lix_internal_binary_blob_manifest_chunk",
  "lix_internal_binary_chunk_store",
  "lix_internal_binary_file_version_ref",
  "lix_internal_file_path_cache",
  "lix_internal_state_untracked",
  "lix_internal_commit_ancestry",
  "lix_internal_state_materialized_v1_lix_file_descriptor",
  "lix_internal_state_materialized_v1_lix_binary_blob_ref",
  "lix_internal_state_materialized_v1_lix_commit",
  "lix_internal_state_materialized_v1_lix_commit_edge",
  "lix_internal_state_materialized_v1_lix_change_set_element",
  "lix_internal_state_materialized_v1_lix_version_pointer",
];

const FOSSIL_STORAGE_TABLES = [
  "blob",
  "delta",
  "event",
  "mlink",
  "filename",
];

const FOSSIL_COMMIT_TRACE_TABLES = [...FOSSIL_STORAGE_TABLES, "plink", "tagxref"];

const SQLITE_MAX_BIND_PARAMETERS = 32_766;
const LIX_FILE_INSERT_PARAM_COUNT = 3;
const LIX_FILE_UPDATE_PARAM_COUNT = 2;

const CONFIG = {
  scenario: parseScenario(process.env.BENCH_SCENARIO ?? "full"),
  target: parseTarget(process.env.BENCH_TARGET ?? "both"),
  filesPerClass: parsePositiveInt("BENCH_FILES_PER_CLASS", 12),
  updateRounds: parsePositiveInt("BENCH_UPDATE_ROUNDS", 3),
  historyReads: parsePositiveInt("BENCH_HISTORY_READS", 24),
  maxBlobBytes: parsePositiveInt("BENCH_MAX_BLOB_BYTES", 1024 * 1024),
  keepArtifacts: process.env.BENCH_KEEP_ARTIFACTS === "1",
  progress: parseBooleanFlag("BENCH_PROGRESS", true),
  progressSlices: parsePositiveInt("BENCH_PROGRESS_SLICES", 8),
  outputPath:
    process.env.BENCH_RESULTS_PATH ??
    (parseScenario(process.env.BENCH_SCENARIO ?? "full") === "commit"
      ? DEFAULT_COMMIT_OUTPUT_PATH
      : DEFAULT_OUTPUT_PATH),
};

const PATHS = {
  lixDbPath: join(CACHE_ROOT, "lix", "fossil-vs-lix.sqlite"),
  fossilRoot: join(CACHE_ROOT, "fossil"),
  fossilRepoPath: join(CACHE_ROOT, "fossil", "repo.fossil"),
  fossilCheckoutPath: join(CACHE_ROOT, "fossil", "checkout"),
};

async function main() {
  const startedAt = new Date().toISOString();
  benchLog(
    `start scenario=${CONFIG.scenario} target=${CONFIG.target} files_per_class=${CONFIG.filesPerClass} update_rounds=${CONFIG.updateRounds} history_reads=${CONFIG.historyReads}`,
  );
  const dataset = buildDataset({
    filesPerClass: CONFIG.filesPerClass,
    maxBlobBytes: CONFIG.maxBlobBytes,
  });
  const datasetSummary = summarizeDataset(dataset);

  if (!CONFIG.keepArtifacts) {
    await rm(CACHE_ROOT, { recursive: true, force: true });
  }
  await mkdir(CACHE_ROOT, { recursive: true });
  await mkdir(OUTPUT_DIR, { recursive: true });

  const report = {
    generatedAt: new Date().toISOString(),
    startedAt,
    config: {
      ...CONFIG,
      scenario: String(CONFIG.scenario),
      target: String(CONFIG.target),
    },
    dataset: datasetSummary,
    targets: {
      lix: null,
      fossil: null,
    },
    comparison: null,
  };

  if (CONFIG.scenario === "commit") {
    if (CONFIG.target === "both" || CONFIG.target === "lix") {
      benchLog("running target=lix");
      const lixResult = await runLixCommitBenchmark({
        dataset: cloneDataset(dataset),
        config: CONFIG,
        dbPath: PATHS.lixDbPath,
      });
      report.targets.lix = lixResult;
    }

    if (CONFIG.target === "both" || CONFIG.target === "fossil") {
      benchLog("running target=fossil");
      await ensureFossilAvailable();
      const fossilResult = await runFossilCommitBenchmark({
        dataset: cloneDataset(dataset),
        config: CONFIG,
        paths: PATHS,
      });
      report.targets.fossil = fossilResult;
    }

    if (report.targets.lix && report.targets.fossil) {
      report.comparison = buildCommitComparison(report.targets.lix, report.targets.fossil);
    }
  } else {
    if (CONFIG.target === "both" || CONFIG.target === "lix") {
      benchLog("running target=lix");
      const lixResult = await runLixBenchmark({
        dataset: cloneDataset(dataset),
        config: CONFIG,
        dbPath: PATHS.lixDbPath,
      });
      report.targets.lix = lixResult;
    }

    if (CONFIG.target === "both" || CONFIG.target === "fossil") {
      benchLog("running target=fossil");
      await ensureFossilAvailable();
      const fossilResult = await runFossilBenchmark({
        dataset: cloneDataset(dataset),
        config: CONFIG,
        paths: PATHS,
      });
      report.targets.fossil = fossilResult;
    }

    if (report.targets.lix && report.targets.fossil) {
      report.comparison = buildComparison(report.targets.lix, report.targets.fossil);
    }
  }

  await writeFile(CONFIG.outputPath, JSON.stringify(report, null, 2) + "\n", "utf8");
  printSummary(report, CONFIG.outputPath);
}

async function runLixBenchmark(args) {
  const { dataset, config, dbPath } = args;
  await mkdir(dirname(dbPath), { recursive: true });
  await rm(dbPath, { force: true });
  await rm(`${dbPath}-wal`, { force: true });
  await rm(`${dbPath}-shm`, { force: true });

  const seedLix = await openLixAtPath(dbPath, true);
  await safeClose(seedLix);
  const baselineBytes = await sqliteArtifactBytes(dbPath);

  let lix = await openLixAtPath(dbPath, false);
  let ingest;
  try {
    benchLog(`lix ingest start files=${dataset.length}`);
    ingest = await runLixIngest({ lix, dataset });
    benchLog(`lix ingest done ops=${ingest.operations} wall_ms=${ingest.wall_ms.toFixed(1)}`);
  } finally {
    await safeClose(lix);
  }
  const afterIngestBytes = await sqliteArtifactBytes(dbPath);

  lix = await openLixAtPath(dbPath, false);
  let update;
  try {
    benchLog(`lix update start rounds=${config.updateRounds} files=${dataset.length}`);
    update = await runLixUpdate({
      lix,
      dataset,
      updateRounds: config.updateRounds,
      maxBlobBytes: config.maxBlobBytes,
    });
    benchLog(`lix update done ops=${update.operations} wall_ms=${update.wall_ms.toFixed(1)}`);
  } finally {
    await safeClose(lix);
  }
  const afterUpdateBytes = await sqliteArtifactBytes(dbPath);

  lix = await openLixAtPath(dbPath, false);
  let readLatest;
  let readHistory;
  try {
    benchLog(`lix read_latest start reads=${config.historyReads}`);
    readLatest = await runLixReadLatest({
      lix,
      dataset,
      reads: config.historyReads,
    });
    benchLog(`lix read_latest done ops=${readLatest.operations} wall_ms=${readLatest.wall_ms.toFixed(1)}`);
    benchLog(`lix read_history start reads=${config.historyReads}`);
    readHistory = await runLixReadHistory({
      lix,
      dataset,
      reads: config.historyReads,
      revisionDepth: config.updateRounds + 1,
    });
    benchLog(`lix read_history done ops=${readHistory.operations} wall_ms=${readHistory.wall_ms.toFixed(1)}`);
  } finally {
    await safeClose(lix);
  }
  const afterReadsBytes = await sqliteArtifactBytes(dbPath);

  const storageDetails = collectSqliteStorageMetrics(dbPath, LIX_STORAGE_TABLES);
  const logicalHistoryBytes = ingest.bytes_written + update.bytes_written;

  return {
    target: "lix",
    artifactPath: dbPath,
    workloads: [ingest, update, readLatest, readHistory],
    storage: {
      baselineBytes,
      afterIngestBytes,
      afterUpdateBytes,
      afterReadsBytes,
      logicalHistoryBytes,
      ingestWriteAmp: ratio(afterIngestBytes - baselineBytes, ingest.bytes_written),
      updateWriteAmp: ratio(afterUpdateBytes - afterIngestBytes, update.bytes_written),
      storageAmpAfterUpdate: ratio(afterUpdateBytes, logicalHistoryBytes),
      storageAmpAfterReads: ratio(afterReadsBytes, logicalHistoryBytes),
      ...storageDetails,
    },
  };
}

async function runLixIngest(args) {
  const { lix, dataset } = args;
  const samples = [];
  let bytesWritten = 0;
  const started = performance.now();

  const maxRowsPerBatch = Math.max(
    1,
    Math.floor(SQLITE_MAX_BIND_PARAMETERS / LIX_FILE_INSERT_PARAM_COUNT),
  );

  for (let batchStart = 0; batchStart < dataset.length; batchStart += maxRowsPerBatch) {
    const batch = dataset.slice(batchStart, batchStart + maxRowsPerBatch);
    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const file of batch) {
      values.push(`(?${paramIndex}, ?${paramIndex + 1}, ?${paramIndex + 2})`);
      params.push(file.id, file.lixPath, file.data);
      paramIndex += LIX_FILE_INSERT_PARAM_COUNT;
      bytesWritten += file.data.byteLength;
    }

    const opStarted = performance.now();
    await lix.execute(`INSERT INTO lix_file (id, path, data) VALUES ${values.join(", ")}`, params);
    const batchMs = performance.now() - opStarted;
    const perFileMs = batchMs / batch.length;
    for (let i = 0; i < batch.length; i++) {
      samples.push(perFileMs);
    }
    maybeLogLoopProgress(
      "lix ingest",
      Math.min(batchStart + batch.length, dataset.length),
      dataset.length,
      started,
    );
  }

  return toWorkloadMetrics({
    name: "ingest_files",
    operations: dataset.length,
    bytesWritten,
    bytesRead: 0,
    wallMs: performance.now() - started,
    samples,
  });
}

async function runLixUpdate(args) {
  const { lix, dataset, updateRounds, maxBlobBytes } = args;
  const samples = [];
  let bytesWritten = 0;
  const started = performance.now();

  const maxRowsPerBatch = Math.max(
    1,
    Math.floor(SQLITE_MAX_BIND_PARAMETERS / LIX_FILE_UPDATE_PARAM_COUNT),
  );

  for (let round = 0; round < updateRounds; round++) {
    for (let batchStart = 0; batchStart < dataset.length; batchStart += maxRowsPerBatch) {
      const batch = dataset.slice(batchStart, batchStart + maxRowsPerBatch);
      const whenClauses = [];
      const whereIds = [];
      const params = [];
      let paramIndex = 1;

      for (let offset = 0; offset < batch.length; offset++) {
        const index = batchStart + offset;
        const file = dataset[index];
        const nextData = mutateData(file, round, index, maxBlobBytes);
        whenClauses.push(`WHEN ?${paramIndex} THEN ?${paramIndex + 1}`);
        whereIds.push(`?${paramIndex}`);
        params.push(file.id, nextData);
        paramIndex += LIX_FILE_UPDATE_PARAM_COUNT;
        bytesWritten += nextData.byteLength;
        file.data = nextData;
      }

      const opStarted = performance.now();
      await lix.execute(
        `UPDATE lix_file \
         SET data = CASE id ${whenClauses.join(" ")} ELSE data END \
         WHERE id IN (${whereIds.join(", ")})`,
        params,
      );
      const batchMs = performance.now() - opStarted;
      const perFileMs = batchMs / batch.length;
      for (let i = 0; i < batch.length; i++) {
        samples.push(perFileMs);
      }
      maybeLogLoopProgress(
        `lix update round ${round + 1}`,
        Math.min(batchStart + batch.length, dataset.length),
        dataset.length,
        started,
      );
    }
  }

  return toWorkloadMetrics({
    name: "update_files",
    operations: dataset.length * updateRounds,
    bytesWritten,
    bytesRead: 0,
    wallMs: performance.now() - started,
    samples,
  });
}

async function runLixReadLatest(args) {
  const { lix, dataset, reads } = args;
  const samples = [];
  let bytesRead = 0;
  const started = performance.now();

  for (let i = 0; i < reads; i++) {
    const file = dataset[i % dataset.length];
    const opStarted = performance.now();
    const result = await lix.execute("SELECT data FROM lix_file WHERE id = ?", [file.id]);
    samples.push(performance.now() - opStarted);
    bytesRead += valueByteLength(result?.rows?.[0]?.[0]);
  }

  return toWorkloadMetrics({
    name: "read_latest",
    operations: reads,
    bytesWritten: 0,
    bytesRead,
    wallMs: performance.now() - started,
    samples,
  });
}

async function runLixReadHistory(args) {
  const { lix, dataset, reads, revisionDepth } = args;
  const samples = [];
  let bytesRead = 0;
  const started = performance.now();

  for (let i = 0; i < reads; i++) {
    const file = dataset[i % dataset.length];
    const offset = i % Math.max(1, revisionDepth);
    const opStarted = performance.now();
    const result = await lix.execute(
      "SELECT commit_id \
       FROM lix_state_history \
       WHERE file_id = ? \
         AND schema_key = 'lix_file_descriptor' \
         AND snapshot_content IS NOT NULL \
       ORDER BY depth DESC \
       LIMIT 1 OFFSET ?",
      [file.id, offset],
    );
    samples.push(performance.now() - opStarted);
    bytesRead += valueByteLength(result?.rows?.[0]?.[0]);
    maybeLogLoopProgress("lix read_history", i + 1, reads, started);
  }

  return toWorkloadMetrics({
    name: "read_history",
    operations: reads,
    bytesWritten: 0,
    bytesRead,
    wallMs: performance.now() - started,
    samples,
  });
}

async function runFossilBenchmark(args) {
  const { dataset, config, paths } = args;
  benchLog(`fossil init repo=${paths.fossilRepoPath}`);

  await rm(paths.fossilRoot, { recursive: true, force: true });
  await mkdir(paths.fossilCheckoutPath, { recursive: true });

  await runFossilCommand(["init", paths.fossilRepoPath], { cwd: paths.fossilRoot });
  await runFossilCommand(["open", paths.fossilRepoPath], { cwd: paths.fossilCheckoutPath });
  await configureFossilCheckout(paths.fossilCheckoutPath);

  const baselineBytes = await sqliteArtifactBytes(paths.fossilRepoPath);

  const ingest = await runFossilIngest({
    checkoutPath: paths.fossilCheckoutPath,
    dataset,
  });
  benchLog(`fossil ingest done ops=${ingest.operations} wall_ms=${ingest.wall_ms.toFixed(1)}`);
  const afterIngestBytes = await sqliteArtifactBytes(paths.fossilRepoPath);

  const update = await runFossilUpdate({
    checkoutPath: paths.fossilCheckoutPath,
    dataset,
    updateRounds: config.updateRounds,
    maxBlobBytes: config.maxBlobBytes,
  });
  benchLog(`fossil update done ops=${update.operations} wall_ms=${update.wall_ms.toFixed(1)}`);
  const afterUpdateBytes = await sqliteArtifactBytes(paths.fossilRepoPath);

  const readLatest = await runFossilReadLatest({
    checkoutPath: paths.fossilCheckoutPath,
    dataset,
    reads: config.historyReads,
  });
  benchLog(`fossil read_latest done ops=${readLatest.operations} wall_ms=${readLatest.wall_ms.toFixed(1)}`);
  const readHistory = await runFossilReadHistory({
    checkoutPath: paths.fossilCheckoutPath,
    dataset,
    reads: config.historyReads,
  });
  benchLog(`fossil read_history done ops=${readHistory.operations} wall_ms=${readHistory.wall_ms.toFixed(1)}`);

  await runFossilCommand(["close", "--force"], {
    cwd: paths.fossilCheckoutPath,
    tolerateFailure: true,
  });
  const afterReadsBytes = await sqliteArtifactBytes(paths.fossilRepoPath);

  const storageDetails = collectSqliteStorageMetrics(paths.fossilRepoPath, FOSSIL_STORAGE_TABLES);
  const logicalHistoryBytes = ingest.bytes_written + update.bytes_written;

  return {
    target: "fossil",
    artifactPath: paths.fossilRepoPath,
    workloads: [ingest, update, readLatest, readHistory],
    storage: {
      baselineBytes,
      afterIngestBytes,
      afterUpdateBytes,
      afterReadsBytes,
      logicalHistoryBytes,
      ingestWriteAmp: ratio(afterIngestBytes - baselineBytes, ingest.bytes_written),
      updateWriteAmp: ratio(afterUpdateBytes - afterIngestBytes, update.bytes_written),
      storageAmpAfterUpdate: ratio(afterUpdateBytes, logicalHistoryBytes),
      storageAmpAfterReads: ratio(afterReadsBytes, logicalHistoryBytes),
      ...storageDetails,
    },
  };
}

async function runLixCommitBenchmark(args) {
  const { dataset, config, dbPath } = args;
  await mkdir(dirname(dbPath), { recursive: true });
  await rm(dbPath, { force: true });
  await rm(`${dbPath}-wal`, { force: true });
  await rm(`${dbPath}-shm`, { force: true });

  const seedLix = await openLixAtPath(dbPath, true);
  await safeClose(seedLix);

  // Setup baseline history with one ingest commit, then measure one update commit.
  let lix = await openLixAtPath(dbPath, false);
  try {
    benchLog(`lix commit-setup ingest start files=${dataset.length}`);
    await runLixIngest({ lix, dataset });
    benchLog("lix commit-setup ingest done");
  } finally {
    await safeClose(lix);
  }

  const beforeCommitBytes = await sqliteArtifactBytes(dbPath);
  const beforeRows = collectSqliteRowCounts(dbPath, LIX_COMMIT_TRACE_TABLES);
  const commitPlan = buildLixSingleCommitUpdatePlan({
    dataset,
    maxBlobBytes: config.maxBlobBytes,
    round: 0,
  });

  const sqlTraceCollector = createSqlTraceCollector();
  lix = await openLixAtPathWithSqlTrace(dbPath, false, sqlTraceCollector);
  let commitWorkload;
  try {
    sqlTraceCollector.calls.length = 0;
    const opStarted = performance.now();
    await lix.execute(commitPlan.sql, commitPlan.params);
    const wallMs = performance.now() - opStarted;
    commitWorkload = toWorkloadMetrics({
      name: "single_commit",
      operations: 1,
      bytesWritten: commitPlan.bytesWritten,
      bytesRead: 0,
      wallMs,
      samples: [wallMs],
    });
  } finally {
    await safeClose(lix);
  }

  const afterCommitBytes = await sqliteArtifactBytes(dbPath);
  const afterRows = collectSqliteRowCounts(dbPath, LIX_COMMIT_TRACE_TABLES);
  const rowDelta = diffRowCounts(beforeRows, afterRows);

  return {
    target: "lix",
    artifactPath: dbPath,
    workloads: [commitWorkload],
    storage: {
      beforeCommitBytes,
      afterCommitBytes,
      commitByteDelta: afterCommitBytes - beforeCommitBytes,
    },
    trace: {
      sql: summarizeSqlTrace(sqlTraceCollector.calls),
      tableRowsBefore: beforeRows,
      tableRowsAfter: afterRows,
      tableRowDelta: rowDelta,
      commitRowsDelta: rowDelta.lix_internal_state_materialized_v1_lix_commit ?? null,
      sqliteBytes: {
        before: beforeCommitBytes,
        after: afterCommitBytes,
        delta: afterCommitBytes - beforeCommitBytes,
      },
    },
  };
}

async function runFossilCommitBenchmark(args) {
  const { dataset, config, paths } = args;
  benchLog(`fossil init repo=${paths.fossilRepoPath}`);
  await rm(paths.fossilRoot, { recursive: true, force: true });
  await mkdir(paths.fossilCheckoutPath, { recursive: true });

  await runFossilCommand(["init", paths.fossilRepoPath], { cwd: paths.fossilRoot });
  await runFossilCommand(["open", paths.fossilRepoPath], { cwd: paths.fossilCheckoutPath });
  await configureFossilCheckout(paths.fossilCheckoutPath);

  // Setup baseline history with one ingest commit.
  benchLog(`fossil commit-setup ingest start files=${dataset.length}`);
  await runFossilIngest({
    checkoutPath: paths.fossilCheckoutPath,
    dataset,
  });
  benchLog("fossil commit-setup ingest done");

  // Prepare one commit worth of changes in the working checkout.
  let bytesWritten = 0;
  for (let index = 0; index < dataset.length; index++) {
    const file = dataset[index];
    const nextData = mutateData(file, 0, index, config.maxBlobBytes);
    const destination = join(paths.fossilCheckoutPath, file.repoPath);
    await writeFile(destination, nextData);
    bytesWritten += nextData.byteLength;
    file.data = nextData;
  }

  const beforeCommitBytes = await sqliteArtifactBytes(paths.fossilRepoPath);
  const beforeRows = collectSqliteRowCounts(paths.fossilRepoPath, FOSSIL_COMMIT_TRACE_TABLES);

  const opStarted = performance.now();
  const commitResult = await runFossilCommit(paths.fossilCheckoutPath, "single commit bench");
  const wallMs = performance.now() - opStarted;
  const commitWorkload = toWorkloadMetrics({
    name: "single_commit",
    operations: 1,
    bytesWritten,
    bytesRead: 0,
    wallMs,
    samples: [wallMs],
  });

  const afterCommitBytes = await sqliteArtifactBytes(paths.fossilRepoPath);
  const afterRows = collectSqliteRowCounts(paths.fossilRepoPath, FOSSIL_COMMIT_TRACE_TABLES);
  const rowDelta = diffRowCounts(beforeRows, afterRows);

  await runFossilCommand(["close", "--force"], {
    cwd: paths.fossilCheckoutPath,
    tolerateFailure: true,
  });

  return {
    target: "fossil",
    artifactPath: paths.fossilRepoPath,
    workloads: [commitWorkload],
    storage: {
      beforeCommitBytes,
      afterCommitBytes,
      commitByteDelta: afterCommitBytes - beforeCommitBytes,
    },
    trace: {
      tableRowsBefore: beforeRows,
      tableRowsAfter: afterRows,
      tableRowDelta: rowDelta,
      commitRowsDelta: rowDelta.event ?? null,
      sqliteBytes: {
        before: beforeCommitBytes,
        after: afterCommitBytes,
        delta: afterCommitBytes - beforeCommitBytes,
      },
      commitStdoutBytes: Buffer.byteLength(String(commitResult.stdout ?? ""), "utf8"),
      commitStderrBytes: Buffer.byteLength(String(commitResult.stderr ?? ""), "utf8"),
    },
  };
}

function buildLixSingleCommitUpdatePlan(args) {
  const { dataset, maxBlobBytes, round } = args;
  const maxRowsPerBatch = Math.max(
    1,
    Math.floor(SQLITE_MAX_BIND_PARAMETERS / LIX_FILE_UPDATE_PARAM_COUNT),
  );
  if (dataset.length > maxRowsPerBatch) {
    throw new Error(
      `single_commit scenario requires one UPDATE batch; dataset has ${dataset.length} rows but SQLite bind-safe limit is ${maxRowsPerBatch}. Reduce BENCH_FILES_PER_CLASS.`,
    );
  }

  const whenClauses = [];
  const whereIds = [];
  const params = [];
  let paramIndex = 1;
  let bytesWritten = 0;
  for (let index = 0; index < dataset.length; index++) {
    const file = dataset[index];
    const nextData = mutateData(file, round, index, maxBlobBytes);
    whenClauses.push(`WHEN ?${paramIndex} THEN ?${paramIndex + 1}`);
    whereIds.push(`?${paramIndex}`);
    params.push(file.id, nextData);
    paramIndex += LIX_FILE_UPDATE_PARAM_COUNT;
    bytesWritten += nextData.byteLength;
    file.data = nextData;
  }

  return {
    bytesWritten,
    sql:
      "UPDATE lix_file " +
      `SET data = CASE id ${whenClauses.join(" ")} ELSE data END ` +
      `WHERE id IN (${whereIds.join(", ")})`,
    params,
  };
}

async function runFossilIngest(args) {
  const { checkoutPath, dataset } = args;
  const samples = [];
  let bytesWritten = 0;
  const started = performance.now();

  for (const file of dataset) {
    const destination = join(checkoutPath, file.repoPath);
    await mkdir(dirname(destination), { recursive: true });
    const opStarted = performance.now();
    await writeFile(destination, file.data);
    samples.push(performance.now() - opStarted);
    bytesWritten += file.data.byteLength;
  }

  await runFossilAddFiles(
    checkoutPath,
    dataset.map((file) => file.repoPath),
  );
  await runFossilCommit(checkoutPath, "ingest files");

  return toWorkloadMetrics({
    name: "ingest_files",
    operations: dataset.length,
    bytesWritten,
    bytesRead: 0,
    wallMs: performance.now() - started,
    samples,
  });
}

async function runFossilUpdate(args) {
  const { checkoutPath, dataset, updateRounds, maxBlobBytes } = args;
  const samples = [];
  let bytesWritten = 0;
  const started = performance.now();

  for (let round = 0; round < updateRounds; round++) {
    for (let index = 0; index < dataset.length; index++) {
      const file = dataset[index];
      const nextData = mutateData(file, round, index, maxBlobBytes);
      const destination = join(checkoutPath, file.repoPath);
      const opStarted = performance.now();
      await writeFile(destination, nextData);
      samples.push(performance.now() - opStarted);
      bytesWritten += nextData.byteLength;
      file.data = nextData;
    }
    await runFossilCommit(checkoutPath, `update round ${round + 1}`);
  }

  return toWorkloadMetrics({
    name: "update_files",
    operations: dataset.length * updateRounds,
    bytesWritten,
    bytesRead: 0,
    wallMs: performance.now() - started,
    samples,
  });
}

async function runFossilReadLatest(args) {
  const { checkoutPath, dataset, reads } = args;
  const samples = [];
  let bytesRead = 0;
  const started = performance.now();

  for (let i = 0; i < reads; i++) {
    const file = dataset[i % dataset.length];
    const opStarted = performance.now();
    const output = await runFossilCommand(["cat", file.repoPath], {
      cwd: checkoutPath,
      capture: "buffer",
    });
    samples.push(performance.now() - opStarted);
    bytesRead += output.stdout.length;
  }

  return toWorkloadMetrics({
    name: "read_latest",
    operations: reads,
    bytesWritten: 0,
    bytesRead,
    wallMs: performance.now() - started,
    samples,
  });
}

async function runFossilReadHistory(args) {
  const { checkoutPath, dataset, reads } = args;
  const samples = [];
  let bytesRead = 0;
  const started = performance.now();

  for (let i = 0; i < reads; i++) {
    const file = dataset[i % dataset.length];
    const opStarted = performance.now();
    const output = await runFossilCommand(["finfo", file.repoPath], {
      cwd: checkoutPath,
      capture: "text",
    });
    samples.push(performance.now() - opStarted);
    bytesRead += Buffer.byteLength(output.stdout, "utf8");
  }

  return toWorkloadMetrics({
    name: "read_history",
    operations: reads,
    bytesWritten: 0,
    bytesRead,
    wallMs: performance.now() - started,
    samples,
  });
}

async function runFossilCommit(checkoutPath, message) {
  const argsWithUserOverride = [
    "commit",
    "-m",
    message,
    "--hash",
    "--no-warnings",
    "--user-override",
    "bench",
  ];
  try {
    return await runFossilCommand(argsWithUserOverride, { cwd: checkoutPath });
  } catch (error) {
    const errorMessage = String(error?.message ?? error);
    if (errorMessage.includes("--user-override")) {
      return await runFossilCommand(["commit", "-m", message, "--hash", "--no-warnings"], {
        cwd: checkoutPath,
      });
    }
    throw error;
  }
}

async function runFossilAddFiles(checkoutPath, paths) {
  if (!Array.isArray(paths) || paths.length === 0) return;
  const chunkSize = 128;
  for (let i = 0; i < paths.length; i += chunkSize) {
    const chunk = paths.slice(i, i + chunkSize);
    await runFossilCommand(["add", "--force", ...chunk], { cwd: checkoutPath });
  }
}

async function configureFossilCheckout(checkoutPath) {
  const settings = [
    ["autosync", "off"],
    // Ensure benchmark content is handled as binary and not transformed.
    ["binary-glob", "*"],
    ["crlf-glob", ""],
    ["crnl-glob", ""],
    ["encoding-glob", ""],
  ];
  for (const [name, value] of settings) {
    await runFossilCommand(["settings", name, value], {
      cwd: checkoutPath,
      tolerateFailure: true,
    });
  }
}

function buildDataset(args) {
  const { filesPerClass, maxBlobBytes } = args;
  const entries = [];
  let index = 0;

  for (const classSpec of DATASET_CLASSES) {
    for (let classIndex = 0; classIndex < filesPerClass; classIndex++) {
      const seed = 0x5f3759df ^ (index * 0x9e3779b1);
      const initialBytes = Math.min(maxBlobBytes, classSpec.initialBytes);
      const data = generateData(classSpec.key, initialBytes, seed >>> 0);
      const leaf = `file-${String(classIndex).padStart(4, "0")}.${classSpec.ext}`;
      const repoPath = join("bench", classSpec.key, String(classIndex % 16), leaf);
      entries.push({
        id: `bench-${classSpec.key}-${String(classIndex).padStart(4, "0")}`,
        classKey: classSpec.key,
        repoPath,
        lixPath: `/${repoPath.replaceAll("\\", "/")}`,
        seed: seed >>> 0,
        data,
      });
      index += 1;
    }
  }
  return entries;
}

function summarizeDataset(dataset) {
  const sizes = dataset.map((entry) => entry.data.byteLength).sort((a, b) => a - b);
  return {
    files: dataset.length,
    totalBytes: sizes.reduce((sum, value) => sum + value, 0),
    p50Bytes: percentileFromSorted(sizes, 0.5),
    p95Bytes: percentileFromSorted(sizes, 0.95),
    maxBytes: sizes[sizes.length - 1] ?? 0,
    classes: DATASET_CLASSES.map((classSpec) => ({
      key: classSpec.key,
      files: dataset.filter((entry) => entry.classKey === classSpec.key).length,
    })),
  };
}

function generateData(classKey, bytes, seed) {
  const next = makeLcg(seed);

  if (classKey === "append_friendly") {
    const buffer = new Uint8Array(bytes);
    // Keep this class highly append/delta-friendly while forcing binary classification.
    const blockSize = 256;
    for (let i = 0; i < bytes; i += blockSize) {
      const marker = next() & 0xff;
      const end = Math.min(bytes, i + blockSize);
      for (let j = i; j < end; j++) {
        buffer[j] = marker;
      }
    }
    if (buffer.byteLength > 0) {
      buffer[0] = 0;
    }
    return buffer;
  }

  const buffer = new Uint8Array(bytes);
  for (let i = 0; i < bytes; i++) {
    if (classKey === "media_like") {
      const windowOffset = Math.floor(i / 2048) % 17;
      buffer[i] = (next() + windowOffset * 31) & 0xff;
    } else {
      buffer[i] = next() & 0xff;
    }
  }
  if (buffer.byteLength > 0) {
    buffer[0] = 0;
  }
  return buffer;
}

function mutateData(file, round, index, maxBlobBytes) {
  const original = file.data;
  const next = new Uint8Array(original);
  const seed = (file.seed ^ (round + 1) ^ ((index + 1) << 8)) >>> 0;
  const rng = makeLcg(seed);

  if (file.classKey === "append_friendly") {
    const suffixSize = Math.min(8192, Math.max(256, Math.floor(original.byteLength * 0.1)));
    const suffix = new Uint8Array(suffixSize);
    for (let i = 0; i < suffixSize; i++) {
      suffix[i] = rng() & 0xff;
    }
    if (suffix.byteLength > 0) {
      suffix[0] = 0;
    }
    const combined = concatBytes(next, suffix);
    if (combined.byteLength <= maxBlobBytes) return combined;
    return combined.slice(combined.byteLength - maxBlobBytes);
  }

  const patchCount = file.classKey === "media_like" ? 6 : 10;
  const patchBytes = file.classKey === "media_like" ? 3072 : 1024;
  for (let patch = 0; patch < patchCount; patch++) {
    if (next.byteLength === 0) break;
    const position = rng() % next.byteLength;
    const until = Math.min(next.byteLength, position + patchBytes);
    for (let i = position; i < until; i++) {
      next[i] = (next[i] ^ (rng() & 0xff)) & 0xff;
    }
  }
  return next;
}

function concatBytes(a, b) {
  const out = new Uint8Array(a.byteLength + b.byteLength);
  out.set(a, 0);
  out.set(b, a.byteLength);
  return out;
}

function cloneDataset(dataset) {
  return dataset.map((entry) => ({
    ...entry,
    data: new Uint8Array(entry.data),
  }));
}

function toWorkloadMetrics(args) {
  const { name, operations, bytesWritten, bytesRead, wallMs, samples } = args;
  return {
    name,
    operations,
    bytes_written: bytesWritten,
    bytes_read: bytesRead,
    wall_ms: wallMs,
    p50_ms: percentile(samples, 0.5),
    p95_ms: percentile(samples, 0.95),
    ops_per_sec: wallMs > 0 ? operations / (wallMs / 1000) : 0,
  };
}

function buildComparison(lix, fossil) {
  const lixWorkload = indexByName(lix.workloads);
  const fossilWorkload = indexByName(fossil.workloads);
  return {
    ingestOpsRatioLixOverFossil: ratio(
      lixWorkload.ingest_files?.ops_per_sec ?? 0,
      fossilWorkload.ingest_files?.ops_per_sec ?? 0,
    ),
    updateOpsRatioLixOverFossil: ratio(
      lixWorkload.update_files?.ops_per_sec ?? 0,
      fossilWorkload.update_files?.ops_per_sec ?? 0,
    ),
    latestReadOpsRatioLixOverFossil: ratio(
      lixWorkload.read_latest?.ops_per_sec ?? 0,
      fossilWorkload.read_latest?.ops_per_sec ?? 0,
    ),
    historyReadOpsRatioLixOverFossil: ratio(
      lixWorkload.read_history?.ops_per_sec ?? 0,
      fossilWorkload.read_history?.ops_per_sec ?? 0,
    ),
    storageAmpRatioLixOverFossil: ratio(
      lix.storage.storageAmpAfterUpdate,
      fossil.storage.storageAmpAfterUpdate,
    ),
    storageAmpAfterReadsRatioLixOverFossil: ratio(
      lix.storage.storageAmpAfterReads,
      fossil.storage.storageAmpAfterReads,
    ),
  };
}

function buildCommitComparison(lix, fossil) {
  const lixWorkload = indexByName(lix.workloads);
  const fossilWorkload = indexByName(fossil.workloads);
  return {
    singleCommitOpsRatioLixOverFossil: ratio(
      lixWorkload.single_commit?.ops_per_sec ?? 0,
      fossilWorkload.single_commit?.ops_per_sec ?? 0,
    ),
    singleCommitWallMsRatioLixOverFossil: ratio(
      lixWorkload.single_commit?.wall_ms ?? 0,
      fossilWorkload.single_commit?.wall_ms ?? 0,
    ),
    commitByteDeltaRatioLixOverFossil: ratio(
      lix.storage?.commitByteDelta ?? 0,
      fossil.storage?.commitByteDelta ?? 0,
    ),
  };
}

function indexByName(workloads) {
  const out = {};
  for (const workload of workloads) {
    out[workload.name] = workload;
  }
  return out;
}

function collectSqliteStorageMetrics(dbPath, trackedTables) {
  const details = {
    dbFileBytes: fileBytesOrZeroSync(dbPath),
    walFileBytes: fileBytesOrZeroSync(`${dbPath}-wal`),
    shmFileBytes: fileBytesOrZeroSync(`${dbPath}-shm`),
    pageSize: 0,
    pageCount: 0,
    freelistCount: 0,
    estimatedDbBytes: 0,
    tableBytes: null,
    indexBytes: null,
    tableRows: {},
    trackedTableBytes: {},
  };

  let db;
  try {
    db = new Database(dbPath, { readonly: true, fileMustExist: true });
    details.pageSize = Number(db.pragma("page_size", { simple: true }) ?? 0);
    details.pageCount = Number(db.pragma("page_count", { simple: true }) ?? 0);
    details.freelistCount = Number(db.pragma("freelist_count", { simple: true }) ?? 0);
    details.estimatedDbBytes = details.pageSize * details.pageCount;

    details.tableBytes = queryDbstatBytesByType(db, "table");
    details.indexBytes = queryDbstatBytesByType(db, "index");

    for (const tableName of trackedTables) {
      details.tableRows[tableName] = queryTableCount(db, tableName);
      details.trackedTableBytes[tableName] = queryDbstatBytesForObject(db, tableName);
    }
  } catch {
    // Best-effort metrics only; file sizes are still populated.
  } finally {
    if (db) {
      db.close();
    }
  }

  return details;
}

function queryDbstatBytesByType(db, schemaType) {
  try {
    const row = db
      .prepare(
        "SELECT CAST(COALESCE(SUM(d.pgsize), 0) AS INTEGER) AS bytes \
         FROM dbstat d \
         JOIN sqlite_schema s ON s.name = d.name \
         WHERE s.type = ?",
      )
      .get(schemaType);
    return Number(row?.bytes ?? 0);
  } catch {
    return null;
  }
}

function queryDbstatBytesForObject(db, objectName) {
  try {
    const row = db
      .prepare("SELECT CAST(COALESCE(SUM(pgsize), 0) AS INTEGER) AS bytes FROM dbstat WHERE name = ?")
      .get(objectName);
    return Number(row?.bytes ?? 0);
  } catch {
    return null;
  }
}

function queryTableCount(db, tableName) {
  try {
    const tableRow = db
      .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")
      .get(tableName);
    if (!tableRow) return null;
    const row = db.prepare(`SELECT COUNT(*) AS c FROM "${tableName}"`).get();
    return Number(row?.c ?? 0);
  } catch {
    return null;
  }
}

async function sqliteArtifactBytes(dbPath) {
  return (
    (await fileBytesOrZero(dbPath)) +
    (await fileBytesOrZero(`${dbPath}-wal`)) +
    (await fileBytesOrZero(`${dbPath}-shm`))
  );
}

async function openLixAtPath(dbPath, seedDeterministic) {
  const backend = await createBetterSqlite3Backend({ filename: dbPath });
  return openLix({
    backend,
    keyValues: seedDeterministic
      ? [
          {
            key: "lix_deterministic_mode",
            value: { enabled: true },
            lixcol_version_id: "global",
          },
        ]
      : undefined,
  });
}

async function openLixAtPathWithSqlTrace(dbPath, seedDeterministic, traceCollector) {
  const backend = await createBetterSqlite3Backend({ filename: dbPath });
  const tracedBackend = wrapBackendWithSqlTrace(backend, traceCollector);
  return openLix({
    backend: tracedBackend,
    keyValues: seedDeterministic
      ? [
          {
            key: "lix_deterministic_mode",
            value: { enabled: true },
            lixcol_version_id: "global",
          },
        ]
      : undefined,
  });
}

function wrapBackendWithSqlTrace(backend, traceCollector) {
  return {
    dialect: backend.dialect,
    async execute(sql, params) {
      const started = performance.now();
      const result = await backend.execute(sql, params);
      traceCollector.calls.push({
        source: "backend",
        sql,
        paramsCount: params.length,
        elapsedMs: performance.now() - started,
      });
      return result;
    },
    async beginTransaction() {
      const tx = await backend.beginTransaction();
      return {
        dialect: tx.dialect,
        async execute(sql, params) {
          const started = performance.now();
          const result = await tx.execute(sql, params);
          traceCollector.calls.push({
            source: "transaction",
            sql,
            paramsCount: params.length,
            elapsedMs: performance.now() - started,
          });
          return result;
        },
        async commit() {
          return tx.commit();
        },
        async rollback() {
          return tx.rollback();
        },
      };
    },
    async exportSnapshot() {
      return backend.exportSnapshot();
    },
    async close() {
      if (typeof backend.close === "function") {
        await backend.close();
      }
    },
  };
}

function createSqlTraceCollector() {
  return { calls: [] };
}

function summarizeSqlTrace(calls) {
  const byType = {
    create_table: 0,
    create_index: 0,
    select: 0,
    insert: 0,
    update: 0,
    delete: 0,
    other: 0,
  };
  const statementMap = new Map();
  const durations = [];
  let totalSqlMs = 0;

  for (const call of calls) {
    const type = classifySqlType(call.sql);
    byType[type] = (byType[type] ?? 0) + 1;
    totalSqlMs += call.elapsedMs;
    durations.push(call.elapsedMs);
    const key = normalizeSqlForTrace(call.sql);
    const existing = statementMap.get(key) ?? { count: 0, totalMs: 0 };
    existing.count += 1;
    existing.totalMs += call.elapsedMs;
    statementMap.set(key, existing);
  }

  const topStatements = [...statementMap.entries()]
    .map(([sql, value]) => ({
      sql,
      count: value.count,
      total_ms: value.totalMs,
    }))
    .sort((a, b) => b.total_ms - a.total_ms || b.count - a.count)
    .slice(0, 12);

  return {
    totalCalls: calls.length,
    totalSqlMs,
    p50Ms: percentile(durations, 0.5),
    p95Ms: percentile(durations, 0.95),
    byType,
    topStatements,
  };
}

function classifySqlType(sql) {
  const normalized = String(sql).trim().toUpperCase();
  if (normalized.startsWith("CREATE TABLE")) return "create_table";
  if (normalized.startsWith("CREATE INDEX")) return "create_index";
  if (normalized.startsWith("SELECT") || normalized.startsWith("WITH")) return "select";
  if (normalized.startsWith("INSERT")) return "insert";
  if (normalized.startsWith("UPDATE")) return "update";
  if (normalized.startsWith("DELETE")) return "delete";
  return "other";
}

function normalizeSqlForTrace(sql) {
  return String(sql).replace(/\s+/g, " ").trim().slice(0, 220);
}

function collectSqliteRowCounts(dbPath, relationNames) {
  const rows = {};
  let db;
  try {
    db = new Database(dbPath, { readonly: true, fileMustExist: true });
    for (const relationName of relationNames) {
      try {
        const row = db.prepare(`SELECT COUNT(*) AS c FROM "${relationName}"`).get();
        rows[relationName] = Number(row?.c ?? 0);
      } catch {
        rows[relationName] = null;
      }
    }
  } catch {
    for (const relationName of relationNames) {
      rows[relationName] = null;
    }
  } finally {
    if (db) {
      db.close();
    }
  }
  return rows;
}

function diffRowCounts(before, after) {
  const output = {};
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const key of keys) {
    const beforeValue = before[key];
    const afterValue = after[key];
    output[key] =
      Number.isFinite(beforeValue) && Number.isFinite(afterValue) ? afterValue - beforeValue : null;
  }
  return output;
}

function fileBytesOrZero(path) {
  return stat(path).then((meta) => Number(meta.size)).catch(() => 0);
}

function fileBytesOrZeroSync(path) {
  try {
    return Number(statSync(path).size);
  } catch {
    return 0;
  }
}

async function runFossilCommand(
  args,
  options = {
    cwd: process.cwd(),
    capture: "text",
    tolerateFailure: false,
  },
) {
  const { cwd, capture = "text", tolerateFailure = false } = options;
  const fossilEnv = {
    ...process.env,
    FOSSIL_USER: process.env.FOSSIL_USER ?? "bench",
    FOSSIL_EMAIL: process.env.FOSSIL_EMAIL ?? "bench@example.com",
  };

  const result = await runCommand({
    cmd: "fossil",
    args,
    cwd,
    env: fossilEnv,
    capture,
  });

  if (result.code !== 0 && !tolerateFailure) {
    throw new Error(
      `fossil ${args.join(" ")} failed (${result.code})\n${result.stderr || "<no stderr>"}`,
    );
  }
  return result;
}

async function ensureFossilAvailable() {
  let result;
  try {
    result = await runCommand({
      cmd: "fossil",
      args: ["version"],
      cwd: process.cwd(),
      capture: "text",
    });
  } catch {
    throw new Error(
      "fossil is not available on PATH. Install Fossil or run with BENCH_TARGET=lix.",
    );
  }
  if (result.code !== 0) {
    throw new Error(
      "fossil is not available on PATH. Install Fossil or run with BENCH_TARGET=lix.\n" +
        (result.stderr || ""),
    );
  }
}

function runCommand(args) {
  const { cmd, cwd, env, capture = "text" } = args;
  const commandArgs = args.args ?? [];
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, commandArgs, {
      cwd,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    const stdoutChunks = [];
    const stderrChunks = [];
    child.stdout.on("data", (chunk) => stdoutChunks.push(chunk));
    child.stderr.on("data", (chunk) => stderrChunks.push(chunk));
    child.on("error", (error) => reject(error));
    child.on("close", (code) => {
      const stdoutBuffer = Buffer.concat(stdoutChunks);
      const stderrBuffer = Buffer.concat(stderrChunks);
      resolve({
        code: Number(code ?? 0),
        stdout: capture === "buffer" ? stdoutBuffer : stdoutBuffer.toString("utf8"),
        stderr: stderrBuffer.toString("utf8"),
      });
    });
  });
}

function valueByteLength(value) {
  if (value === null || value === undefined) return 0;
  if (typeof value === "string") return Buffer.byteLength(value, "utf8");
  if (typeof value === "number") return 8;
  if (typeof value === "bigint") return 8;
  if (value instanceof Uint8Array) return value.byteLength;
  if (value instanceof ArrayBuffer) return value.byteLength;
  if (typeof value.asBlob === "function") {
    const blob = value.asBlob();
    if (blob instanceof Uint8Array) return blob.byteLength;
  }
  if (typeof value.asText === "function") {
    const text = value.asText();
    if (typeof text === "string") return Buffer.byteLength(text, "utf8");
  }
  return Buffer.byteLength(String(value), "utf8");
}

function makeLcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state;
  };
}

function percentile(samples, ratioValue) {
  if (!samples || samples.length === 0) return 0;
  const sorted = [...samples].sort((a, b) => a - b);
  return percentileFromSorted(sorted, ratioValue);
}

function percentileFromSorted(sorted, ratioValue) {
  if (!sorted || sorted.length === 0) return 0;
  const index = Math.max(0, Math.min(sorted.length - 1, Math.floor(sorted.length * ratioValue)));
  return sorted[index];
}

function ratio(numerator, denominator) {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator)) return 0;
  if (denominator === 0) return 0;
  return numerator / denominator;
}

function parsePositiveInt(name, defaultValue) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw.trim() === "") return defaultValue;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be > 0, got "${raw}"`);
  }
  return parsed;
}

function parseBooleanFlag(name, defaultValue) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw.trim() === "") return defaultValue;
  const normalized = raw.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  throw new Error(`${name} must be a boolean-like value, got "${raw}"`);
}

function parseScenario(value) {
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "full" || normalized === "commit") {
    return normalized;
  }
  throw new Error(`BENCH_SCENARIO must be one of full|commit, got "${value}"`);
}

function parseTarget(value) {
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "lix" || normalized === "fossil" || normalized === "both") {
    return normalized;
  }
  throw new Error(`BENCH_TARGET must be one of both|lix|fossil, got "${value}"`);
}

function benchLog(message) {
  if (!CONFIG.progress) return;
  console.log(`[bench] ${message}`);
}

function maybeLogLoopProgress(label, current, total, startedAt) {
  if (!CONFIG.progress || total <= 1) return;
  const interval = Math.max(1, Math.floor(total / Math.max(1, CONFIG.progressSlices)));
  if (current !== 1 && current !== total && current % interval !== 0) return;
  const elapsedMs = performance.now() - startedAt;
  const opsPerSec = elapsedMs > 0 ? current / (elapsedMs / 1000) : 0;
  console.log(
    `[bench] ${label} ${current}/${total} elapsed_ms=${elapsedMs.toFixed(1)} ops_per_sec=${opsPerSec.toFixed(2)}`,
  );
}

async function safeClose(lix) {
  try {
    await lix.close();
  } catch {
    // ignore
  }
}

function printSummary(report, outputPath) {
  const commitScenario = report.config?.scenario === "commit";
  const lines = [];
  lines.push("");
  lines.push(commitScenario ? "Fossil vs Lix single-commit benchmark" : "Fossil vs Lix benchmark");
  lines.push(`dataset files=${report.dataset.files} total_bytes=${report.dataset.totalBytes}`);

  if (report.targets.lix) {
    lines.push(commitScenario ? formatCommitTargetSummary(report.targets.lix) : formatTargetSummary(report.targets.lix));
  }
  if (report.targets.fossil) {
    lines.push(commitScenario ? formatCommitTargetSummary(report.targets.fossil) : formatTargetSummary(report.targets.fossil));
  }
  if (report.comparison) {
    lines.push("");
    lines.push("comparison ratios (lix / fossil)");
    if (commitScenario) {
      lines.push(
        `single_commit_ops=${report.comparison.singleCommitOpsRatioLixOverFossil.toFixed(3)} ` +
          `single_commit_wall_ms=${report.comparison.singleCommitWallMsRatioLixOverFossil.toFixed(3)}`,
      );
      lines.push(
        `commit_byte_delta_ratio=${report.comparison.commitByteDeltaRatioLixOverFossil.toFixed(3)}`,
      );
    } else {
      lines.push(
        `ingest=${report.comparison.ingestOpsRatioLixOverFossil.toFixed(3)} ` +
          `update=${report.comparison.updateOpsRatioLixOverFossil.toFixed(3)} ` +
          `read_latest=${report.comparison.latestReadOpsRatioLixOverFossil.toFixed(3)} ` +
          `read_history=${report.comparison.historyReadOpsRatioLixOverFossil.toFixed(3)}`,
      );
      lines.push(
        `storage_amp_ratio=${report.comparison.storageAmpRatioLixOverFossil.toFixed(3)}`,
      );
      lines.push(
        `storage_amp_after_reads_ratio=${report.comparison.storageAmpAfterReadsRatioLixOverFossil.toFixed(3)}`,
      );
    }
  }

  lines.push("");
  lines.push(`report: ${outputPath}`);
  console.log(lines.join("\n"));
}

function formatTargetSummary(target) {
  const byName = indexByName(target.workloads);
  return (
    "" +
    `\n${target.target}\n` +
    `  ingest ops/s=${(byName.ingest_files?.ops_per_sec ?? 0).toFixed(2)}\n` +
    `  update ops/s=${(byName.update_files?.ops_per_sec ?? 0).toFixed(2)}\n` +
    `  read_latest ops/s=${(byName.read_latest?.ops_per_sec ?? 0).toFixed(2)}\n` +
    `  read_history ops/s=${(byName.read_history?.ops_per_sec ?? 0).toFixed(2)}\n` +
    `  storage_amp_after_update=${(target.storage.storageAmpAfterUpdate ?? 0).toFixed(3)}\n` +
    `  storage_amp_after_reads=${(target.storage.storageAmpAfterReads ?? 0).toFixed(3)}\n` +
    `  artifact=${target.artifactPath}`
  );
}

function formatCommitTargetSummary(target) {
  const byName = indexByName(target.workloads);
  const commit = byName.single_commit;
  const traceSqlCalls = target.trace?.sql?.totalCalls;
  return (
    "" +
    `\n${target.target}\n` +
    `  single_commit ops/s=${(commit?.ops_per_sec ?? 0).toFixed(2)}\n` +
    `  single_commit wall_ms=${(commit?.wall_ms ?? 0).toFixed(2)}\n` +
    `  commit_byte_delta=${target.storage?.commitByteDelta ?? 0}\n` +
    `  trace_sql_calls=${traceSqlCalls ?? "n/a"}\n` +
    `  artifact=${target.artifactPath}`
  );
}

main().catch((error) => {
  console.error("fossil-vs-lix benchmark failed");
  console.error(String(error?.stack ?? error));
  process.exitCode = 1;
});
