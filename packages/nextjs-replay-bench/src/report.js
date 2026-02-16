export function summarizeSamples(samples) {
  if (samples.length === 0) {
    return {
      count: 0,
      meanMs: 0,
      minMs: 0,
      maxMs: 0,
      p50Ms: 0,
      p95Ms: 0,
    };
  }

  const sorted = [...samples].sort((a, b) => a - b);
  const meanMs = samples.reduce((sum, value) => sum + value, 0) / samples.length;

  return {
    count: samples.length,
    meanMs,
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
  };
}

export function printProgress(state) {
  const {
    label,
    index,
    total,
    commitSha,
    elapsedMs,
    changedPaths,
    commitsApplied,
    commitsNoop,
  } = state;
  const pct = ((index / total) * 100).toFixed(1);
  const elapsedSec = (elapsedMs / 1000).toFixed(1);
  const phase = label ? `:${label}` : "";

  console.log(
    color(
      `[progress${phase}] ${index}/${total} (${pct}%) commit=${commitSha.slice(0, 12)} changed_paths=${changedPaths} applied=${commitsApplied} noop=${commitsNoop} elapsed=${elapsedSec}s`,
      "dim",
    ),
  );
}

export function printSummary(report) {
  const warmupRequested = Number(report.commitTotals.warmupRequested ?? 0);
  const warmupUsed = Number(report.commitTotals.warmupUsed ?? 0);
  const warmupApplied = Number(report.commitTotals.warmupApplied ?? 0);
  const warmupNoop = Number(report.commitTotals.warmupNoop ?? 0);
  const measuredDiscovered = Number(
    report.commitTotals.measuredDiscovered ?? report.commitTotals.discovered,
  );

  console.log("");
  console.log(color("Next.js Replay Benchmark", "bold"));
  console.log(color(`Commits requested (measured): ${report.config.commitLimit}`, "dim"));
  if (warmupUsed > 0 || warmupRequested > 0) {
    console.log(color(`Commits requested (warmup): ${warmupRequested}`, "dim"));
    console.log(color(`Commits used (warmup): ${warmupUsed}`, "dim"));
  }
  console.log(color(`Commits discovered (total): ${report.commitTotals.discovered}`, "dim"));
  console.log(color(`Commits discovered (measured): ${measuredDiscovered}`, "dim"));
  if (warmupUsed > 0) {
    console.log(color(`Warmup applied: ${warmupApplied}`, "dim"));
    console.log(color(`Warmup skipped (no file changes): ${warmupNoop}`, "dim"));
  }
  console.log(color(`Commits applied (measured): ${report.commitTotals.applied}`, "dim"));
  console.log(color(`Commits skipped (measured, no file changes): ${report.commitTotals.noop}`, "dim"));
  if (Number(report.timings.warmupMs ?? 0) > 0) {
    console.log(color(`Warmup duration: ${formatMs(report.timings.warmupMs)}`, "dim"));
  }
  console.log(color(`Replay duration (measured): ${formatMs(report.timings.replayMs)}`, "dim"));
  if (typeof report.timings.overallReplayMs === "number") {
    console.log(color(`Replay duration (overall): ${formatMs(report.timings.overallReplayMs)}`, "dim"));
  }
  if (typeof report.timings.commitLoopMs === "number") {
    console.log(color(`Commit loop duration (measured): ${formatMs(report.timings.commitLoopMs)}`, "dim"));
  }

  console.log("");
  console.log(`Commit throughput: ${report.throughput.commitsPerSecond.toFixed(2)} commits/s`);
  if (typeof report.throughput.commitsPerSecondCommitLoop === "number") {
    console.log(
      `Commit throughput (apply loop only): ${report.throughput.commitsPerSecondCommitLoop.toFixed(2)} commits/s`,
    );
  }
  console.log(`Changed paths/sec: ${report.throughput.changedPathsPerSecond.toFixed(2)}`);
  console.log(`Blob ingest MB/s: ${report.throughput.blobMegabytesPerSecond.toFixed(2)}`);
  if (typeof report.throughput.executeStatementsPerSecond === "number") {
    console.log(
      `SQL statements/sec (engine): ${report.throughput.executeStatementsPerSecond.toFixed(2)}`,
    );
  }

  console.log("");
  console.log("Commit execution latency");
  console.log(`  mean: ${formatMs(report.timings.commit.meanMs)}`);
  console.log(`  p50:  ${formatMs(report.timings.commit.p50Ms)}`);
  console.log(`  p95:  ${formatMs(report.timings.commit.p95Ms)}`);
  console.log(`  max:  ${formatMs(report.timings.commit.maxMs)}`);

  if (Array.isArray(report.timings.phaseBreakdown) && report.timings.phaseBreakdown.length > 0) {
    console.log("");
    console.log("Replay phase breakdown");
    for (const phase of report.timings.phaseBreakdown) {
      const label = String(phase.phase).padEnd(21, " ");
      const sharePct = Number(phase.sharePct ?? 0).toFixed(1);
      console.log(`  ${label} ${formatMs(Number(phase.totalMs ?? 0))} (${sharePct}%)`);
    }
  }

  if (report.timings.setup) {
    console.log("");
    console.log("Setup timing");
    console.log(`  repo setup: ${formatMs(Number(report.timings.setup.repoSetupMs ?? 0))}`);
    console.log(
      `  commit discovery: ${formatMs(Number(report.timings.setup.commitDiscoveryMs ?? 0))}`,
    );
    console.log(`  openLix: ${formatMs(Number(report.timings.setup.lixOpenMs ?? 0))}`);
    console.log(`  plugin install: ${formatMs(Number(report.timings.setup.pluginInstallMs ?? 0))}`);
  }

  if (report.timings.postReplay) {
    console.log("");
    console.log("Post-replay timing");
    console.log(
      `  storage counters: ${formatMs(Number(report.timings.postReplay.storageQueryMs ?? 0))}`,
    );
    console.log(
      `  snapshot export: ${formatMs(Number(report.timings.postReplay.snapshotExportMs ?? 0))}`,
    );
  }

  console.log("");
  console.log("Storage counters");
  console.log(`  lix_file rows: ${report.storage.fileRows}`);
  console.log(`  internal changes: ${report.storage.internalChangeRows}`);
  console.log(`  internal snapshots: ${report.storage.internalSnapshotRows}`);

  if (report.storage.topSchemaCounts.length > 0) {
    console.log("");
    console.log("Top schema change counts");
    for (const row of report.storage.topSchemaCounts) {
      console.log(`  ${String(row.schemaKey).padEnd(28, " ")} ${row.rowCount}`);
    }
  }

  if (Array.isArray(report.slowestStatements) && report.slowestStatements.length > 0) {
    console.log("");
    console.log("Slowest SQL statements");
    for (const row of report.slowestStatements.slice(0, 5)) {
      const commit = String(row.commitSha ?? "").slice(0, 12);
      const statementIndex = Number(row.statementIndex ?? -1);
      const sqlChars = Number(row.sqlChars ?? 0);
      console.log(
        `  ${formatMs(Number(row.durationMs ?? 0))} commit=${commit} stmt=${statementIndex} chars=${sqlChars}`,
      );
    }
  }
}

function percentile(sorted, ratio) {
  if (sorted.length === 0) {
    return 0;
  }
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * ratio)));
  return sorted[idx];
}

function formatMs(value) {
  return `${value.toFixed(2)}ms`;
}

const ANSI = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
};

function color(text, style) {
  const supportsColor = !process.env.NO_COLOR && Boolean(process.stdout?.isTTY);
  if (!supportsColor) {
    return text;
  }
  const code = ANSI[style];
  if (!code) {
    return text;
  }
  return `${code}${text}${ANSI.reset}`;
}
