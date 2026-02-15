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

  console.log(
    color(
      `[progress] ${index}/${total} (${pct}%) commit=${commitSha.slice(0, 12)} changed_paths=${changedPaths} applied=${commitsApplied} noop=${commitsNoop} elapsed=${elapsedSec}s`,
      "dim",
    ),
  );
}

export function printSummary(report) {
  console.log("");
  console.log(color("Next.js Replay Benchmark", "bold"));
  console.log(color(`Commits requested: ${report.config.commitLimit}`, "dim"));
  console.log(color(`Commits discovered: ${report.commitTotals.discovered}`, "dim"));
  console.log(color(`Commits applied: ${report.commitTotals.applied}`, "dim"));
  console.log(color(`Commits skipped (no file changes): ${report.commitTotals.noop}`, "dim"));
  console.log(color(`Replay duration: ${formatMs(report.timings.replayMs)}`, "dim"));

  console.log("");
  console.log(`Commit throughput: ${report.throughput.commitsPerSecond.toFixed(2)} commits/s`);
  console.log(`Changed paths/sec: ${report.throughput.changedPathsPerSecond.toFixed(2)}`);
  console.log(`Blob ingest MB/s: ${report.throughput.blobMegabytesPerSecond.toFixed(2)}`);

  console.log("");
  console.log("Commit execution latency");
  console.log(`  mean: ${formatMs(report.timings.commit.meanMs)}`);
  console.log(`  p50:  ${formatMs(report.timings.commit.p50Ms)}`);
  console.log(`  p95:  ${formatMs(report.timings.commit.p95Ms)}`);
  console.log(`  max:  ${formatMs(report.timings.commit.maxMs)}`);

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
