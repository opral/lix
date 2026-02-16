import { mkdtemp, mkdir, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { ensureGitRepo, listLinearCommits, readCommitPatchSet } from "./git-history.js";
import { printProgress, summarizeSamples } from "./report.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = join(__dirname, "..", "results");
const OUTPUT_PATH = join(OUTPUT_DIR, "nextjs-replay.git-files.bench.json");
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
  reportPath: process.env.BENCH_GIT_FILES_REPORT_PATH ?? OUTPUT_PATH,
};

async function main() {
  const startedAt = new Date().toISOString();
  const benchmarkStarted = performance.now();

  const repoSetupStarted = performance.now();
  const sourceRepo = await ensureGitRepo({
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
  const commits = await listLinearCommits(sourceRepo.repoPath, {
    ref: CONFIG.repoRef,
    maxCount: totalRequestedCommits,
    firstParent: CONFIG.firstParent,
  });
  const commitDiscoveryMs = performance.now() - commitDiscoveryStarted;

  if (commits.length === 0) {
    throw new Error(`no commits found at ${sourceRepo.repoPath} (${CONFIG.repoRef})`);
  }

  const warmupCommitCount = Math.min(CONFIG.warmupCommitCount, commits.length);
  const measuredCommits = commits.slice(warmupCommitCount);
  if (measuredCommits.length === 0) {
    throw new Error(
      `warmup consumed all discovered commits (warmup=${warmupCommitCount}, discovered=${commits.length})`,
    );
  }

  const tempRoot = await mkdtemp(join(tmpdir(), "nextjs-git-files-replay-"));
  const gitRepoPath = join(tempRoot, "repo");
  await mkdir(gitRepoPath, { recursive: true });
  await runCommand("git", ["init", "-q"], { cwd: gitRepoPath });
  await runCommand("git", ["config", "user.email", "bench@example.com"], { cwd: gitRepoPath });
  await runCommand("git", ["config", "user.name", "nextjs-replay-bench"], { cwd: gitRepoPath });

  if (CONFIG.showProgress) {
    console.log(
      `[progress] replaying ${commits.length} commits (warmup=${warmupCommitCount}, measured=${measuredCommits.length}) from ${sourceRepo.repoPath} (${sourceRepo.source})`,
    );
  }

  const phaseTotalsMs = {
    readPatchSetMs: 0,
    applyFilesMs: 0,
    gitStageCommitMs: 0,
  };
  const commitDurations = [];
  const slowestCommits = [];

  let warmupApplied = 0;
  let warmupNoop = 0;
  let measuredApplied = 0;
  let measuredNoop = 0;
  let totalChangedPaths = 0;
  let warmupChangedPaths = 0;
  let totalBlobBytes = 0;

  const loopStarted = performance.now();
  let measuredStarted = warmupCommitCount === 0 ? loopStarted : null;
  let warmupMs = 0;

  for (let index = 0; index < commits.length; index++) {
    if (index === warmupCommitCount && measuredStarted === null) {
      warmupMs = performance.now() - loopStarted;
      measuredStarted = performance.now();
    }
    const isWarmup = index < warmupCommitCount;
    const commitSha = commits[index];

    const readPatchSetStarted = performance.now();
    const patchSet = await readCommitPatchSet(sourceRepo.repoPath, commitSha);
    const readPatchSetMs = performance.now() - readPatchSetStarted;
    if (!isWarmup) {
      phaseTotalsMs.readPatchSetMs += readPatchSetMs;
    }

    if (!isWarmup) {
      totalChangedPaths += patchSet.changes.length;
    } else {
      warmupChangedPaths += patchSet.changes.length;
    }

    if (patchSet.changes.length === 0) {
      if (isWarmup) {
        warmupNoop += 1;
      } else {
        measuredNoop += 1;
      }
      printMaybeProgress({
        commitSha,
        commitIndex: index,
        commits,
        warmupCommitCount,
        warmupChangedPaths,
        measuredChangedPaths: totalChangedPaths,
        warmupApplied,
        warmupNoop,
        measuredApplied,
        measuredNoop,
        benchmarkStarted,
      });
      continue;
    }

    const commitStarted = performance.now();

    const applyFilesStarted = performance.now();
    const appliedBlobBytes = await applyPatchSetToRepo(gitRepoPath, patchSet);
    const applyFilesMs = performance.now() - applyFilesStarted;

    const gitStageCommitStarted = performance.now();
    await runCommand("git", ["add", "-A"], { cwd: gitRepoPath });
    await runCommand(
      "git",
      ["commit", "-q", "--allow-empty", "-m", `replay ${commitSha.slice(0, 12)}`],
      { cwd: gitRepoPath },
    );
    const gitStageCommitMs = performance.now() - gitStageCommitStarted;

    const commitTotalMs = performance.now() - commitStarted;

    if (isWarmup) {
      warmupApplied += 1;
    } else {
      measuredApplied += 1;
      totalBlobBytes += appliedBlobBytes;
      phaseTotalsMs.applyFilesMs += applyFilesMs;
      phaseTotalsMs.gitStageCommitMs += gitStageCommitMs;
      commitDurations.push(commitTotalMs);
      pushSlowCommit(slowestCommits, {
        commitSha,
        durationMs: commitTotalMs,
        changedPaths: patchSet.changes.length,
      });
    }

    printMaybeProgress({
      commitSha,
      commitIndex: index,
      commits,
      warmupCommitCount,
      warmupChangedPaths,
      measuredChangedPaths: totalChangedPaths,
      warmupApplied,
      warmupNoop,
      measuredApplied,
      measuredNoop,
      benchmarkStarted,
    });
  }

  if (measuredStarted === null) {
    throw new Error("internal error: measured replay did not start");
  }
  const measuredMs = performance.now() - measuredStarted;
  const overallMs = performance.now() - benchmarkStarted;

  const commitSummary = summarizeSamples(commitDurations);
  const phaseBreakdown = [
    { phase: "readPatchSetMs", totalMs: phaseTotalsMs.readPatchSetMs },
    { phase: "applyFilesMs", totalMs: phaseTotalsMs.applyFilesMs },
    { phase: "gitStageCommitMs", totalMs: phaseTotalsMs.gitStageCommitMs },
  ].map((entry) => ({
    ...entry,
    sharePct: measuredMs > 0 ? (entry.totalMs / measuredMs) * 100 : 0,
  }));

  const report = {
    generatedAt: new Date().toISOString(),
    startedAt,
    kind: "git-files-replay",
    config: { ...CONFIG },
    sourceRepo: {
      path: sourceRepo.repoPath,
      source: sourceRepo.source,
      ref: CONFIG.repoRef,
    },
    targetRepo: {
      path: gitRepoPath,
      kind: "fresh-git-working-tree",
    },
    commitTotals: {
      requested: totalRequestedCommits,
      discovered: commits.length,
      measuredDiscovered: measuredCommits.length,
      warmupRequested: CONFIG.warmupCommitCount,
      warmupUsed: warmupCommitCount,
      warmupApplied,
      warmupNoop,
      applied: measuredApplied,
      noop: measuredNoop,
    },
    io: {
      changedPaths: totalChangedPaths,
      blobBytes: totalBlobBytes,
    },
    timings: {
      measuredMs,
      overallMs,
      warmupMs,
      setup: {
        repoSetupMs,
        commitDiscoveryMs,
      },
      phases: phaseTotalsMs,
      phaseBreakdown,
      commit: commitSummary,
    },
    throughput: {
      commitsPerSecond: measuredMs > 0 ? (CONFIG.commitLimit / measuredMs) * 1000 : 0,
      changedPathsPerSecond: measuredMs > 0 ? (totalChangedPaths / measuredMs) * 1000 : 0,
      blobMegabytesPerSecond:
        measuredMs > 0 ? ((totalBlobBytes / (1024 * 1024)) / measuredMs) * 1000 : 0,
    },
    slowestCommits,
  };

  await mkdir(dirname(CONFIG.reportPath), { recursive: true });
  await writeFile(CONFIG.reportPath, JSON.stringify(report, null, 2), "utf8");

  printSummary(report);
  console.log("");
  console.log(`Wrote git file-replay benchmark report: ${CONFIG.reportPath}`);
}

async function applyPatchSetToRepo(repoPath, patchSet) {
  let blobBytes = 0;

  for (const change of patchSet.changes) {
    const status = normalizeStatus(change.status);
    const oldPath = toRepoPath(repoPath, change.oldPath);
    const newPath = toRepoPath(repoPath, change.newPath);

    if (status === "D") {
      if (oldPath) {
        await safeDeleteFile(oldPath);
      }
      continue;
    }

    if (status === "R" && oldPath && newPath && oldPath !== newPath) {
      await safeDeleteFile(oldPath);
    }

    if (!newPath || !change.newOid || change.newOid === "0000000000000000000000000000000000000000") {
      if (oldPath && status !== "C") {
        await safeDeleteFile(oldPath);
      }
      continue;
    }

    const bytes = patchSet.blobByOid.get(change.newOid);
    if (!bytes) {
      throw new Error(
        `missing blob for ${change.newOid} while applying ${status} ${change.newPath ?? "<none>"}`,
      );
    }
    blobBytes += bytes.byteLength;

    await mkdir(dirname(newPath), { recursive: true });
    await writeFile(newPath, bytes);
  }

  return blobBytes;
}

function toRepoPath(repoPath, rawPath) {
  if (!rawPath) {
    return null;
  }
  const normalized = String(rawPath).replace(/\\/g, "/");
  if (normalized.includes("\0")) {
    throw new Error(`path contains NUL byte: ${rawPath}`);
  }
  const trimmed = normalized.startsWith("/") ? normalized.slice(1) : normalized;
  if (trimmed.length === 0) {
    return null;
  }
  const absolute = resolve(repoPath, trimmed);
  const allowedPrefix = repoPath.endsWith(sep) ? repoPath : `${repoPath}${sep}`;
  if (absolute !== repoPath && !absolute.startsWith(allowedPrefix)) {
    throw new Error(`path escapes target repo: ${rawPath}`);
  }
  return absolute;
}

async function safeDeleteFile(path) {
  try {
    await unlink(path);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}

function normalizeStatus(value) {
  if (!value || typeof value !== "string") {
    return "M";
  }
  return value[0].toUpperCase();
}

function printMaybeProgress(state) {
  if (!CONFIG.showProgress) {
    return;
  }

  const {
    commitSha,
    commitIndex,
    commits,
    warmupCommitCount,
    warmupChangedPaths,
    measuredChangedPaths,
    warmupApplied,
    warmupNoop,
    measuredApplied,
    measuredNoop,
    benchmarkStarted,
  } = state;

  const isWarmup = commitIndex < warmupCommitCount;
  const phaseIndex = isWarmup ? commitIndex + 1 : commitIndex - warmupCommitCount + 1;
  const phaseTotal = isWarmup ? warmupCommitCount : commits.length - warmupCommitCount;
  if (phaseTotal <= 0) {
    return;
  }
  if (
    phaseIndex !== 1 &&
    phaseIndex % CONFIG.progressEvery !== 0 &&
    phaseIndex !== phaseTotal
  ) {
    return;
  }

  printProgress({
    label: isWarmup ? "warmup" : "measure",
    index: phaseIndex,
    total: phaseTotal,
    commitSha,
    elapsedMs: performance.now() - benchmarkStarted,
    changedPaths: isWarmup ? warmupChangedPaths : measuredChangedPaths,
    commitsApplied: isWarmup ? warmupApplied : measuredApplied,
    commitsNoop: isWarmup ? warmupNoop : measuredNoop,
  });
}

function printSummary(report) {
  console.log("");
  console.log("Git File-Replay Benchmark");
  console.log(`Commits requested (measured): ${report.config.commitLimit}`);
  console.log(`Commits requested (warmup): ${report.commitTotals.warmupRequested}`);
  console.log(`Commits used (warmup): ${report.commitTotals.warmupUsed}`);
  console.log(`Commits discovered (total): ${report.commitTotals.discovered}`);
  console.log(`Commits discovered (measured): ${report.commitTotals.measuredDiscovered}`);
  console.log(`Warmup applied: ${report.commitTotals.warmupApplied}`);
  console.log(`Warmup skipped (no file changes): ${report.commitTotals.warmupNoop}`);
  console.log(`Commits applied (measured): ${report.commitTotals.applied}`);
  console.log(`Commits skipped (measured, no file changes): ${report.commitTotals.noop}`);
  console.log(`Warmup duration: ${formatMs(report.timings.warmupMs ?? 0)}`);
  console.log(`Replay duration (measured): ${formatMs(report.timings.measuredMs)}`);
  console.log(`Replay duration (overall): ${formatMs(report.timings.overallMs)}`);

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
  console.log("Replay phase breakdown");
  for (const phase of report.timings.phaseBreakdown) {
    const label = String(phase.phase).padEnd(21, " ");
    console.log(`  ${label} ${formatMs(phase.totalMs)} (${Number(phase.sharePct).toFixed(1)}%)`);
  }

  console.log("");
  console.log("Setup timing");
  console.log(`  repo setup: ${formatMs(report.timings.setup.repoSetupMs)}`);
  console.log(`  commit discovery: ${formatMs(report.timings.setup.commitDiscoveryMs)}`);

  if (Array.isArray(report.slowestCommits) && report.slowestCommits.length > 0) {
    console.log("");
    console.log("Slowest commits");
    for (const row of report.slowestCommits.slice(0, 5)) {
      console.log(
        `  ${formatMs(Number(row.durationMs ?? 0))} commit=${String(row.commitSha).slice(0, 12)} changed_paths=${Number(row.changedPaths ?? 0)}`,
      );
    }
  }

  console.log("");
  console.log(`Target git repo: ${report.targetRepo.path}`);
}

function formatMs(value) {
  return `${Number(value).toFixed(2)}ms`;
}

function parseEnvInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function parseEnvNonNegativeInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) && value >= 0 ? value : fallback;
}

function parseEnvBool(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined) {
    return fallback;
  }
  const normalized = raw.trim().toLowerCase();
  if (normalized === "1" || normalized === "true" || normalized === "yes") {
    return true;
  }
  if (normalized === "0" || normalized === "false" || normalized === "no") {
    return false;
  }
  return fallback;
}

function pushSlowCommit(slowest, row) {
  slowest.push(row);
  slowest.sort((left, right) => right.durationMs - left.durationMs);
  if (slowest.length > 20) {
    slowest.length = 20;
  }
}

async function runCommand(command, args, options = {}) {
  return await new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      stdio: ["pipe", "pipe", "pipe"],
    });

    const stdoutChunks = [];
    const stderrChunks = [];

    child.stdout.on("data", (chunk) => stdoutChunks.push(chunk));
    child.stderr.on("data", (chunk) => stderrChunks.push(chunk));
    child.on("error", rejectPromise);
    child.on("exit", (code) => {
      if (code === 0) {
        resolvePromise(Buffer.concat(stdoutChunks).toString("utf8"));
        return;
      }
      rejectPromise(
        new Error(
          `${command} ${args.join(" ")} failed with exit code ${code}:\n${Buffer.concat(stderrChunks).toString("utf8")}`,
        ),
      );
    });
    child.stdin.end();
  });
}

main().catch((error) => {
  console.error("Git file-replay benchmark failed:");
  console.error(error);
  process.exitCode = 1;
});
