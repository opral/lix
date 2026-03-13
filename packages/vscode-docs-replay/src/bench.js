import { mkdir, readFile, readdir, rm, stat, unlink, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { createBetterSqlite3Backend } from "@lix-js/better-sqlite3-backend";
import {
  NULL_OID,
  ensureGitRepo,
  listLinearCommits,
  readCommitPatchSet,
} from "./git-history.js";
import {
  buildReplayCommitStatements,
  createReplayState,
  prepareCommitChanges,
} from "./apply-to-lix.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = join(__dirname, "..");
const REPO_ROOT = join(__dirname, "..", "..", "..");

const DEFAULTS = {
  repoUrl: process.env.VSCODE_REPLAY_REPO_URL ?? "https://github.com/microsoft/vscode-docs.git",
  repoPath: process.env.VSCODE_REPLAY_REPO_PATH ?? join(REPO_ROOT, "artifact", "vscode-docs"),
  anchorPath:
    process.env.VSCODE_REPLAY_ANCHOR_PATH ??
    join(PACKAGE_ROOT, ".cache", "vscode-docs.anchor"),
  lixPath:
    process.env.VSCODE_REPLAY_OUTPUT_PATH ??
    join(PACKAGE_ROOT, "results", "vscode-docs-first-100.lix"),
  reportPath:
    process.env.VSCODE_BENCH_REPORT_PATH ??
    join(PACKAGE_ROOT, "results", "vscode-docs.bench.json"),
  gitReplayPath:
    process.env.VSCODE_BENCH_GIT_REPLAY_PATH ??
    join(PACKAGE_ROOT, "results", "vscode-docs.git-replay"),
  commitLimit: parseEnvInt("VSCODE_REPLAY_COMMITS", 100),
  queryRuns: parseEnvInt("VSCODE_BENCH_QUERY_RUNS", 10),
  queryWarmupRuns: parseEnvNonNegativeInt("VSCODE_BENCH_QUERY_WARMUP", 2),
  progressEvery: parseEnvInt("VSCODE_BENCH_PROGRESS_EVERY", 10),
  insertBatchRows: parseEnvInt("VSCODE_BENCH_INSERT_BATCH_ROWS", 100),
  verifyState: parseEnvBool("VSCODE_BENCH_VERIFY_STATE", false),
  includeCountQuery: parseEnvBool("VSCODE_BENCH_INCLUDE_COUNT_QUERY", false),
  skipReplay: parseEnvBool("VSCODE_BENCH_SKIP_REPLAY", false),
};

async function main() {
  const args = parseArgs();
  if (args.help) {
    printHelp();
    return;
  }

  const config = {
    ...DEFAULTS,
    ...args,
    reportPath: resolveOutputPath(args.reportPath ?? DEFAULTS.reportPath),
    gitReplayPath: resolveOutputPath(args.gitReplayPath ?? DEFAULTS.gitReplayPath),
  };

  const startedAt = new Date().toISOString();
  const totalStarted = performance.now();

  const anchorSha = await readAnchorSha(config.anchorPath);

  const sourceRepoStarted = performance.now();
  const sourceRepo = await ensureGitRepo({
    repoPath: config.repoPath,
    repoUrl: config.repoUrl,
    cacheDir: join(PACKAGE_ROOT, ".cache"),
    defaultDirName: "vscode-docs",
    syncRemote: false,
    ref: anchorSha,
  });
  const commits = await listLinearCommits(sourceRepo.repoPath, {
    ref: anchorSha,
    maxCount: config.commitLimit,
    firstParent: true,
  });
  if (commits.length === 0) {
    throw new Error(`no commits found at ${sourceRepo.repoPath} (${anchorSha})`);
  }
  const sourceRepoSetupMs = performance.now() - sourceRepoStarted;

  if (config.skipReplay) {
    console.log(
      "[bench] --skip-replay is currently ignored; replay is required for ingestion + query benchmarking",
    );
  }

  const replayStarted = performance.now();
  const replay = await replayCommitsToLix({
    sourceRepoPath: sourceRepo.repoPath,
    commitShas: commits,
    outputPath: config.lixPath,
    progressEvery: config.progressEvery,
    insertBatchRows: config.insertBatchRows,
    verifyState: config.verifyState,
  });
  const replayMs = performance.now() - replayStarted;

  const gitReplayStarted = performance.now();
  const gitReplay = await buildGitReplayRepo({
    sourceRepoPath: sourceRepo.repoPath,
    commitShas: commits,
    targetPath: config.gitReplayPath,
    progressEvery: config.progressEvery,
  });
  const gitReplayMs = performance.now() - gitReplayStarted;

  const storageStarted = performance.now();
  const storage = await collectStorage({
    sourceRepoPath: sourceRepo.repoPath,
    gitReplayPath: gitReplay.path,
    lixPath: replay.outputPath ?? config.lixPath,
    lixBytes: replay.imageBytes,
  });
  const storageMs = performance.now() - storageStarted;

  let queryMs = 0;
  let queryBench;
  try {
    const queryStarted = performance.now();
    queryBench = await benchmarkQueries({
      lix: replay.lix,
      gitRepoPath: gitReplay.path,
      measuredRuns: config.queryRuns,
      warmupRuns: config.queryWarmupRuns,
      includeCountQuery: config.includeCountQuery,
    });
    queryMs = performance.now() - queryStarted;
  } finally {
    await replay.lix.close();
  }

  const report = {
    generatedAt: new Date().toISOString(),
    startedAt,
    config: {
      ...config,
    },
    replay: {
      anchorSha,
      requestedCommits: config.commitLimit,
      discoveredCommits: commits.length,
      firstCommitSha: commits[0],
      lastCommitSha: commits[commits.length - 1],
      appliedCommits: replay.appliedCommits,
      noopCommits: replay.noopCommits,
      changedPaths: replay.changedPaths,
      outputPath: replay.outputPath,
      outputBytes: replay.imageBytes,
      outputExported: replay.imageExported,
      pageSize: replay.pageSize,
      pageCount: replay.pageCount,
      estimatedBytes: replay.estimatedBytes,
      verifyState: replay.verifyState,
      verifiedCommits: replay.verifiedCommits,
      verificationMs: replay.verificationMs,
    },
    sourceRepo: {
      path: sourceRepo.repoPath,
      source: sourceRepo.source,
    },
    gitReplay,
    storage,
    queries: queryBench,
    timings: {
      totalMs: performance.now() - totalStarted,
      replayMs,
      sourceRepoSetupMs,
      gitReplayMs,
      storageMs,
      queryMs,
    },
  };

  await mkdir(dirname(config.reportPath), { recursive: true });
  await writeFile(config.reportPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  printSummary(report, config.reportPath);
}

function parseArgs() {
  const out = {};
  const argv = process.argv.slice(2);
  if (argv[0] === "--") {
    argv.shift();
  }

  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];

    if (arg === "--help" || arg === "-h") {
      return { help: true };
    }
    if (arg === "--skip-replay") {
      out.skipReplay = true;
      continue;
    }
    if (arg === "--commits") {
      out.commitLimit = parsePositiveCliInt("--commits", argv[++index]);
      continue;
    }
    if (arg === "--query-runs") {
      out.queryRuns = parsePositiveCliInt("--query-runs", argv[++index]);
      continue;
    }
    if (arg === "--query-warmup") {
      out.queryWarmupRuns = parseNonNegativeCliInt("--query-warmup", argv[++index]);
      continue;
    }
    if (arg === "--insert-batch-rows") {
      out.insertBatchRows = parsePositiveCliInt("--insert-batch-rows", argv[++index]);
      continue;
    }
    if (arg === "--verify-state") {
      out.verifyState = true;
      continue;
    }
    if (arg === "--include-count-query") {
      out.includeCountQuery = true;
      continue;
    }
    if (arg === "--output") {
      out.reportPath = argv[++index];
      if (!out.reportPath) {
        throw new Error("--output requires a value");
      }
      continue;
    }
    if (arg === "--git-replay-path") {
      out.gitReplayPath = argv[++index];
      if (!out.gitReplayPath) {
        throw new Error("--git-replay-path requires a value");
      }
      continue;
    }

    throw new Error(`unknown argument '${arg}'`);
  }

  return out;
}

function printHelp() {
  console.log("Usage:");
  console.log("  bench [--commits 100] [--query-runs 10] [--query-warmup 2]");
  console.log(
    "       [--insert-batch-rows 100] [--verify-state] [--include-count-query] [--skip-replay] [--output path] [--git-replay-path path]",
  );
}

function resolveOutputPath(value) {
  if (!value) {
    return value;
  }
  return value.startsWith("/") ? value : join(PACKAGE_ROOT, value);
}

async function replayCommitsToLix(args) {
  const { sourceRepoPath, commitShas, outputPath, progressEvery, insertBatchRows, verifyState } = args;
  const { openLix } = await import("@lix-js/sdk");
  await mkdir(dirname(outputPath), { recursive: true });
  await rm(outputPath, { force: true });
  const backend = await createBetterSqlite3Backend({ filename: outputPath });

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

  const state = createReplayState();
  let appliedCommits = 0;
  let noopCommits = 0;
  let changedPaths = 0;
  let verifiedCommits = 0;
  let verificationMs = 0;

  console.log(`[bench] replaying ${commitShas.length} commits (no plugins)`);

  for (let index = 0; index < commitShas.length; index++) {
    const commitSha = commitShas[index];
    const patchSet = await readCommitPatchSet(sourceRepoPath, commitSha);
    changedPaths += patchSet.changes.length;

    const prepared = prepareCommitChanges(state, patchSet.changes, patchSet.blobByOid);
    const statements = buildReplayCommitStatements(prepared, {
      maxInsertRows: insertBatchRows,
      maxInsertSqlChars: 1_500_000,
    });

    if (statements.length === 0) {
      noopCommits += 1;
    } else {
      await executeStatementsInTransaction(lix, statements);
      appliedCommits += 1;
    }

    if (verifyState) {
      const verifyStarted = performance.now();
      await verifyCommitStatePaths({
        lix,
        sourceRepoPath,
        commitSha,
      });
      verificationMs += performance.now() - verifyStarted;
      verifiedCommits += 1;
    }

    if (
      index === 0 ||
      (index + 1) % progressEvery === 0 ||
      index + 1 === commitShas.length
    ) {
      console.log(
        `[bench:replay] ${index + 1}/${commitShas.length} commits (applied=${appliedCommits}, noop=${noopCommits}, changedPaths=${changedPaths})`,
      );
    }
  }

  const pageSize = await queryScalarNumber(lix, "PRAGMA page_size", "page_size");
  const pageCount = await queryScalarNumber(lix, "PRAGMA page_count", "page_count");
  const estimatedBytes = pageSize * pageCount;
  const imageBytes = await fileOrFallbackSize(outputPath, estimatedBytes);
  const imageExported = true;

  return {
    lix,
    outputPath,
    imageBytes,
    imageExported,
    pageSize,
    pageCount,
    estimatedBytes,
    appliedCommits,
    noopCommits,
    changedPaths,
    verifyState,
    verifiedCommits,
    verificationMs,
  };
}

async function collectStorage(args) {
  const { sourceRepoPath, gitReplayPath, lixPath, lixBytes } = args;

  const sourceWorktreeBytes = await directorySize(sourceRepoPath, {
    ignoreNames: new Set([".git"]),
  });
  const sourceGitBytes = await directorySize(join(sourceRepoPath, ".git"));
  const sourceTotalBytes = sourceWorktreeBytes + sourceGitBytes;

  const gitReplayWorktreeBytes = await directorySize(gitReplayPath, {
    ignoreNames: new Set([".git"]),
  });
  const gitReplayGitBytes = await directorySize(join(gitReplayPath, ".git"));
  const gitReplayTotalBytes = gitReplayWorktreeBytes + gitReplayGitBytes;

  const lixFileBytes = Number(lixBytes);
  if (!Number.isFinite(lixFileBytes) || lixFileBytes <= 0) {
    throw new Error(`invalid lix snapshot byte size: ${lixBytes}`);
  }

  return {
    lix: {
      path: lixPath,
      bytes: lixFileBytes,
    },
    gitReplay: {
      path: gitReplayPath,
      worktreeBytes: gitReplayWorktreeBytes,
      metadataBytes: gitReplayGitBytes,
      totalBytes: gitReplayTotalBytes,
    },
    sourceGitClone: {
      path: sourceRepoPath,
      worktreeBytes: sourceWorktreeBytes,
      metadataBytes: sourceGitBytes,
      totalBytes: sourceTotalBytes,
    },
    ratios: {
      lixVsGitReplayTotal: lixFileBytes / Math.max(gitReplayTotalBytes, 1),
      lixVsSourceGitTotal: lixFileBytes / Math.max(sourceTotalBytes, 1),
    },
  };
}

async function buildGitReplayRepo(args) {
  const { sourceRepoPath, commitShas, targetPath, progressEvery } = args;

  await rm(targetPath, { recursive: true, force: true });
  await mkdir(targetPath, { recursive: true });

  const gitEnv = process.env;
  await runCommand("git", ["init", "-q"], { cwd: targetPath, env: gitEnv });
  await runCommand("git", ["config", "user.email", "bench@example.com"], {
    cwd: targetPath,
    env: gitEnv,
  });
  await runCommand("git", ["config", "user.name", "vscode-docs-replay-bench"], {
    cwd: targetPath,
    env: gitEnv,
  });
  await runCommand("git", ["config", "gc.auto", "0"], { cwd: targetPath, env: gitEnv });
  await runCommand("git", ["config", "maintenance.auto", "false"], {
    cwd: targetPath,
    env: gitEnv,
  });
  await runCommand("git", ["config", "gc.autoDetach", "false"], {
    cwd: targetPath,
    env: gitEnv,
  });

  let appliedCommits = 0;
  let noopCommits = 0;
  let changedPaths = 0;
  let blobBytes = 0;

  console.log(
    `[bench] building git replay baseline at ${targetPath} (${commitShas.length} commits)`,
  );

  for (let index = 0; index < commitShas.length; index++) {
    const commitSha = commitShas[index];
    const patchSet = await readCommitPatchSet(sourceRepoPath, commitSha);
    changedPaths += patchSet.changes.length;

    const appliedBlobBytes = await applyPatchSetToRepo(targetPath, patchSet);
    blobBytes += appliedBlobBytes;

    await runCommand("git", ["add", "-A"], {
      cwd: targetPath,
      env: gitEnv,
    });
    await runCommand(
      "git",
      ["commit", "-q", "--allow-empty", "-m", `replay ${commitSha.slice(0, 12)}`],
      {
        cwd: targetPath,
        env: gitEnv,
      },
    );

    if (patchSet.changes.length === 0) {
      noopCommits += 1;
    } else {
      appliedCommits += 1;
    }

    if (
      index === 0 ||
      (index + 1) % progressEvery === 0 ||
      index + 1 === commitShas.length
    ) {
      console.log(
        `[bench:git-replay] ${index + 1}/${commitShas.length} commits (applied=${appliedCommits}, noop=${noopCommits}, changedPaths=${changedPaths})`,
      );
    }
  }

  const headSha = (await runGit(targetPath, ["rev-parse", "HEAD"])).trim();

  return {
    path: targetPath,
    headSha,
    discoveredCommits: commitShas.length,
    appliedCommits,
    noopCommits,
    changedPaths,
    blobBytes,
  };
}

async function applyPatchSetToRepo(repoPath, patchSet) {
  let totalBlobBytes = 0;

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

    if (!newPath || !change.newOid || change.newOid === NULL_OID) {
      if (oldPath && status !== "C") {
        await safeDeleteFile(oldPath);
      }
      continue;
    }

    const bytes = patchSet.blobByOid.get(change.newOid);
    if (!(bytes instanceof Uint8Array)) {
      throw new Error(
        `missing blob for ${change.newOid} while applying ${status} ${change.newPath ?? "<none>"}`,
      );
    }
    totalBlobBytes += bytes.byteLength;

    await mkdir(dirname(newPath), { recursive: true });
    await writeFile(newPath, bytes);
  }

  return totalBlobBytes;
}

function normalizeStatus(value) {
  if (!value || typeof value !== "string") {
    return "M";
  }
  return value[0].toUpperCase();
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
  if (!(absolute === repoPath || absolute.startsWith(allowedPrefix))) {
    throw new Error(`path escapes repo root: ${rawPath}`);
  }
  return absolute;
}

async function safeDeleteFile(path) {
  try {
    await unlink(path);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return;
    }
    throw error;
  }
}

async function verifyCommitStatePaths(args) {
  const { lix, sourceRepoPath, commitSha } = args;
  const gitPaths = (await runGit(sourceRepoPath, ["ls-tree", "-r", "--name-only", commitSha]))
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .sort();

  const lixRows = await lix.execute(
    "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_file_descriptor'",
    [],
  );
  const lixPaths = statementRows(lixRows)
    .map((row, index) => fromLixPath(scalarToString(row?.[0], `verify.entity_id[${index}]`)))
    .sort();

  if (gitPaths.length !== lixPaths.length) {
    throw new Error(
      `state mismatch at ${commitSha}: path count differs (git=${gitPaths.length}, lix=${lixPaths.length})`,
    );
  }

  for (let index = 0; index < gitPaths.length; index++) {
    if (gitPaths[index] !== lixPaths[index]) {
      throw new Error(
        `state mismatch at ${commitSha}: first differing path git='${gitPaths[index]}' lix='${lixPaths[index]}'`,
      );
    }
  }
}

async function fileOrFallbackSize(path, fallback) {
  try {
    const info = await stat(path);
    if (info.isFile() && Number.isFinite(info.size) && info.size > 0) {
      return info.size;
    }
  } catch {
    // fall through to fallback
  }
  return fallback;
}

async function benchmarkQueries(args) {
  const { lix, gitRepoPath, measuredRuns, warmupRuns, includeCountQuery } = args;
  const sampleStarted = performance.now();
  const samples = await loadQuerySamples(gitRepoPath);
  const sampleLoadMs = performance.now() - sampleStarted;
  const primeStarted = performance.now();
  await lix.execute("SELECT 1 FROM lix_file LIMIT 1", []);
  const lixPrimeMs = performance.now() - primeStarted;
  const pairedQueries = [];
  const lixOnlyQueries = [];

  const batchPlaceholders = samples.batchPaths.map(() => "?").join(", ");

  const runAndPushPaired = async (query) => {
    console.log(`[bench:query] paired ${query.id}`);
    pairedQueries.push(await runPairedQuery(query));
  };
  const runAndPushLixOnly = async (query) => {
    console.log(`[bench:query] lix-only ${query.id}`);
    lixOnlyQueries.push(await runSingleQuery(query));
  };

  if (includeCountQuery) {
    await runAndPushPaired({
      id: "file_count",
      description: "Count files in final repository state",
      measuredRuns,
      warmupRuns,
      lixSql: "SELECT COUNT(*) FROM lix_file",
      lixRun: async () =>
        await queryScalarNumber(lix, "SELECT COUNT(*) FROM lix_file", "lix_file count"),
      gitCommand: ["git", "-C", gitRepoPath, "ls-tree", "-r", "--name-only", "HEAD"],
      gitRun: async () =>
        countNonEmptyLines(await runGit(gitRepoPath, ["ls-tree", "-r", "--name-only", "HEAD"])),
    });
  }

  const prefixGitArgs = ["ls-tree", "-r", "--name-only", "HEAD"];
  if (samples.prefixGitPathspec) {
    prefixGitArgs.push("--", samples.prefixGitPathspec);
  }

  await runAndPushPaired({
    id: "prefix_list_100",
    description: "List up to 100 files under a sampled top-level directory",
    measuredRuns,
    warmupRuns,
    lixSql: "SELECT path FROM lix_file WHERE path LIKE ? ORDER BY path LIMIT 100",
    lixParamsPreview: [samples.prefixLixLike],
    lixRun: async () => {
      const result = await lix.execute(
        "SELECT path FROM lix_file WHERE path LIKE ? ORDER BY path LIMIT 100",
        [samples.prefixLixLike],
      );
      return statementRows(result).length;
    },
    gitCommand: ["git", "-C", gitRepoPath, ...prefixGitArgs],
    gitRun: async () => {
      const stdout = await runGit(gitRepoPath, prefixGitArgs);
      return Math.min(100, countNonEmptyLines(stdout));
    },
  });

  await runAndPushPaired({
    id: "exact_file_lookup",
    description: "Lookup one sampled file by exact path",
    measuredRuns,
    warmupRuns,
    lixSql: "SELECT id FROM lix_file WHERE path = ? LIMIT 1",
    lixParamsPreview: [samples.exactLixPath],
    lixRun: async () =>
      await queryScalarText(
        lix,
        "SELECT id FROM lix_file WHERE path = ? LIMIT 1",
        "exact file lookup",
        [samples.exactLixPath],
      ),
    gitCommand: ["git", "-C", gitRepoPath, "cat-file", "-e", `HEAD:${samples.exactGitPath}`],
    gitRun: async () => {
      await runGit(gitRepoPath, ["cat-file", "-e", `HEAD:${samples.exactGitPath}`]);
      return 1;
    },
  });

  if (samples.batchPaths.length > 0) {
    await runAndPushPaired({
      id: "batch_file_lookup_count",
      description: `Lookup ${samples.batchPaths.length} sampled files by path`,
      measuredRuns,
      warmupRuns,
      lixSql: `SELECT COUNT(*) FROM lix_file WHERE path IN (${batchPlaceholders})`,
      lixParamsPreview: samples.batchPaths,
      lixRun: async () =>
        await queryScalarNumber(
          lix,
          `SELECT COUNT(*) FROM lix_file WHERE path IN (${batchPlaceholders})`,
          "batch file lookup count",
          samples.batchPaths,
        ),
      gitCommand: [
        "git",
        "-C",
        gitRepoPath,
        "ls-tree",
        "-r",
        "--name-only",
        "HEAD",
        "--",
        ...samples.batchGitPaths,
      ],
      gitRun: async () => {
        const stdout = await runGit(gitRepoPath, [
          "ls-tree",
          "-r",
          "--name-only",
          "HEAD",
          "--",
          ...samples.batchGitPaths,
        ]);
        return countNonEmptyLines(stdout);
      },
    });
  }

  await runAndPushLixOnly({
    id: "select_star_limit_100",
    description: "Read first 100 rows from lix_file with full payload",
    measuredRuns,
    warmupRuns,
    query: "SELECT * FROM lix_file LIMIT 100",
    run: async () => {
      const result = await lix.execute("SELECT * FROM lix_file LIMIT 100", []);
      return statementRows(result).length;
    },
  });

  await runAndPushLixOnly({
    id: "exact_file_select_star",
    description: "Read one sampled file row from lix_file",
    measuredRuns,
    warmupRuns,
    query: "SELECT * FROM lix_file WHERE path = ? LIMIT 1",
    run: async () => {
      const result = await lix.execute("SELECT * FROM lix_file WHERE path = ? LIMIT 1", [
        samples.exactLixPath,
      ]);
      return statementRows(result).length;
    },
  });

  await runAndPushLixOnly({
    id: "sum_file_bytes",
    description: "Sum payload bytes stored in lix_file",
    measuredRuns,
    warmupRuns,
    query: "SELECT SUM(LENGTH(data)) FROM lix_file",
    run: async () =>
      await queryScalarNumber(lix, "SELECT SUM(LENGTH(data)) FROM lix_file", "sum file bytes"),
  });

  return {
    warmupRuns,
    measuredRuns,
    sampleLoadMs,
    lixPrimeMs,
    sampledInputs: {
      exactLixPath: samples.exactLixPath,
      exactGitPath: samples.exactGitPath,
      prefixLixLike: samples.prefixLixLike,
      prefixGitPathspec: samples.prefixGitPathspec,
      batchCount: samples.batchPaths.length,
    },
    pairedQueries,
    lixOnlyQueries,
  };
}

async function loadQuerySamples(gitRepoPath) {
  const gitPaths = (await runGit(gitRepoPath, ["ls-tree", "-r", "--name-only", "HEAD"]))
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const usable = gitPaths.filter((path) => isGitPathUsable(path));
  if (usable.length === 0) {
    throw new Error("no files available in git replay baseline; cannot benchmark queries");
  }

  const exactGitPath = usable[0];
  const exactLixPath = toLixPath(exactGitPath);
  const prefixSegment = firstPathSegment(exactGitPath);
  const prefixLixLike = prefixSegment ? `/${encodePathSegment(prefixSegment)}/%` : "/%";
  const prefixGitPathspec = prefixSegment ? prefixSegment : "";

  const batchGitPaths = [];
  for (const path of usable) {
    if (/\s/.test(path)) {
      continue;
    }
    batchGitPaths.push(path);
    if (batchGitPaths.length >= 20) {
      break;
    }
  }

  return {
    exactLixPath,
    exactGitPath,
    prefixLixLike,
    prefixGitPathspec,
    batchPaths: batchGitPaths.map((path) => toLixPath(path)),
    batchGitPaths,
  };
}

function firstPathSegment(lixPath) {
  const parts = String(lixPath).split("/").filter(Boolean);
  return parts[0] ?? "";
}

function toLixPath(path) {
  const normalized = String(path).replace(/\\/g, "/");
  const withoutLeadingSlash = normalized.startsWith("/") ? normalized.slice(1) : normalized;
  const encoded = withoutLeadingSlash
    .split("/")
    .map((segment) => encodePathSegment(segment))
    .join("/");
  return `/${encoded}`;
}

function encodePathSegment(segment) {
  const bytes = new TextEncoder().encode(String(segment));
  let encoded = "";
  for (const byte of bytes) {
    const isAlphaNum =
      (byte >= 0x30 && byte <= 0x39) ||
      (byte >= 0x41 && byte <= 0x5a) ||
      (byte >= 0x61 && byte <= 0x7a);
    const isSafe =
      byte === 0x2e || // .
      byte === 0x5f || // _
      byte === 0x7e || // ~
      byte === 0x2d; // -
    if (isAlphaNum || isSafe) {
      encoded += String.fromCharCode(byte);
    } else {
      encoded += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
    }
  }
  return encoded;
}

function decodePathSegment(segment) {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}

function fromLixPath(lixPath) {
  const raw = String(lixPath);
  const trimmed = raw.startsWith("/") ? raw.slice(1) : raw;
  if (!trimmed) {
    return "";
  }
  return trimmed
    .split("/")
    .map((segment) => decodePathSegment(segment))
    .join("/");
}

function isGitPathUsable(path) {
  return Boolean(path) && !path.includes("\0") && !path.includes("\n") && !path.startsWith("-");
}

async function runPairedQuery(config) {
  const {
    id,
    description,
    measuredRuns,
    warmupRuns,
    lixSql,
    lixParamsPreview,
    lixRun,
    gitCommand,
    gitRun,
  } = config;

  const lix = await runMeasured(lixRun, { measuredRuns, warmupRuns });
  const git = await runMeasured(gitRun, { measuredRuns, warmupRuns });

  return {
    id,
    description,
    lix: {
      sql: lixSql,
      paramsPreview: lixParamsPreview ?? [],
      ...lix,
    },
    git: {
      command: gitCommand,
      ...git,
    },
    comparison: {
      meanRatioLixToGit: lix.stats.meanMs / Math.max(git.stats.meanMs, 0.000001),
      p50RatioLixToGit: lix.stats.p50Ms / Math.max(git.stats.p50Ms, 0.000001),
      p95RatioLixToGit: lix.stats.p95Ms / Math.max(git.stats.p95Ms, 0.000001),
    },
  };
}

async function runSingleQuery(config) {
  const { id, description, measuredRuns, warmupRuns, query, run } = config;
  const measured = await runMeasured(run, { measuredRuns, warmupRuns });
  return {
    id,
    description,
    query,
    ...measured,
  };
}

async function runMeasured(run, options) {
  const { measuredRuns, warmupRuns } = options;
  let sampleValue = null;

  for (let index = 0; index < warmupRuns; index++) {
    await run();
  }

  const samples = [];
  for (let index = 0; index < measuredRuns; index++) {
    const started = performance.now();
    sampleValue = await run();
    samples.push(performance.now() - started);
  }

  return {
    warmupRuns,
    measuredRuns,
    sampleValue,
    stats: summarizeSamples(samples),
  };
}

function summarizeSamples(samples) {
  if (samples.length === 0) {
    return {
      count: 0,
      meanMs: 0,
      minMs: 0,
      p50Ms: 0,
      p95Ms: 0,
      maxMs: 0,
    };
  }

  const sorted = [...samples].sort((left, right) => left - right);
  const meanMs = samples.reduce((sum, value) => sum + value, 0) / samples.length;

  return {
    count: samples.length,
    meanMs,
    minMs: sorted[0],
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
    maxMs: sorted[sorted.length - 1],
  };
}

function percentile(sorted, ratio) {
  if (sorted.length === 0) {
    return 0;
  }
  const index = Math.floor(sorted.length * ratio);
  const boundedIndex = Math.min(sorted.length - 1, Math.max(0, index));
  return sorted[boundedIndex];
}

function printSummary(report, reportPath) {
  console.log("\nVscode Docs Replay Bench Summary");
  console.log(`  replay commits: ${report.replay.discoveredCommits}`);
  console.log(`  replay anchor : ${report.replay.anchorSha}`);
  if (report.replay.verifyState) {
    console.log(
      `  verification  : ${report.replay.verifiedCommits}/${report.replay.discoveredCommits} commits (${report.replay.verificationMs.toFixed(2)}ms)`,
    );
  }
  console.log(
    `  lix size      : ${formatBytes(report.storage.lix.bytes)} (${report.storage.lix.bytes} bytes)`,
  );
  console.log(
    `  git replay    : ${formatBytes(report.storage.gitReplay.totalBytes)} (${report.storage.gitReplay.totalBytes} bytes)`,
  );
  console.log(
    `  lix/git ratio : ${report.storage.ratios.lixVsGitReplayTotal.toFixed(4)}x (replay baseline)`,
  );

  console.log("\nPaired query timings (mean ms)");
  for (const query of report.queries.pairedQueries) {
    console.log(
      `  ${query.id}: lix=${query.lix.stats.meanMs.toFixed(3)} git=${query.git.stats.meanMs.toFixed(3)} ratio=${query.comparison.meanRatioLixToGit.toFixed(3)}x`,
    );
  }

  if (report.queries.lixOnlyQueries.length > 0) {
    console.log("\nLix-only query timings (mean ms)");
    for (const query of report.queries.lixOnlyQueries) {
      console.log(`  ${query.id}: lix=${query.stats.meanMs.toFixed(3)}`);
    }
  }

  console.log("\nTimings");
  console.log(`  replay        : ${report.timings.replayMs.toFixed(2)}ms`);
  console.log(`  git baseline  : ${report.timings.gitReplayMs.toFixed(2)}ms`);
  console.log(`  storage       : ${report.timings.storageMs.toFixed(2)}ms`);
  console.log(`  query prep    : ${report.queries.sampleLoadMs.toFixed(2)}ms`);
  console.log(`  query prime   : ${report.queries.lixPrimeMs.toFixed(2)}ms`);
  console.log(`  query bench   : ${report.timings.queryMs.toFixed(2)}ms`);
  console.log(`  total         : ${report.timings.totalMs.toFixed(2)}ms`);
  console.log(`\nWrote benchmark report: ${reportPath}`);
}

async function directorySize(path, options = {}) {
  const ignoreNames = options.ignoreNames ?? new Set();
  let total = 0;
  const queue = [path];

  while (queue.length > 0) {
    const current = queue.pop();
    let stats;
    try {
      stats = await stat(current);
    } catch {
      continue;
    }

    if (stats.isFile()) {
      total += stats.size;
      continue;
    }

    if (!stats.isDirectory()) {
      continue;
    }

    let entries;
    try {
      entries = await readdir(current, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      if (ignoreNames.has(entry.name)) {
        continue;
      }
      queue.push(join(current, entry.name));
    }
  }

  return total;
}

async function queryScalarNumber(lix, sql, context, params = []) {
  const result = await lix.execute(sql, params);
  return scalarToNumber(statementRows(result)?.[0]?.[0], context);
}

async function queryScalarText(lix, sql, context, params = []) {
  const result = await lix.execute(sql, params);
  return scalarToString(statementRows(result)?.[0]?.[0], context);
}

function statementRows(result, statementIndex = 0) {
  return result?.statements?.[statementIndex]?.rows ?? [];
}

async function executeStatementsInTransaction(lix, statements) {
  await lix.transaction(async (tx) => {
    for (const statement of statements) {
      await tx.execute(statement.sql, statement.params ?? []);
    }
  });
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

function countNonEmptyLines(stdout) {
  return stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean).length;
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) {
    return String(bytes);
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

async function readAnchorSha(anchorPath) {
  let raw;
  try {
    raw = await readFile(anchorPath, "utf8");
  } catch {
    throw new Error(
      `anchor file missing at ${anchorPath}. Run 'pnpm --filter vscode-docs-replay run bootstrap' first.`,
    );
  }

  const anchorSha = raw.split(/\r?\n/, 1)[0]?.trim();
  if (!anchorSha) {
    throw new Error(
      `anchor file at ${anchorPath} is empty. Run 'pnpm --filter vscode-docs-replay run bootstrap' again.`,
    );
  }

  return anchorSha;
}

async function runGit(repoPath, args, options = {}) {
  const output = await runCommand("git", ["-C", repoPath, ...args], options);
  return output.toString("utf8");
}

async function runCommand(command, args, options = {}) {
  const { cwd, env, stdin } = options;
  return await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const stdout = [];
    const stderr = [];

    child.stdout.on("data", (chunk) => stdout.push(chunk));
    child.stderr.on("data", (chunk) => stderr.push(chunk));
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve(Buffer.concat(stdout));
      } else {
        reject(
          new Error(
            `${command} ${args.join(" ")} failed with exit code ${code}:\n${Buffer.concat(stderr).toString("utf8")}`,
          ),
        );
      }
    });

    if (stdin) {
      child.stdin.write(stdin);
    }
    child.stdin.end();
  });
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

function parsePositiveCliInt(flag, raw) {
  if (!raw) {
    throw new Error(`${flag} requires a value`);
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer, got '${raw}'`);
  }
  return parsed;
}

function parseNonNegativeCliInt(flag, raw) {
  if (!raw) {
    throw new Error(`${flag} requires a value`);
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${flag} must be a non-negative integer, got '${raw}'`);
  }
  return parsed;
}

main().catch((error) => {
  console.error("bench failed");
  console.error(error);
  process.exitCode = 1;
});
