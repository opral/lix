import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { openLix, Value } from "js-sdk";
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
import { printProgress } from "./report.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..", "..");
const OUTPUT_DIR = join(__dirname, "..", "results");
const DEFAULT_OUTPUT_PATH = join(OUTPUT_DIR, "nextjs-replay.parity.json");
const DEFAULT_CACHE_DIR = join(__dirname, "..", ".cache", "nextjs-replay");

const CONFIG = {
  repoUrl: process.env.BENCH_REPLAY_REPO_URL ?? "https://github.com/vercel/next.js.git",
  repoPath: process.env.BENCH_REPLAY_REPO_PATH ?? "",
  repoRef: process.env.BENCH_REPLAY_REF ?? "HEAD",
  cacheDir: process.env.BENCH_REPLAY_CACHE_DIR ?? DEFAULT_CACHE_DIR,
  commitLimit: parseEnvInt("BENCH_REPLAY_COMMITS", 100),
  firstParent: parseEnvBool("BENCH_REPLAY_FIRST_PARENT", true),
  syncRemote: parseEnvBool("BENCH_REPLAY_FETCH", false),
  progressEvery: parseEnvInt("BENCH_REPLAY_PROGRESS_EVERY", 25),
  showProgress: parseEnvBool("BENCH_REPLAY_PROGRESS", true),
  installTextPlugin: parseTextPluginInstallFlag(true),
  maxInsertRows: parseEnvInt("BENCH_REPLAY_MAX_INSERT_ROWS", 200),
  maxInsertSqlChars: parseEnvInt("BENCH_REPLAY_MAX_INSERT_SQL_CHARS", 1_500_000),
  parityEveryCommits: parseEnvInt("BENCH_PARITY_EVERY", 1),
  maxMismatchSamples: parseEnvInt("BENCH_PARITY_MAX_MISMATCH_SAMPLES", 20),
  failFastOnMismatch: parseEnvBool("BENCH_PARITY_FAIL_FAST", true),
  outputPath: process.env.BENCH_PARITY_REPORT_PATH ?? DEFAULT_OUTPUT_PATH,
};

const TEXT_PLUGIN_MANIFEST = {
  key: "text_plugin",
  runtime: "wasm-component-v1",
  api_version: "0.1.0",
  detect_changes_glob: "**/*",
  entry: "plugin.wasm",
};

async function main() {
  const startedAt = new Date().toISOString();
  const runStarted = performance.now();

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
  const commits = await listLinearCommits(repo.repoPath, {
    ref: CONFIG.repoRef,
    maxCount: CONFIG.commitLimit,
    firstParent: CONFIG.firstParent,
  });
  const commitDiscoveryMs = performance.now() - commitDiscoveryStarted;

  if (commits.length === 0) {
    throw new Error(`no commits found at ${repo.repoPath} (${CONFIG.repoRef})`);
  }

  const objectFormat = await readGitObjectFormat(repo.repoPath);

  if (CONFIG.showProgress) {
    console.log(
      `[progress] replaying ${commits.length} commits with parity checks (every=${CONFIG.parityEveryCommits}) from ${repo.repoPath} (${repo.source})`,
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
    if (CONFIG.installTextPlugin) {
      const pluginWasmBytes = await loadTextPluginWasmBytes();
      const installStarted = performance.now();
      await lix.installPlugin({
        manifestJson: TEXT_PLUGIN_MANIFEST,
        wasmBytes: pluginWasmBytes,
      });
      pluginInstallMs = performance.now() - installStarted;
    }

    const state = createReplayState();

    let commitsApplied = 0;
    let commitsNoop = 0;
    let totalChangedPaths = 0;
    let parityChecks = 0;
    let parityFailures = 0;
    let firstFailure = null;

    const parityResults = [];
    const applyDurations = [];
    const parityDurations = [];

    for (let index = 0; index < commits.length; index++) {
      const commitSha = commits[index];
      const patchSet = await readCommitPatchSet(repo.repoPath, commitSha);
      totalChangedPaths += patchSet.changes.length;

      const prepared = prepareCommitChanges(state, patchSet.changes, patchSet.blobByOid);
      const statements = buildReplayCommitStatements(prepared, {
        maxInsertRows: CONFIG.maxInsertRows,
        maxInsertSqlChars: CONFIG.maxInsertSqlChars,
      });

      const applyStarted = performance.now();
      if (statements.length === 0) {
        commitsNoop += 1;
      } else {
        await executeStatements(lix, statements);
        commitsApplied += 1;
      }
      applyDurations.push(performance.now() - applyStarted);

      const shouldVerify =
        CONFIG.parityEveryCommits <= 1 ||
        index === commits.length - 1 ||
        (index + 1) % CONFIG.parityEveryCommits === 0;

      if (shouldVerify) {
        const parityStarted = performance.now();
        const parity = await checkCommitParity({
          repoPath: repo.repoPath,
          commitSha,
          lix,
          objectFormat,
          maxMismatchSamples: CONFIG.maxMismatchSamples,
        });
        parity.totalMs = performance.now() - parityStarted;

        parityChecks += 1;
        parityDurations.push(parity.totalMs);
        parityResults.push(parity);

        if (!parity.ok) {
          parityFailures += 1;
          if (!firstFailure) {
            firstFailure = parity;
          }
          if (CONFIG.failFastOnMismatch) {
            break;
          }
        }
      }

      if (
        CONFIG.showProgress &&
        (index === 0 || (index + 1) % CONFIG.progressEvery === 0 || index + 1 === commits.length)
      ) {
        printProgress({
          label: "measure",
          index: index + 1,
          total: commits.length,
          commitSha,
          elapsedMs: performance.now() - runStarted,
          changedPaths: totalChangedPaths,
          commitsApplied,
          commitsNoop,
        });
      }
    }

    const report = {
      generatedAt: new Date().toISOString(),
      startedAt,
      kind: "state-parity",
      config: { ...CONFIG },
      repo: {
        path: repo.repoPath,
        source: repo.source,
        ref: CONFIG.repoRef,
      },
      commitTotals: {
        requested: CONFIG.commitLimit,
        discovered: commits.length,
        applied: commitsApplied,
        noop: commitsNoop,
        changedPaths: totalChangedPaths,
      },
      parity: {
        objectFormat,
        checks: parityChecks,
        failures: parityFailures,
        pass: parityFailures === 0,
        firstFailure,
      },
      timings: {
        totalMs: performance.now() - runStarted,
        setup: {
          repoSetupMs,
          commitDiscoveryMs,
          lixOpenMs,
          pluginInstallMs,
        },
        apply: summarizeSamples(applyDurations),
        parity: summarizeSamples(parityDurations),
      },
      checks: parityResults,
    };

    await mkdir(dirname(CONFIG.outputPath), { recursive: true });
    await writeFile(CONFIG.outputPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

    printParitySummary(report);
    console.log(`\nWrote parity report: ${CONFIG.outputPath}`);

    if (parityFailures > 0) {
      process.exitCode = 1;
    }
  } finally {
    await lix.close();
  }
}

async function executeStatements(lix, statements) {
  for (const statement of statements) {
    await lix.execute(statement.sql, statement.params ?? []);
  }
}

async function checkCommitParity(args) {
  const { repoPath, commitSha, lix, objectFormat, maxMismatchSamples } = args;

  const gitTreeStarted = performance.now();
  const gitPathToOid = await readGitTreePathToBlobOid(repoPath, commitSha);
  const gitTreeMs = performance.now() - gitTreeStarted;

  const lixReadStarted = performance.now();
  const lixPathToOid = await readLixPathToBlobOid(lix, objectFormat);
  const lixReadMs = performance.now() - lixReadStarted;

  const compareStarted = performance.now();
  const missingInLix = [];
  const extraInLix = [];
  const contentMismatches = [];

  for (const [path, gitOid] of gitPathToOid) {
    const lixOid = lixPathToOid.get(path);
    if (!lixOid) {
      if (missingInLix.length < maxMismatchSamples) {
        missingInLix.push({ path, gitOid });
      }
      continue;
    }
    if (lixOid !== gitOid) {
      if (contentMismatches.length < maxMismatchSamples) {
        contentMismatches.push({ path, gitOid, lixOid });
      }
    }
  }

  for (const [path, lixOid] of lixPathToOid) {
    if (!gitPathToOid.has(path) && extraInLix.length < maxMismatchSamples) {
      extraInLix.push({ path, lixOid });
    }
  }

  const compareMs = performance.now() - compareStarted;

  const missingCount = gitPathToOid.size - intersectSize(gitPathToOid, lixPathToOid);
  const extraCount = lixPathToOid.size - intersectSize(lixPathToOid, gitPathToOid);
  const mismatchCount = countMismatches(gitPathToOid, lixPathToOid);

  return {
    commitSha,
    ok: missingCount === 0 && extraCount === 0 && mismatchCount === 0,
    gitFileCount: gitPathToOid.size,
    lixFileCount: lixPathToOid.size,
    missingInLixCount: missingCount,
    extraInLixCount: extraCount,
    contentMismatchCount: mismatchCount,
    missingInLix,
    extraInLix,
    contentMismatches,
    timings: {
      gitTreeMs,
      lixReadMs,
      compareMs,
    },
  };
}

async function readGitTreePathToBlobOid(repoPath, commitSha) {
  const raw = await runGit(repoPath, ["ls-tree", "-r", "-z", "--full-tree", commitSha]);
  const tokens = raw.toString("utf8").split("\0");
  if (tokens[tokens.length - 1] === "") {
    tokens.pop();
  }

  const pathToOid = new Map();
  for (const token of tokens) {
    if (!token) {
      continue;
    }

    const tabIndex = token.indexOf("\t");
    if (tabIndex < 0) {
      continue;
    }

    const meta = token.slice(0, tabIndex);
    const path = token.slice(tabIndex + 1);
    const fields = meta.split(" ");
    if (fields.length < 3) {
      continue;
    }

    const type = fields[1];
    const oid = fields[2];
    if (type !== "blob") {
      continue;
    }
    pathToOid.set(path, oid);
  }

  return pathToOid;
}

async function readLixPathToBlobOid(lix, objectFormat) {
  const result = await lix.execute("SELECT path, data FROM lix_file ORDER BY path", []);
  const rows = Array.isArray(result?.rows) ? result.rows : [];

  const pathToOid = new Map();
  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 2) {
      continue;
    }

    const encodedPath = readTextCell(row[0], "lix_file.path");
    const path = decodeLixPath(encodedPath);
    const bytes = readBlobCell(row[1], "lix_file.data");
    const oid = gitBlobOid(bytes, objectFormat);
    pathToOid.set(path, oid);
  }

  return pathToOid;
}

function decodeLixPath(encodedPath) {
  const normalized = String(encodedPath);
  const withoutLeadingSlash = normalized.startsWith("/")
    ? normalized.slice(1)
    : normalized;

  if (withoutLeadingSlash.length === 0) {
    return "";
  }

  return withoutLeadingSlash
    .split("/")
    .map((segment) => decodeURIComponent(segment))
    .join("/");
}

function readTextCell(cell, label) {
  const text = Value.from(cell).asText();
  if (text === undefined) {
    throw new Error(`${label} is not a text value`);
  }
  return text;
}

function readBlobCell(cell, label) {
  const parsed = Value.from(cell);
  const blob = parsed.asBlob();
  if (blob instanceof Uint8Array) {
    return blob;
  }
  const text = parsed.asText();
  if (text !== undefined) {
    return new TextEncoder().encode(text);
  }
  throw new Error(`${label} is not a blob value`);
}

function gitBlobOid(bytes, objectFormat) {
  const header = Buffer.from(`blob ${bytes.byteLength}\0`, "utf8");
  const digest = createHash(objectFormat).update(header).update(bytes).digest("hex");
  return digest;
}

function intersectSize(left, right) {
  let matches = 0;
  for (const key of left.keys()) {
    if (right.has(key)) {
      matches += 1;
    }
  }
  return matches;
}

function countMismatches(gitPathToOid, lixPathToOid) {
  let mismatches = 0;
  for (const [path, gitOid] of gitPathToOid) {
    const lixOid = lixPathToOid.get(path);
    if (lixOid !== undefined && lixOid !== gitOid) {
      mismatches += 1;
    }
  }
  return mismatches;
}

async function readGitObjectFormat(repoPath) {
  const raw = await runGit(repoPath, ["rev-parse", "--show-object-format"]);
  const format = raw.toString("utf8").trim().toLowerCase();
  if (format !== "sha1" && format !== "sha256") {
    throw new Error(`unsupported git object format: ${format}`);
  }
  return format;
}

function summarizeSamples(samples) {
  if (samples.length === 0) {
    return {
      count: 0,
      meanMs: 0,
      p50Ms: 0,
      p95Ms: 0,
      maxMs: 0,
    };
  }

  const sorted = [...samples].sort((left, right) => left - right);
  const meanMs = samples.reduce((sum, value) => sum + value, 0) / samples.length;

  return {
    count: sorted.length,
    meanMs,
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
    maxMs: sorted[sorted.length - 1],
  };
}

function percentile(sorted, ratio) {
  if (sorted.length === 0) {
    return 0;
  }
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * ratio));
  return sorted[index];
}

function printParitySummary(report) {
  console.log("");
  console.log("Next.js Replay State Parity");
  console.log(`Commits requested: ${report.config.commitLimit}`);
  console.log(`Commits discovered: ${report.commitTotals.discovered}`);
  console.log(`Commits applied: ${report.commitTotals.applied}`);
  console.log(`Commits skipped (no file changes): ${report.commitTotals.noop}`);
  console.log(`Parity checks: ${report.parity.checks}`);
  console.log(`Parity failures: ${report.parity.failures}`);
  console.log(`Object format: ${report.parity.objectFormat}`);

  console.log("");
  console.log("Timing");
  console.log(`  total: ${formatMs(report.timings.totalMs)}`);
  console.log(`  apply mean: ${formatMs(report.timings.apply.meanMs)}`);
  console.log(`  parity mean: ${formatMs(report.timings.parity.meanMs)}`);

  if (report.parity.firstFailure) {
    const first = report.parity.firstFailure;
    console.log("");
    console.log(`First mismatch commit: ${String(first.commitSha).slice(0, 12)}`);
    console.log(`  missing in lix: ${first.missingInLixCount}`);
    console.log(`  extra in lix: ${first.extraInLixCount}`);
    console.log(`  content mismatches: ${first.contentMismatchCount}`);
  }
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

function parseTextPluginInstallFlag(fallback) {
  if (process.env.BENCH_REPLAY_INSTALL_TEXT_PLUGIN !== undefined) {
    return parseEnvBool("BENCH_REPLAY_INSTALL_TEXT_PLUGIN", fallback);
  }
  return parseEnvBool("BENCH_REPLAY_INSTALL_TEXT_LINES_PLUGIN", fallback);
}

async function loadTextPluginWasmBytes() {
  const packageReleasePath = join(
    REPO_ROOT,
    "packages",
    "text-plugin",
    "target",
    "wasm32-wasip2",
    "release",
    "text_plugin.wasm",
  );
  const workspaceReleasePath = join(
    REPO_ROOT,
    "target",
    "wasm32-wasip2",
    "release",
    "text_plugin.wasm",
  );

  try {
    return new Uint8Array(await readFile(packageReleasePath));
  } catch {
    return new Uint8Array(await readFile(workspaceReleasePath));
  }
}

async function runGit(repoPath, args, options = {}) {
  return await runCommand("git", ["-C", repoPath, ...args], options);
}

async function runCommand(command, args, options = {}) {
  const { stdin } = options;

  return await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    const stdoutChunks = [];
    const stderrChunks = [];

    child.stdout.on("data", (chunk) => {
      stdoutChunks.push(chunk);
    });

    child.stderr.on("data", (chunk) => {
      stderrChunks.push(chunk);
    });

    child.on("error", reject);

    child.on("exit", (code) => {
      if (code === 0) {
        resolve(Buffer.concat(stdoutChunks));
        return;
      }

      const stderr = Buffer.concat(stderrChunks).toString("utf8");
      reject(
        new Error(
          `${command} ${args.join(" ")} failed with exit code ${code}:\n${stderr}`,
        ),
      );
    });

    if (stdin) {
      child.stdin.write(stdin);
    }
    child.stdin.end();
  });
}

main().catch((error) => {
  console.error("State parity run failed:");
  console.error(error);
  process.exitCode = 1;
});
