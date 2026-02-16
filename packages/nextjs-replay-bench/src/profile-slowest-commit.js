import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { openLix, createWasmSqliteBackend } from "js-sdk";
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

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..", "..");
const RESULTS_DIR = join(__dirname, "..", "results");
const DEFAULT_REPORT_PATH = join(RESULTS_DIR, "nextjs-replay.bench.json");
const DEFAULT_OUTPUT_PATH = join(RESULTS_DIR, "nextjs-replay.slowest-commit.explain.txt");

const TEXT_LINES_MANIFEST = {
  key: "plugin_text_lines",
  runtime: "wasm-component-v1",
  api_version: "0.1.0",
  detect_changes_glob: "**/*",
  entry: "plugin.wasm",
};

async function main() {
  const reportPath = process.env.BENCH_REPLAY_REPORT_PATH ?? DEFAULT_REPORT_PATH;
  const outputPath = process.env.BENCH_REPLAY_EXPLAIN_OUTPUT ?? DEFAULT_OUTPUT_PATH;
  const report = JSON.parse(await readFile(reportPath, "utf8"));

  const targetCommit =
    process.env.BENCH_REPLAY_TARGET_COMMIT ??
    report?.slowestCommits?.[0]?.commitSha;
  if (!targetCommit) {
    throw new Error("failed to resolve target commit (BENCH_REPLAY_TARGET_COMMIT or slowestCommits[0])");
  }

  const config = report.config ?? {};
  const replayConfig = {
    repoUrl: process.env.BENCH_REPLAY_REPO_URL ?? config.repoUrl ?? "https://github.com/vercel/next.js.git",
    repoPath: process.env.BENCH_REPLAY_REPO_PATH ?? report?.repo?.path ?? "",
    repoRef: process.env.BENCH_REPLAY_REF ?? report?.repo?.ref ?? "HEAD",
    cacheDir: process.env.BENCH_REPLAY_CACHE_DIR ?? config.cacheDir,
    commitLimit: parseEnvInt(
      "BENCH_REPLAY_COMMITS",
      Number(config.commitLimit ?? report?.commitTotals?.discovered ?? 1000),
    ),
    firstParent: parseEnvBool("BENCH_REPLAY_FIRST_PARENT", config.firstParent ?? true),
    syncRemote: parseEnvBool("BENCH_REPLAY_FETCH", false),
    installTextLinesPlugin: parseEnvBool(
      "BENCH_REPLAY_INSTALL_TEXT_LINES_PLUGIN",
      config.installTextLinesPlugin ?? true,
    ),
    maxInsertRows: parseEnvInt("BENCH_REPLAY_MAX_INSERT_ROWS", Number(config.maxInsertRows ?? 200)),
    maxInsertSqlChars: parseEnvInt(
      "BENCH_REPLAY_MAX_INSERT_SQL_CHARS",
      Number(config.maxInsertSqlChars ?? 1_500_000),
    ),
    progressEvery: parseEnvInt("BENCH_REPLAY_PROGRESS_EVERY", 25),
  };

  const repo = await ensureGitRepo({
    repoPath: replayConfig.repoPath || undefined,
    repoUrl: replayConfig.repoUrl,
    cacheDir: replayConfig.cacheDir,
    defaultDirName: "next.js",
    syncRemote: replayConfig.syncRemote,
    ref: replayConfig.repoRef,
  });

  const commits = await listLinearCommits(repo.repoPath, {
    ref: replayConfig.repoRef,
    maxCount: replayConfig.commitLimit,
    firstParent: replayConfig.firstParent,
  });
  const targetIndex = commits.findIndex((commit) => commit === targetCommit);
  if (targetIndex < 0) {
    throw new Error(
      `target commit ${targetCommit} not found in first ${commits.length} commits of ${replayConfig.repoRef}`,
    );
  }

  console.log(
    `[profile] target commit ${targetCommit.slice(0, 12)} at index ${targetIndex + 1}/${commits.length}`,
  );
  console.log("[profile] opening lix and replaying history up to target commit");

  const baseBackend = await createWasmSqliteBackend();
  const tracedBackend = createTracingBackend(baseBackend);
  const lix = await openLix({
    backend: tracedBackend.backend,
    keyValues: [{
      key: "lix_deterministic_mode",
      value: { enabled: true },
      lixcol_version_id: "global",
    }],
  });

  try {
    if (replayConfig.installTextLinesPlugin) {
      const wasmBytes = await loadTextLinesPluginWasmBytes();
      await lix.installPlugin({
        manifestJson: TEXT_LINES_MANIFEST,
        wasmBytes,
      });
    }

    const state = createReplayState();
    const replayStarted = performance.now();
    let profile = null;

    for (let index = 0; index <= targetIndex; index++) {
      const commitSha = commits[index];
      const patchSet = await readCommitPatchSet(repo.repoPath, commitSha);
      const prepared = prepareCommitChanges(state, patchSet.changes, patchSet.blobByOid);
      const statements = buildReplayCommitStatements(prepared, {
        maxInsertRows: replayConfig.maxInsertRows,
        maxInsertSqlChars: replayConfig.maxInsertSqlChars,
      });

      if (index < targetIndex) {
        for (const statement of statements) {
          await lix.execute(statement.sql, statement.params ?? []);
        }
      } else {
        console.log(
          `[profile] profiling target commit with ${statements.length} statement(s), changed_paths=${patchSet.changes.length}`,
        );
        profile = await profileCommitStatements(
          lix,
          tracedBackend,
          commitSha,
          patchSet.changes.length,
          prepared,
          statements,
        );
      }

      if ((index + 1) % replayConfig.progressEvery === 0 || index === targetIndex) {
        const elapsedMs = performance.now() - replayStarted;
        console.log(
          `[profile] replay progress ${index + 1}/${targetIndex + 1} (${(((index + 1) / (targetIndex + 1)) * 100).toFixed(1)}%) elapsed=${(elapsedMs / 1000).toFixed(1)}s`,
        );
      }
    }

    if (!profile) {
      throw new Error("internal error: target profile not collected");
    }

    const reportText = formatExplainReport({
      reportPath,
      outputPath,
      repo,
      replayConfig,
      targetIndex,
      totalCommits: commits.length,
      profile,
    });
    await mkdir(dirname(outputPath), { recursive: true });
    await writeFile(outputPath, reportText, "utf8");
    console.log(`[profile] wrote explain report: ${outputPath}`);
  } finally {
    await lix.close();
  }
}

async function profileCommitStatements(
  lix,
  tracedBackend,
  commitSha,
  changedPaths,
  prepared,
  statements,
) {
  const statementProfiles = [];
  let commitTotalMs = 0;
  const traceStart = tracedBackend.trace.length;

  for (let index = 0; index < statements.length; index++) {
    const statement = statements[index];
    const sql = statement.sql;
    const params = statement.params ?? [];
    const started = performance.now();
    await lix.execute(sql, params);
    const durationMs = performance.now() - started;
    commitTotalMs += durationMs;

    statementProfiles.push({
      statementIndex: index,
      durationMs,
      sqlChars: sql.length,
      sql: sanitizeSqlForReport(sql),
    });
  }

  const rawTrace = tracedBackend.trace.slice(traceStart);
  const rawTraceByKind = summarizeRawTraceByKind(rawTrace);
  const rawTraceTemplates = summarizeRawTraceTemplates(rawTrace);
  const rawTraceSummaries = summarizeRawTraceExact(rawTrace);
  const explainPlans = await collectExplainPlansForRawTrace(tracedBackend, rawTraceSummaries);

  const sorted = [...statementProfiles].sort((a, b) => b.durationMs - a.durationMs);
  return {
    commitSha,
    changedPaths,
    inserts: prepared.inserts.length,
    updates: prepared.updates.length,
    deletes: prepared.deletes.length,
    statementCount: statements.length,
    commitTotalMs,
    slowestStatements: sorted.slice(0, 10),
    statements: statementProfiles,
    rawTraceTotalCount: rawTrace.length,
    rawTraceByKind,
    rawTraceTemplates,
    rawTraceSummaries,
    rawExplainPlans: explainPlans,
  };
}

function formatExplainReport(payload) {
  const {
    reportPath,
    repo,
    replayConfig,
    targetIndex,
    totalCommits,
    profile,
  } = payload;
  const lines = [];
  lines.push("Next.js Replay Slowest-Commit Explain Report");
  lines.push("");
  lines.push(`source_report: ${reportPath}`);
  lines.push(`repo_path: ${repo.repoPath}`);
  lines.push(`repo_source: ${repo.source}`);
  lines.push(`repo_ref: ${replayConfig.repoRef}`);
  lines.push(`target_commit: ${profile.commitSha}`);
  lines.push(`target_position: ${targetIndex + 1}/${totalCommits}`);
  lines.push(`changed_paths: ${profile.changedPaths}`);
  lines.push(`writes: inserts=${profile.inserts} updates=${profile.updates} deletes=${profile.deletes}`);
  lines.push(`statement_count: ${profile.statementCount}`);
  lines.push(`commit_total_ms: ${profile.commitTotalMs.toFixed(3)}`);
  lines.push(`raw_backend_statement_count: ${profile.rawTraceTotalCount}`);
  lines.push("");
  lines.push("Top statements by execution time");
  for (const stmt of profile.slowestStatements) {
    lines.push(
      `- stmt=${stmt.statementIndex} duration_ms=${stmt.durationMs.toFixed(3)} sql_chars=${stmt.sqlChars}`,
    );
  }
  lines.push("");

  lines.push("Raw backend trace by kind");
  for (const item of profile.rawTraceByKind) {
    lines.push(
      `- kind=${item.kind} count=${item.count} total_ms=${item.totalMs.toFixed(3)} avg_ms=${(item.totalMs / Math.max(item.count, 1)).toFixed(3)}`,
    );
  }
  lines.push("");

  lines.push("Top raw SQL templates by total time");
  for (const item of profile.rawTraceTemplates) {
    lines.push(
      `- template=${item.templateDigest} count=${item.count} total_ms=${item.totalMs.toFixed(3)} max_ms=${item.maxMs.toFixed(3)} avg_ms=${(item.totalMs / Math.max(item.count, 1)).toFixed(3)}`,
    );
    lines.push(`  preview: ${compactSqlPreview(item.sampleSql)}`);
  }
  lines.push("");

  for (const stmt of profile.statements) {
    lines.push("--------------------------------------------------------------------------------");
    lines.push(`statement_index: ${stmt.statementIndex}`);
    lines.push(`duration_ms: ${stmt.durationMs.toFixed(3)}`);
    lines.push(`sql_chars: ${stmt.sqlChars}`);
    lines.push("");
    lines.push("SQL");
    lines.push(stmt.sql);
    lines.push("");
  }

  lines.push("================================================================================");
  lines.push("Top Raw Backend Statements");
  for (const item of profile.rawTraceSummaries) {
    const digest = digestSql(item.sql);
    lines.push(
      `- digest=${digest} count=${item.count} total_ms=${item.totalMs.toFixed(3)} max_ms=${item.maxMs.toFixed(3)} avg_ms=${(item.totalMs / Math.max(item.count, 1)).toFixed(3)} kind=${item.kind}`,
    );
    lines.push(`  sql_preview: ${compactSqlPreview(item.sql)}`);
  }
  lines.push("");

  lines.push("================================================================================");
  lines.push("EXPLAIN QUERY PLAN (Raw Backend SQL)");
  for (const plan of profile.rawExplainPlans) {
    const digest = digestSql(plan.sql);
    lines.push("--------------------------------------------------------------------------------");
    lines.push(`digest: ${digest}`);
    lines.push(`kind: ${plan.kind}`);
    lines.push(`count: ${plan.count}`);
    lines.push(`total_ms: ${plan.totalMs.toFixed(3)}`);
    lines.push(`max_ms: ${plan.maxMs.toFixed(3)}`);
    lines.push(`avg_ms: ${(plan.totalMs / Math.max(plan.count, 1)).toFixed(3)}`);
    lines.push("SQL");
    lines.push(compactSqlPreview(plan.sql, 1200));
    lines.push("EXPLAIN");
    if (plan.error) {
      lines.push(`ERROR: ${plan.error}`);
    } else if (plan.rows.length === 0) {
      lines.push("(no rows)");
    } else {
      const explainCounts = summarizeExplainRows(plan.rows);
      lines.push(
        `summary: scan=${explainCounts.scan} search=${explainCounts.search} temp_btree=${explainCounts.tempBtree} materialize=${explainCounts.materialize} coroutine=${explainCounts.coroutine}`,
      );
      for (const row of plan.rows) {
        lines.push(`- ${row.join(" | ")}`);
      }
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function sanitizeSqlForReport(sql) {
  const withoutBlobs = String(sql).replace(/x'([0-9a-fA-F]+)'/g, (_match, hex) => {
    const bytes = Math.floor(hex.length / 2);
    return `x'<blob:${bytes} bytes>'`;
  });
  return withoutBlobs;
}

function compactSqlPreview(sql, maxChars = 320) {
  const normalized = sanitizeSqlForReport(sql).replace(/\s+/g, " ").trim();
  if (normalized.length <= maxChars) {
    return normalized;
  }
  return `${normalized.slice(0, maxChars)}... [${normalized.length} chars]`;
}

function digestSql(sql) {
  return createHash("sha1")
    .update(String(sql))
    .digest("hex")
    .slice(0, 12);
}

function createTracingBackend(baseBackend) {
  const trace = [];
  let capture = true;

  const withTrace = async (kind, sql, params, fn) => {
    const started = performance.now();
    try {
      const result = await fn();
      if (capture) {
        trace.push({
          kind,
          sql: String(sql),
          params: [...params],
          durationMs: performance.now() - started,
          error: null,
        });
      }
      return result;
    } catch (error) {
      if (capture) {
        trace.push({
          kind,
          sql: String(sql),
          params: [...params],
          durationMs: performance.now() - started,
          error: firstErrorLine(error),
        });
      }
      throw error;
    }
  };

  const backend = {
    dialect: baseBackend.dialect,
    async execute(sql, params) {
      return await withTrace("backend", sql, params, () => baseBackend.execute(sql, params));
    },
    async beginTransaction() {
      const tx = await baseBackend.beginTransaction();
      return {
        dialect: tx.dialect,
        async execute(sql, params) {
          return await withTrace("tx", sql, params, () => tx.execute(sql, params));
        },
        async commit() {
          return await withTrace("tx-commit", "COMMIT", [], () => tx.commit());
        },
        async rollback() {
          return await withTrace("tx-rollback", "ROLLBACK", [], () => tx.rollback());
        },
      };
    },
    async exportSnapshot() {
      return await baseBackend.exportSnapshot();
    },
    async close() {
      if (typeof baseBackend.close === "function") {
        await baseBackend.close();
      }
    },
  };

  return {
    backend,
    trace,
    setCapture(next) {
      capture = next;
    },
    async executeRaw(sql, params = []) {
      return await baseBackend.execute(sql, params);
    },
  };
}

function summarizeRawTraceExact(rawTrace) {
  const grouped = new Map();
  for (const item of rawTrace) {
    if (item.error) {
      continue;
    }
    const normalizedSql = item.sql.trim();
    if (!normalizedSql) {
      continue;
    }
    const key = `${item.kind}::${normalizedSql}`;
    const existing = grouped.get(key);
    if (existing) {
      existing.count += 1;
      existing.totalMs += item.durationMs;
      existing.maxMs = Math.max(existing.maxMs, item.durationMs);
      continue;
    }
    grouped.set(key, {
      kind: item.kind,
      sql: normalizedSql,
      params: item.params,
      count: 1,
      totalMs: item.durationMs,
      maxMs: item.durationMs,
    });
  }

  return [...grouped.values()]
    .filter((item) => explainableSql(item.sql))
    .sort((a, b) => b.totalMs - a.totalMs)
    .slice(0, 20);
}

function summarizeRawTraceByKind(rawTrace) {
  const grouped = new Map();
  for (const item of rawTrace) {
    if (item.error) {
      continue;
    }
    const existing = grouped.get(item.kind);
    if (existing) {
      existing.count += 1;
      existing.totalMs += item.durationMs;
      continue;
    }
    grouped.set(item.kind, {
      kind: item.kind,
      count: 1,
      totalMs: item.durationMs,
    });
  }
  return [...grouped.values()].sort((a, b) => b.totalMs - a.totalMs);
}

function summarizeRawTraceTemplates(rawTrace) {
  const grouped = new Map();
  for (const item of rawTrace) {
    if (item.error) {
      continue;
    }
    const sql = item.sql.trim();
    if (!sql || !explainableSql(sql)) {
      continue;
    }
    const templateKey = `${item.kind}::${normalizeSqlTemplate(sql)}`;
    const existing = grouped.get(templateKey);
    if (existing) {
      existing.count += 1;
      existing.totalMs += item.durationMs;
      existing.maxMs = Math.max(existing.maxMs, item.durationMs);
      continue;
    }
    grouped.set(templateKey, {
      kind: item.kind,
      templateDigest: digestSql(templateKey),
      sampleSql: sql,
      count: 1,
      totalMs: item.durationMs,
      maxMs: item.durationMs,
    });
  }
  return [...grouped.values()]
    .sort((a, b) => b.totalMs - a.totalMs)
    .slice(0, 20);
}

function normalizeSqlTemplate(sql) {
  return String(sql)
    .replace(/x'([0-9a-fA-F]+)'/g, "x'<blob>'")
    .replace(/'([^']|'')*'/g, "'?'")
    .replace(/\b\d+\b/g, "?")
    .replace(/\s+/g, " ")
    .trim();
}

async function collectExplainPlansForRawTrace(tracedBackend, summaries) {
  const plans = [];
  for (const item of summaries) {
    const explainSql = `EXPLAIN QUERY PLAN ${item.sql}`;
    let rows = [];
    let error = null;
    tracedBackend.setCapture(false);
    try {
      const explainResult = await tracedBackend.executeRaw(explainSql, item.params);
      rows = (explainResult?.rows ?? []).map((row) => row.map(scalarToText));
    } catch (cause) {
      error = firstErrorLine(cause);
    } finally {
      tracedBackend.setCapture(true);
    }

    plans.push({
      kind: item.kind,
      sql: item.sql,
      count: item.count,
      totalMs: item.totalMs,
      maxMs: item.maxMs,
      rows,
      error,
    });
  }
  return plans;
}

function summarizeExplainRows(rows) {
  const summary = {
    scan: 0,
    search: 0,
    tempBtree: 0,
    materialize: 0,
    coroutine: 0,
  };
  for (const row of rows) {
    const detail = String(row?.[3] ?? "").toUpperCase();
    if (detail.includes("SCAN ")) {
      summary.scan += 1;
    }
    if (detail.includes("SEARCH ")) {
      summary.search += 1;
    }
    if (detail.includes("USE TEMP B-TREE")) {
      summary.tempBtree += 1;
    }
    if (detail.includes("MATERIALIZE ")) {
      summary.materialize += 1;
    }
    if (detail.includes("CO-ROUTINE")) {
      summary.coroutine += 1;
    }
  }
  return summary;
}

function explainableSql(sql) {
  const upper = String(sql).trim().toUpperCase();
  return (
    upper.startsWith("SELECT ") ||
    upper.startsWith("INSERT ") ||
    upper.startsWith("UPDATE ") ||
    upper.startsWith("DELETE ")
  );
}

function firstErrorLine(error) {
  return String(error?.message ?? error).split("\n")[0];
}

function scalarToText(value) {
  if (value === null || value === undefined) {
    return "NULL";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  if (typeof value === "object" && value && "kind" in value) {
    return String(value.value);
  }
  return JSON.stringify(value);
}

async function loadTextLinesPluginWasmBytes() {
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
      await ensureTextLinesPluginWasmBuilt();
      try {
        return await readFile(packageDebugPath);
      } catch {
        return await readFile(workspaceDebugPath);
      }
    }
  }
}

async function ensureTextLinesPluginWasmBuilt() {
  const manifestPath = join(REPO_ROOT, "packages", "plugin-text-lines", "Cargo.toml");
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
  console.error("Slowest commit profile failed:");
  console.error(error);
  process.exitCode = 1;
});
