import { mkdir, readFile, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { openLix } from "@lix-js/sdk";
import { createBetterSqlite3Backend } from "@lix-js/better-sqlite3-backend";
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

const CONFIG = {
  repoUrl: process.env.VSCODE_REPLAY_REPO_URL ?? "https://github.com/microsoft/vscode-docs.git",
  repoPath: process.env.VSCODE_REPLAY_REPO_PATH ?? join(REPO_ROOT, "artifact", "vscode-docs"),
  replayRef: process.env.VSCODE_REPLAY_REF?.trim() || null,
  anchorPath:
    process.env.VSCODE_REPLAY_ANCHOR_PATH ??
    join(__dirname, "..", ".cache", "vscode-docs.anchor"),
  outputPath:
    process.env.VSCODE_REPLAY_OUTPUT_PATH ??
    join(__dirname, "..", "results", "vscode-docs-first-100.lix"),
  commitLimit: parseEnvInt("VSCODE_REPLAY_COMMITS", 100),
  progressEvery: parseEnvInt("VSCODE_REPLAY_PROGRESS_EVERY", 10),
  installTextPlugin: parseEnvBool("VSCODE_REPLAY_INSTALL_TEXT_PLUGIN", true),
  installMdPlugin: parseEnvBool("VSCODE_REPLAY_INSTALL_MD_PLUGIN", true),
  verifyState: parseEnvBool("VSCODE_REPLAY_VERIFY_STATE", false),
};

async function main() {
  const started = performance.now();
  const replayRef = await resolveReplayRef();

  const repo = await ensureGitRepo({
    repoPath: CONFIG.repoPath,
    repoUrl: CONFIG.repoUrl,
    cacheDir: join(__dirname, "..", ".cache"),
    defaultDirName: "vscode-docs",
    syncRemote: false,
    ref: replayRef,
  });

  const commits = await listLinearCommits(repo.repoPath, {
    ref: replayRef,
    maxCount: CONFIG.commitLimit,
    firstParent: true,
  });

  if (commits.length === 0) {
    throw new Error(`no commits found at ${repo.repoPath} (${replayRef})`);
  }

  await mkdir(dirname(CONFIG.outputPath), { recursive: true });
  await rm(CONFIG.outputPath, { force: true });

  const backend = await createBetterSqlite3Backend({
    filename: CONFIG.outputPath,
  });

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

  try {
    if (CONFIG.installTextPlugin) {
      await installPluginFromWorkspace(lix, {
        packageDirName: "text-plugin",
        wasmFileName: "text_plugin.wasm",
      });
    } else {
      console.log("[replay] skipping text plugin install");
    }

    if (CONFIG.installMdPlugin) {
      await installPluginFromWorkspace(lix, {
        packageDirName: "plugin-md-v2",
        wasmFileName: "plugin_md_v2.wasm",
      });
    } else {
      console.log("[replay] skipping markdown plugin install");
    }

    const state = createReplayState();
    const expectedStateById = new Map();
    let applied = 0;
    let noop = 0;
    let changedPaths = 0;
    let verifiedCommits = 0;
    let verificationMs = 0;

    for (let index = 0; index < commits.length; index++) {
      const commitSha = commits[index];
      const patchSet = await readCommitPatchSet(repo.repoPath, commitSha);
      changedPaths += patchSet.changes.length;

      const prepared = prepareCommitChanges(state, patchSet.changes, patchSet.blobByOid);
      const statements = buildReplayCommitStatements(prepared, {
        maxInsertRows: 1,
        maxInsertSqlChars: 1_500_000,
      });

      if (statements.length === 0) {
        noop += 1;
      } else {
        applied += 1;
      }

      if (statements.length > 0) {
        try {
          await executeStatementsInTransaction(lix, statements);
        } catch (error) {
          const payload = error && typeof error === "object" ? error.payload : undefined;
          const payloadJson =
            payload === undefined
              ? "undefined"
              : (() => {
                  try {
                    return JSON.stringify(payload);
                  } catch {
                    return String(payload);
                  }
                })();
          const sqlPreview = String(statements[0]?.sql ?? "")
            .replace(/\s+/g, " ")
            .slice(0, 200);
          throw new Error(
            `failed at commit ${commitSha} replay transaction (statements=${statements.length}, firstSql=${sqlPreview}): ${String(error?.message ?? error)}; payload=${payloadJson}`,
          );
        }
      }

      if (CONFIG.verifyState) {
        const verifyStarted = performance.now();
        applyPreparedBatchToExpectedState(expectedStateById, prepared);
        await verifyCommitStateHashes({
          lix,
          commitSha,
          expectedStateById,
        });
        verificationMs += performance.now() - verifyStarted;
        verifiedCommits += 1;
      }

      if (
        index === 0 ||
        (index + 1) % CONFIG.progressEvery === 0 ||
        index + 1 === commits.length
      ) {
        console.log(
          `[replay] ${index + 1}/${commits.length} commits (applied=${applied}, noop=${noop}, changedPaths=${changedPaths})`,
        );
      }
    }

    const elapsedMs = performance.now() - started;

    console.log(`[replay] done in ${(elapsedMs / 1000).toFixed(2)}s`);
    console.log(`[replay] replay ref: ${replayRef}`);
    console.log(`[replay] output: ${CONFIG.outputPath}`);
    console.log(`[replay] commits replayed: ${commits.length}`);
    console.log(`[replay] commits applied: ${applied}`);
    console.log(`[replay] commits noop: ${noop}`);
    console.log(`[replay] changed paths total: ${changedPaths}`);
    if (CONFIG.verifyState) {
      console.log(
        `[replay] verified commits: ${verifiedCommits}/${commits.length} (${verificationMs.toFixed(2)}ms)`,
      );
    }
  } finally {
    await lix.close();
  }
}

async function resolveReplayRef() {
  if (CONFIG.replayRef) {
    return CONFIG.replayRef;
  }

  return await readAnchorSha(CONFIG.anchorPath);
}

async function installPluginFromWorkspace(lix, options) {
  const { packageDirName, wasmFileName } = options;
  const packageDir = join(REPO_ROOT, "packages", packageDirName);
  const manifestPath = join(packageDir, "manifest.json");
  const manifestJson = JSON.parse(await readFile(manifestPath, "utf8"));
  const wasmBytes = await loadPluginWasmBytes({ packageDir, wasmFileName });

  await lix.installPlugin({
    manifestJson,
    wasmBytes,
  });
}

async function loadPluginWasmBytes(options) {
  const { packageDir, wasmFileName } = options;
  const outputCandidates = [
    join(packageDir, "target", "wasm32-wasip2", "release", wasmFileName),
    join(REPO_ROOT, "target", "wasm32-wasip2", "release", wasmFileName),
    join(packageDir, "target", "wasm32-wasip2", "debug", wasmFileName),
    join(REPO_ROOT, "target", "wasm32-wasip2", "debug", wasmFileName),
  ];

  for (const path of outputCandidates) {
    try {
      return await readFile(path);
    } catch {
      // continue to next candidate
    }
  }

  await ensurePluginWasmBuilt(packageDir);

  for (const path of outputCandidates) {
    try {
      return await readFile(path);
    } catch {
      // continue to next candidate
    }
  }

  throw new Error(
    `failed to locate built wasm artifact '${wasmFileName}' for plugin at ${packageDir}`,
  );
}

function applyPreparedBatchToExpectedState(expectedStateById, prepared) {
  for (const id of prepared.deletes) {
    expectedStateById.delete(String(id));
  }

  for (const row of prepared.inserts) {
    expectedStateById.set(String(row.id), {
      path: String(row.path),
      hash: hashBytes(row.data),
    });
  }

  for (const row of prepared.updates) {
    expectedStateById.set(String(row.id), {
      path: String(row.path),
      hash: hashBytes(row.data),
    });
  }
}

async function verifyCommitStateHashes(args) {
  const { lix, commitSha, expectedStateById } = args;
  const result = await lix.execute("SELECT id, path, data FROM lix_file", []);
  const rows = statementRows(result);
  if (rows.length !== expectedStateById.size) {
    throw new Error(
      `state mismatch at ${commitSha}: row count differs (lix=${rows.length}, expected=${expectedStateById.size})`,
    );
  }

  const seen = new Set();
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    const id = scalarToString(row?.[0], `verify.id[${index}]`);
    const path = scalarToString(row?.[1], `verify.path[${index}]`);
    const data = scalarToBlob(row?.[2], `verify.data[${index}]`);
    const hash = hashBytes(data);

    const expected = expectedStateById.get(id);
    if (!expected) {
      throw new Error(`state mismatch at ${commitSha}: unexpected id ${id}`);
    }
    if (path !== expected.path) {
      throw new Error(
        `state mismatch at ${commitSha}: path differs for id ${id} (lix=${path}, expected=${expected.path})`,
      );
    }
    if (hash !== expected.hash) {
      throw new Error(`state mismatch at ${commitSha}: hash differs for id ${id}`);
    }
    seen.add(id);
  }

  if (seen.size !== expectedStateById.size) {
    throw new Error(
      `state mismatch at ${commitSha}: missing rows (lix=${seen.size}, expected=${expectedStateById.size})`,
    );
  }
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

function hashBytes(bytes) {
  return createHash("sha256").update(Buffer.from(bytes)).digest("hex");
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
  if (typeof value === "object" && value !== null) {
    if (
      value.kind === "Text" ||
      value.kind === "text" ||
      value.kind === "Integer" ||
      value.kind === "integer" ||
      value.kind === "Real" ||
      value.kind === "real"
    ) {
      return String(value.value);
    }
  }
  throw new Error(`unsupported scalar value for ${context}: ${JSON.stringify(value)}`);
}

function scalarToBlob(value, context) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return Uint8Array.from(value);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (typeof value === "object" && value !== null) {
    if (Array.isArray(value.value)) {
      return Uint8Array.from(value.value);
    }
    if (typeof value.base64 === "string") {
      return Uint8Array.from(Buffer.from(value.base64, "base64"));
    }
    if (
      (value.kind === "Blob" || value.kind === "blob") &&
      typeof value.value === "string"
    ) {
      return Uint8Array.from(Buffer.from(value.value, "base64"));
    }
  }
  throw new Error(`unsupported blob value for ${context}: ${JSON.stringify(value)}`);
}

async function ensurePluginWasmBuilt(packageDir) {
  const manifestPath = join(packageDir, "Cargo.toml");

  try {
    await runCommand("cargo", [
      "build",
      "--release",
      "--manifest-path",
      manifestPath,
      "--target",
      "wasm32-wasip2",
    ]);
    return;
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
  console.error("replay failed");
  console.error(error);
  process.exitCode = 1;
});
