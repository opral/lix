import { mkdir, readFile, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { spawn } from "node:child_process";
import { openLix } from "js-sdk";
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
  anchorPath:
    process.env.VSCODE_REPLAY_ANCHOR_PATH ??
    join(__dirname, "..", ".cache", "vscode-docs.anchor"),
  outputPath:
    process.env.VSCODE_REPLAY_OUTPUT_PATH ??
    join(__dirname, "..", "results", "vscode-docs-first-100.lix"),
  commitLimit: parseEnvInt("VSCODE_REPLAY_COMMITS", 100),
  progressEvery: parseEnvInt("VSCODE_REPLAY_PROGRESS_EVERY", 10),
};

async function main() {
  const started = performance.now();
  const anchorSha = await readAnchorSha(CONFIG.anchorPath);

  const repo = await ensureGitRepo({
    repoPath: CONFIG.repoPath,
    repoUrl: CONFIG.repoUrl,
    cacheDir: join(__dirname, "..", ".cache"),
    defaultDirName: "vscode-docs",
    syncRemote: false,
    ref: anchorSha,
  });

  const commits = await listLinearCommits(repo.repoPath, {
    ref: anchorSha,
    maxCount: CONFIG.commitLimit,
    firstParent: true,
  });

  if (commits.length === 0) {
    throw new Error(`no commits found at ${repo.repoPath} (${anchorSha})`);
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
    await installPluginFromWorkspace(lix, {
      packageDirName: "text-plugin",
      wasmFileName: "text_plugin.wasm",
    });
    await installPluginFromWorkspace(lix, {
      packageDirName: "plugin-md-v2",
      wasmFileName: "plugin_md_v2.wasm",
    });

    const state = createReplayState();
    let applied = 0;
    let noop = 0;
    let changedPaths = 0;

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

      for (const statement of statements) {
        try {
          await lix.execute(statement.sql, statement.params ?? []);
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
          const sqlPreview = statement.sql.replace(/\s+/g, " ").slice(0, 240);
          const paramPreview = (statement.params ?? [])
            .slice(0, 9)
            .map((value) => {
              if (value instanceof Uint8Array) {
                return `blob(${value.byteLength})`;
              }
              if (typeof value === "string") {
                return `text(${value.slice(0, 48)})`;
              }
              if (value === null) {
                return "null";
              }
              return typeof value;
            })
            .join(", ");
          throw new Error(
            `failed at commit ${commitSha} statement execution (sql chars=${statement.sql.length}, sql=${sqlPreview}, params=[${paramPreview}]): ${String(error?.message ?? error)}; payload=${payloadJson}`,
          );
        }
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
    console.log(`[replay] anchor commit: ${anchorSha}`);
    console.log(`[replay] output: ${CONFIG.outputPath}`);
    console.log(`[replay] commits replayed: ${commits.length}`);
    console.log(`[replay] commits applied: ${applied}`);
    console.log(`[replay] commits noop: ${noop}`);
    console.log(`[replay] changed paths total: ${changedPaths}`);
  } finally {
    await lix.close();
  }
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
