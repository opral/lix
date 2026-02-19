import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..", "..");

const CONFIG = {
  repoUrl: process.env.VSCODE_REPLAY_REPO_URL ?? "https://github.com/microsoft/vscode-docs.git",
  repoPath: process.env.VSCODE_REPLAY_REPO_PATH ?? join(REPO_ROOT, "artifact", "vscode-docs"),
  anchorPath:
    process.env.VSCODE_REPLAY_ANCHOR_PATH ??
    join(__dirname, "..", ".cache", "vscode-docs.anchor"),
  fetch: parseEnvBool("VSCODE_REPLAY_FETCH", true),
  resetAnchor: parseEnvBool("VSCODE_REPLAY_RESET_ANCHOR", false),
};

async function main() {
  await ensureRepo();
  const anchor = await resolveAnchorCommitSha();

  console.log(`[bootstrap] repo path: ${CONFIG.repoPath}`);
  console.log(`[bootstrap] repo url : ${CONFIG.repoUrl}`);
  console.log(`[bootstrap] anchor   : ${anchor}`);
  console.log(`[bootstrap] anchor file: ${CONFIG.anchorPath}`);
}

async function ensureRepo() {
  if (await isGitRepo(CONFIG.repoPath)) {
    const isShallow = await runGit(CONFIG.repoPath, ["rev-parse", "--is-shallow-repository"]);
    if (isShallow === "true") {
      await runGit(CONFIG.repoPath, ["fetch", "--unshallow", "--tags", "origin"]);
      return;
    }

    if (CONFIG.fetch) {
      await runGit(CONFIG.repoPath, ["fetch", "--tags", "origin"]);
    }
    return;
  }

  await mkdir(dirname(CONFIG.repoPath), { recursive: true });
  await runCommand("git", ["clone", CONFIG.repoUrl, CONFIG.repoPath]);
}

async function resolveAnchorCommitSha() {
  if (!CONFIG.resetAnchor) {
    const existing = await readAnchorShaFromFile();
    if (existing && (await isCommitAvailable(existing))) {
      return existing;
    }
  }

  const latest = await resolveLatestCommitSha();
  await mkdir(dirname(CONFIG.anchorPath), { recursive: true });
  await writeFile(CONFIG.anchorPath, `${latest}\n`, "utf8");
  return latest;
}

async function readAnchorShaFromFile() {
  try {
    const raw = await readFile(CONFIG.anchorPath, "utf8");
    const firstLine = raw.split(/\r?\n/, 1)[0]?.trim();
    return firstLine || null;
  } catch {
    return null;
  }
}

async function resolveLatestCommitSha() {
  try {
    const value = await runGit(CONFIG.repoPath, ["rev-parse", "origin/HEAD^{commit}"]);
    return value;
  } catch {
    return await runGit(CONFIG.repoPath, ["rev-parse", "HEAD^{commit}"]);
  }
}

async function isCommitAvailable(sha) {
  try {
    await runGit(CONFIG.repoPath, ["rev-parse", `${sha}^{commit}`]);
    return true;
  } catch {
    return false;
  }
}

async function isGitRepo(repoPath) {
  try {
    const stats = await stat(join(repoPath, ".git"));
    return stats.isDirectory() || stats.isFile();
  } catch {
    return false;
  }
}

async function runGit(repoPath, args) {
  const stdout = await runCommand("git", ["-C", repoPath, ...args]);
  return stdout.toString("utf8").trim();
}

async function runCommand(command, args) {
  return await new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
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
  });
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

main().catch((error) => {
  console.error("bootstrap failed");
  console.error(error);
  process.exitCode = 1;
});
