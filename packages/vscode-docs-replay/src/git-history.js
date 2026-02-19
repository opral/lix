import { mkdir, readFile, stat } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { spawn } from "node:child_process";

export const NULL_OID = "0000000000000000000000000000000000000000";
const LFS_OID_REGEX = /^oid sha256:([0-9a-f]{64})$/i;
const lfsFetchedAllRepos = new Set();

const LFS_CONFIG = {
  resolvePointers: parseEnvBool("VSCODE_REPLAY_RESOLVE_LFS_POINTERS", true),
  fetchMissingObjects: parseEnvBool("VSCODE_REPLAY_FETCH_MISSING_LFS_OBJECTS", true),
};

export async function ensureGitRepo(options) {
  const {
    repoPath,
    repoUrl,
    cacheDir,
    defaultDirName,
    syncRemote,
    ref,
  } = options;

  if (repoPath) {
    await assertGitRepo(repoPath);
    if (syncRemote) {
      await runGit(repoPath, ["fetch", "--tags", "origin"]);
    }
    return { repoPath, source: "explicit-path", ref: ref ?? "HEAD" };
  }

  if (!repoUrl) {
    throw new Error("repoUrl is required when BENCH_REPLAY_REPO_PATH is not set");
  }

  const targetPath = join(cacheDir, defaultDirName ?? "repo");
  await mkdir(cacheDir, { recursive: true });

  const hasRepo = await isGitRepo(targetPath);
  if (!hasRepo) {
    await runCommand("git", [
      "clone",
      "--filter=blob:none",
      "--no-checkout",
      repoUrl,
      targetPath,
    ]);
  } else if (syncRemote) {
    await runGit(targetPath, ["fetch", "--tags", "origin"]);
  }

  return { repoPath: targetPath, source: hasRepo ? "cache" : "clone", ref: ref ?? "HEAD" };
}

export async function listLinearCommits(repoPath, { ref = "HEAD", maxCount, firstParent = true }) {
  const args = ["rev-list", "--reverse"];
  if (firstParent) {
    args.push("--first-parent");
  }
  args.push(ref);

  const stdout = await runGit(repoPath, args);
  const commits = stdout
    .toString("utf8")
    .split("\n")
    .map((entry) => entry.trim())
    .filter(Boolean);
  if (maxCount && maxCount > 0) {
    return commits.slice(0, maxCount);
  }
  return commits;
}

export async function readCommitPatchSet(repoPath, commitSha) {
  const raw = await runGit(repoPath, [
    "diff-tree",
    "--root",
    "--raw",
    "-r",
    "-z",
    "-m",
    "--first-parent",
    "--find-renames",
    "--no-commit-id",
    commitSha,
  ]);

  const changes = parseRawDiffTree(raw);
  const wantedBlobIds = new Set();
  for (const change of changes) {
    if (!change.newPath) {
      continue;
    }
    if (change.newOid && change.newOid !== NULL_OID) {
      wantedBlobIds.add(change.newOid);
    }
  }

  const blobByOid = await readBlobs(repoPath, [...wantedBlobIds]);
  return { changes, blobByOid };
}

function parseRawDiffTree(rawBuffer) {
  if (rawBuffer.length === 0) {
    return [];
  }

  const tokens = rawBuffer.toString("utf8").split("\0");
  if (tokens[tokens.length - 1] === "") {
    tokens.pop();
  }

  const changes = [];
  let index = 0;

  while (index < tokens.length) {
    const headerToken = tokens[index++];
    if (!headerToken || !headerToken.startsWith(":")) {
      continue;
    }

    const fields = headerToken.slice(1).split(" ");
    if (fields.length < 5) {
      continue;
    }

    const [oldMode, newMode, oldOid, newOid, statusToken] = fields;
    const status = statusToken[0] ?? "M";
    const firstPath = tokens[index++] ?? "";

    if (status === "R" || status === "C") {
      const secondPath = tokens[index++] ?? "";
      changes.push({
        status,
        oldMode,
        newMode,
        oldOid,
        newOid,
        oldPath: firstPath,
        newPath: secondPath,
      });
      continue;
    }

    changes.push({
      status,
      oldMode,
      newMode,
      oldOid,
      newOid,
      oldPath: status === "A" ? null : firstPath,
      newPath: status === "D" ? null : firstPath,
    });
  }

  return changes;
}

async function readBlobs(repoPath, blobIds) {
  if (blobIds.length === 0) {
    return new Map();
  }

  const requestBody = Buffer.from(`${blobIds.join("\n")}\n`, "utf8");
  const stdout = await runGit(repoPath, ["cat-file", "--batch"], {
    stdin: requestBody,
  });

  const blobs = new Map();
  let offset = 0;

  while (offset < stdout.length) {
    const lineEnd = stdout.indexOf(0x0a, offset);
    if (lineEnd < 0) {
      break;
    }

    const header = stdout.toString("utf8", offset, lineEnd).trim();
    offset = lineEnd + 1;

    if (!header) {
      continue;
    }

    const [oid, type, sizeToken] = header.split(" ");
    if (type === "missing") {
      blobs.set(oid, null);
      continue;
    }

    const size = Number.parseInt(sizeToken, 10);
    if (!Number.isFinite(size) || size < 0) {
      throw new Error(`invalid cat-file size '${sizeToken}' for ${oid}`);
    }

    const dataStart = offset;
    const dataEnd = dataStart + size;
    if (dataEnd > stdout.length) {
      throw new Error(`cat-file output truncated while reading ${oid}`);
    }

    blobs.set(oid, Uint8Array.from(stdout.subarray(dataStart, dataEnd)));
    offset = dataEnd;
    if (offset < stdout.length && stdout[offset] === 0x0a) {
      offset += 1;
    }
  }

  if (!LFS_CONFIG.resolvePointers) {
    return blobs;
  }

  return await resolveGitLfsPointers(repoPath, blobs);
}

async function resolveGitLfsPointers(repoPath, blobs) {
  const pointerEntries = [];

  for (const [gitBlobOid, bytes] of blobs.entries()) {
    if (!(bytes instanceof Uint8Array)) {
      continue;
    }
    const pointer = parseGitLfsPointer(bytes);
    if (!pointer) {
      continue;
    }
    pointerEntries.push({ gitBlobOid, lfsOid: pointer.oid });
  }

  if (pointerEntries.length === 0) {
    return blobs;
  }

  const gitDir = await resolveGitDir(repoPath);
  const unresolved = [];

  for (const entry of pointerEntries) {
    const resolved = await readLfsObjectBytes(gitDir, entry.lfsOid);
    if (resolved) {
      blobs.set(entry.gitBlobOid, resolved);
    } else {
      unresolved.push(entry);
    }
  }

  if (unresolved.length === 0 || !LFS_CONFIG.fetchMissingObjects) {
    return blobs;
  }

  await fetchMissingLfsObjects(repoPath, unresolved.map((entry) => entry.lfsOid));

  for (const entry of unresolved) {
    const resolved = await readLfsObjectBytes(gitDir, entry.lfsOid);
    if (resolved) {
      blobs.set(entry.gitBlobOid, resolved);
    }
  }

  return blobs;
}

function parseGitLfsPointer(bytes) {
  if (bytes.byteLength < 48 || bytes.byteLength > 1024) {
    return null;
  }

  let text;
  try {
    text = Buffer.from(bytes).toString("utf8");
  } catch {
    return null;
  }

  if (!text.startsWith("version https://git-lfs.github.com/spec/v1")) {
    return null;
  }

  const lines = text.split(/\r?\n/);
  for (const line of lines) {
    const match = line.match(LFS_OID_REGEX);
    if (match) {
      return { oid: match[1].toLowerCase() };
    }
  }

  return null;
}

async function resolveGitDir(repoPath) {
  const gitDirRaw = await runGit(repoPath, ["rev-parse", "--git-dir"]);
  const gitDirText =
    typeof gitDirRaw === "string"
      ? gitDirRaw.trim()
      : Buffer.from(gitDirRaw).toString("utf8").trim();
  const gitDir = isAbsolute(gitDirText) ? gitDirText : join(repoPath, gitDirText);
  return gitDir;
}

async function readLfsObjectBytes(gitDir, oid) {
  const objectPath = join(gitDir, "lfs", "objects", oid.slice(0, 2), oid.slice(2, 4), oid);
  try {
    const bytes = await readFile(objectPath);
    return Uint8Array.from(bytes);
  } catch {
    return null;
  }
}

async function fetchMissingLfsObjects(repoPath, objectIds) {
  if (objectIds.length === 0) {
    return;
  }
  if (lfsFetchedAllRepos.has(repoPath)) {
    return;
  }
  await runGit(repoPath, ["lfs", "fetch", "--all", "origin"]);
  lfsFetchedAllRepos.add(repoPath);
}

async function assertGitRepo(repoPath) {
  if (!(await isGitRepo(repoPath))) {
    throw new Error(`not a git repository: ${repoPath}`);
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
