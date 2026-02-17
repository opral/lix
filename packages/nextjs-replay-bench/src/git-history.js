import { mkdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { spawn } from "node:child_process";

export const NULL_OID = "0000000000000000000000000000000000000000";

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

  return blobs;
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
