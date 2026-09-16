import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { isContentPath } from "./ci-content-scope.mjs";
import { binaryManifest, cacheKey } from "./ci-sdk-cache.mjs";
import { readArtifactJson } from "./ci-merge-reuse.mjs";
import { matchesBrowserBuild } from "./release-browser-artifact.mjs";

const git = (root, ...args) => execFileSync("git", args, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], maxBuffer: 16 * 1024 * 1024 }).trim();

// Include ALL non-content tracked files, including handwritten TypeScript and
// tests. A binary-only fingerprint is insufficient for copying the whole SDK.
export function codeTree(root, revision) {
  if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error("Invalid source revision");
  const hash = createHash("sha256");
  for (const entry of git(root, "ls-tree", "-r", "-z", revision).split("\0").filter(Boolean)) {
    const path = entry.slice(entry.indexOf("\t") + 1);
    if (!isContentPath(path)) hash.update(entry + "\0");
  }
  return hash.digest("hex");
}

export async function selectContentArtifact({ github, context, core, root = process.cwd(), readJson = readArtifactJson, env = process.env }) {
  const target = git(root, "rev-parse", "HEAD");
  const expected = codeTree(root, target);
  try {
    const { data } = await github.rest.actions.listWorkflowRuns({ ...context.repo, workflow_id: "ci.yml", status: "success", per_page: 50 });
    for (const run of data.workflow_runs) {
      if (run.conclusion !== "success" || run.path !== ".github/workflows/ci.yml" ||
          run.head_repository?.full_name !== `${context.repo.owner}/${context.repo.repo}`) continue;
      try { if (codeTree(root, run.head_sha) !== expected) continue; } catch { continue; }
      const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, { ...context.repo, run_id: run.id, per_page: 100 });
      const name = `lix-browser-sdk-${run.head_sha}`;
      if (!artifacts.some(a => a.name === name && !a.expired)) continue;
      const provenance = artifacts.find(a => a.name === "ci-browser-build" && !a.expired);
      if (!provenance) continue;
      try {
        const manifest = await readJson({ github, repo: context.repo, artifact: provenance, filename: "browser.json" });
        if (!matchesBrowserBuild(manifest, { revision: run.head_sha, tree: git(root, "rev-parse", `${run.head_sha}^{tree}`), key: cacheKey(root, "browser", env) })) continue;
      } catch { continue; }
      core.setOutput("revision", run.head_sha);
      core.setOutput("run_id", run.id);
      core.info(`Reusing SDK from ${run.id}: every non-content tracked input is identical.`);
      return;
    }
    core.info("No retained matching SDK artifact. Content validation needs no build; an exact-revision consumer can request artifacts_only explicitly.");
  } catch (error) {
    core.warning(`Content artifact lookup unavailable; no compilation scheduled: ${error.message}`);
  }
}

export function promoteContentArtifact(root, revision, target, sourceRun, env = process.env) {
  if (codeTree(root, revision) !== codeTree(root, target)) throw new Error("SDK code inputs differ");
  const path = join(root, "ci-artifact/browser.json");
  const manifest = JSON.parse(readFileSync(path, "utf8"));
  const key = cacheKey(root, "browser", env);
  if (!matchesBrowserBuild(manifest, { revision, tree: git(root, "rev-parse", `${revision}^{tree}`), key })) throw new Error("Content artifact provenance mismatch");
  if (JSON.stringify(binaryManifest(join(root, "packages/js-sdk"), "browser", key)) !== JSON.stringify(manifest.releaseBuild.binaries)) throw new Error("Content artifact checksum mismatch");
  manifest.reusedFromRevision = revision;
  manifest.reusedFromRun = sourceRun;
  manifest.sourceRevision = target;
  manifest.releaseBuild.sourceTree = git(root, "rev-parse", `${target}^{tree}`);
  writeFileSync(path, JSON.stringify(manifest));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  promoteContentArtifact(process.cwd(), process.env.SOURCE_REVISION, process.env.TARGET_REVISION, process.env.SOURCE_RUN);
}
