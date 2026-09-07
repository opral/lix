import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// A green head SHA alone is insufficient: Rust tests the PR merge tree, while
// SDK jobs check out the PR head. Both must match the landed tree exactly.
export function matchesTestedSource(evidence, { revision, tree }) {
	return evidence.schemaVersion === 1 &&
		evidence.sourceRevision === revision &&
		evidence.sourceTree === tree && evidence.testedTree === tree;
}

export async function findReusableRun({ github, repository, sha, tree, readEvidence }) {
	const [owner, repo] = repository.split("/");
	const scope = { owner, repo };
	const { data: prs } = await github.rest.repos.listPullRequestsAssociatedWithCommit({ ...scope, commit_sha: sha, per_page: 100 });
	for (const pr of prs) {
		if (!pr.merged_at || pr.merge_commit_sha !== sha || pr.head.repo?.full_name !== repository) continue;
		const revision = pr.head.sha;
		const { data } = await github.rest.actions.listWorkflowRuns({
			...scope, workflow_id: "ci.yml", event: "pull_request",
			head_sha: revision, status: "success", per_page: 100,
		});
		for (const run of data.workflow_runs) {
			if (run.conclusion !== "success" || run.event !== "pull_request" ||
				run.head_sha !== revision || run.head_repository?.full_name !== repository ||
				run.path !== ".github/workflows/ci.yml") continue;
			const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, { ...scope, run_id: run.id, per_page: 100 });
			const evidenceArtifact = artifacts.find(a => a.name === "ci-tested-source" && !a.expired);
			const browser = artifacts.find(a => a.name === `lix-browser-sdk-${revision}` && !a.expired);
			if (!evidenceArtifact || !browser) continue;
			const evidence = await readEvidence(evidenceArtifact);
			if (matchesTestedSource(evidence, { revision, tree })) return { runId: run.id, revision };
		}
	}
	return null;
}

export async function selectMergeReuse({ github, context, core }) {
	core.setOutput("reuse", "false");
	if (context.eventName !== "push") return;
	try {
		const tree = execFileSync("git", ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim();
		const result = await findReusableRun({
			github, repository: `${context.repo.owner}/${context.repo.repo}`, sha: context.sha, tree,
			readEvidence: async artifact => {
				if (artifact.size_in_bytes > 65536) throw new Error("Oversized CI provenance");
				const response = await github.rest.actions.downloadArtifact({ ...context.repo, artifact_id: artifact.id, archive_format: "zip" });
				const download = await fetch(response.url);
				if (!download.ok) throw new Error(`Provenance download: ${download.status}`);
				const zip = Buffer.from(await download.arrayBuffer());
				if (artifact.digest && artifact.digest !== `sha256:${createHash("sha256").update(zip).digest("hex")}`) throw new Error("CI provenance digest mismatch");
				const directory = mkdtempSync(join(tmpdir(), "ci-provenance-"));
				try {
					const archive = join(directory, "source.zip");
					writeFileSync(archive, zip);
					return JSON.parse(execFileSync("unzip", ["-p", archive, "tested-source.json"], { encoding: "utf8", maxBuffer: 65536 }));
				} finally { rmSync(directory, { recursive: true, force: true }); }
			},
		});
		if (!result) { core.info("No successful PR run proves this exact tree; running full CI."); return; }
		await core.summary.addRaw(`Reusing successful PR CI run ${result.runId}: both tested source trees match ${tree}. SDK artifacts are promoted to ${context.sha}; compilation and tests are not repeated.`).write();
		core.setOutput("reuse", "true");
		core.setOutput("run_id", result.runId);
		core.setOutput("revision", result.revision);
	} catch (error) {
		// Lookup failures must never suppress validation.
		core.warning(`Cannot prove prior CI coverage; running full CI: ${error.message}`);
	}
}
