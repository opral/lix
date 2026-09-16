import { execFileSync } from "node:child_process";
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { binaryManifest, cacheKey, restoreBinaries } from "./ci-sdk-cache.mjs";
import { findReusableRun, readArtifactJson } from "./ci-merge-reuse.mjs";

function sourceTree(root) {
	return execFileSync("git", ["rev-parse", "HEAD^{tree}"], { cwd: root, encoding: "utf8" }).trim();
}

export function describeBrowser(root, revision, env = process.env) {
	const key = cacheKey(root, "browser", env);
	return {
		schemaVersion: 1,
		kind: "lix-browser-sdk",
		sourceRevision: revision,
		target: "wasm32-unknown-unknown",
		releaseBuild: {
			schemaVersion: 1,
			sourceTree: sourceTree(root),
			binaries: binaryManifest(join(root, "packages/js-sdk"), "browser", key),
		},
	};
}

export function matchesBrowserBuild(manifest, { revision, tree, key }) {
	return manifest?.schemaVersion === 1 && manifest.kind === "lix-browser-sdk" &&
		manifest.sourceRevision === revision && manifest.target === "wasm32-unknown-unknown" &&
		manifest.releaseBuild?.schemaVersion === 1 && manifest.releaseBuild.sourceTree === tree &&
		manifest.releaseBuild.binaries?.key === key;
}

export async function selectReleaseBrowser({ github, context, core, sha, root = process.cwd(), readJson = readArtifactJson }) {
	core.setOutput("reuse", "false");
	try {
		const tree = sourceTree(root);
		const key = cacheKey(root, "browser");
		const read = (artifact, filename) => readJson({ github, repo: context.repo, artifact, filename });
		const result = await findReusableRun({
			github, repository: `${context.repo.owner}/${context.repo.repo}`, sha, tree,
			readEvidence: artifact => read(artifact, "tested-source.json"),
			acceptBrowser: async ({ artifacts, revision }) => {
				const artifact = artifacts.find(a => a.name === "ci-browser-build" && !a.expired);
				return Boolean(artifact) && matchesBrowserBuild(await read(artifact, "browser.json"), { revision, tree, key });
			},
		});
		if (!result) { core.info("No tested browser artifact matches the release tree and build settings; build from source."); return; }
		core.setOutput("reuse", "true");
		core.setOutput("run_id", result.runId);
		core.setOutput("revision", result.revision);
		core.info(`Selected tested browser SDK from CI run ${result.runId}; verify payload before reuse.`);
	} catch (error) {
		core.warning(`Browser artifact lookup failed; build from source: ${error.message}`);
	}
}

export function restoreReleaseBrowser(root, downloaded, revision, env = process.env) {
	const manifest = JSON.parse(readFileSync(join(downloaded, "ci-artifact/browser.json"), "utf8"));
	const expected = { revision, tree: sourceTree(root), key: cacheKey(root, "browser", env) };
	if (!matchesBrowserBuild(manifest, expected)) throw new Error("Browser artifact provenance does not match release");
	const sdk = join(downloaded, "packages/js-sdk");
	// Restore only hashed binaries. Rebuild TypeScript and package metadata from
	// the release checkout, never execute scripts from a downloaded artifact.
	writeFileSync(join(sdk, "manifest.json"), JSON.stringify(manifest.releaseBuild.binaries));
	restoreBinaries(join(root, "packages/js-sdk"), sdk, "browser", expected.key);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	const [command, downloaded, revision] = process.argv.slice(2);
	if (command === "describe") {
		mkdirSync("ci-artifact", { recursive: true });
		writeFileSync("ci-artifact/browser.json", JSON.stringify(describeBrowser(process.cwd(), process.env.LIX_SOURCE_SHA)));
	} else if (command === "restore") {
		try {
			restoreReleaseBrowser(process.cwd(), downloaded, revision);
			appendFileSync(process.env.GITHUB_OUTPUT, "reuse=true\n");
		} catch (error) {
			console.log(`Browser artifact unavailable or invalid; build from source: ${error.message}`);
			appendFileSync(process.env.GITHUB_OUTPUT, "reuse=false\n");
		}
	} else throw new Error(`Unknown command: ${command}`);
}
