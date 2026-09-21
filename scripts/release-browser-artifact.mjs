import { execFileSync } from "node:child_process";
import { appendFileSync, closeSync, cpSync, openSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { binaryManifest, cacheKey, restoreBinaries, saveBinaries } from "./ci-sdk-cache.mjs";
import { findReusableRun, readArtifactJson } from "./ci-merge-reuse.mjs";

export function downloadVerifiedArchive(repository, runId, name, staged, run = execFileSync) {
    const metadata = JSON.parse(run("gh", ["api", `repos/${repository}/actions/runs/${runId}/artifacts?per_page=100`], { encoding: "utf8", timeout: 30_000 }));
    const artifact = metadata.artifacts.find(item => item.name === name && !item.expired);
    if (!artifact || !/^sha256:[a-f0-9]{64}$/.test(artifact.digest)) throw new Error("Missing artifact or archive digest");
    const archive = join(staged, "download.zip");
    const descriptor = openSync(archive, "w");
    try {
        run("gh", ["api", `repos/${repository}/actions/artifacts/${artifact.id}/zip`], { stdio: ["ignore", descriptor, "inherit"], timeout: 180_000 });
    } finally { closeSync(descriptor); }
    const digest = `sha256:${createHash("sha256").update(readFileSync(archive)).digest("hex")}`;
    if (digest !== artifact.digest) throw new Error("Artifact archive checksum mismatch");
    run("unzip", ["-q", archive, "-d", staged], { stdio: "inherit", timeout: 60_000 });
    rmSync(archive);
}

// Stage each attempt separately: a downloader may exit successfully without
// materializing the complete archive. Never promote such a partial download.
export function downloadMergedBrowser(root, revision, runId, repository, download = downloadVerifiedArchive) {
    if (!/^[a-f0-9]{40}$/.test(revision) || !/^\d+$/.test(String(runId))) {
        throw new Error("Invalid browser artifact source");
    }
    for (let attempt = 1; attempt <= 3; attempt++) {
        const staged = mkdtempSync(join(tmpdir(), "lix-browser-download-"));
        try {
            download(repository, runId, `lix-browser-sdk-${revision}`, staged);
            const manifest = JSON.parse(readFileSync(join(staged, "ci-artifact/browser.json"), "utf8"));
            if (manifest.schemaVersion !== 1 || manifest.kind !== "lix-browser-sdk" ||
                manifest.sourceRevision !== revision || manifest.target !== "wasm32-unknown-unknown") {
                throw new Error("Unexpected source artifact provenance");
            }
            const binaries = binaryManifest(join(staged, "packages/js-sdk"), "browser", manifest.releaseBuild?.binaries?.key);
            if (JSON.stringify(binaries) !== JSON.stringify(manifest.releaseBuild?.binaries)) {
                throw new Error("Browser artifact checksum mismatch");
            }
            for (const path of ["packages/js-sdk/dist/index.js", "packages/storage-opfs/dist/index.js"]) {
                readFileSync(join(staged, path));
            }
            for (const path of ["packages/js-sdk/dist", "packages/storage-opfs/dist", "ci-artifact"]) {
                mkdirSync(join(root, path), { recursive: true });
                cpSync(join(staged, path), join(root, path), { recursive: true });
            }
            return;
        } catch (error) {
            if (attempt === 3) throw new Error("Browser artifact download failed verification after 3 attempts", { cause: error });
            console.log(`Browser artifact attempt ${attempt} incomplete or invalid; retrying.`);
        } finally {
            rmSync(staged, { recursive: true, force: true });
        }
    }
}

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

// The exact-tree merge path skips SDK jobs, so their PR-scoped Actions cache
// never becomes visible to subsequent PRs. Seed main from the verified artifact.
export function prepareMergedBrowserCache(root, revision, env = process.env) {
	const manifest = JSON.parse(readFileSync(join(root, "ci-artifact/browser.json"), "utf8"));
	const key = cacheKey(root, "browser", env);
	if (!matchesBrowserBuild(manifest, { revision, tree: sourceTree(root), key })) {
		throw new Error("Merged browser artifact does not match checkout/build settings");
	}
	const sdk = join(root, "packages/js-sdk");
	if (JSON.stringify(binaryManifest(sdk, "browser", key)) !== JSON.stringify(manifest.releaseBuild.binaries)) {
		throw new Error("Merged browser artifact checksum mismatch");
	}
	saveBinaries(sdk, join(root, ".ci-sdk-cache/browser"), "browser", key);
	return key;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	const [command, downloaded, revision] = process.argv.slice(2);
	if (command === "download-merged") {
        downloadMergedBrowser(process.cwd(), process.env.SOURCE_REVISION, process.env.SOURCE_RUN, process.env.GITHUB_REPOSITORY);
    } else if (command === "describe") {
		mkdirSync("ci-artifact", { recursive: true });
		writeFileSync("ci-artifact/browser.json", JSON.stringify(describeBrowser(process.cwd(), process.env.LIX_SOURCE_SHA)));
	} else if (command === "prepare-merged-cache") {
		// Cache warming is optional; an old or incompatible artifact must not
		// invalidate otherwise valid merge evidence or enter the shared cache.
		try {
			const key = prepareMergedBrowserCache(process.cwd(), process.env.SOURCE_REVISION);
			appendFileSync(process.env.GITHUB_OUTPUT, `key=${key}\n`);
		} catch (error) {
			console.log(`Not seeding browser cache: ${error.message}`);
		}
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
