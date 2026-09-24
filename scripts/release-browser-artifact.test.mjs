import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { cacheKey, validCache } from "./ci-sdk-cache.mjs";
import { downloadVerifiedArchive, downloadMergedBrowser, describeBrowser, prepareMergedBrowserCache, matchesBrowserBuild, restoreReleaseBrowser, selectReleaseBrowser } from "./release-browser-artifact.mjs";

function fixture(t) {
	const root = mkdtempSync(join(tmpdir(), "release-browser-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	const write = (path, content) => {
		mkdirSync(dirname(join(root, path)), { recursive: true });
		writeFileSync(join(root, path), content);
	};
	const git = (...args) => execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
	git("init", "--quiet");
	write("Cargo.lock", "locked dependencies");
	write("rust-toolchain.toml", '[toolchain]\nchannel = "nightly-2026-05-21"\n');
	write("packages/js-sdk/src/index.ts", "export {};");
	git("add", ".");
	git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "fixture");
	const revision = git("rev-parse", "HEAD");
	const tree = git("rev-parse", "HEAD^{tree}");
	for (const path of [
		"wasm/lix_js_sdk.js", "wasm/lix_js_sdk.d.ts", "wasm/lix_js_sdk_bg.wasm",
		"migration-wasm/lix_js_sdk.js", "migration-wasm/lix_js_sdk.d.ts", "migration-wasm/lix_js_sdk_bg.wasm",
	]) write(`packages/js-sdk/dist/${path}`, path);
	const manifest = describeBrowser(root, revision, {});
	write("ci-artifact/browser.json", JSON.stringify(manifest));
	const downloaded = join(root, "download");
	mkdirSync(downloaded);
	cpSync(join(root, "packages"), join(downloaded, "packages"), { recursive: true });
	cpSync(join(root, "ci-artifact"), join(downloaded, "ci-artifact"), { recursive: true });
	return { root, write, git, revision, tree, manifest, downloaded };
}

test("same-tree merge restores both WASM variants, but never downloaded TypeScript", t => {
	const f = fixture(t);
	f.git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "--allow-empty", "-m", "merge");
	f.write("download/packages/js-sdk/dist/index.js", "untrusted TypeScript output");
	rmSync(join(f.root, "packages/js-sdk/dist"), { recursive: true });
	restoreReleaseBrowser(f.root, f.downloaded, f.revision, {});
	assert.equal(realpathSync(join(f.root, "packages/js-sdk/src/migration-wasm")), join(f.root, "packages/js-sdk/dist/migration-wasm"));
	assert.equal(readFileSync(join(f.root, "packages/js-sdk/dist/wasm/lix_js_sdk_bg.wasm"), "utf8"), "wasm/lix_js_sdk_bg.wasm");
	assert.throws(() => readFileSync(join(f.root, "packages/js-sdk/dist/index.js")), /ENOENT/);
});

for (const [name, mutate] of [
	["wrong revision", f => f.revision = "other"],
	["changed source tree", f => { f.write("new-source", "changed"); f.git("add", "new-source"); f.git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "changed"); }],
	["changed build script or toolchain", f => f.write("rust-toolchain.toml", "different toolchain")],
	["missing migration WASM", f => rmSync(join(f.downloaded, "packages/js-sdk/dist/migration-wasm/lix_js_sdk_bg.wasm"))],
	["corrupt binary", f => f.write("download/packages/js-sdk/dist/wasm/lix_js_sdk_bg.wasm", "corrupt")],
	["legacy provenance", f => { delete f.manifest.releaseBuild; f.write("download/ci-artifact/browser.json", JSON.stringify(f.manifest)); }],
	["symlink payload", f => { rmSync(join(f.downloaded, "packages/js-sdk/dist/wasm"), { recursive: true }); symlinkSync(join(f.root, "packages/js-sdk/dist/wasm"), join(f.downloaded, "packages/js-sdk/dist/wasm")); }],
]) {
	test(`${name} cannot be reused`, t => {
		const f = fixture(t);
		mutate(f);
		assert.throws(() => restoreReleaseBrowser(f.root, f.downloaded, f.revision, {}));
	});
}

test("browser fingerprints ignore native-only profile but include browser compiler settings", t => {
	const f = fixture(t);
	assert.equal(cacheKey(f.root, "browser", { LIX_NATIVE_PROFILE: "test" }), cacheKey(f.root, "browser", {}));
	for (const env of [
		{ CARGO_PROFILE_RELEASE_OPT_LEVEL: "3" }, { LIX_WASM_PROFILE: "dev" },
		{ RUSTUP_TOOLCHAIN: "stable" }, { CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS: "--cfg=other" },
	]) assert.throws(() => restoreReleaseBrowser(f.root, f.downloaded, f.revision, env), /provenance/);
});

test("release lookup requires successful same-repository CI, tested trees and matching browser settings", async t => {
	const f = fixture(t);
	const manifest = describeBrowser(f.root, f.revision);
	const artifacts = [{ name: "ci-tested-source" }, { name: `lix-browser-sdk-${f.revision}` }, { name: "ci-browser-build" }];
	const run = { id: 42, conclusion: "success", event: "pull_request", head_sha: f.revision, head_repository: { full_name: "opral/lix" }, path: ".github/workflows/ci.yml" };
	const pr = { merged_at: "date", merge_commit_sha: "merge", head: { sha: f.revision, repo: { full_name: "opral/lix" } } };
	const github = { rest: {
		repos: { listPullRequestsAssociatedWithCommit: async () => ({ data: [pr] }) },
		actions: { listWorkflowRuns: async () => ({ data: { workflow_runs: [run] } }), listWorkflowRunArtifacts() {} },
	}, paginate: async () => artifacts };
	const evidence = { schemaVersion: 1, sourceRevision: f.revision, sourceTree: f.tree, testedTree: f.tree };
	const select = async () => {
		const outputs = {};
		await selectReleaseBrowser({ github, root: f.root, sha: "merge", context: { repo: { owner: "opral", repo: "lix" } },
			core: { setOutput: (k, v) => outputs[k] = v, info() {}, warning() {} },
			readJson: async ({ filename }) => filename === "browser.json" ? manifest : evidence,
		});
		return outputs;
	};
	assert.deepEqual(await select(), { reuse: "true", run_id: 42, revision: f.revision });
	for (const [object, field, value] of [
		[run, "conclusion", "failure"], [evidence, "testedTree", "different"],
		[pr.head.repo, "full_name", "fork/lix"], [artifacts[2], "expired", true],
		[manifest.releaseBuild.binaries, "key", "different-settings"],
	]) {
		const old = object[field]; object[field] = value;
		assert.deepEqual(await select(), { reuse: "false" });
		object[field] = old;
	}
	github.paginate = async () => { throw new Error("API unavailable"); };
	assert.deepEqual(await select(), { reuse: "false" });
});

test("legacy manifest is not release evidence and missing download falls back to compilation", t => {
	const f = fixture(t);
	assert.equal(matchesBrowserBuild({ schemaVersion: 1 }, { revision: f.revision, tree: f.tree, key: "key" }), false);
	const output = join(f.root, "outputs");
	const result = spawnSync(process.execPath, [new URL("./release-browser-artifact.mjs", import.meta.url).pathname, "restore", join(f.root, "missing"), f.revision], {
		cwd: f.root, env: { ...process.env, GITHUB_OUTPUT: output }, encoding: "utf8",
	});
	assert.equal(result.status, 0, result.stderr);
	assert.equal(readFileSync(output, "utf8"), "reuse=false\n");
});


test("identical-tree promotion seeds validated binaries without compiling or copying TypeScript", t => {
  const f = fixture(t);
  f.git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "--allow-empty", "-m", "merge");
  f.write("packages/js-sdk/dist/index.js", "not cached");
  const key = prepareMergedBrowserCache(f.root, f.revision, {});
  assert.equal(validCache(join(f.root, ".ci-sdk-cache/browser"), "browser", key), true);
  assert.throws(() => readFileSync(join(f.root, ".ci-sdk-cache/browser/dist/index.js")), /ENOENT/);
  assert.throws(() => prepareMergedBrowserCache(f.root, "wrong-revision", {}), /does not match/);
  assert.throws(() => prepareMergedBrowserCache(f.root, f.revision, { LIX_WASM_PROFILE: "dev" }), /does not match/);
  f.write("packages/js-sdk/dist/migration-wasm/lix_js_sdk_bg.wasm", "corrupt");
  assert.throws(() => prepareMergedBrowserCache(f.root, f.revision, {}), /checksum/);
});

for (const failure of ["empty", "corrupt", "wrong-revision", "transport"]) {
    test(`merged download retries ${failure} without publishing partial contents`, t => {
        const f = fixture(t);
        f.write("download/packages/js-sdk/dist/index.js", "sdk");
        f.write("download/packages/storage-opfs/dist/index.js", "opfs");
        const destination = join(f.root, "promoted");
        let attempts = 0;
        downloadMergedBrowser(destination, f.revision, "123", "opral/lix", (_repository, _runId, _name, staged) => {
            attempts++;
            if (attempts === 1 && failure === "transport") throw new Error("network interrupted");
            if (attempts === 1 && failure === "empty") return;
            cpSync(f.downloaded, staged, { recursive: true });
            if (attempts === 1 && failure === "corrupt") writeFileSync(join(staged, "packages/js-sdk/dist/wasm/lix_js_sdk_bg.wasm"), "corrupt");
            if (attempts === 1 && failure === "wrong-revision") writeFileSync(join(staged, "ci-artifact/browser.json"), JSON.stringify({...f.manifest, sourceRevision: "0".repeat(40)}));
        });
        assert.equal(attempts, 2);
        assert.equal(readFileSync(join(destination, "packages/storage-opfs/dist/index.js"), "utf8"), "opfs");
    });
}

test("merged download fails closed after three incomplete attempts", t => {
    const f = fixture(t);
    let attempts = 0;
    assert.throws(() => downloadMergedBrowser(join(f.root, "promoted"), f.revision, "123", "opral/lix", () => { attempts++; }), /after 3 attempts/);
    assert.equal(attempts, 3);
    assert.throws(() => readFileSync(join(f.root, "promoted/ci-artifact/browser.json")), /ENOENT/);
});

test("archive integrity rejects an incomplete OPFS payload before extraction", t => {
    const f = fixture(t);
    const staged = join(f.root, "archive-stage");
    mkdirSync(staged);
    let extracted = false;
    assert.throws(() => downloadVerifiedArchive("opral/lix", "123", "browser", staged, (command, args, options) => {
        if (command === "python3") { extracted = true; return; }
        if (args[1].includes("?")) return JSON.stringify({artifacts:[{name:"browser",id:1,digest:`sha256:${"0".repeat(64)}`} ]});
        writeFileSync(options.stdio[1], "truncated zip missing OPFS modules");
    }), /archive checksum mismatch/);
    assert.equal(extracted, false);
});
