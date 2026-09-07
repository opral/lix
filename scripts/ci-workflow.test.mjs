import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const workflow = readFileSync(
	resolve(repositoryRoot, ".github/workflows/ci.yml"),
	"utf8",
);
const releasePrWorkflow = readFileSync(
	resolve(repositoryRoot, ".github/workflows/release-pr.yml"),
	"utf8",
);
const publishWorkflow = readFileSync(
	resolve(repositoryRoot, ".github/workflows/publish-packages.yml"),
	"utf8",
);

test("draft reruns cannot cancel ready-candidate CI", () => {
	assert.match(
		workflow,
		/group: \$\{\{ github\.workflow \}\}-\$\{\{ github\.event\.pull_request\.number \|\| github\.ref \}\}/,
	);
	assert.match(workflow, /cancel-in-progress: true/);
	assert.match(workflow, /github\.event\.pull_request\.draft && 'draft' \|\| 'ready'/);
});

test("Rust test scopes run independently with workspace-specific caches", () => {
	for (const [name, task, workspace, runner] of [
		["Clippy", "clippy", ".", "blacksmith-32vcpu-ubuntu-2404"],
		["Test", "test", ".", "blacksmith-16vcpu-ubuntu-2404"],
		["Tooling Test", "tooling", "tooling", "blacksmith-16vcpu-ubuntu-2404"],
		["E2E Test", "e2e", "tooling", "blacksmith-16vcpu-ubuntu-2404"],
	]) {
		assert.match(
			workflow,
			new RegExp(
				`- name: ${name}\\n\\s+task: ${task}\\n\\s+workspace: ${workspace === "." ? "\\." : workspace}[\\s\\S]*?blacksmith_runner: ${runner}`,
			),
		);
	}
	assert.match(
		workflow,
		/workspaces: \$\{\{ matrix\.cache_workspaces \|\| matrix\.workspace \}\}/,
	);
	assert.match(workflow, /name: rust-nextest-junit-\$\{\{ matrix\.task \}\}/);
	assert.match(workflow, /name: rust-cargo-timings-\$\{\{ matrix\.task \}\}/);
	assert.match(
		workflow,
		/name: Cargo \$\{\{ matrix\.name \}\}[\s\S]*?runs-on: \$\{\{ inputs\.runner_provider == 'blacksmith' && matrix\.blacksmith_runner \|\| matrix\.default_runner \|\| 'ubuntu-24\.04' \}\}/,
	);
	assert.match(
		workflow,
		/- name: Test\n\s+task: test[\s\S]*?default_runner: blacksmith-8vcpu-ubuntu-2404\n\s+blacksmith_runner: blacksmith-16vcpu-ubuntu-2404/,
	);
});

test("Rust scope gates only Rust jobs and keeps both SDK integration suites", () => {
	const cargo = workflow
		.split("\n  cargo:\n")[1]
		.split("\n  js-sdk-test:\n")[0];
	assert.match(cargo, /needs: changelog/);
	assert.match(cargo, /if: needs\.changelog\.outputs\.rust != 'false'/);
	const sdk = workflow
		.split("\n  js-sdk-test:\n")[1]
		.split("\n  preview-artifact-changes:\n")[0];
	assert.match(sdk, /needs: merge-reuse/);
	assert.doesNotMatch(sdk, /outputs\.rust/);
	assert.match(workflow, /fetch-depth: 2/);
	assert.match(workflow, /rust: \$\{\{ steps\.scope\.outputs\.rust \}\}/);
	assert.match(workflow, /run: node scripts\/ci-rust-scope\.mjs/);
});

test("Clippy caches both workspaces and SDK build modes have separate main-seeded caches", () => {
	assert.match(
		workflow,
		/cache_workspaces: \|\n\s+\. -> target\n\s+tooling -> target/,
	);
	assert.match(
		workflow,
		/shared-key: ci-js-\$\{\{ matrix\.runtime \}\}\n\s+save-if: \$\{\{ github\.ref == 'refs\/heads\/main' \}\}/,
	);
});

test("Cargo output directories match the workspace caches and timing uploads", () => {
	assert.match(
		workflow,
		/CARGO_TARGET_DIR: \$\{\{ github\.workspace \}\}\/\$\{\{ matrix\.workspace \}\}\/target/,
	);
	assert.match(
		workflow,
		/export CARGO_TARGET_DIR="\$GITHUB_WORKSPACE\/tooling\/target"\n\s+cargo clippy/,
	);
	assert.match(
		workflow,
		/path: \$\{\{ matrix\.workspace \}\}\/target\/cargo-timings\/\*\.html/,
	);
});

test("short support jobs use free standard runners for the public repository", () => {
	assert.match(workflow, /name: Changelog[\s\S]*?runs-on: ubuntu-24\.04/);
	for (const [name, runner] of [
		["Linux x64", "ubuntu-24.04"],
		["macOS arm64", "macos-15"],
		["Windows x64", "windows-2025"],
	]) {
		assert.match(
			workflow,
			new RegExp(
				`- name: ${name}\\n\\s+runner: ${runner.replaceAll(".", "\\.")}`,
			),
		);
	}
});

test("SDK CI defaults to free GitHub runners with an explicit Blacksmith fallback", () => {
	assert.match(
		workflow,
		/- name: Native\n\s+runtime: native\n\s+blacksmith_runner: blacksmith-16vcpu-ubuntu-2404/,
	);
	assert.match(
		workflow,
		/- name: Browser\n\s+runtime: browser\n\s+blacksmith_runner: blacksmith-32vcpu-ubuntu-2404/,
	);
	assert.match(
		workflow,
		/name: JS SDK \$\{\{ matrix\.name \}\} Test[\s\S]*?runs-on: \$\{\{ inputs\.runner_provider == 'blacksmith' && matrix\.blacksmith_runner \|\| 'ubuntu-24\.04' \}\}/,
	);
});

test("green SDK jobs retain exact-revision artifacts for submodule consumers", () => {
	assert.match(
		workflow,
		/LIX_SOURCE_SHA: \$\{\{ github\.event\.pull_request\.head\.sha \|\| github\.sha \}\}/,
	);
	assert.match(workflow, /ref: \$\{\{ env\.LIX_SOURCE_SHA \}\}/);
	assert.match(
		workflow,
		/name: lix-browser-sdk-\$\{\{ env\.LIX_SOURCE_SHA \}\}/,
	);
	assert.match(workflow, /retention-days: 90/);
	assert.doesNotMatch(workflow, /name: lix-native-sdk-/);
	assert.doesNotMatch(workflow, /CARGO_PROFILE_RELEASE_CODEGEN_UNITS: "16"/);
});

test("browser artifacts publish after functional tests without OPFS benchmarks", () => {
	const sdk = workflow
		.split("\n  js-sdk-test:\n")[1]
		.split("\n  preview-artifact-changes:\n")[0];
	const upload = sdk
		.split("name: Upload tested browser SDK for submodule consumers\n")[1]
		.split("\n      - name:")[0];
	assert.match(upload, /if: matrix\.runtime == 'browser'/);
	assert.match(upload, /uses: actions\/upload-artifact@v4/);
	assert.match(upload, /name: lix-browser-sdk-\$\{\{ env\.LIX_SOURCE_SHA \}\}/);
	for (const path of [
		"packages/js-sdk/dist",
		"packages/storage-opfs/dist",
		"ci-artifact/browser.json",
	]) {
		assert.ok(upload.includes(path));
	}
	assert.match(upload, /retention-days: 90/);
	assert.ok(
		sdk.indexOf("name: Upload tested browser SDK for submodule consumers") >
			sdk.indexOf("name: Run OPFS storage browser tests"),
	);
	for (const source of [workflow, publishWorkflow]) {
		assert.doesNotMatch(
			source,
			/opfs-benchmarks:|OPFS performance budgets|npm run benchmark/,
		);
		assert.doesNotMatch(source, /lix-browser-sdk-build-/);
	}
});

test("server-changing pull requests retain one reusable preview image", () => {
	assert.match(workflow, /preview-artifact-changes:/);
	assert.match(workflow, /preview-server-image:/);
	assert.match(workflow, /file: packages\/server\/Dockerfile/);
	assert.match(
		workflow,
		/name: lix-server-image-linux-x64-\$\{\{ env\.LIX_SOURCE_SHA \}\}/,
	);
	assert.match(workflow, /retention-days: 14/);
	assert.match(publishWorkflow, /node scripts\/ci-server-image\.mjs/);
});

test("release PR updates validate metadata and defer full CI until ready for review", () => {
	assert.match(releasePrWorkflow, /draft: true/);
	assert.doesNotMatch(releasePrWorkflow, /draft: always-true/);
	assert.match(releasePrWorkflow, /steps\.freeze\.outputs\.frozen != 'true'/);
	assert.match(releasePrWorkflow, /steps\.recheck\.outputs\.frozen == 'false'/);
	assert.match(releasePrWorkflow, /select\(\.isDraft == true/);
	assert.match(
		releasePrWorkflow,
		/node scripts\/validate-publish-surface\.mjs/,
	);
	assert.match(releasePrWorkflow, /node --test scripts\/release\.test\.mjs/);
	assert.doesNotMatch(releasePrWorkflow, /gh workflow run|actions: write/);
	assert.match(workflow, /ready_for_review/);
});

test("the readiness gate aggregates real results and draft runs have a distinct check name", () => {
	const gate = workflow.split("\n  release-ready:\n")[1].split("\n  merge-reuse:\n")[0];
	assert.match(gate, /always\(\)/);
	assert.match(gate, /'Draft - full CI deferred' \|\| 'Release ready'/);
	for (const job of ["merge-reuse", "promote-browser-sdk", "changelog", "cargo-config", "cargo", "js-sdk-test", "preview-artifact-changes", "preview-server-image"]) {
		assert.ok(gate.split("    needs: ")[1].split("\n")[0].includes(job));
	}
	assert.match(gate, /github\.rest\.pulls\.get/);
	assert.match(gate, /assertReleaseReady/);
	const metadata = readFileSync(resolve(repositoryRoot, ".github/workflows/release-metadata.yml"), "utf8");
	assert.match(metadata, /group: release-metadata-/);
	assert.match(metadata, /converted_to_draft/);
	assert.doesNotMatch(metadata, /cargo |build:wasm|build:native/);
});

test("SDK binary reuse never skips TypeScript builds or integration tests", () => {
	assert.match(workflow, /uses: actions\/cache\/restore@v4/);
	assert.match(workflow, /uses: actions\/cache\/save@v4/);
	assert.match(workflow, /node scripts\/ci-sdk-cache\.mjs check/);
	assert.match(workflow, /fi\n\s+npm --prefix packages\/js-sdk run build:ts/);
	assert.ok(
		workflow.indexOf("name: Cache successfully tested SDK binaries") >
			workflow.indexOf("name: Run OPFS storage browser tests"),
	);
	for (const name of [
		"Run native JS SDK tests",
		"Run browser JS SDK tests",
		"Run OPFS storage browser tests",
	]) {
		const step = workflow
			.split(`name: ${name}\n`)[1]
			.split("\n      - name:")[0];
		assert.doesNotMatch(step, /binary-cache|reuse/);
	}
});

test("server publishing and SDK package tests use free GitHub runners", () => {
	assert.match(
		publishWorkflow,
		/name: Publish Lix reference server\n\s+runs-on: ubuntu-24\.04/,
	);
	assert.match(
		publishWorkflow,
		/id: publish\n\s+if: needs\.release-version\.outputs\.server_digest == ''/,
	);
	assert.match(publishWorkflow, /imagetools create --prefer-index=false/);
	assert.match(
		publishWorkflow,
		/name: Test @lix-js\/sdk\n\s+runs-on: ubuntu-24\.04/,
	);
});

test("nextest compiles test targets without building unused examples", () => {
	const commands = workflow.match(/^\s*run: cargo nextest run .+$/gm) ?? [];
	assert.equal(commands.length, 3);
	for (const command of commands) {
		assert.match(command, /--tests/);
		if (command.includes("-p lix_e2e")) {
			assert.doesNotMatch(command, /--lib\b/);
		} else {
			assert.match(command, /--lib --tests/);
		}
		assert.doesNotMatch(command, /--examples|--all-targets/);
	}
});

test("tooling Clippy excludes the benchmark-only DuckDB feature", () => {
	assert.match(
		workflow,
		/cargo clippy .*--manifest-path tooling\/Cargo\.toml .*--workspace --exclude lix_e2e --all-targets --all-features/,
	);
	const e2eClippy = workflow
		.split("\n")
		.find(
			(line) => line.includes("cargo clippy") && line.includes("-p lix_e2e"),
		);
	assert.ok(e2eClippy);
	assert.match(e2eClippy, /--all-targets --features /);
	assert.doesNotMatch(e2eClippy, /\btpch\b|--all-features/);
});

test("tested merges skip validation but retain SDK artifacts for the landed revision", () => {
	for (const job of ["changelog", "cargo-config", "js-sdk-test"]) {
		const definition = workflow.split(`\n  ${job}:\n`)[1].split("\n    steps:")[0];
		assert.match(definition, /needs: merge-reuse/);
		assert.match(definition, /needs\.merge-reuse\.outputs\.reuse != 'true'/);
	}
	assert.match(workflow, /name: ci-tested-source/);
	assert.match(workflow, /sourceTree: tree\(process\.env\.SOURCE_REVISION\)/);
	assert.match(workflow, /testedTree: tree\('HEAD'\)/);
	const promotion = workflow.split("\n  promote-browser-sdk:\n")[1].split("\n  changelog:\n")[0];
	assert.match(promotion, /if: needs\.merge-reuse\.outputs\.reuse == 'true'/);
	assert.match(promotion, /run-id: \$\{\{ needs\.merge-reuse\.outputs\.run_id \}\}/);
	assert.match(promotion, /name: lix-browser-sdk-\$\{\{ github\.sha \}\}/);
	assert.doesNotMatch(promotion, /\bcargo\b|\bnpm\b/);
});
