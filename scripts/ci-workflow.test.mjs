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

test("superseded CI runs are cancelled per pull request or branch", () => {
	assert.match(
		workflow,
		/group: \$\{\{ github\.workflow \}\}-\$\{\{ github\.event\.pull_request\.number \|\| github\.ref \}\}/,
	);
	assert.match(workflow, /cancel-in-progress: true/);
});

test("Rust test scopes run independently with workspace-specific caches", () => {
	for (const [name, task, workspace, runner] of [
		["Clippy", "clippy", ".", "blacksmith-32vcpu-ubuntu-2404"],
		["Test", "test", ".", "blacksmith-32vcpu-ubuntu-2404"],
		["Tooling Test", "tooling", "tooling", "blacksmith-16vcpu-ubuntu-2404"],
		["E2E Test", "e2e", "tooling", "blacksmith-16vcpu-ubuntu-2404"],
	]) {
		assert.match(
			workflow,
			new RegExp(
				`- name: ${name}\\n\\s+task: ${task}\\n\\s+workspace: ${workspace === "." ? "\\." : workspace}[\\s\\S]*?runner: ${runner}`,
			),
		);
	}
	assert.match(workflow, /workspaces: \$\{\{ matrix\.workspace \}\}/);
	assert.match(workflow, /name: rust-nextest-junit-\$\{\{ matrix\.task \}\}/);
	assert.match(workflow, /name: rust-cargo-timings-\$\{\{ matrix\.task \}\}/);
	assert.match(workflow, /name: Cargo \$\{\{ matrix\.name \}\}[\s\S]*?runs-on: \$\{\{ matrix\.runner \}\}/);
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
			new RegExp(`- name: ${name}\\n\\s+runner: ${runner.replaceAll(".", "\\.")}`),
		);
	}
});

test("JS SDK native CI is right-sized without changing browser architecture coverage", () => {
	assert.match(
		workflow,
		/- name: Native\n\s+runtime: native\n\s+runner: blacksmith-16vcpu-ubuntu-2404/,
	);
	assert.match(
		workflow,
		/- name: Browser\n\s+runtime: browser\n\s+runner: blacksmith-32vcpu-ubuntu-2404/,
	);
	assert.match(workflow, /name: JS SDK \$\{\{ matrix\.name \}\} Test[\s\S]*?runs-on: \$\{\{ matrix\.runner \}\}/);
});

test("green SDK jobs retain exact-revision artifacts for submodule consumers", () => {
	assert.match(
		workflow,
		/LIX_SOURCE_SHA: \$\{\{ github\.event\.pull_request\.head\.sha \|\| github\.sha \}\}/,
	);
	assert.match(workflow, /ref: \$\{\{ env\.LIX_SOURCE_SHA \}\}/);
	assert.match(workflow, /name: lix-browser-sdk-\$\{\{ env\.LIX_SOURCE_SHA \}\}/);
	assert.match(
		workflow,
		/name: lix-native-sdk-linux-x64-\$\{\{ env\.LIX_SOURCE_SHA \}\}/,
	);
	assert.match(workflow, /retention-days: 90/);
	assert.doesNotMatch(workflow, /CARGO_PROFILE_RELEASE_CODEGEN_UNITS: "16"/);
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
	assert.match(publishWorkflow, /lix-server:content-\$server_inputs/);
});

test("release PR automation reuses same-SHA pull request CI with a dispatch fallback", () => {
	assert.match(
		releasePrWorkflow,
		/RELEASE_SHA: \$\{\{ steps\.release_pr\.outputs\.pull-request-head-sha \}\}/,
	);
	assert.match(releasePrWorkflow, /--event pull_request/);
	assert.match(releasePrWorkflow, /--commit "\$RELEASE_SHA"/);
	for (const conclusion of ["action_required", "cancelled", "skipped", "stale"]) {
		assert.match(releasePrWorkflow, new RegExp(`\\. == "${conclusion}"`));
	}
	assert.match(releasePrWorkflow, /gh workflow run ci\.yml --ref "\$TARGET_BRANCH"/);
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
		.find((line) => line.includes("cargo clippy") && line.includes("-p lix_e2e"));
	assert.ok(e2eClippy);
	assert.match(e2eClippy, /--all-targets --features /);
	assert.doesNotMatch(e2eClippy, /\btpch\b|--all-features/);
});
