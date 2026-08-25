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

test("superseded CI runs are cancelled per pull request or branch", () => {
	assert.match(
		workflow,
		/group: \$\{\{ github\.workflow \}\}-\$\{\{ github\.event\.pull_request\.number \|\| github\.ref \}\}/,
	);
	assert.match(workflow, /cancel-in-progress: true/);
});

test("Rust test scopes run independently with workspace-specific caches", () => {
	for (const [name, task, workspace] of [
		["Test", "test", "."],
		["Tooling Test", "tooling", "tooling"],
		["E2E Test", "e2e", "tooling"],
	]) {
		assert.match(
			workflow,
			new RegExp(
				`- name: ${name}\\n\\s+task: ${task}\\n\\s+workspace: ${workspace === "." ? "\\." : workspace}`,
			),
		);
	}
	assert.match(workflow, /workspaces: \$\{\{ matrix\.workspace \}\}/);
	assert.match(workflow, /name: rust-nextest-junit-\$\{\{ matrix\.task \}\}/);
	assert.match(workflow, /name: rust-cargo-timings-\$\{\{ matrix\.task \}\}/);
});

test("nextest compiles test targets without building unused examples", () => {
	const commands = workflow.match(/^\s*run: cargo nextest run .+$/gm) ?? [];
	assert.equal(commands.length, 3);
	for (const command of commands) {
		assert.match(command, /--lib --tests/);
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
