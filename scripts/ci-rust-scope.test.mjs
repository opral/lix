import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { isSdkOnlyChange, selectRustScope } from "./ci-rust-scope.mjs";

test("SDK TypeScript and changenotes do not require Rust-only suites", () => {
	assert.equal(
		isSdkOnlyChange([
			"packages/js-sdk/src/binding.browser.ts",
			"packages/js-sdk/src/remote/client-initialization.test.ts",
			".changenotes/share-sdk-wasm-initialization.md",
		]),
		true,
	);
});

test("mixed, unknown, generated, and Rust inputs always require Rust CI", () => {
	for (const path of [
		"packages/lix/src/lib.rs",
		"packages/js-sdk/src/lib.rs",
		"packages/js-sdk/src/wasm/lix_js_sdk.d.ts",
		"packages/js-sdk/build.rs",
		"packages/js-sdk/package.json",
		"packages/js-sdk/scripts/build-wasm.js",
		"packages/e2e/tests/fixtures/example.json",
		"plugins/markdown/schema/markdown_node.json",
		"Cargo.lock",
		"tooling/Cargo.toml",
		"rust-toolchain.toml",
		".cargo/config.toml",
		".github/workflows/ci.yml",
		"scripts/ci-rust-scope.mjs",
		"new-package/source.ts",
		"packages/js-sdk/src/file.ts\nCargo.toml",
	]) {
		assert.equal(
			isSdkOnlyChange(["packages/js-sdk/src/index.ts", path]),
			false,
			path,
		);
	}
	assert.equal(isSdkOnlyChange([]), false);
	assert.equal(isSdkOnlyChange([".changenotes/release.md"]), false);
});

test("pushes, manual runs and unavailable history keep full CI", () => {
	for (const eventName of ["push", "workflow_dispatch", undefined]) {
		assert.equal(selectRustScope({ eventName }), true);
	}
	assert.equal(
		selectRustScope({ eventName: "pull_request", cwd: "/nonexistent-ci-repo" }),
		true,
	);
});

test("classify the tested merge, preserve rename deletions, and fail open on shallow history", (t) => {
	const root = mkdtempSync(join(tmpdir(), "lix-ci-scope-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	const cwd = join(root, "repo");
	mkdirSync(cwd);
	const git = (...args) =>
		execFileSync("git", args, {
			cwd,
			encoding: "utf8",
			stdio: ["ignore", "pipe", "pipe"],
		}).trim();
	const write = (path, contents) => {
		mkdirSync(dirname(join(cwd, path)), { recursive: true });
		writeFileSync(join(cwd, path), contents);
	};
	const commit = () => {
		git("add", ".");
		git("commit", "-qm", "fixture");
		return git("rev-parse", "HEAD");
	};
	git("init", "-b", "main");
	git("config", "user.name", "CI test");
	git("config", "user.email", "ci@example.invalid");
	write("packages/lix/src/lib.rs", "pub fn initial() {}\n");
	write("packages/js-sdk/src/index.ts", "export const initial = true;\n");
	const originalBase = commit();
	git("checkout", "-qb", "sdk");
	write("packages/js-sdk/src/index.ts", "export const changed = true;\n");
	const prHead = commit();
	const event = {
		pull_request: { base: { sha: originalBase }, head: { sha: prHead } },
	};
	// A direct head checkout is insufficient evidence to skip.
	assert.equal(
		selectRustScope({ eventName: "pull_request", event, cwd }),
		true,
	);
	git("checkout", "main");
	write("packages/lix/src/lib.rs", "pub fn updated_base() {}\n");
	commit();
	git("merge", "--no-ff", "sdk", "-m", "tested merge");
	assert.equal(
		selectRustScope({ eventName: "pull_request", event, cwd }),
		false,
	);
	assert.equal(
		selectRustScope({ eventName: "pull_request", event: {}, cwd }),
		true,
	);
	const shallow = join(root, "shallow");
	git("clone", "--depth=1", `file://${cwd}`, shallow);
	assert.equal(
		selectRustScope({ eventName: "pull_request", event, cwd: shallow }),
		true,
	);
	const twoParents = join(root, "two-parents");
	git("clone", "--depth=2", `file://${cwd}`, twoParents);
	assert.equal(
		selectRustScope({ eventName: "pull_request", event, cwd: twoParents }),
		false,
	);
	// Renaming an existing Rust file to .ts must not hide the Rust deletion.
	git("checkout", "-qb", "rename");
	git("mv", "packages/lix/src/lib.rs", "packages/js-sdk/src/renamed.ts");
	const renameHead = commit();
	git("checkout", "main");
	git("merge", "--no-ff", "rename", "-m", "rename merge");
	assert.equal(
		selectRustScope({
			eventName: "pull_request",
			event: { pull_request: { head: { sha: renameHead } } },
			cwd,
		}),
		true,
	);
});
