import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { existingDigest, serverInputHash } from "./ci-server-image.mjs";

test("server tags are stable across CI/SDK-only commits and change with server source", (t) => {
	const root = mkdtempSync(join(tmpdir(), "lix-server-tag-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	const git = (...args) => execFileSync("git", args, { cwd: root });
	const commit = (path, content) => {
		mkdirSync(dirname(join(root, path)), { recursive: true });
		writeFileSync(join(root, path), content);
		git("add", ".");
		git(
			"-c",
			"user.name=CI test",
			"-c",
			"user.email=ci@example.test",
			"commit",
			"--quiet",
			"-m",
			"fixture",
		);
	};
	git("init", "--quiet");
	commit("Cargo.toml", "workspace");
	commit("packages/server/src/main.rs", "server");
	const hash = serverInputHash(root);
	commit(".github/workflows/ci.yml", "workflow");
	commit("packages/js-sdk/src/api.ts", "typescript");
	assert.equal(serverInputHash(root), hash);
	commit("packages/server/src/main.rs", "updated server");
	assert.notEqual(serverInputHash(root), hash);
	const sourceHash = serverInputHash(root);
	commit(".dockerignore", "build context exclusions");
	assert.notEqual(serverInputHash(root), sourceHash);
});

test("image lookup distinguishes a cache miss from registry failures", () => {
	const digest = `sha256:${"a".repeat(64)}`;
	assert.equal(
		existingDigest("image", () => ({
			status: 0,
			stdout: JSON.stringify(digest),
		})),
		digest,
	);
	assert.equal(
		existingDigest("image", () => ({ status: 1, stderr: "manifest unknown" })),
		"",
	);
	assert.throws(
		() =>
			existingDigest("image", () => ({ status: 1, stderr: "unauthorized" })),
		/Cannot inspect/,
	);
	assert.throws(
		() =>
			existingDigest("image", () => ({
				status: 1,
				stderr: "connection timed out",
			})),
		/Cannot inspect/,
	);
	assert.throws(
		() => existingDigest("image", () => ({ status: 0, stdout: '"invalid"' })),
		/Invalid image digest/,
	);
});
