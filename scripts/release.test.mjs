import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import assert from "node:assert/strict";

import {
	bumpVersion,
	changelogEntry,
	loadChanges,
	updateCargoToml,
	updateChangelog,
	updatePackageVersion,
} from "./release.mjs";

test("bumpVersion applies semver changes", () => {
	assert.equal(bumpVersion("0.6.0", "patch"), "0.6.1");
	assert.equal(bumpVersion("0.6.0", "minor"), "0.7.0");
	assert.equal(bumpVersion("0.6.0", "major"), "1.0.0");
});

test("loadChanges validates and parses fragments", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, ".changenotes"));
	writeFileSync(
		join(root, ".changenotes", "native-bindings.md"),
		`---\ntype: patch\n---\n\nFixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)\n`,
	);
	assert.deepEqual(loadChanges(root), [
		{
			path: ".changenotes/native-bindings.md",
			type: "patch",
			body: "Fixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)",
			summary: "Fixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)",
			details: [],
		},
	]);
});

test("loadChanges rejects major releases", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, ".changenotes"));
	writeFileSync(
		join(root, ".changenotes", "breaking-change.md"),
		`---\ntype: major\n---\n\nChanged a user-facing API.\n`,
	);
	assert.throws(() => loadChanges(root), /type must be one of minor, patch/);
});

test("loadChanges preserves changelog summary and explainer paragraphs", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, ".changenotes"));
	writeFileSync(
		join(root, ".changenotes", "sqlite-reads.md"),
		`---\ntype: patch\n---\n\nImproved SQLite storage read performance.\n\nThe storage now avoids loading values for key-only reads.\nWrapped lines stay in the same paragraph.\n`,
	);
	assert.deepEqual(loadChanges(root), [
		{
			path: ".changenotes/sqlite-reads.md",
			type: "patch",
			body: "Improved SQLite storage read performance.\n\nThe storage now avoids loading values for key-only reads. Wrapped lines stay in the same paragraph.",
			summary: "Improved SQLite storage read performance.",
			details: ["The storage now avoids loading values for key-only reads. Wrapped lines stay in the same paragraph."],
		},
	]);
});

test("loadChanges preserves fenced code blocks", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, ".changenotes"));
	writeFileSync(
		join(root, ".changenotes", "file-api.md"),
		`---\ntype: patch\n---\n\nAdded a typed file API:\n\n\`\`\`js\nawait lix.fs.writeFile("/orders.xlsx", bytes);\nconst bytes = await lix.fs.readFile("/orders.xlsx");\n\`\`\`\n`,
	);
	assert.deepEqual(loadChanges(root), [
		{
			path: ".changenotes/file-api.md",
			type: "patch",
			body: 'Added a typed file API:\n\n```js\nawait lix.fs.writeFile("/orders.xlsx", bytes);\nconst bytes = await lix.fs.readFile("/orders.xlsx");\n```',
			summary: "Added a typed file API:",
			details: [
				'```js\nawait lix.fs.writeFile("/orders.xlsx", bytes);\nconst bytes = await lix.fs.readFile("/orders.xlsx");\n```',
			],
		},
	]);
});

test("changelogEntry groups entries by type", () => {
	assert.equal(
		changelogEntry("0.7.0", "2026-05-29", [
			{ type: "minor", body: "Added branch merge preview support." },
			{
				type: "patch",
				body: "Fixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)",
			},
			{
				type: "patch",
				body: "Improved SQLite storage read performance.\n\nThe storage now avoids loading values for key-only reads.",
			},
		]),
		`## 0.7.0 - 2026-05-29\n\n### Minor\n\n- Added branch merge preview support.\n\n### Patch\n\n- Fixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)\n- Improved SQLite storage read performance.\n\n  The storage now avoids loading values for key-only reads.\n\n`,
	);
});

test("changelogEntry indents fenced code block details", () => {
	assert.equal(
		changelogEntry("0.6.2", "2026-06-02", [
			{
				type: "patch",
				body: 'Added a typed file API:\n\n```js\nawait lix.fs.writeFile("/orders.xlsx", bytes);\nconst bytes = await lix.fs.readFile("/orders.xlsx");\n```',
			},
		]),
		'## 0.6.2 - 2026-06-02\n\n### Patch\n\n- Added a typed file API:\n\n  ```js\n  await lix.fs.writeFile("/orders.xlsx", bytes);\n  const bytes = await lix.fs.readFile("/orders.xlsx");\n  ```\n\n',
	);
});

test("updateChangelog inserts new entries after heading", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	writeFileSync(
		join(root, "CHANGELOG.md"),
		`# Changelog\n\n## 0.6.0 - 2026-05-28\n\n### Patch\n\n- js-sdk: Previous release.\n`,
	);

	updateChangelog(root, "0.6.1", "2026-05-29", [
		{ type: "patch", body: "Fixed native binding loading on Linux." },
	]);

	assert.equal(
		readFileSync(join(root, "CHANGELOG.md"), "utf8"),
		`# Changelog\n\n## 0.6.1 - 2026-05-29\n\n### Patch\n\n- Fixed native binding loading on Linux.\n\n## 0.6.0 - 2026-05-28\n\n### Patch\n\n- js-sdk: Previous release.\n`,
	);
});

test("updateCargoToml bumps every lockstep Rust package and exact dependency pin", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, "packages", "js-sdk"), { recursive: true });
	mkdirSync(join(root, "packages", "lix"), { recursive: true });
	mkdirSync(join(root, "packages", "storage-rocksdb"), { recursive: true });
	mkdirSync(join(root, "packages", "storage-slatedb"), { recursive: true });
	writeFileSync(
		join(root, "Cargo.toml"),
		`[workspace.package]\nversion = "0.6.2"\n\n[workspace.dependencies]\nlix_storage_rocksdb = { path = "packages/storage-rocksdb", version = "=0.6.2" }\nlix_storage_slatedb = { path = "packages/storage-slatedb", version = "=0.6.2" }\nlix = { path = "packages/lix", version = "=0.6.2" }\n`,
	);
	writeFileSync(
		join(root, "packages", "lix", "Cargo.toml"),
		`[package]\nname = "lix"\nversion.workspace = true\n`,
	);
	writeFileSync(
		join(root, "packages", "js-sdk", "Cargo.toml"),
		`[package]\nname = "lix_js_sdk"\nversion.workspace = true\n\n[dependencies]\nlix = { path = "../lix", version = "=0.6.2", default-features = false }\n`,
	);
	writeFileSync(
		join(root, "packages", "storage-rocksdb", "Cargo.toml"),
		`[package]\nname = "lix-storage-rocksdb"\nversion.workspace = true\n\n[dependencies]\nlix = { path = "../lix", version = "=0.6.2", default-features = false }\n`,
	);
	writeFileSync(
		join(root, "packages", "storage-slatedb", "Cargo.toml"),
		`[package]\nname = "lix-storage-slatedb"\nversion.workspace = true\n`,
	);

	updateCargoToml(root, "0.7.0");

	const rootCargoToml = readFileSync(join(root, "Cargo.toml"), "utf8");
	assert.match(rootCargoToml, /\[workspace\.package\]\nversion = "0\.7\.0"/);
	assert.match(rootCargoToml, /lix_storage_rocksdb = \{ path = "packages\/storage-rocksdb", version = "=0\.7\.0"/);
	assert.match(rootCargoToml, /lix_storage_slatedb = \{ path = "packages\/storage-slatedb", version = "=0\.7\.0"/);
	assert.match(rootCargoToml, /lix = \{ path = "packages\/lix", version = "=0\.7\.0"/);
	assert.match(readFileSync(join(root, "packages", "js-sdk", "Cargo.toml"), "utf8"), /lix = \{ path = "\.\.\/lix", version = "=0\.6\.2"/);
	assert.match(readFileSync(join(root, "packages", "storage-rocksdb", "Cargo.toml"), "utf8"), /version\.workspace = true/);
	assert.match(readFileSync(join(root, "packages", "storage-rocksdb", "Cargo.toml"), "utf8"), /lix = \{ path = "\.\.\/lix", version = "=0\.6\.2"/);
});

test("updatePackageVersion pins native optional dependencies", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, "packages", "js-sdk"), { recursive: true });
	mkdirSync(join(root, "packages", "storage-filesystem"), { recursive: true });
	writeFileSync(
		join(root, "packages", "js-sdk", "package.json"),
		`${JSON.stringify({ name: "@lix-js/sdk", version: "0.6.0" }, null, "\t")}\n`,
	);
	writeFileSync(
		join(root, "packages", "js-sdk", "package-lock.json"),
		`${JSON.stringify(
			{
				name: "@lix-js/sdk",
				version: "0.6.0",
				lockfileVersion: 3,
				requires: true,
				packages: {
					"": { name: "@lix-js/sdk", version: "0.6.0" },
					"node_modules/@lix-js/sdk-linux-x64": {
						version: "0.6.0",
						resolved: "https://registry.npmjs.org/@lix-js/sdk-linux-x64/-/sdk-linux-x64-0.6.0.tgz",
						optional: true,
					},
				},
			},
			null,
			"\t",
		)}\n`,
	);
	writeFileSync(
		join(root, "packages", "storage-filesystem", "package.json"),
		`${JSON.stringify({ name: "@lix-js/storage-filesystem", version: "0.1.0", peerDependencies: { "@lix-js/sdk": "^0.6.0" } }, null, "\t")}\n`,
	);
	writeFileSync(
		join(root, "packages", "storage-filesystem", "package-lock.json"),
		`${JSON.stringify({ name: "@lix-js/storage-filesystem", version: "0.1.0", lockfileVersion: 3, packages: { "": { name: "@lix-js/storage-filesystem", version: "0.1.0", peerDependencies: { "@lix-js/sdk": "^0.6.0" } } } }, null, "\t")}\n`,
	);

	updatePackageVersion(root, "0.7.0");

	const packageJson = JSON.parse(readFileSync(join(root, "packages", "js-sdk", "package.json"), "utf8"));
	const lock = JSON.parse(readFileSync(join(root, "packages", "js-sdk", "package-lock.json"), "utf8"));
	assert.equal(packageJson.optionalDependencies["@lix-js/sdk-linux-x64"], "0.7.0");
	assert.equal(lock.packages[""].optionalDependencies["@lix-js/sdk-darwin-arm64"], "0.7.0");
	assert.equal(lock.packages["node_modules/@lix-js/sdk-linux-x64"].version, "0.7.0");
	assert.equal(
		lock.packages["node_modules/@lix-js/sdk-linux-x64"].resolved,
		"https://registry.npmjs.org/@lix-js/sdk-linux-x64/-/sdk-linux-x64-0.7.0.tgz",
	);
	const storagePackage = JSON.parse(
		readFileSync(join(root, "packages", "storage-filesystem", "package.json"), "utf8"),
	);
	const storageLock = JSON.parse(
		readFileSync(join(root, "packages", "storage-filesystem", "package-lock.json"), "utf8"),
	);
	assert.equal(storagePackage.version, "0.7.0");
	assert.equal(storagePackage.peerDependencies["@lix-js/sdk"], "0.7.0");
	assert.equal(storageLock.version, "0.7.0");
	assert.equal(storageLock.packages[""].peerDependencies["@lix-js/sdk"], "0.7.0");
});
