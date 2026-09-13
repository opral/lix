import { existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import assert from "node:assert/strict";

import {
	bumpVersion,
	currentVersion,
	prepareRelease,
	prepareManualRelease,
	releaseTag,
	releaseBranch,
	releaseTarget,
	changelogEntry,
	loadChanges,
	manualReleaseVersion,
	updateCargoToml,
	updateCargoLockfiles,
	updateChangelog,
	updatePackageVersion,
	validateCargoLockstepVersions,
} from "./release.mjs";

test("updateCargoLockfiles refreshes the root and tooling workspaces", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, "tooling"));
	writeFileSync(join(root, "Cargo.toml"), "[workspace]\n");
	writeFileSync(join(root, "tooling", "Cargo.toml"), "[workspace]\n");
	const calls = [];

	updateCargoLockfiles(root, {
		runCargo(command, args, options) {
			calls.push({ command, args, options });
		},
	});

	assert.deepEqual(
		calls.map(({ command, args, options }) => ({ command, args, cwd: options.cwd })),
		[
			{
				command: "cargo",
				args: ["update", "--workspace", "--manifest-path", "Cargo.toml"],
				cwd: root,
			},
			{
				command: "cargo",
				args: ["update", "--workspace", "--manifest-path", "tooling/Cargo.toml"],
				cwd: root,
			},
		],
	);
});

test("bumpVersion applies semver changes", () => {
	assert.equal(bumpVersion("0.6.0", "patch"), "0.6.1");
	assert.equal(bumpVersion("0.6.0", "minor"), "0.7.0");
	assert.equal(bumpVersion("0.6.0", "major"), "1.0.0");
});

test("manualReleaseVersion accepts an explicit newer stable version", () => {
	assert.equal(manualReleaseVersion("0.12.3", "0.14.0"), "0.14.0");
	assert.equal(manualReleaseVersion("0.12.3", "1.0.0"), "1.0.0");
});

test("manualReleaseVersion rejects malformed or non-incrementing versions", () => {
	assert.throws(() => manualReleaseVersion("0.12.3", "v0.14.0"), /Unsupported manual release/);
	assert.throws(() => manualReleaseVersion("0.12.3", "0.14"), /Unsupported manual release/);
	assert.throws(() => manualReleaseVersion("0.12.3", "0.12.3"), /must be greater/);
	assert.throws(() => manualReleaseVersion("0.12.3", "0.11.9"), /must be greater/);
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
			target: "lix",
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
			target: "lix",
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
			target: "lix",
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
	assert.match(readFileSync(join(root, "packages", "js-sdk", "Cargo.toml"), "utf8"), /lix = \{ path = "\.\.\/lix", version = "=0\.7\.0"/);
	assert.match(readFileSync(join(root, "packages", "storage-rocksdb", "Cargo.toml"), "utf8"), /version\.workspace = true/);
	assert.match(readFileSync(join(root, "packages", "storage-rocksdb", "Cargo.toml"), "utf8"), /lix = \{ path = "\.\.\/lix", version = "=0\.7\.0"/);
	assert.doesNotThrow(() => validateCargoLockstepVersions(root, "0.7.0"));
});

test("lockstep preflight reports every partial Cargo version bump", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	for (const packageName of ["app", "binding-a", "binding-b"]) {
		mkdirSync(join(root, "packages", packageName), { recursive: true });
	}
	writeFileSync(
		join(root, "Cargo.toml"),
		`[workspace.package]\nversion = "0.12.0"\n`,
	);
	writeFileSync(
		join(root, "packages", "app", "Cargo.toml"),
		`[package]\nname = "app"\nversion.workspace = true\n\n[dependencies]\nbinding-a = {\n\tpath = "../binding-a",\n\tversion = "=0.11.0",\n}\n\n[dependencies.binding-b]\npath = "../binding-b"\nversion = "=0.10.0"\n`,
	);
	for (const packageName of ["binding-a", "binding-b"]) {
		writeFileSync(
			join(root, "packages", packageName, "Cargo.toml"),
			`[package]\nname = "${packageName}"\nversion.workspace = true\n`,
		);
	}

	assert.throws(
		() => validateCargoLockstepVersions(root),
		(error) => {
			assert.match(error.message, /binding-a requires =0\.11\.0, expected =0\.12\.0/);
			assert.match(error.message, /binding-b requires =0\.10\.0, expected =0\.12\.0/);
			return true;
		},
	);

	updateCargoToml(root, "0.12.0");
	assert.doesNotThrow(() => validateCargoLockstepVersions(root));
});

test("updateCargoToml restores every manifest after a commit failure", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	for (const packageName of ["app", "binding"]) {
		mkdirSync(join(root, "packages", packageName), { recursive: true });
	}
	const rootManifest = `[workspace.package]\nversion = "0.11.0"\n`;
	const appManifest = `[package]\nname = "app"\nversion.workspace = true\n\n[dependencies]\nbinding = { path = "../binding", version = "=0.11.0" }\n`;
	writeFileSync(join(root, "Cargo.toml"), rootManifest);
	writeFileSync(join(root, "packages", "app", "Cargo.toml"), appManifest);
	writeFileSync(
		join(root, "packages", "binding", "Cargo.toml"),
		`[package]\nname = "binding"\nversion.workspace = true\n`,
	);

	let commits = 0;
	assert.throws(
		() =>
			updateCargoToml(root, "0.12.0", {
				renameManifest(source, destination) {
					commits += 1;
					if (commits === 2) throw new Error("injected commit failure");
					renameSync(source, destination);
				},
			}),
		/injected commit failure/,
	);
	assert.equal(readFileSync(join(root, "Cargo.toml"), "utf8"), rootManifest);
	assert.equal(readFileSync(join(root, "packages", "app", "Cargo.toml"), "utf8"), appManifest);
});

test("updatePackageVersion pins every lockstep npm package", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, "packages", "js-sdk"), { recursive: true });
	mkdirSync(join(root, "packages", "storage-filesystem"), { recursive: true });
	mkdirSync(join(root, "packages", "storage-opfs"), { recursive: true });
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
	writeFileSync(
		join(root, "packages", "storage-opfs", "package.json"),
		`${JSON.stringify({ name: "@lix-js/storage-opfs", version: "0.1.0", peerDependencies: { "@lix-js/sdk": "^0.6.0" } }, null, "\t")}\n`,
	);
	writeFileSync(
		join(root, "packages", "storage-opfs", "package-lock.json"),
		`${JSON.stringify({ name: "@lix-js/storage-opfs", version: "0.1.0", lockfileVersion: 3, packages: { "": { name: "@lix-js/storage-opfs", version: "0.1.0", peerDependencies: { "@lix-js/sdk": "^0.6.0" } }, "../js-sdk": { name: "@lix-js/sdk", version: "0.6.0", optionalDependencies: { "@lix-js/sdk-linux-x64": "0.6.0" } } } }, null, "\t")}\n`,
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

	const opfsPackage = JSON.parse(
		readFileSync(join(root, "packages", "storage-opfs", "package.json"), "utf8"),
	);
	const opfsLock = JSON.parse(
		readFileSync(join(root, "packages", "storage-opfs", "package-lock.json"), "utf8"),
	);
	assert.equal(opfsPackage.version, "0.7.0");
	assert.equal(opfsPackage.peerDependencies["@lix-js/sdk"], "0.7.0");
	assert.equal(opfsLock.version, "0.7.0");
	assert.equal(opfsLock.packages[""].version, "0.7.0");
	assert.equal(opfsLock.packages[""].peerDependencies["@lix-js/sdk"], "0.7.0");
	assert.equal(opfsLock.packages["../js-sdk"].version, "0.7.0");
	assert.equal(
		opfsLock.packages["../js-sdk"].optionalDependencies[
			"@lix-js/sdk-linux-x64"
		],
		"0.7.0",
	);
});

function releaseFixture() {
	const root = mkdtempSync(join(tmpdir(), "lix-release-target-test-"));
	const put = (path, text) => {
		mkdirSync(join(root, path, ".."), { recursive: true });
		writeFileSync(join(root, path), text);
	};
	put("Cargo.toml", '[workspace.package]\nversion = "0.16.1"\n[workspace.dependencies]\nlix = { path = "packages/lix", version = "=0.16.1" }\n');
	put("packages/lix/Cargo.toml", '[package]\nname = "lix"\nversion.workspace = true\n');
	for (const key of ["json", "csv"]) {
		put(`plugins/${key}/Cargo.toml`, `[package]\nname = "plugin_${key}"\nversion = "0.16.1"\n`);
	}
	for (const path of ["js-sdk", "storage-filesystem", "storage-opfs"]) {
		put(`packages/${path}/package.json`, JSON.stringify({ name: `@lix-js/${path === "js-sdk" ? "sdk" : path}`, version: "0.16.1" }));
		put(`packages/${path}/package-lock.json`, JSON.stringify({ version: "0.16.1", packages: { "": { version: "0.16.1" } } }));
	}
	put("CHANGELOG.md", "# Changelog\n");
	put(".changenotes/core.md", "---\ntype: patch\n---\n\nCore fix.\n");
	put(".changenotes/json.md", "---\ntype: minor\ntarget: plugin_json\n---\n\nJSON feature.\n");
	put(".changenotes/csv.md", "---\ntype: patch\ntarget: plugin_csv\n---\n\nCSV fix.\n");
	return { root, put };
}

const preparation = { date: "2026-09-15", runCargo() {} };

test("plugin release changes only its version, changelog, and notes", () => {
	const { root } = releaseFixture();
	const coreManifest = readFileSync(join(root, "Cargo.toml"), "utf8");
	const sdk = readFileSync(join(root, "packages/js-sdk/package.json"), "utf8");
	const result = prepareRelease(root, { ...preparation, target: "plugin_json" });
	assert.equal(result.version, "0.17.0");
	assert.equal(result.tag, "plugin_json/v0.17.0");
	assert.equal(result.branch, "release/plugin_json/v0.17.0");
	assert.equal(currentVersion(root, "plugin_json"), "0.17.0");
	assert.equal(currentVersion(root, "plugin_csv"), "0.16.1");
	assert.equal(readFileSync(join(root, "Cargo.toml"), "utf8"), coreManifest);
	assert.equal(readFileSync(join(root, "packages/js-sdk/package.json"), "utf8"), sdk);
	assert.equal(readFileSync(join(root, "CHANGELOG.md"), "utf8"), "# Changelog\n");
	assert.match(readFileSync(join(root, "plugins/json/CHANGELOG.md"), "utf8"), /JSON feature/);
	assert.deepEqual(loadChanges(root).map(change => change.target).sort(), ["lix", "plugin_csv"]);
	assert.equal(prepareRelease(root, { ...preparation, target: "plugin_json" }), null);
});

test("default Lix release leaves plugin versions and fragments independent", () => {
	const { root } = releaseFixture();
	const result = prepareRelease(root, preparation);
	assert.equal(result.version, "0.16.2");
	assert.equal(result.tag, "v0.16.2");
	assert.equal(result.branch, "release/v0.16.2");
	assert.equal(currentVersion(root, "plugin_json"), "0.16.1");
	assert.equal(currentVersion(root, "plugin_csv"), "0.16.1");
	assert.deepEqual(loadChanges(root).map(change => change.target).sort(), ["plugin_csv", "plugin_json"]);
	assert.doesNotMatch(readFileSync(join(root, "CHANGELOG.md"), "utf8"), /JSON feature|CSV fix/);
	assert.equal(existsSync(join(root, "plugins/json/CHANGELOG.md")), false);
});

test("manual Lix releases never consume plugin fragments", () => {
	const { root } = releaseFixture();
	const result = prepareManualRelease(root, "0.18.0", preparation);
	assert.equal(result.changes.length, 1);
	assert.equal(result.changes[0].target, "lix");
	assert.equal(currentVersion(root, "plugin_json"), "0.16.1");
	assert.equal(loadChanges(root).length, 2);
});

test("unknown targets fail before preparing release files", () => {
	const { root, put } = releaseFixture();
	for (const target of ["plugin_jsno", "__proto__", "plugin_json/v1.0.0"]) {
		assert.throws(() => releaseTarget(target), /Unknown release target/);
		assert.throws(() => prepareRelease(root, { ...preparation, target }), /Unknown release target/);
	}
	put(".changenotes/invalid.md", "---\ntype: patch\ntarget: plugin_jsno\n---\n\nTypo.\n");
	assert.throws(() => prepareRelease(root, preparation), /Unknown release target/);
	assert.equal(currentVersion(root), "0.16.1");
});

test("Lix lockstep updates exclude explicitly versioned plugin packages", () => {
	const { root, put } = releaseFixture();
	put("tooling/Cargo.toml", '[dependencies]\nplugin_json = { path = "../plugins/json", version = "=0.16.1" }\nlix = { path = "../packages/lix", version = "=0.16.1" }\n');
	updateCargoToml(root, "0.17.0");
	validateCargoLockstepVersions(root, "0.17.0");
	assert.equal(currentVersion(root, "plugin_json"), "0.16.1");
	const tooling = readFileSync(join(root, "tooling/Cargo.toml"), "utf8");
	assert.match(tooling, /plugin_json = .*version = "=0.16.1"/);
	assert.match(tooling, /lix = .*version = "=0.17.0"/);
});

test("release tag and branch helpers validate stable versions", () => {
	assert.equal(releaseTag("plugin_csv", "1.2.3"), "plugin_csv/v1.2.3");
	assert.equal(releaseBranch("lix", "1.2.3"), "release/v1.2.3");
	assert.throws(() => releaseTag("plugin_csv", "1.2.3-beta"), /Unsupported release version/);
});

test("mistyped or duplicate target metadata cannot silently become a Lix note", () => {
	const { root, put } = releaseFixture();
	for (const metadata of ["targets: plugin_json", "target: plugin_json\ntarget: lix"]) {
		put(".changenotes/invalid.md", `---\ntype: patch\n${metadata}\n---\n\nInvalid.\n`);
		assert.throws(() => loadChanges(root), /frontmatter field/);
	}
});
