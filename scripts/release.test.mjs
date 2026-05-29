import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import assert from "node:assert/strict";

import { bumpVersion, changelogEntry, loadChanges } from "./release.mjs";

test("bumpVersion applies semver changes", () => {
	assert.equal(bumpVersion("0.6.0", "patch"), "0.6.1");
	assert.equal(bumpVersion("0.6.0", "minor"), "0.7.0");
	assert.equal(bumpVersion("0.6.0", "major"), "1.0.0");
});

test("loadChanges validates and parses fragments", () => {
	const root = mkdtempSync(join(tmpdir(), "lix-release-test-"));
	mkdirSync(join(root, ".changes"));
	writeFileSync(
		join(root, ".changes", "pr-1.md"),
		`---\ntype: patch\nscope: js-sdk\n---\n\nFixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)\n`,
	);
	assert.deepEqual(loadChanges(root), [
		{
			path: ".changes/pr-1.md",
			type: "patch",
			scope: "js-sdk",
			body: "Fixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)",
		},
	]);
});

test("changelogEntry groups entries by type", () => {
	assert.equal(
		changelogEntry("0.7.0", "2026-05-29", [
			{ type: "minor", scope: "lix-sdk", body: "Added branch merge preview support." },
			{
				type: "patch",
				scope: "js-sdk",
				body: "Fixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)",
			},
		]),
		`## 0.7.0 - 2026-05-29\n\n### Minor\n\n- lix-sdk: Added branch merge preview support.\n\n### Patch\n\n- js-sdk: Fixed native binding loading on Linux. [#1](https://github.com/opral/lix/pull/1)\n\n`,
	);
});
