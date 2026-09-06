import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
	mkdtempSync,
	mkdirSync,
	readFileSync,
	realpathSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import {
	cacheKey,
	isBuildInput,
	restoreBinaries,
	saveBinaries,
	validCache,
} from "./ci-sdk-cache.mjs";

function fixture(t) {
	const root = mkdtempSync(join(tmpdir(), "lix-binaries-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	const write = (path, content = path) => {
		mkdirSync(dirname(join(root, path)), { recursive: true });
		writeFileSync(join(root, path), content);
	};
	return { root, write };
}

test("handwritten SDK TypeScript and release prose do not invalidate binaries; unknown inputs do", () => {
	for (const path of [
		"packages/js-sdk/src/foo.ts",
		".changenotes/fix.md",
		"CHANGELOG.md",
	])
		assert.equal(isBuildInput(path), false, path);
	for (const path of [
		"Cargo.lock",
		"rust-toolchain.toml",
		".cargo/config.toml",
		"packages/lix/src/lib.rs",
		"packages/js-sdk/src/wasm/generated.d.ts",
		"packages/js-sdk/build.rs",
		"packages/js-sdk/scripts/build-wasm.js",
		"plugins/csv/schema/csv_row.json",
		"new-build-config.json",
		".github/workflows/ci.yml",
	])
		assert.equal(isBuildInput(path), true, path);
});

test("keys track actual tracked bytes, filenames, runtime and build settings without depending on commit SHA", (t) => {
	const { root, write } = fixture(t);
	write("Cargo.lock", "one");
	write("packages/js-sdk/src/foo.ts", "one");
	execFileSync("git", ["init", "--quiet", root]);
	execFileSync("git", ["add", "."], { cwd: root });
	const key = cacheKey(root, "native", {});
	write("packages/js-sdk/src/foo.ts", "two");
	assert.equal(cacheKey(root, "native", {}), key);
	assert.notEqual(cacheKey(root, "browser", {}), key);
	assert.notEqual(
		cacheKey(root, "native", { LIX_NATIVE_PROFILE: "release" }),
		key,
	);
	assert.notEqual(
		cacheKey(root, "native", { RUSTFLAGS: "-C opt-level=1" }),
		key,
	);
	write("Cargo.lock", "two");
	assert.notEqual(cacheKey(root, "native", {}), key);
	write("Cargo.lock", "one");
	write("new-build-input.txt");
	execFileSync("git", ["add", "."], { cwd: root });
	assert.notEqual(cacheKey(root, "native", {}), key);
});

for (const runtime of ["native", "browser"]) {
	test(`${runtime} binary snapshots restore generated imports and reject incomplete, corrupt or mismatched outputs`, (t) => {
		const { root, write } = fixture(t);
		const sdk = join(root, "sdk");
		const cache = join(root, "cache");
		for (const path of [
			"dist/wasm/lix_js_sdk.js",
			"dist/wasm/lix_js_sdk.d.ts",
			"dist/wasm/lix_js_sdk_bg.wasm",
			"dist/bundled-plugins/plugin_csv.lixplugin",
			"dist/bundled-plugins/plugin_markdown.lixplugin",
			...(runtime === "native" ? ["lix_js_sdk.node"] : []),
		])
			write(`sdk/${path}`);
		saveBinaries(sdk, cache, runtime, "key");
		assert.equal(validCache(cache, runtime, "key"), true);
		assert.equal(validCache(cache, runtime, "other-key"), false);
		rmSync(sdk, { recursive: true });
		restoreBinaries(sdk, cache, runtime, "key");
		assert.equal(realpathSync(join(sdk, "src/wasm")), join(sdk, "dist/wasm"));
		assert.equal(
			readFileSync(join(sdk, "dist/wasm/lix_js_sdk.js"), "utf8"),
			"sdk/dist/wasm/lix_js_sdk.js",
		);
		write("cache/dist/wasm/lix_js_sdk_bg.wasm", "corrupt");
		assert.equal(validCache(cache, runtime, "key"), false);
		assert.throws(
			() => restoreBinaries(sdk, cache, runtime, "key"),
			/validation failed/,
		);
		saveBinaries(sdk, cache, runtime, "key");
		rmSync(join(cache, "dist/bundled-plugins/plugin_csv.lixplugin"));
		assert.equal(validCache(cache, runtime, "key"), false);
	});
}

test("symlinked binary payloads are rejected", (t) => {
	const { root } = fixture(t);
	mkdirSync(join(root, "dist"));
	symlinkSync(root, join(root, "dist/wasm"));
	assert.equal(validCache(root, "browser", "key"), false);
});
