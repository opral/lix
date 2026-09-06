import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
	appendFileSync,
	cpSync,
	lstatSync,
	mkdirSync,
	readFileSync,
	readdirSync,
	readlinkSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";

function validateRuntime(runtime) {
	if (!["native", "browser"].includes(runtime))
		throw new Error(`Invalid runtime: ${runtime}`);
}

export function isBuildInput(path) {
	// Only handwritten SDK TypeScript and release prose are proven independent
	// of the binaries. Unknown paths, fixtures, build scripts and manifests all
	// invalidate the cache. TypeScript is rebuilt and tested even on a hit.
	return (
		!(
			/^packages\/js-sdk\/src\/.+\.ts$/.test(path) &&
			!path.startsWith("packages/js-sdk/src/wasm/")
		) &&
		!/^\.changenotes\/[^/]+\.md$/.test(path) &&
		path !== "CHANGELOG.md"
	);
}

export function cacheKey(root, runtime, env = process.env) {
	validateRuntime(runtime);
	const hash = createHash("sha256");
	const buildEnv = Object.fromEntries(
		Object.entries(env)
			.filter(([name]) =>
				/^(CARGO_PROFILE_|CARGO_ENCODED_RUSTFLAGS$|CARGO_INCREMENTAL$|RUSTFLAGS$|LIX_.*PROFILE$|LIX_WASM_STORAGE_BENCH$|CC$|CXX$|CFLAGS$|CXXFLAGS$)/.test(
					name,
				),
			)
			.sort(([a], [b]) => a.localeCompare(b)),
	);
	hash.update(
		JSON.stringify({
			runtime,
			platform: process.platform,
			arch: process.arch,
			node: process.versions.node.split(".")[0],
			buildEnv,
		}),
	);
	const paths = execFileSync("git", ["ls-files", "-z"], {
		cwd: root,
		encoding: "utf8",
		maxBuffer: 16 * 1024 * 1024,
	})
		.split("\0")
		.filter(Boolean)
		.filter(isBuildInput)
		.sort();
	for (const path of paths) {
		const file = join(root, path);
		const info = lstatSync(file);
		hash.update(`${path}\0${info.mode & 0o777}\0`);
		hash.update(
			info.isSymbolicLink() ? readlinkSync(file) : readFileSync(file),
		);
		hash.update("\0");
	}
	return `sdk-binaries-v1-${runtime}-${hash.digest("hex")}`;
}

function outputs(runtime) {
	validateRuntime(runtime);
	return [
		"dist/wasm",
		"dist/bundled-plugins",
		...(runtime === "native" ? ["lix_js_sdk.node"] : []),
	];
}

function files(root, relative) {
	const info = lstatSync(join(root, relative));
	if (info.isFile()) return [relative];
	if (!info.isDirectory())
		throw new Error(`Unexpected binary cache entry: ${relative}`);
	return readdirSync(join(root, relative))
		.sort()
		.flatMap((name) => files(root, `${relative}/${name}`));
}

function manifest(root, runtime, key) {
	const entries = outputs(runtime).flatMap((path) => files(root, path));
	for (const path of [
		"dist/wasm/lix_js_sdk.js",
		"dist/wasm/lix_js_sdk.d.ts",
		"dist/wasm/lix_js_sdk_bg.wasm",
		"dist/bundled-plugins/plugin_csv.lixplugin",
		"dist/bundled-plugins/plugin_markdown.lixplugin",
	]) {
		if (!entries.includes(path))
			throw new Error(`Missing binary output: ${path}`);
	}
	return {
		key,
		files: Object.fromEntries(
			entries.map((path) => [
				path,
				createHash("sha256")
					.update(readFileSync(join(root, path)))
					.digest("hex"),
			]),
		),
	};
}

export function validCache(cache, runtime, key) {
	try {
		return (
			JSON.stringify(manifest(cache, runtime, key)) ===
			JSON.stringify(
				JSON.parse(readFileSync(join(cache, "manifest.json"), "utf8")),
			)
		);
	} catch {
		return false;
	}
}

export function saveBinaries(sdk, cache, runtime, key) {
	const description = manifest(sdk, runtime, key);
	rmSync(cache, { recursive: true, force: true });
	for (const path of outputs(runtime)) {
		mkdirSync(dirname(join(cache, path)), { recursive: true });
		cpSync(join(sdk, path), join(cache, path), { recursive: true });
	}
	writeFileSync(join(cache, "manifest.json"), JSON.stringify(description));
}

export function restoreBinaries(sdk, cache, runtime, key) {
	if (!validCache(cache, runtime, key))
		throw new Error("Binary cache validation failed");
	for (const path of outputs(runtime)) {
		rmSync(join(sdk, path), { recursive: true, force: true });
		mkdirSync(dirname(join(sdk, path)), { recursive: true });
		cpSync(join(cache, path), join(sdk, path), { recursive: true });
	}
	mkdirSync(join(sdk, "src"), { recursive: true });
	rmSync(join(sdk, "src/wasm"), { recursive: true, force: true });
	symlinkSync("../dist/wasm", join(sdk, "src/wasm"), "dir");
}

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(process.argv[1]).href
) {
	const [command, runtime, key] = process.argv.slice(2);
	const root = process.cwd();
	const cache = join(root, ".ci-sdk-cache", runtime ?? "");
	const sdk = join(root, "packages/js-sdk");
	validateRuntime(runtime);
	if (command === "key")
		appendFileSync(
			process.env.GITHUB_OUTPUT,
			`key=${cacheKey(root, runtime)}\n`,
		);
	else if (command === "check") {
		const reuse = validCache(cache, runtime, key);
		appendFileSync(process.env.GITHUB_OUTPUT, `reuse=${reuse}\n`);
		console.log(
			reuse
				? "Validated exact-input binaries; skip Rust compilation."
				: "No valid exact-input binaries; compile from source.",
		);
	} else if (command === "save") saveBinaries(sdk, cache, runtime, key);
	else if (command === "restore") restoreBinaries(sdk, cache, runtime, key);
	else throw new Error(`Unknown command: ${command}`);
}
