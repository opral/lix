#!/usr/bin/env node
import { spawn } from "node:child_process";
import { mkdir, rm, symlink } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageDir = join(__dirname, "..");
const repoRoot = join(packageDir, "..", "..");
const profile = process.env.LIX_WASM_PROFILE ?? "release";
const cargoProfile = profile === "release" ? "release" : "dev";
const artifactProfile = cargoProfile === "release" ? "release" : "debug";
const outDir = join(packageDir, "dist", "wasm");
const sourceOutDir = join(packageDir, "src", "wasm");

function run(command, args, options = {}) {
	return new Promise((resolve, reject) => {
		const child = spawn(command, args, { stdio: "inherit", ...options });
		child.on("error", reject);
		child.on("exit", (code) => {
			if (code === 0) resolve();
			else reject(new Error(`${command} exited with code ${code ?? 1}`));
		});
	});
}

function output(command, args, options = {}) {
	return new Promise((resolve, reject) => {
		let stdout = "";
		const child = spawn(command, args, {
			stdio: ["ignore", "pipe", "inherit"],
			...options,
		});
		child.stdout.setEncoding("utf8");
		child.stdout.on("data", (chunk) => {
			stdout += chunk;
		});
		child.on("error", reject);
		child.on("exit", (code) => {
			if (code === 0) resolve(stdout);
			else reject(new Error(`${command} exited with code ${code ?? 1}`));
		});
	});
}

async function cargoTargetDir() {
	const metadata = JSON.parse(
		await output(
			"cargo",
			["metadata", "--format-version", "1", "--no-deps"],
			{ cwd: repoRoot },
		),
	);
	if (typeof metadata.target_directory !== "string") {
		throw new Error("cargo metadata did not include target_directory");
	}
	return metadata.target_directory;
}

// Sync bootstrap and canonical replay use the same deep engine graph as the
// native sync worker, which explicitly reserves a 4 MiB stack. Keep browser
// WASM on that contract as well; the linker default can trap during the first
// scoped pull before Rust has a chance to report an error.
const rustFlags = `${process.env.RUSTFLAGS ?? ""} --cfg getrandom_backend="wasm_js" -C link-arg=-zstack-size=4194304`.trim();
const cargoEnv = { ...process.env, RUSTFLAGS: rustFlags };
if (cargoProfile === "release") {
	// The engine pulls in DataFusion. Optimizing for raw speed produces a WASM
	// module that is prohibitively large for browsers, while `s` keeps build
	// times reasonable and dramatically reduces download/compile overhead.
	cargoEnv.CARGO_PROFILE_RELEASE_OPT_LEVEL ??= "s";
	cargoEnv.CARGO_PROFILE_RELEASE_STRIP ??= "symbols";
}
const cargoArgs = [
	"build",
	"-p",
	"lix_js_sdk",
	"--target",
	"wasm32-unknown-unknown",
	"--profile",
	cargoProfile,
];
if (process.env.LIX_WASM_STORAGE_BENCH === "1") {
	cargoArgs.push("--features", "storage-bridge-bench");
}
await run(
	"cargo",
	cargoArgs,
	{
		cwd: repoRoot,
		env: cargoEnv,
	},
);

const wasmArtifact = join(
	await cargoTargetDir(),
	"wasm32-unknown-unknown",
	artifactProfile,
	"lix_js_sdk.wasm",
);
await rm(outDir, { recursive: true, force: true });
await mkdir(outDir, { recursive: true });
await run("wasm-bindgen", [
	wasmArtifact,
	"--target",
	"web",
	"--out-dir",
	outDir,
	"--out-name",
	"lix_js_sdk",
]);
// Source imports (`../wasm/lix_js_sdk.js`) resolve here for Node vitest.
await rm(sourceOutDir, { recursive: true, force: true });
await symlink(outDir, sourceOutDir, "dir");
