#!/usr/bin/env node
import { spawn } from "node:child_process";
import { mkdir, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageDir = join(__dirname, "..");
const repoRoot = join(packageDir, "..", "..");
const profile = process.env.LIX_WASM_PROFILE ?? "release";
const cargoProfile = profile === "release" ? "release" : "dev";
const artifactProfile = cargoProfile === "release" ? "release" : "debug";
const outDir = join(packageDir, "dist", "wasm");

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

const rustFlags = `${process.env.RUSTFLAGS ?? ""} --cfg getrandom_backend="wasm_js"`.trim();
const cargoEnv = { ...process.env, RUSTFLAGS: rustFlags };
if (cargoProfile === "release") {
	// The engine pulls in DataFusion. Optimizing for raw speed produces a WASM
	// module that is prohibitively large for browsers, while `s` keeps build
	// times reasonable and dramatically reduces download/compile overhead.
	cargoEnv.CARGO_PROFILE_RELEASE_OPT_LEVEL ??= "s";
	cargoEnv.CARGO_PROFILE_RELEASE_STRIP ??= "symbols";
}
await run(
	"cargo",
	[
		"build",
		"-p",
		"lix_js_sdk",
		"--target",
		"wasm32-unknown-unknown",
		"--profile",
		cargoProfile,
	],
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
