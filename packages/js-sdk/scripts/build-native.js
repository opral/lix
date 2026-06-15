#!/usr/bin/env node
import { spawn } from "node:child_process";
import { cp, mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageDir = join(__dirname, "..");
const manifestPath = join(packageDir, "Cargo.toml");
const profile = process.env.LIX_NATIVE_PROFILE ?? "release";
const isRelease = profile === "release";
const artifactName =
	process.platform === "darwin"
		? "liblix_js_sdk.dylib"
		: process.platform === "win32"
			? "lix_js_sdk.dll"
			: "liblix_js_sdk.so";
const destination = join(packageDir, "lix_js_sdk.node");

function run(cmd, args, opts = {}) {
	return new Promise((resolve, reject) => {
		const child = spawn(cmd, args, { stdio: "inherit", ...opts });
		child.on("error", reject);
		child.on("exit", (code) => {
			if (code === 0) resolve();
			else reject(new Error(`${cmd} exited with code ${code ?? 1}`));
		});
	});
}

function output(cmd, args, opts = {}) {
	return new Promise((resolve, reject) => {
		let stdout = "";
		const child = spawn(cmd, args, {
			stdio: ["ignore", "pipe", "inherit"],
			...opts,
		});
		child.stdout.setEncoding("utf8");
		child.stdout.on("data", (chunk) => {
			stdout += chunk;
		});
		child.on("error", reject);
		child.on("exit", (code) => {
			if (code === 0) resolve(stdout);
			else reject(new Error(`${cmd} exited with code ${code ?? 1}`));
		});
	});
}

async function cargoTargetDir() {
	const metadata = JSON.parse(
		await output("cargo", [
			"metadata",
			"--manifest-path",
			manifestPath,
			"--format-version",
			"1",
			"--no-deps",
		]),
	);
	if (typeof metadata.target_directory !== "string") {
		throw new Error("cargo metadata did not include target_directory");
	}
	return metadata.target_directory;
}

const args = ["build", "--manifest-path", manifestPath];
if (isRelease) {
	args.push("--release");
}

await run("cargo", args);
await mkdir(packageDir, { recursive: true });
await cp(join(await cargoTargetDir(), profile, artifactName), destination);
