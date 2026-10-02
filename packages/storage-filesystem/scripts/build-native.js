#!/usr/bin/env node
import { spawn } from "node:child_process";
import { cp, mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageDir = join(__dirname, "..");
const manifestPath = join(packageDir, "../storage-filesystem-native/Cargo.toml");
const target = process.env.LIX_NATIVE_TARGET;
const requestedProfile = process.env.LIX_NATIVE_PROFILE ?? "release";
const cargoProfile = requestedProfile === "debug" ? "dev" : requestedProfile;
const artifactProfile =
	cargoProfile === "dev" || cargoProfile === "test" ? "debug" : cargoProfile;
const targetArtifacts = {
	"aarch64-unknown-linux-gnu": "liblix_storage_filesystem_node.so",
	"x86_64-unknown-linux-gnu": "liblix_storage_filesystem_node.so",
	"aarch64-apple-darwin": "liblix_storage_filesystem_node.dylib",
	"x86_64-pc-windows-msvc": "lix_storage_filesystem_node.dll",
};
if (target && !Object.hasOwn(targetArtifacts, target)) {
	throw new Error(`Unsupported native target: ${target}`);
}
const artifactName = target ? targetArtifacts[target] :
	process.platform === "darwin"
		? "liblix_storage_filesystem_node.dylib"
		: process.platform === "win32"
			? "lix_storage_filesystem_node.dll"
			: "liblix_storage_filesystem_node.so";
const destination = join(packageDir, "lix_storage_filesystem.node");

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

const args = [
	"build",
	"--manifest-path",
	manifestPath,
	"--profile",
	cargoProfile,
];

// Published native binaries do not need the local symbol table: it adds
// ~107 MiB (.symtab + .strtab) to the linux-x64 addon. The N-API entry point
// lives in the dynamic symbol table, which stripping keeps.
if (cargoProfile === "release") {
	args.push("--config", 'profile.release.strip="symbols"');
}
if (target) args.push("--target", target);
args.push("--timings");

await run("cargo", args);
await mkdir(packageDir, { recursive: true });
await cp(
	join(await cargoTargetDir(), target ?? "", artifactProfile, artifactName),
	destination,
);
