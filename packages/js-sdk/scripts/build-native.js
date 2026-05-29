#!/usr/bin/env node
import { spawn } from "node:child_process";
import { cp, mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageDir = join(__dirname, "..");
const repoRoot = join(packageDir, "..", "..");
const profile = process.env.LIX_NATIVE_PROFILE ?? "release";
const isRelease = profile === "release";
const targetDir = join(repoRoot, "target", profile);
const source = join(
	targetDir,
	process.platform === "darwin"
		? "liblix_js_sdk.dylib"
		: process.platform === "win32"
			? "lix_js_sdk.dll"
			: "liblix_js_sdk.so",
);
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

const rustflags = [
	process.env.RUSTFLAGS ?? "",
	isRelease ? "-C strip=symbols" : "",
	process.platform === "darwin"
		? "-C link-arg=-undefined -C link-arg=dynamic_lookup"
		: "",
]
	.join(" ")
	.trim();

const args = ["build", "--manifest-path", join(packageDir, "Cargo.toml")];
if (isRelease) {
	args.push("--release");
}

await run("cargo", args, {
	env: {
		...process.env,
		RUSTFLAGS: rustflags,
	},
});
await mkdir(packageDir, { recursive: true });
await cp(source, destination);
