#!/usr/bin/env node
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { cp, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..", "..");
const jsSdkDir = join(repoRoot, "packages", "js-sdk");
const wasmProfile = process.env.LIX_WASM_PROFILE ?? "release";
const targetDir = join(
	repoRoot,
	"target",
	"wasm32-unknown-unknown",
	wasmProfile,
);
const engineWasmPath = join(targetDir, "lix_engine_wasm_bindgen.wasm");
const engineOutDir = join(jsSdkDir, "src", "engine-wasm", "wasm");
const engineDistOutDir = join(jsSdkDir, "dist", "engine-wasm", "wasm");
const distDir = join(jsSdkDir, "dist");
const wasmBindgenOutName = "lix_engine";

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

async function buildEngineWasm() {
	const existingRustFlags = process.env.RUSTFLAGS ?? "";
	const wasmRustFlags =
		`${existingRustFlags} --cfg getrandom_backend="wasm_js"`.trim();
	const cargoArgs = [
		"build",
		"-p",
		"lix_engine_wasm_bindgen",
		"--target",
		"wasm32-unknown-unknown",
	];
	if (wasmProfile === "release") {
		cargoArgs.push("--release");
	}
	await run("cargo", cargoArgs, {
		env: {
			...process.env,
			RUSTFLAGS: wasmRustFlags,
		},
	});

	await rm(engineOutDir, { recursive: true, force: true });
	await run("wasm-bindgen", [
		engineWasmPath,
		"--target",
		"web",
		"--out-dir",
		engineOutDir,
		"--out-name",
		wasmBindgenOutName,
	]);
	await normalizeWasmBindgenOutput(engineOutDir);
	await mkdir(engineDistOutDir, { recursive: true });
	await cp(engineOutDir, engineDistOutDir, { recursive: true, force: true });
}

async function normalizeWasmBindgenOutput(outputDir) {
	const generatedWasm = join(outputDir, `${wasmBindgenOutName}_bg.wasm`);
	const generatedWasmTypes = join(outputDir, `${wasmBindgenOutName}_bg.wasm.d.ts`);
	const normalizedWasm = join(outputDir, `${wasmBindgenOutName}.wasm`);
	const normalizedWasmTypes = join(outputDir, `${wasmBindgenOutName}.wasm.d.ts`);
	await rename(generatedWasm, normalizedWasm);
	await rename(generatedWasmTypes, normalizedWasmTypes);

	const jsPath = join(outputDir, `${wasmBindgenOutName}.js`);
	const js = await readFile(jsPath, "utf8");
	await writeFile(
		jsPath,
		js.replaceAll(`${wasmBindgenOutName}_bg.wasm`, `${wasmBindgenOutName}.wasm`),
	);
}

async function syncBuiltinSchemas() {
	await run("node", ["./scripts/sync-builtin-schemas.js"], { cwd: jsSdkDir });
}

async function buildTypescriptDist() {
	await run("tsc", ["-p", "tsconfig.json"], { cwd: jsSdkDir });
}

async function main() {
	await rm(distDir, { recursive: true, force: true });
	await syncBuiltinSchemas();
	await buildEngineWasm();
	await buildTypescriptDist();
}

main().catch((error) => {
	console.error("[build-wasm] Failed to generate wasm payloads:\n", error);
	process.exit(1);
});
