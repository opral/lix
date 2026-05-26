#!/usr/bin/env node
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { cp, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..", "..");
const jsSdkDir = join(repoRoot, "packages", "js-sdk");
const wasmProfile = process.env.LIX_WASM_PROFILE ?? "release";
const useWasmSizeOptimizations =
	wasmProfile === "release" && process.env.LIX_WASM_SIZE_OPT !== "0";
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
	const wasmSizeRustFlags = useWasmSizeOptimizations
		? " -C opt-level=z -C lto=fat -C embed-bitcode=yes -C codegen-units=1 -C panic=abort"
		: "";
	const wasmRustFlags =
		`${existingRustFlags} --cfg getrandom_backend="wasm_js"${wasmSizeRustFlags}`.trim();
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
	await stripWasmCustomSections(engineOutDir);
	await mkdir(engineDistOutDir, { recursive: true });
	await cp(engineOutDir, engineDistOutDir, { recursive: true, force: true });
}

async function normalizeWasmBindgenOutput(outputDir) {
	const generatedWasm = join(outputDir, `${wasmBindgenOutName}_bg.wasm`);
	const generatedWasmTypes = join(
		outputDir,
		`${wasmBindgenOutName}_bg.wasm.d.ts`,
	);
	const normalizedWasm = join(outputDir, `${wasmBindgenOutName}.wasm`);
	const normalizedWasmTypes = join(
		outputDir,
		`${wasmBindgenOutName}.wasm.d.ts`,
	);
	const fsmod = await import("node:fs");
	if (fsmod.existsSync(generatedWasm))
		await rename(generatedWasm, normalizedWasm);
	if (fsmod.existsSync(generatedWasmTypes))
		await rename(generatedWasmTypes, normalizedWasmTypes);

	const jsPath = join(outputDir, `${wasmBindgenOutName}.js`);
	const js = await readFile(jsPath, "utf8");
	await writeFile(
		jsPath,
		js.replaceAll(
			`${wasmBindgenOutName}_bg.wasm`,
			`${wasmBindgenOutName}.wasm`,
		),
	);
}

async function stripWasmCustomSections(outputDir) {
	const wasmPath = join(outputDir, `${wasmBindgenOutName}.wasm`);
	const strippedWasmPath = join(
		outputDir,
		`${wasmBindgenOutName}.stripped.wasm`,
	);
	await run("wasm-tools", ["strip", "--all", wasmPath, "-o", strippedWasmPath]);
	await rename(strippedWasmPath, wasmPath);
}

async function syncBuiltinSchemas() {
	await run("node", ["./scripts/sync-builtin-schemas.js"], { cwd: jsSdkDir });
}

async function syncEngineSource() {
	await run("node", ["./scripts/sync-engine-src.js"], { cwd: jsSdkDir });
}

async function buildTypescriptDist() {
	await run("tsc", ["-p", "tsconfig.json"], { cwd: jsSdkDir });
}

async function main() {
	await rm(distDir, { recursive: true, force: true });
	await syncBuiltinSchemas();
	await syncEngineSource();
	await buildEngineWasm();
	await buildTypescriptDist();
}

main().catch((error) => {
	console.error("[build-wasm] Failed to generate wasm payloads:\n", error);
	process.exit(1);
});
