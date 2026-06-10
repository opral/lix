#!/usr/bin/env node
import { spawn } from "node:child_process";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { zipSync } from "fflate";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageDir = join(__dirname, "..");
const repoRoot = join(packageDir, "..", "..");
const profile = "release";
const targetDir = join(repoRoot, "target", "wasm32-wasip2", profile);
const outDir = join(packageDir, "dist", "bundled-plugins");

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

const cargoArgs = [
	"build",
	"--manifest-path",
	join(repoRoot, "Cargo.toml"),
	"-p",
	"plugin_csv",
	"-p",
	"plugin_md_v2",
	"--target",
	"wasm32-wasip2",
];
cargoArgs.push("--profile", profile);
await run("cargo", cargoArgs);

await mkdir(outDir, { recursive: true });
await writeBundledPlugin({
	crateName: "plugin_csv",
	fileName: "plugin_csv.lixplugin",
	files: [
		["manifest.json", join(repoRoot, "plugins", "csv", "manifest.json")],
		[
			"schema/csv_table.json",
			join(repoRoot, "plugins", "csv", "schema", "csv_table.json"),
		],
		[
			"schema/csv_row.json",
			join(repoRoot, "plugins", "csv", "schema", "csv_row.json"),
		],
	],
});
await writeBundledPlugin({
	crateName: "plugin_md_v2",
	fileName: "plugin_md_v2.lixplugin",
	files: [
		["manifest.json", join(repoRoot, "plugins", "markdown", "manifest.json")],
		[
			"schema/markdown_document.json",
			join(repoRoot, "plugins", "markdown", "schema", "markdown_document.json"),
		],
		[
			"schema/markdown_block.json",
			join(repoRoot, "plugins", "markdown", "schema", "markdown_block.json"),
		],
	],
});

async function writeBundledPlugin({ crateName, fileName, files }) {
	const wasm = await readFile(await findWasm(crateName));
	const entries = {};
	for (const [archivePath, sourcePath] of files) {
		entries[archivePath] = await readFile(sourcePath);
	}
	entries["plugin.wasm"] = wasm;
	await writeFile(join(outDir, fileName), zipSync(entries, { level: 0 }));
}

async function findWasm(crateName) {
	const direct = join(targetDir, `${crateName}.wasm`);
	if (existsSync(direct)) {
		return direct;
	}
	const matches = await findFiles(targetDir, `${crateName}.wasm`);
	if (matches.length === 0) {
		throw new Error(`Could not find ${crateName}.wasm under ${targetDir}`);
	}
	matches.sort(
		(left, right) => left.length - right.length || left.localeCompare(right),
	);
	return matches[0];
}

async function findFiles(root, fileName) {
	const matches = [];
	for (const entry of await readdir(root, { withFileTypes: true })) {
		const path = join(root, entry.name);
		if (entry.isDirectory()) {
			matches.push(...(await findFiles(path, fileName)));
		} else if (entry.isFile() && entry.name === fileName) {
			matches.push(path);
		}
	}
	return matches;
}
