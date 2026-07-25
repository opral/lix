#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createReadStream } from "node:fs";
import { cp, mkdtemp, readFile, readdir, rm, stat } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, extname, join, normalize, relative } from "node:path";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const fixtureSource = join(
	packageDir,
	"test-fixtures",
	"vite-production",
);
const viteBin = join(packageDir, "node_modules", "vite", "bin", "vite.js");
const base = "/lix-sdk-smoke/";
const tempRoot = await mkdtemp(join(tmpdir(), "lix-sdk-vite-smoke-"));
const fixtureDir = join(tempRoot, "app");
let server;
let browser;

try {
	await cp(fixtureSource, fixtureDir, { recursive: true });
	const packOutput = await output(
		"npm",
		[
			"pack",
			"--json",
			"--pack-destination",
			tempRoot,
		],
		{ cwd: packageDir },
	);
	const packed = JSON.parse(packOutput);
	const tarballName = packed[0]?.filename;
	if (typeof tarballName !== "string") {
		throw new Error(`npm pack did not report a tarball: ${packOutput}`);
	}
	const tarballPath = join(tempRoot, tarballName);

	await run(
		"npm",
		[
			"install",
			"--ignore-scripts",
			"--no-audit",
			"--no-fund",
			"--no-package-lock",
			"--omit=optional",
			tarballPath,
		],
		{ cwd: fixtureDir },
	);
	const nodeEntry = (
		await output(
			process.execPath,
			[
				"--input-type=module",
				"--eval",
				"console.log(import.meta.resolve('@lix-js/sdk'))",
			],
			{ cwd: fixtureDir },
		)
	).trim();
	assert.match(nodeEntry, /\/dist\/index\.js$/);
	await run(
		process.execPath,
		[
			"--input-type=module",
			"--eval",
			`try {
				import.meta.resolve("@lix-js/sdk/remote");
				throw new Error("@lix-js/sdk/remote unexpectedly resolved");
			} catch (error) {
				if (error?.code !== "ERR_PACKAGE_PATH_NOT_EXPORTED") throw error;
			}`,
		],
		{ cwd: fixtureDir },
	);
	await run(process.execPath, [viteBin, "build", "--base", base], {
		cwd: fixtureDir,
	});

	const distDir = join(fixtureDir, "dist");
	const assetsDir = join(distDir, "assets");
	const builtAssets = await readdir(assetsDir);
	const mainBundle = findBuiltAsset(builtAssets, /^index-.*\.js$/, "main bundle");
	const browserWorker = findBuiltAsset(
		builtAssets,
		/^entry\.browser-.*\.js$/,
		"browser worker",
	);
	const engineWasm = findBuiltAsset(
		builtAssets,
		/^lix_js_sdk_bg-.*\.wasm$/,
		"engine WASM",
	);
	const workerSource = await readFile(join(assetsDir, browserWorker), "utf8");
	const browserJavaScriptSources = await Promise.all(
		builtAssets
			.filter((file) => file.endsWith(".js"))
			.map((file) => readFile(join(assetsDir, file), "utf8")),
	);
	assert.ok(
		browserJavaScriptSources.some((source) => source.includes(browserWorker)),
		"The browser bundle does not reference the emitted browser worker",
	);
	assert.ok(
		workerSource.includes(engineWasm),
		"The browser worker does not reference the emitted engine WASM",
	);
	assert.ok(
		builtAssets.every((file) => !file.startsWith("entry.node-")),
		"Vite included the Node worker in the browser build",
	);
	assert.ok(
		builtAssets.every((file) => !file.endsWith(".node")),
		"Vite emitted a native Node binding in the browser build",
	);
	const browserJavaScript = browserJavaScriptSources.join("\n");
	for (const [label, nodeRuntimeMarker] of [
		["Node worker module", "entry.node"],
		["Node worker implementation", "Lix worker requires a parent port"],
		["Node worker_threads import", "worker_threads"],
		["Node binding module", "binding.node"],
		["native Node binding", "lix_js_sdk.node"],
		["native Node package", "@lix-js/sdk-darwin-arm64"],
	]) {
		assert.ok(
			!browserJavaScript.includes(nodeRuntimeMarker),
			`Vite included ${label} marker ${nodeRuntimeMarker} in the browser build`,
		);
	}
	browser = await chromium.launch({ headless: true });
	for (const cspMode of ["worker-scoped", "global"]) {
		server = await serve(distDir, cspMode);
		await runBrowserSmoke(browser, server.port, cspMode);
		await server.close();
		server = undefined;
	}
	console.log("Packed Vite production smoke passed.");
} finally {
	await browser?.close();
	await server?.close();
	if (process.env.LIX_KEEP_VITE_SMOKE === "1") {
		console.log(`Kept smoke fixture at ${tempRoot}`);
	} else {
		await rm(tempRoot, { recursive: true, force: true });
	}
}

function findBuiltAsset(assets, pattern, label) {
	const matches = assets.filter((file) => pattern.test(file));
	assert.equal(
		matches.length,
		1,
		`Expected exactly one ${label}, found ${matches.join(", ") || "none"}`,
	);
	return matches[0];
}

async function runBrowserSmoke(browser, port, cspMode) {
	const page = await browser.newPage();
	const browserErrors = [];
	page.on("console", (message) => {
		if (message.type() === "error") browserErrors.push(message.text());
	});
	page.on("pageerror", (error) => browserErrors.push(error.stack ?? error.message));
	page.setDefaultTimeout(120_000);
	try {
		const response = await page.goto(`http://127.0.0.1:${port}${base}`, {
			waitUntil: "load",
		});
		assert.ok(response, `No document response for ${cspMode} CSP smoke`);
		const documentCsp = response.headers()["content-security-policy"] ?? "";
		if (cspMode === "worker-scoped") {
			assert.ok(
				!documentCsp.includes("data:"),
				"Worker-scoped document CSP unexpectedly allows data: scripts",
			);
			assert.ok(
				!documentCsp.includes("wasm-unsafe-eval"),
				"Worker-scoped document CSP unexpectedly allows WebAssembly compilation",
			);
		}
		await page.waitForFunction(() => "__lixProductionSmoke" in globalThis);
		const result = await page.evaluate(
			() => globalThis.__lixProductionSmoke,
		);

		assert.deepEqual(result, {
			message: "production",
			bundledPluginKeys: [
				"plugin_csv_v2",
				"plugin_markdown_incremental_v2",
			],
		});
		assert.deepEqual(browserErrors, []);
	} finally {
		await page.close();
	}
}

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

async function serve(root, cspMode) {
	const httpServer = createServer(async (request, response) => {
		try {
			const requestUrl = new URL(request.url ?? "/", "http://localhost");
			if (!requestUrl.pathname.startsWith(base)) {
				response.writeHead(404).end();
				return;
			}
			const pathWithinRoot =
				requestUrl.pathname === base
					? "index.html"
					: decodeURIComponent(requestUrl.pathname.slice(base.length));
			const filePath = normalize(join(root, pathWithinRoot));
			if (relative(root, filePath).startsWith("..")) {
				response.writeHead(403).end();
				return;
			}
			const fileStat = await stat(filePath);
			if (!fileStat.isFile()) throw new Error("Not a file");
			response.writeHead(200, {
				"Content-Type": contentType(filePath),
				"Cache-Control": "no-store",
				"Content-Security-Policy": contentSecurityPolicy(
					pathWithinRoot,
					cspMode,
				),
			});
			createReadStream(filePath).pipe(response);
		} catch {
			response.writeHead(404).end();
		}
	});
	await new Promise((resolve, reject) => {
		httpServer.once("error", reject);
		httpServer.listen(0, "127.0.0.1", resolve);
	});
	const address = httpServer.address();
	if (!address || typeof address === "string") {
		throw new Error("Smoke server did not bind a TCP port");
	}
	return {
		port: address.port,
		close: () =>
			new Promise((resolve, reject) =>
				httpServer.close((error) => (error ? reject(error) : resolve())),
			),
	};
}

function contentSecurityPolicy(pathWithinRoot, mode) {
	const isBrowserWorker = /^assets\/entry\.browser-.*\.js$/.test(pathWithinRoot);
	if (mode === "global" || isBrowserWorker) {
		return (
			"default-src 'none'; " +
			"script-src 'self' 'wasm-unsafe-eval'; " +
			"worker-src 'self'; connect-src 'self'"
		);
	}
	return (
		"default-src 'none'; script-src 'self'; " +
		"worker-src 'self'; connect-src 'self'"
	);
}

function contentType(path) {
	switch (extname(path)) {
		case ".html":
			return "text/html; charset=utf-8";
		case ".js":
		case ".mjs":
			return "text/javascript; charset=utf-8";
		case ".wasm":
			return "application/wasm";
		case ".json":
			return "application/json; charset=utf-8";
		default:
			return "application/octet-stream";
	}
}
