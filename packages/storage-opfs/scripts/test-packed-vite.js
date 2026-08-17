#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createReadStream } from "node:fs";
import { cp, mkdtemp, rm, stat } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, extname, join, normalize, relative } from "node:path";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const sdkPackageDir = join(packageDir, "..", "js-sdk");
const fixtureSource = join(packageDir, "test-fixtures", "vite-production");
const viteBin = join(packageDir, "node_modules", "vite", "bin", "vite.js");
const base = "/lix-storage-opfs-smoke/";
const tempRoot = await mkdtemp(join(tmpdir(), "lix-storage-opfs-vite-smoke-"));
const fixtureDir = join(tempRoot, "app");
let server;
let browser;

try {
	await cp(fixtureSource, fixtureDir, { recursive: true });
	const sdkTarball = await pack(sdkPackageDir, tempRoot, "SDK");
	const storageTarball = await pack(packageDir, tempRoot, "OPFS storage");
	await run(
		"npm",
		[
			"install",
			"--ignore-scripts",
			"--no-audit",
			"--no-fund",
			"--no-package-lock",
			"--omit=optional",
			sdkTarball,
			storageTarball,
		],
		{ cwd: fixtureDir },
	);
	await run(process.execPath, [viteBin, "build", "--base", base], {
		cwd: fixtureDir,
	});

	server = await serve(join(fixtureDir, "dist"));
	browser = await chromium.launch({ headless: true });
	const page = await browser.newPage();
	const browserErrors = [];
	page.on("console", (message) => {
		if (message.type() === "error") browserErrors.push(message.text());
	});
	page.on("pageerror", (error) => browserErrors.push(error.stack ?? error.message));
	page.setDefaultTimeout(120_000);
	try {
		await page.goto(`http://127.0.0.1:${server.port}${base}`, {
			waitUntil: "load",
		});
		await page.waitForFunction(
			() => "__storageOpfsProductionSmoke" in globalThis,
		);
		const result = await page.evaluate(
			() => globalThis.__storageOpfsProductionSmoke,
		);
		assert.deepEqual(result, { message: "persistent-production" });
		assert.deepEqual(browserErrors, []);
	} finally {
		await page.close();
	}
	console.log("Packed OPFS storage Vite production smoke passed.");
} finally {
	await browser?.close();
	await server?.close();
	if (process.env.LIX_KEEP_VITE_SMOKE === "1") {
		console.log(`Kept smoke fixture at ${tempRoot}`);
	} else {
		await rm(tempRoot, { recursive: true, force: true });
	}
}

async function pack(directory, destination, label) {
	const outputValue = await output(
		"npm",
		["pack", "--json", "--pack-destination", destination],
		{ cwd: directory },
	);
	const packed = JSON.parse(outputValue);
	const filename = packed[0]?.filename;
	if (typeof filename !== "string") {
		throw new Error(`npm pack did not report a ${label} tarball: ${outputValue}`);
	}
	return join(destination, filename);
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

async function serve(root) {
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

function contentType(path) {
	switch (extname(path)) {
		case ".html":
			return "text/html; charset=utf-8";
		case ".js":
		case ".mjs":
			return "text/javascript; charset=utf-8";
		case ".wasm":
			return "application/wasm";
		default:
			return "application/octet-stream";
	}
}
