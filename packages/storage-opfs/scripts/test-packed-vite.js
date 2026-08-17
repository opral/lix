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
const benchmarkEnabled = process.env.LIX_OPFS_BENCHMARK === "1";
let server;
let browser;
let context;

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
	context = await browser.newContext();
	const pages = await Promise.all([context.newPage(), context.newPage()]);
	const browserErrors = pages.map(() => []);
	for (const [index, page] of pages.entries()) {
		page.on("console", (message) => {
			if (message.type() === "error") browserErrors[index].push(message.text());
		});
		page.on("pageerror", (error) => browserErrors[index].push(error.stack ?? error.message));
		page.setDefaultTimeout(120_000);
	}
	try {
		const results = [];
		for (const page of pages) {
			progress("loading smoke page");
			await page.goto(`http://127.0.0.1:${server.port}${base}`, { waitUntil: "load" });
			await page.waitForFunction(() => "__storageOpfsProductionSmoke" in globalThis);
			results.push(
				await page.evaluate(() => globalThis.__storageOpfsProductionSmoke),
			);
		}
		for (const result of results) assert.deepEqual(result, { message: "persistent-production" });
		assert.deepEqual(browserErrors, [[], []]);
		progress("checking durable reload recovery");
		await pages[0].evaluate(() =>
			globalThis.__storageOpfsWriteValue("reload-recovery", "after-reload"),
		);
		await pages[0].reload({ waitUntil: "load" });
		await pages[0].waitForFunction(() => "__storageOpfsProductionSmoke" in globalThis);
		assert.deepEqual(
			await pages[0].evaluate(() => globalThis.__storageOpfsProductionSmoke),
			{ message: "persistent-production" },
		);
		assert.equal(
			await pages[0].evaluate(() =>
				globalThis.__storageOpfsReadValue("reload-recovery"),
			),
			"after-reload",
		);
		progress("checking hydrated offline reads and writes");
		assert.equal(
			await pages[1].evaluate(() =>
				globalThis.__storageOpfsPrepareOfflineSession(),
			),
			"persistent-production",
		);
		await context.setOffline(true);
		try {
			assert.equal(
				await pages[1].evaluate(() =>
					globalThis.__storageOpfsOfflineReadWrite("written-offline"),
				),
				"written-offline",
			);
		} finally {
			await context.setOffline(false);
		}
		assert.equal(
			await pages[1].evaluate(() =>
				globalThis.__storageOpfsFinishOfflineSession(),
			),
			"written-offline",
		);
		progress("checking divergent clients converge");
		await Promise.all([
			pages[0].evaluate(() =>
				globalThis.__storageOpfsWriteValue("divergent-left", "left"),
			),
			pages[1].evaluate(() =>
				globalThis.__storageOpfsWriteValue("divergent-right", "right"),
			),
		]);
		const convergedRows = [
			["divergent-left", "left"],
			["divergent-right", "right"],
		];
		assert.deepEqual(
			await pages[0].evaluate(() =>
				globalThis.__storageOpfsReadValues([
					"divergent-left",
					"divergent-right",
				]),
			),
			convergedRows,
		);
		assert.deepEqual(
			await pages[1].evaluate(() =>
				globalThis.__storageOpfsReadValues([
					"divergent-left",
					"divergent-right",
				]),
			),
			convergedRows,
		);
		await Promise.all([
			pages[0].evaluate(() =>
				globalThis.__storageOpfsWriteValue("divergent-same-key", "left"),
			),
			pages[1].evaluate(() =>
				globalThis.__storageOpfsWriteValue("divergent-same-key", "right"),
			),
		]);
		const sameKeyValues = await Promise.all([
			pages[0].evaluate(() =>
				globalThis.__storageOpfsReadValue("divergent-same-key"),
			),
			pages[1].evaluate(() =>
				globalThis.__storageOpfsReadValue("divergent-same-key"),
			),
		]);
		assert.ok(["left", "right"].includes(sameKeyValues[0]));
		assert.equal(sameKeyValues[1], sameKeyValues[0]);
		assert.deepEqual(browserErrors, [[], []]);
		progress("checking abrupt tab termination recovery");
		await checkCrashRecovery(context, server.port);
		progress("checking cross-tab observation");
		await pages[0].evaluate(() => globalThis.__storageOpfsStartObservation());
		await pages[1].evaluate(() =>
			globalThis.__storageOpfsCommitObservedValue("cross-tab-production"),
		);
		assert.equal(
			await pages[0].evaluate(() => globalThis.__storageOpfsFinishObservation()),
			"cross-tab-production",
		);
		if (benchmarkEnabled) {
			await pages[1].evaluate(() =>
				globalThis.__storageOpfsPrepareBenchmarkWriter(),
			);
			const samples = await pages[0].evaluate(() =>
				globalThis.__storageOpfsBenchmarkCrossTab(30),
			);
			console.log(
				JSON.stringify({
					benchmark: "lix-opfs-cross-tab-observer-delivery",
					...summarize(samples),
				}),
			);
		}
		progress("checking observation across owner failover");
		await pages[1].evaluate(() => globalThis.__storageOpfsStartObservation());
		await pages[0].close();
		const failover = await pages[1].evaluate(() => {
			return globalThis.__storageOpfsRecoverObservation("owner-failover-production");
		});
		assert.equal(failover.value, "owner-failover-production");
		progress("owner failover observation passed");
		const failoverSamples = [failover.elapsedMs];
		if (benchmarkEnabled) {
			for (let index = 1; index < 10; index += 1) {
				failoverSamples.push(
					await measureOwnerFailover(context, server.port, `owner-failover-${index}`),
				);
			}
			console.log(
				JSON.stringify({
					benchmark: "lix-opfs-owner-failover-observer-recovery",
					...summarize(failoverSamples),
				}),
			);
		}
	} finally {
		await Promise.all(pages.map((page) => page.close()));
	}
	console.log("Packed OPFS storage Vite production smoke passed.");
} finally {
	await context?.close();
	await browser?.close();
	await server?.close();
	if (process.env.LIX_KEEP_VITE_SMOKE === "1") {
		console.log(`Kept smoke fixture at ${tempRoot}`);
	} else {
		await rm(tempRoot, { recursive: true, force: true });
	}
}

async function measureOwnerFailover(browserContext, port, storageName) {
	const owner = await browserContext.newPage();
	const follower = await browserContext.newPage();
	const errors = [];
	for (const page of [owner, follower]) {
		page.on("console", (message) => {
			if (message.type() === "error") errors.push(message.text());
		});
		page.on("pageerror", (error) => errors.push(error.stack ?? error.message));
		page.setDefaultTimeout(120_000);
	}
	const url = `http://127.0.0.1:${port}${base}?storage=${encodeURIComponent(storageName)}`;
	try {
		for (const page of [owner, follower]) {
			await page.goto(url, { waitUntil: "load" });
			await page.waitForFunction(() => "__storageOpfsProductionSmoke" in globalThis);
			assert.deepEqual(await page.evaluate(() => globalThis.__storageOpfsProductionSmoke), {
				message: "persistent-production",
			});
		}
		await follower.evaluate(() => globalThis.__storageOpfsStartObservation());
		await owner.close();
		const recovered = await follower.evaluate(() =>
			globalThis.__storageOpfsRecoverObservation("owner-failover-benchmark"),
		);
		assert.equal(recovered.value, "owner-failover-benchmark");
		assert.deepEqual(errors, []);
		return recovered.elapsedMs;
	} finally {
		await Promise.all([owner.close(), follower.close()]);
	}
}

async function checkCrashRecovery(browserContext, port) {
	const storageName = `crash-recovery-${crypto.randomUUID()}`;
	const url = `http://127.0.0.1:${port}${base}?storage=${encodeURIComponent(storageName)}`;
	const writer = await browserContext.newPage();
	const errors = [];
	writer.on("console", (message) => {
		if (message.type() === "error") errors.push(message.text());
	});
	writer.on("pageerror", (error) => errors.push(error.stack ?? error.message));
	writer.setDefaultTimeout(120_000);
	await writer.goto(url, { waitUntil: "load" });
	await writer.waitForFunction(() => "__storageOpfsProductionSmoke" in globalThis);
	assert.deepEqual(await writer.evaluate(() => globalThis.__storageOpfsProductionSmoke), {
		message: "persistent-production",
	});
	assert.equal(
		await writer.evaluate(() =>
			globalThis.__storageOpfsStartCrashWrite("survived-crash"),
		),
		"survived-crash",
	);
	await writer.close();

	const recovery = await browserContext.newPage();
	recovery.on("console", (message) => {
		if (message.type() === "error") errors.push(message.text());
	});
	recovery.on("pageerror", (error) => errors.push(error.stack ?? error.message));
	recovery.setDefaultTimeout(120_000);
	try {
		await recovery.goto(url, { waitUntil: "load" });
		await recovery.waitForFunction(() => "__storageOpfsProductionSmoke" in globalThis);
		assert.deepEqual(
			await recovery.evaluate(() => globalThis.__storageOpfsProductionSmoke),
			{ message: "persistent-production" },
		);
		assert.equal(
			await recovery.evaluate(() =>
				globalThis.__storageOpfsReadValue("crash-recovery"),
			),
			"survived-crash",
		);
		assert.deepEqual(errors, []);
	} finally {
		await recovery.close();
	}
}

function summarize(samples) {
	const sorted = [...samples].sort((left, right) => left - right);
	return {
		samples,
		p50Ms: sorted[Math.max(0, Math.ceil(sorted.length * 0.5) - 1)],
		p95Ms: sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)],
	};
}

function progress(message) {
	if (process.env.LIX_OPFS_VERBOSE === "1") console.log(message);
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
