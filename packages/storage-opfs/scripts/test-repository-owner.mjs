import assert from "node:assert/strict";
import { mkdtemp, cp, rm, writeFile, readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { join, dirname, extname } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { chromium, webkit } from "playwright";
const storage = join(dirname(fileURLToPath(import.meta.url)), "..");
const sdk = join(storage, "../js-sdk");
const tmp = await mkdtemp(join(tmpdir(), "lix-repository-owner-"));
async function run(command, args, cwd) {
	await new Promise((resolve, reject) => {
		const p = spawn(command, args, { cwd, stdio: "inherit" });
		p.on("error", reject);
		p.on("exit", (code) =>
			code === 0 ? resolve() : reject(new Error(`${command} failed: ${code}`)),
		);
	});
}
let server;
try {
	await cp(join(storage, "test-fixtures/repository-owner"), tmp, {
		recursive: true,
	});
	await writeFile(
		join(tmp, "package.json"),
		JSON.stringify({
			type: "module",
			dependencies: {
				"@lix-js/sdk": `file:${sdk}`,
				"@lix-js/storage-opfs": `file:${storage}`,
			},
		}),
	);
	await writeFile(
		join(tmp, "vite.config.js"),
		`export default {worker:{format:'es'}};`,
	);
	await run(
		"npm",
		[
			"install",
			"--ignore-scripts",
			"--omit=optional",
			"--no-audit",
			"--no-fund",
		],
		tmp,
	);
	await run(
		process.execPath,
		[join(storage, "node_modules/vite/bin/vite.js"), "build"],
		tmp,
	);
	server = createServer(async (req, res) => {
		try {
			const path = join(
				tmp,
				"dist",
				req.url === "/"
					? "index.html"
					: new URL(req.url, "http://localhost").pathname,
			);
			res.setHeader(
				"Content-Type",
				{
					".html": "text/html",
					".js": "text/javascript",
					".wasm": "application/wasm",
				}[extname(path)] ?? "application/octet-stream",
			);
			res.end(await readFile(path));
		} catch {
			res.writeHead(404).end();
		}
	});
	await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
	const url = `http://127.0.0.1:${server.address().port}`;
	const results = {};
	for (const name of (process.env.LIX_TEST_BROWSERS ?? "chromium").split(",")) {
		const browser = await { chromium, webkit }[name].launch();
		const context = await browser.newContext();
		// A broken idle observation must fail CI rather than wait forever.
		let timedOut = false;
		const deadline = setTimeout(() => {
			timedOut = true;
			void browser.close().catch(() => {});
		}, 120_000);
		const pages = [];
		const errors = [];
		const storageName = `owner-test-${crypto.randomUUID()}`;
		const page = async () => {
			const p = await context.newPage();
			p.on("pageerror", (e) => errors.push(String(e)));
			await p.goto(url);
			await p.waitForFunction(() => window.api);
			pages.push(p);
			return p;
		};
		try {
			const [a, b] = await Promise.all([page(), page()]);
			await Promise.all(
				[a, b].map((p) => p.evaluate((name) => api.open(name), storageName)),
			);
			const locks = await a.evaluate(() => navigator.locks.query());
			const owner = locks.held.find((l) =>
				l.name.startsWith("lix:repository-owner:"),
			);
			const sqlite = locks.held.find((l) =>
				l.name.startsWith("lix:opfs-sqlite:"),
			);
			assert.ok(owner);
			assert.equal(owner.clientId, sqlite.clientId);
			await a.evaluate(() => api.write("shared", "durable"));
			assert.equal(await b.evaluate(() => api.read("shared")), "durable");
			await a.evaluate(() => api.watch());
			const changed = a.evaluate(() => api.next());
			await b.evaluate(() => api.write("observe", "change"));
			await changed;
			await a.evaluate(() => api.unwatch());
			await a.evaluate(() => api.begin());
			await a.evaluate(() => api.txWrite("rolled-back", "hidden"));
			await a.evaluate(() => api.rollback());
			assert.equal(await b.evaluate(() => api.read("rolled-back")), undefined);
			// Closing a local handle must preserve another tab's active session.
			await a.evaluate(() => api.close());
			assert.equal(await b.evaluate(() => api.read("shared")), "durable");
			await b.evaluate(() => api.close());
			await a.close();
			await b.close();
			for (let i = 0; i < 5; i++) {
				const p = await page();
				await p.evaluate((name) => api.open(name), storageName);
				assert.equal(await p.evaluate(() => api.read("shared")), "durable");
				await p.close();
			}
			// Force a known hosting tab by opening it before the follower.
			const host = await page();
			await host.evaluate((name) => api.open(name), storageName);
			const follower = await page();
			await follower.evaluate((name) => api.open(name), storageName);
			const childBranch = await follower.evaluate(() => api.childBranch());
			await follower.evaluate(() => api.begin());
			await follower.evaluate(() =>
				api.txWrite("lost-transaction", "must roll back"),
			);
			await follower.evaluate(() => api.watch());
			await follower.evaluate(() => {
				window.recovered = api.next();
			});
			await host.close();
			await follower.evaluate(() => window.recovered);
			assert.equal(
				await follower.evaluate(() => api.childBranchId()),
				childBranch,
			);
			assert.equal(
				await follower.evaluate(() => api.read("lost-transaction")),
				undefined,
			);
			assert.equal(
				await follower.evaluate(() =>
					api.txWrite("bad", "stale").catch((error) => error.code),
				),
				"LIX_TRANSACTION_LOST",
			);
			await follower.evaluate(() => api.closeChild());
			assert.equal(
				await follower.evaluate(() => api.read("shared")),
				"durable",
			);
			const update = follower.evaluate(() => api.next());
			await follower.evaluate(() => api.write("observe", "after recovery"));
			await update;
			await follower.evaluate(() => api.unwatch());
			await follower.evaluate(() => api.close());
			assert.deepEqual(errors, []);
			results[name] = {
				status: "passed",
				checks: [
					"same-realm ownership",
					"cross-tab read/write",
					"observations",
					"transaction rollback",
					"remote session survives local close",
					"five warm reopens",
					"owner loss restores session and observation",
					"writes and live updates after recovery",
					"child branch context survives recovery",
					"interrupted transaction rolls back and cannot be reused",
				],
			};
		} catch (error) {
			results[name] = {
				status: "failed",
				error: timedOut
					? "Repository ownership suite exceeded 120 seconds"
					: String(error),
				pageErrors: errors,
			};
			throw error;
		} finally {
			clearTimeout(deadline);
			console.log(JSON.stringify(results[name]));
			await context.close();
			await browser.close();
		}
	}
} finally {
	server?.close();
	await rm(tmp, { recursive: true, force: true });
}
