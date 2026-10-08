import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { expect, test } from "vitest";
import { workerExecArgv } from "./factory.node.js";

const execFileAsync = promisify(execFile);

test("removes process defaults while preserving worker module and security options", () => {
	expect(workerExecArgv([
		"--stack-trace-limit=10", "--v8-pool-size", "4",
		"--trace-event-file-pattern=node_trace.${rotation}.log",
		"--secure-heap-min=2", "--tls-cipher-list=DEFAULT",
		"--use-largepages=off", "--node-snapshot", "--secure-heap=0",
		"--max-old-space-size=4096", "--max-semi-space-size=16",
		"--conditions=custom", "--import", "./loader.mjs",
		"--experimental-strip-types", "--permission", "--allow-worker",
	])).toEqual([
		"--conditions=custom", "--import", "./loader.mjs",
		"--experimental-strip-types", "--permission", "--allow-worker",
	]);
});

test("opens memory WASM from a Node test-runner child", async () => {
	const directory = await mkdtemp(join(tmpdir(), "lix-node-test-worker-"));
	try {
		const script = join(directory, "worker.test.mjs");
		const sdk = pathToFileURL(join(process.cwd(), "dist/index.js")).href;
		await writeFile(script, `import test from "node:test";
import assert from "node:assert/strict";
import {openLix} from ${JSON.stringify(sdk)};
test("memory worker inherits valid host options", async () => {
  const lix = await openLix();
  try { assert.equal((await lix.execute("SELECT 42 AS answer")).rows[0].answer, 42); }
  finally { await lix.close(); }
});`);
		await execFileAsync(process.execPath, ["--test", "--experimental-strip-types", script]);
	} finally {
		await rm(directory, {recursive: true, force: true});
	}
});

test("preserves host security arguments", () => {
	expect(
		workerExecArgv([
			"--permission",
			"--allow-worker",
			"--allow-fs-read=/workspace",
			"--expose-gc",
			"--input-type=module",
		]),
	).toEqual([
		"--permission",
		"--allow-worker",
		"--allow-fs-read=/workspace",
	]);
});

test("starts when the host has worker-incompatible exec arguments", async () => {
	await execFileAsync(process.execPath, [
		"--expose-gc",
		"--max-old-space-size=4096",
		"--max-semi-space-size=16",
		"--input-type=module",
		"--eval",
		`const { createWorkerConnection } = await import("./dist/worker/factory.node.js");
const connection = createWorkerConnection();
const opened = new Promise((resolve, reject) => {
	connection.onFatal(reject);
	connection.onMessage((message) => {
		if ("kind" in message) return;
		if (message.id !== 1) return;
		if (message.ok) resolve();
		else reject(new Error(message.error.message));
	});
});
connection.postMessage({
	id: 1,
	sessionId: 0,
	operation: {
		kind: "open",
		storage: { kind: "memory" },
		telemetryEnabled: false,
	},
});
await opened;
await connection.terminate();`,
	]);
});
