import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { test } from "vitest";

const execFileAsync = promisify(execFile);

test("starts when the host has worker-incompatible exec arguments", async () => {
	await execFileAsync(process.execPath, [
		"--expose-gc",
		"--input-type=module",
		"--eval",
		`const { createWorkerConnection } = await import("./dist/worker/factory.node.js");
const connection = createWorkerConnection();
const opened = new Promise((resolve, reject) => {
	connection.onFatal(reject);
	connection.onMessage((message) => {
		if (message.id !== 1) return;
		if (message.ok) resolve();
		else reject(new Error(message.error.message));
	});
});
connection.postMessage({
	id: 1,
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
