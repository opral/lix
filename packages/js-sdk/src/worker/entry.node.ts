import { parentPort } from "node:worker_threads";
import { startWorkerHost } from "./host.js";
import type { WorkerInput, WorkerResponse } from "./protocol.js";

const port = parentPort;
if (!port) throw new Error("Lix worker requires a parent port");

startWorkerHost({
	postMessage(message: WorkerResponse) {
		port.postMessage(message);
	},
	onMessage(listener) {
		port.on("message", (message: WorkerInput) => listener(message));
	},
});
