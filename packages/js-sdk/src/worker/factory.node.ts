import { Worker } from "node:worker_threads";
import type {
	WorkerConnection,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";

export function createWorkerConnection(): WorkerConnection {
	const worker = new Worker(new URL("./entry.node.js", import.meta.url), {
		name: "lix",
	});
	let terminating = false;
	return {
		postMessage(message: WorkerInput) {
			worker.postMessage(message);
		},
		onMessage(listener) {
			worker.on("message", (message: WorkerResponse) => listener(message));
		},
		onFatal(listener) {
			worker.on("error", (error) => {
				if (!terminating) listener(error);
			});
			worker.on("exit", (code) => {
				if (!terminating) listener(new Error(`Lix worker exited with code ${code}`));
			});
		},
		ref() {
			worker.ref();
		},
		unref() {
			worker.unref();
		},
		async terminate() {
			terminating = true;
			await worker.terminate();
		},
	};
}
