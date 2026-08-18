import { Worker } from "node:worker_threads";
import { openNativeLixBinding } from "../binding.node.js";
import type { LixBinding, LixStorageConfig, TelemetryDispatch } from "../binding-types.js";
import type {
	WorkerConnection,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";

export function createWorkerConnection(): WorkerConnection {
	const worker = new Worker(new URL("./entry.node.js", import.meta.url), {
		name: "lix",
		execArgv: process.execArgv.filter(
			(arg, index, args) =>
				!arg.startsWith("--input-type=") &&
				!(index > 0 && args[index - 1] === "--input-type"),
		),
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
		terminateImmediately() {
			void worker.terminate();
		},
		async terminate() {
			terminating = true;
			await worker.terminate();
		},
	};
}

/// Native Lix already owns a dedicated serialized engine actor. Routing it
/// through a second JavaScript worker adds two message-port hops per query
/// without adding isolation or concurrency.
export const openDirectLixBinding = async (
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
	server?: { url: string; headers: [string, string][] },
): Promise<LixBinding | undefined> => {
	try {
		return await openNativeLixBinding(storage, telemetry, server?.url);
	} catch (error) {
		if (server !== undefined) throw error;
		if (storage.kind === "memory") return undefined;
		throw error;
	}
};
