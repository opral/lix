import { Worker } from "node:worker_threads";
import { openLixBinding } from "../binding.node.js";
import type {
	LixBinding,
	LixStorageConfig,
	SyncServerBindingOptions,
	TelemetryDispatch,
	TelemetryParentContext,
	OpenProgressDispatch,
} from "../binding-types.js";
import type {
	WorkerConnection,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";

export function createWorkerConnection(): WorkerConnection {
	const worker = new Worker(new URL("./entry.node.js", import.meta.url), {
		name: "lix",
		execArgv: workerExecArgv(process.execArgv),
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

export function workerExecArgv(execArgv: readonly string[]): string[] {
	const filtered: string[] = [];
	for (let index = 0; index < execArgv.length; index++) {
		const arg = execArgv[index];
		if (arg === "--input-type") {
			index += 1;
			continue;
		}
		if (arg === "--expose-gc" || arg.startsWith("--input-type=")) continue;
		filtered.push(arg);
	}
	return filtered;
}

/// Native Lix already owns a dedicated serialized engine actor. Routing it
/// through a second JavaScript worker adds two message-port hops per query
/// without adding isolation or concurrency.
export const openDirectLixBinding = async (
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
	telemetryParent?: TelemetryParentContext,
	server?: SyncServerBindingOptions,
	openProgress?: OpenProgressDispatch,
	snapshot?: ReadableStream<Uint8Array>,
): Promise<LixBinding | undefined> => {
	return openLixBinding(
		storage,
		telemetry,
		telemetryParent,
		server,
		openProgress,
		snapshot,
	);
};
