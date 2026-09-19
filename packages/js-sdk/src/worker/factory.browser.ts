/// <reference lib="webworker" />

import type {
	LixBinding,
	LixStorageConfig,
	TelemetryDispatch,
	TelemetryParentContext,
	OpenProgressDispatch,
	SyncServerBindingOptions,
} from "../binding-types.js";
import type { WorkerConnection, WorkerResponse } from "./protocol.js";

// Browser/Wasm execution stays off the main thread.
export const openDirectLixBinding:
	| undefined
	| ((
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
	telemetryParent?: TelemetryParentContext,
	server?: SyncServerBindingOptions,
	openProgress?: OpenProgressDispatch,
	snapshot?: ReadableStream<Uint8Array>,
) => Promise<LixBinding>) = undefined;

export function createWorkerConnection(): WorkerConnection {
	const worker = new Worker(new URL("./entry.browser.js", import.meta.url), {
		type: "module",
		name: "lix",
	});
	return {
		postMessage(message) {
			worker.postMessage(message);
		},
		onMessage(listener) {
			worker.onmessage = (event: MessageEvent<WorkerResponse>) =>
				listener(event.data);
		},
		onFatal(listener) {
			worker.onerror = (event) =>
				listener(workerFailure(event));
		},
		ref() {},
		unref() {},
		async terminate() {
			worker.terminate();
		},
	};
}

export { createRepositoryConnection } from "./repository-connection.js";

function workerFailure(event: ErrorEvent): Error {
	if (event.error instanceof Error) return event.error;
	const location = event.filename
		? ` (${event.filename}:${event.lineno ?? 0}:${event.colno ?? 0})`
		: "";
	return Object.assign(
		new Error(
			`${event.message || "Lix worker failed to load or execute"}${location}`,
		),
		{ code: "LIX_WORKER_FAILED" },
	);
}
