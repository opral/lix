/// <reference lib="webworker" />

import type {
	LixBinding,
	LixStorageConfig,
	TelemetryDispatch,
	TelemetryParentContext,
	OpenProgressDispatch,
	SyncServerBindingOptions,
} from "../binding-types.js";
import type {
	WorkerConnection,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";

// Browser/Wasm execution stays off the main thread.
export const openDirectLixBinding: undefined | ((
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
				listener(event.error ?? new Error(event.message ?? "Lix worker failed"));
		},
		ref() {},
		unref() {},
		async terminate() {
			worker.terminate();
		},
	};
}
