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

// TypeScript's worker-only library omits the window SharedWorker constructor.
// Keep the literal constructor spelling for bundlers' worker-entry detection.
declare const SharedWorker: new (url: URL, options: {type: string; name: string}) => {
 port: MessagePort;
 onerror: ((event: ErrorEvent) => void) | null;
};

/** Package-private provider identity selects one engine, not one engine per tab. */
export function createSharedWorkerConnection(key: string): WorkerConnection {
 if (typeof SharedWorker === "undefined" || !navigator.locks) throw new Error("Shared partial replicas require SharedWorker and Web Locks");
 const worker = new SharedWorker(new URL("./entry.shared.browser.js", import.meta.url), {type:"module",name:key});
 const port = worker.port;
 const leaseName = `lix:shared-client:${crypto.randomUUID()}`;
 let release!: () => void;
 const lifetime = new Promise<void>(resolve => {release=resolve;});
 let ready!: () => void;
 const acquired = new Promise<void>(resolve => {ready=resolve;});
 let failure: ((error: Error) => void) | undefined;
 void navigator.locks.request(leaseName, async () => {ready(); await lifetime;}).catch(error => failure?.(error));
 void acquired.then(() => port.postMessage({kind:"shared.clientLease",name:leaseName}));
 port.start();
 let closed = false;
 return {
  postMessage(message) {if (closed) throw new Error("Shared engine connection closed"); port.postMessage(message);},
  onMessage(listener) {port.onmessage = event => listener(event.data);},
  onFatal(listener) {failure=listener; worker.onerror=event => listener(new Error(event.message || "Shared engine failed"));},
  ref() {}, unref() {},
  async terminate() {if(closed)return;closed=true;port.postMessage({kind:"shared.disconnect"});release();port.close();},
 };
}
