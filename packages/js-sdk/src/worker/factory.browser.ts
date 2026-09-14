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

import { deserializeWorkerError } from "./protocol.js";

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
				listener(workerFailure(event));
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
 let termination: Promise<void> | undefined;
 let disconnected!: (error?: Error) => void;
 const detached = new Promise<void>((resolve, reject) => { disconnected = error => error ? reject(error) : resolve(); });
 return {
  postMessage(message) {if (closed) throw new Error("Shared engine connection closed"); port.postMessage(message);},
  onMessage(listener) {port.onmessage = event => {
   if (event.data?.kind === "shared.disconnected") {
    const error = event.data.error;
    disconnected(error ? deserializeWorkerError(error) : undefined);
   } else listener(event.data);
  };},
  onFatal(listener) {failure=listener; worker.onerror=event => listener(workerFailure(event));},
  ref() {}, unref() {},
  terminate() {
   if(termination)return termination;
   termination = (async () => {
   closed=true;
   port.postMessage({kind:"shared.disconnect"});
   release();
   let timeout: ReturnType<typeof setTimeout> | undefined;
   try {
    await Promise.race([detached, new Promise<never>((_, reject) => {
     timeout = setTimeout(() => reject(Object.assign(new Error("Shared engine close was not acknowledged"), {code:"LIX_SHARED_ENGINE_CLOSE_UNCONFIRMED"})), 10000);
    })]);
   } finally {if(timeout !== undefined)clearTimeout(timeout);port.close();}
   })();
   return termination;
  },
 };
}

function workerFailure(event: ErrorEvent): Error {
    if (event.error instanceof Error) return event.error;
    const location = event.filename ? ` (${event.filename}:${event.lineno ?? 0}:${event.colno ?? 0})` : "";
    return Object.assign(new Error(`${event.message || "Lix worker failed to load or execute"}${location}`), {code: "LIX_WORKER_FAILED"});
}
