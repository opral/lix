/// <reference lib="webworker" />

import { startWorkerHost } from "./host.js";
import type { WorkerInput, WorkerResponse } from "./protocol.js";

const scope = globalThis as unknown as DedicatedWorkerGlobalScope;
startWorkerHost({
	postMessage(message) {
		scope.postMessage(message);
	},
	onMessage(listener) {
		scope.onmessage = (event: MessageEvent<WorkerInput>) => listener(event.data);
	},
});
