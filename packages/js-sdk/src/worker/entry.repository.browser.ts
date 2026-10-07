/// <reference lib="webworker" />
import { createRepositoryHost } from "./repository-host.js";
import {
	CLOSE_TIMEOUT_MS,
	type RepositoryMessage,
} from "./repository-protocol.js";
import {
	isSessionCloseTransportResult,
	isSessionCloseTransportResponse,
} from "./protocol.js";

let runtimeWarm = false;
const start = (event: MessageEvent) => {
	if (event.data?.kind !== "start") return;
	const buildId = location.href;
	const { key, channelName, token } = event.data as {
		key: string;
		channelName: string;
		token: string;
	};
	postMessage({ kind: "build", buildId, token });
	const channel = new BroadcastChannel(channelName);
	const generation = crypto.randomUUID();
	const clients = new Map<string, MessagePort>();
	const departed = new Set<string>();
	const teardownCallbacks = new Map<string, Set<number>>();
	const opening = new Map<string, number>();
	const host = createRepositoryHost();
	let active = false;
	let retained = true;
	let retiring = false;
	let cleanupFailed = false;
	const lockAbort = new AbortController();
	let releaseOwnership!: () => void;
	const ownershipLifetime = new Promise<void>((resolve) => { releaseOwnership = resolve; });
	let ownership: Promise<void>;
	const retire = () => {
		if (retained || clients.size || retiring) return;
		retiring = true;
		active = false;
		const timer = setTimeout(() => {
			postMessage({ kind: "failure", token, message: "Repository owner cleanup timed out" });
			close();
		}, CLOSE_TIMEOUT_MS);
		void (async () => {
			try {
				await host.close();
			} catch {
				cleanupFailed = true;
			}
			// A warm compiler realm must retain neither storage nor its owner fence.
			channel.close();
			releaseOwnership();
			lockAbort.abort();
			await ownership;
			clearTimeout(timer);
			if (!cleanupFailed) onmessage = start;
			postMessage({ kind: "retired", token, reusable: !cleanupFailed, runtimeWarm });
			if (cleanupFailed) close();
		})();
	};
	onmessage = (event) => {
		if (event.data?.kind === "retain") retained = true;
		if (event.data?.kind === "release") {
			retained = false;
			retire();
		}
	};
	const send = (message: RepositoryMessage) => channel.postMessage(message);
	channel.onmessage = (event: MessageEvent<RepositoryMessage>) => {
		const message = event.data;
		if (!active) return;
		if (message.kind === "discover") {
			send({
				kind: "owner",
				client: message.client,
				nonce: message.nonce,
				generation,
				buildId,
			});
			return;
		}
		if (
			!("generation" in message) ||
			message.generation !== generation ||
			!("client" in message)
		)
			return;
		if (message.kind === "connect") {
			if (departed.has(message.client)) return;
			if (!clients.has(message.client)) {
				const { port1, port2 } = new MessageChannel();
				clients.set(message.client, port1);
				teardownCallbacks.set(message.client, new Set());
				port1.onmessage = (event) => {
					const response = event.data;
					if (opening.has(message.client) && opening.get(message.client) === event.data?.id) {
						opening.delete(message.client);
						// Failed opening may have left a rejected compiler or uncertain
						// provider cleanup. Do not cache that runtime for another open.
						cleanupFailed ||= event.data.ok !== true;
					}
					// A successful engine context proves compiler initialization finished.
					runtimeWarm ||= event.data?.ok === true && event.data.context !== undefined;
					if (event.data?.kind === "repository.disconnected") {
						cleanupFailed ||= event.data.error !== undefined;
						clients.delete(message.client);
						teardownCallbacks.delete(message.client);
						departed.delete(message.client);
						port1.close();
						send({
							kind: "disconnected",
							client: message.client,
							generation,
							error: event.data.error,
						});
						retire();
					} else if (
						!departed.has(message.client) ||
						isSessionCloseTransportResponse(response)
					) {
						if (departed.has(message.client) && "kind" in response && "requestId" in response) {
							teardownCallbacks.get(message.client)?.add(response.requestId);
						}
						send({
							kind: "output",
							client: message.client,
							generation,
							message: event.data,
						});
					}
				};
				port1.start();
				host.connect(port2);
				// Death detection closes the host outside its finite-operation queue.
				void navigator.locks.request(message.lease, async () => {
					if (!clients.has(message.client)) return;
					departed.add(message.client);
					const timer = setTimeout(() => {
						postMessage({ kind: "failure", token, message: "Repository client cleanup timed out" });
						send({ kind: "gone", generation });
						close();
					}, CLOSE_TIMEOUT_MS);
					port1.addEventListener("message", (event) => {
						if (event.data?.kind === "repository.disconnected")
							clearTimeout(timer);
					});
					port1.postMessage({ kind: "repository.disconnect" });
				});
			}
			send({ kind: "connected", client: message.client, generation });
		} else if (message.kind === "input") {
			const teardownRequestId = "requestId" in message.message
				? message.message.requestId
				: undefined;
			const allowedTeardownResult = departed.has(message.client) &&
				teardownRequestId !== undefined &&
				isSessionCloseTransportResult(message.message) &&
				teardownCallbacks.get(message.client)?.has(teardownRequestId);
			if (!departed.has(message.client) || allowedTeardownResult) {
				if ("operation" in message.message && message.message.operation.kind === "open")
					opening.set(message.client, message.message.id);
				clients.get(message.client)?.postMessage(message.message);
				if (allowedTeardownResult && teardownRequestId !== undefined)
					teardownCallbacks.get(message.client)?.delete(teardownRequestId);
			}
		} else if (message.kind === "disconnect") {
			departed.add(message.client);
			clients
				.get(message.client)
				?.postMessage({ kind: "repository.disconnect" });
		}
	};
	// This unversioned owner lock belongs to the same realm as engine and OPFS.
	// Backend physical locks remain the fence against an incompatible old build.
	ownership = navigator.locks
		.request(`lix:repository-owner:${key}`, { signal: lockAbort.signal }, async () => {
			if (retiring) return;
			active = true;
			send({ kind: "available" });
			await ownershipLifetime;
		})
		.then(() => undefined)
		.catch((error) => {
			if (retiring && lockAbort.signal.aborted && error?.name === "AbortError") return;
			cleanupFailed = true;
			postMessage({ kind: "failure", token, message: String(error) });
			close();
		});
};

onmessage = start;
