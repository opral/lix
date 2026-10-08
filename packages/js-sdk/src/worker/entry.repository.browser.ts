/// <reference lib="webworker" />
import { createRepositoryHost, type RepositoryHostConnection } from "./repository-host.js";
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
	const clients = new Map<string, RepositoryHostConnection>();
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
				teardownCallbacks.set(message.client, new Set());
				let retired = false;
				const connection = host.connect((response) => {
					if (retired) return;
					if ("ok" in response && opening.has(message.client) && opening.get(message.client) === response.id) {
						opening.delete(message.client);
						// Failed opening may have left a rejected compiler or uncertain
						// provider cleanup. Do not cache that runtime for another open.
						cleanupFailed ||= !("ok" in response) || response.ok !== true;
					}
					// A successful engine context proves compiler initialization finished.
					runtimeWarm ||= "ok" in response && response.ok === true && "context" in response && response.context !== undefined;
					if ("kind" in response && response.kind === "repository.disconnected") {
						retired = true;
						cleanupFailed ||= response.error !== undefined;
						clients.delete(message.client);
						teardownCallbacks.delete(message.client);
						departed.delete(message.client);
						send({
							kind: "disconnected",
							client: message.client,
							generation,
							error: response.error,
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
							message: response,
						});
					}
				});
				clients.set(message.client, connection);
				// Death detection closes the host outside its finite-operation queue.
				void navigator.locks.request(message.lease, async () => {
					if (!clients.has(message.client)) return;
					departed.add(message.client);
					const timer = setTimeout(() => {
						postMessage({ kind: "failure", token, message: "Repository client cleanup timed out" });
						send({ kind: "gone", generation });
						close();
					}, CLOSE_TIMEOUT_MS);
					// Keep cleanup callbacks routable until disconnect finishes.
					void connection.disconnect().then(
						() => clearTimeout(timer),
						() => { /* Keep the watchdog armed when cleanup is uncertain. */ },
					);
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
				clients.get(message.client)?.receive(message.message);
				if (allowedTeardownResult && teardownRequestId !== undefined)
					teardownCallbacks.get(message.client)?.delete(teardownRequestId);
			}
		} else if (message.kind === "disconnect") {
			departed.add(message.client);
			void clients.get(message.client)?.disconnect();
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
