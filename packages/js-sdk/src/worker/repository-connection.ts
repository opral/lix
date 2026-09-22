/// <reference lib="webworker" />
import { RepositorySession } from "./repository-session.js";
import type { WorkerConnection, WorkerResponse } from "./protocol.js";
import { deserializeWorkerError } from "./protocol.js";
import {
	OPEN_TIMEOUT_MS,
	CLOSE_TIMEOUT_MS,
	repositoryError,
	type RepositoryMessage,
} from "./repository-protocol.js";

// One candidate per repository per document, retained by all local SDK handles.
const hubs = new Map<string, { worker: Worker; references: number; buildId?: string }>();
export function createRepositoryConnection(key: string): WorkerConnection {
	if (!navigator.locks || typeof BroadcastChannel === "undefined") {
		throw repositoryError(
			"LIX_STORAGE_UNSUPPORTED",
			"Shared OPFS repositories require Web Locks and BroadcastChannel",
		);
	}
	const channelName = `lix:repository-rpc:v1:${key}`;
	let hub = hubs.get(key);
	if (!hub) {
		const worker = new Worker(
			new URL("./entry.repository.browser.js", import.meta.url),
			{
				type: "module",
				name: "lix-repository-owner",
			},
		);
		hub = { worker, references: 0 };
		hubs.set(key, hub);
		const created = hub;
		worker.addEventListener("message", (event) => {
			if (event.data?.kind === "build" && typeof event.data.buildId === "string")
				created.buildId = event.data.buildId;
			if (event.data?.kind === "retired" && hubs.get(key) === created)
				hubs.delete(key);
		});
		worker.postMessage({ kind: "start", key, channelName });
	}
	const retained = hub;
	retained.references++;
	retained.worker.postMessage({ kind: "retain" });
	const channel = new BroadcastChannel(channelName);
	const client = crypto.randomUUID();
	const lease = `lix:repository-client:${client}`;
	let release!: () => void;
	let leased = false,
		closed = false,
		connected = false;
	let generation: string | undefined, nonce: string | undefined;
	let lastSeen = Date.now();
	let lastPoll = lastSeen;
	let listener: ((message: WorkerResponse) => void) | undefined;
	let fatal: ((error: Error) => void) | undefined;
	let failure: Error | undefined;

	let finishClose: ((error?: Error) => void) | undefined;
	let termination: Promise<void> | undefined;
	const lifetime = new Promise<void>((resolve) => {
		release = resolve;
	});
	const send = (message: RepositoryMessage) => channel.postMessage(message);
	const fail = (error: Error) => {
		if (failure || closed) return;
		failure = error;
		session.close();
		fatal?.(error);
	};
	const session = new RepositorySession(
		(message) => {
			if (generation) send({ kind: "input", client, generation, message });
		},
		(message) => listener?.(message),
		fail,
	);
	const discover = () => {
		if (closed || !leased || failure || !retained.buildId) return;
		nonce = crypto.randomUUID();
		send({ kind: "discover", client, nonce });
	};
	const poll = setInterval(() => {
		const now = Date.now();
		// A suspended/throttled event loop cannot establish owner liveness.
		// Give a fresh probe a full response window when polling resumes.
		if (now - lastPoll > 1000 || now < lastPoll) lastSeen = now;
		lastPoll = now;
		if (connected && now - lastSeen > OPEN_TIMEOUT_MS)
			fail(
				repositoryError(
					"LIX_OWNER_LOST",
					"Repository owner stopped responding; reopen the repository",
				),
			);
		discover();
	}, 250);
	const deadline = setTimeout(
		() =>
			fail(
				repositoryError(
					"LIX_OPEN_TIMEOUT",
					"Repository owner did not accept the connection in time",
				),
			),
		OPEN_TIMEOUT_MS,
	);
	const workerError = (event: ErrorEvent) =>
		fail(repositoryError("LIX_WORKER_FAILED", event.message));
	retained.worker.addEventListener("error", workerError);
	void navigator.locks
		.request(lease, async () => {
			leased = true;
			discover();
			await lifetime;
		})
		.catch((error) => fail(error));
	channel.onmessage = (event: MessageEvent<RepositoryMessage>) => {
		const message = event.data;
		if (closed) {
			if (
				message.kind === "disconnected" &&
				message.client === client &&
				message.generation === generation
			)
				finishClose?.(
					message.error ? deserializeWorkerError(message.error) : undefined,
				);
			return;
		}
		if (failure) return;
		if (message.kind === "available") {
			discover();
			return;
		}
		if (message.kind === "gone" && message.generation === generation) {
			connected = false;
			generation = undefined;
			session.lost();
			discover();
			return;
		}
		if (!("client" in message) || message.client !== client) return;
		if (message.kind === "owner" && message.nonce === nonce) {
			// The worker asset URL fingerprints its bundled engine too. An older
			// tab must not silently execute this page's operations with old code.
			if (message.buildId !== retained.buildId) {
				fail(
					repositoryError(
						"LIX_OWNER_VERSION_MISMATCH",
						"Another tab is running a different Lix version. Close all other lixray.com tabs and app windows, then retry. Your saved local changes are preserved.",
					),
				);
				return;
			}
			if (generation && generation !== message.generation) {
				connected = false;
				session.lost();
			}
			lastSeen = Date.now();
			generation = message.generation;
			if (!connected) send({ kind: "connect", client, generation, lease });
		} else if (
			message.kind === "connected" &&
			message.generation === generation &&
			!connected
		) {
			connected = true;
			clearTimeout(deadline);
			session.connected();
		} else if (
			message.kind === "output" &&
			message.generation === generation &&
			!failure
		) {
			session.receive(message.message);
		} else if (
			message.kind === "disconnected" &&
			message.generation === generation
		) {
			finishClose?.(
				message.error ? deserializeWorkerError(message.error) : undefined,
			);
		}
	};
	return {
		postMessage(message) {
			if (closed || failure)
				throw (
					failure ??
					repositoryError("LIX_ERROR_CLOSED", "Repository connection closed")
				);
			session.post(message);
		},
		onMessage(callback) {
			listener = callback;
		},
		onFatal(callback) {
			fatal = callback;
			if (failure) callback(failure);
		},
		ref() {},
		unref() {},
		terminate() {
			return (termination ??= (async () => {
				closed = true;
				clearInterval(poll);
				clearTimeout(deadline);
				session.close();
				try {
					if (connected && generation && !failure)
						await new Promise<void>((resolve, reject) => {
							const timer = setTimeout(
								() =>
									reject(
										repositoryError(
											"LIX_OWNER_CLOSE_FAILED",
											"Repository client close was not acknowledged",
										),
									),
								CLOSE_TIMEOUT_MS,
							);
							finishClose = (error) => {
								clearTimeout(timer);
								error ? reject(error) : resolve();
							};
							send({ kind: "disconnect", client, generation: generation! });
							release();
						});
				} finally {
					release();
					channel.close();
					retained.worker.removeEventListener("error", workerError);
					if (--retained.references === 0) {
						// Remaining remote sessions retain the owner until their hosts close.
						retained.worker.postMessage({ kind: "release" });
						if (hubs.get(key) === retained) hubs.delete(key);
					}
				}
			})());
		},
	};
}
