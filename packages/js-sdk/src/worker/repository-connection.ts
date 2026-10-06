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
type RepositoryHub = {
	worker: Worker;
	references: number;
	buildId?: string;
	failure?: Error;
	failures: Set<(error: Error) => void>;
};
const hubs = new Map<string, RepositoryHub>();
// Keep only a successfully retired realm warm; it owns no repository or storage.
let idleWorker: Worker | undefined;
let idleWarm = false;
let idleFailure: (() => void) | undefined;
function takeIdleWorker(): Worker | undefined {
	const worker = idleWorker;
	if (worker && idleFailure) {
		worker.removeEventListener("error", idleFailure);
		worker.removeEventListener("messageerror", idleFailure);
	}
	idleWorker = undefined;
	idleFailure = undefined;
	return worker;
}
function cacheIdleWorker(worker: Worker, runtimeWarm: boolean): void {
	// Standby candidates never initialize the compiler. Do not let their later
	// retirement evict the realm whose expensive runtime is already ready.
	if (idleWorker && idleWarm && !runtimeWarm) {
		worker.terminate();
		return;
	}
	takeIdleWorker()?.terminate();
	idleWorker = worker;
	idleWarm = runtimeWarm;
	idleFailure = () => {
		if (idleWorker === worker) takeIdleWorker()?.terminate();
	};
	worker.addEventListener("error", idleFailure);
	worker.addEventListener("messageerror", idleFailure);
}
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
		const worker = takeIdleWorker() ?? new Worker(
			new URL("./entry.repository.browser.js", import.meta.url),
			{
				type: "module",
				name: "lix-repository-owner",
			},
		);
		let token = crypto.randomUUID();
		hub = { worker, references: 0, failures: new Set() };
		hubs.set(key, hub);
		const created = hub;
		const invalidate = (error: Error) => {
			created.failure ??= error;
			if (hubs.get(key) === created) hubs.delete(key);
			worker.terminate();
			for (const fail of created.failures) fail(created.failure);
		};
		const onWorkerError = (event: ErrorEvent) => invalidate(repositoryError("LIX_WORKER_FAILED", event.message));
		const onMessageError = () => invalidate(repositoryError("LIX_WORKER_FAILED", "Repository worker message could not be decoded"));
		const onWorkerMessage = (event: MessageEvent) => {
			if (event.data?.token !== token) return;
			if (event.data?.kind === "build" && typeof event.data.buildId === "string")
				created.buildId = event.data.buildId;
			if (event.data?.kind === "failure") {
				invalidate(repositoryError("LIX_WORKER_FAILED", event.data.message));
				return;
			}
			if (event.data?.kind === "retired") {
				if (created.references > 0) {
					if (event.data.reusable !== true || created.failure) {
						invalidate(repositoryError("LIX_WORKER_FAILED", "Repository worker cleanup failed during reopen"));
						return;
					}
					// A local reopen can race the last remote session's retirement.
					// Restart only after cleanup, in the same warmed realm.
					token = crypto.randomUUID();
					worker.postMessage({ kind: "start", key, channelName, token });
					return;
				}
				worker.removeEventListener("message", onWorkerMessage);
				worker.removeEventListener("error", onWorkerError);
				worker.removeEventListener("messageerror", onMessageError);
				if (hubs.get(key) === created) hubs.delete(key);
				if (event.data.reusable === true && !created.failure) cacheIdleWorker(worker, event.data.runtimeWarm === true);
				else worker.terminate();
			}
		};
		worker.addEventListener("message", onWorkerMessage);
		worker.addEventListener("error", onWorkerError);
		worker.addEventListener("messageerror", onMessageError);
		worker.postMessage({ kind: "start", key, channelName, token });
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
	retained.failures.add(fail);
	if (retained.failure) queueMicrotask(() => fail(retained.failure!));
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
					retained.failures.delete(fail);
					if (--retained.references === 0) {
						// Remote sessions can still retain this host. Keep its hub until
						// retirement so a same-key reopen retains or restarts this realm.
						retained.worker.postMessage({ kind: "release" });
					}
				}
			})());
		},
	};
}
