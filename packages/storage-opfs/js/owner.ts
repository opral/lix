import { OpfsBackend } from "./provider.js";
import {
	OPFS_RPC_CHANNEL,
	serializeError,
	type OpfsBeginReadPayload,
	type OpfsCommitPayload,
	type OpfsRpcRequest,
	type OpfsRpcResponse,
	type OpfsScanPagePayload,
	type OpfsStorageState,
} from "./rpc.js";

type BackendEntry = {
	backend?: OpfsBackend;
	epoch?: string;
	opening?: Promise<OpfsBackend | undefined>;
	fatalError?: unknown;
	clients: Map<string, number>;
	queue: Promise<void>;
	idleTimer?: ReturnType<typeof setTimeout>;
};

const backends = new Map<string, BackendEntry>();
const channel = new BroadcastChannel(OPFS_RPC_CHANNEL);
const inFlightRequests = new Map<string, Promise<void>>();
const completedRequests = new Map<string, number>();
const MAX_WARM_IDLE_BACKENDS = 2;
const COMPLETED_REQUEST_TTL_MS = 30_000;

setInterval(() => {
	const now = Date.now();
	const cutoff = now - 15_000;
	for (const [name, entry] of backends) {
		for (const [clientId, lastSeen] of entry.clients) {
			if (lastSeen < cutoff) entry.clients.delete(clientId);
		}
		scheduleIdleClose(name, entry);
	}
	pruneCompletedRequests(now);
}, 5_000);

channel.onmessage = (event: MessageEvent<OpfsRpcRequest>) => {
	const request = event.data;
	if (!request || request.kind !== "request") return;
	// BroadcastChannel retries can deliver the same request while a SQLite
	// operation is still running, and queued retry events can arrive after it
	// completes. Retain completed IDs past the client's retry deadline so one
	// logical read cannot be replayed into an ever-growing owner backlog.
	if (
		inFlightRequests.has(request.requestId) ||
		completedRequests.has(request.requestId)
	) {
		return;
	}
	const pending = dispatch(request).then((responded) => {
		if (responded) rememberCompletedRequest(request.requestId);
	});
	inFlightRequests.set(request.requestId, pending);
	void pending
		.finally(() => inFlightRequests.delete(request.requestId))
		.catch(() => undefined);
};

async function dispatch(request: OpfsRpcRequest): Promise<boolean> {
	const entry = getEntry(request.storageName);
	if (request.operation === "close") {
		entry.clients.delete(request.clientId);
		scheduleIdleClose(request.storageName, entry);
		postResponse({
			kind: "response",
			requestId: request.requestId,
			clientId: request.clientId,
			ok: true,
			result: undefined,
		});
		return true;
	}
	entry.clients.set(request.clientId, Date.now());
	if (entry.idleTimer) {
		clearTimeout(entry.idleTimer);
		entry.idleTimer = undefined;
	}
	const backend = await ensureBackend(request.storageName, entry);
	// A different tab may already own this name. Its owner worker will answer
	// the broadcast request; relay workers intentionally stay silent so the
	// first response cannot be mistaken for a failure.
	if (!backend) {
		if (entry.fatalError) {
			postResponse({
				kind: "response",
				requestId: request.requestId,
				clientId: request.clientId,
				ok: false,
				error: serializeError(entry.fatalError),
			});
			return true;
		}
		return false;
	}
	const operation = entry.queue.then(async () => {
		switch (request.operation) {
			case "open":
				return storageState(request.storageName, entry, backend);
			case "heartbeat":
				entry.clients.set(request.clientId, Date.now());
				if (entry.idleTimer) {
					clearTimeout(entry.idleTimer);
					entry.idleTimer = undefined;
				}
				return storageState(request.storageName, entry, backend);
			case "beginRead": {
				const read = await backend.beginRead(
					request.payload as OpfsBeginReadPayload,
				);
				return {
					generation: Number(read.snapshotCacheKey() ?? "0"),
					snapshotCacheKey: read.snapshotCacheKey() ?? "0",
					ownerEpoch: entry.epoch!,
				};
			}
			case "readMany": {
				const payload = request.payload as {
					requests: Parameters<OpfsBackend["readMany"]>[0];
					generation: number;
					ownerEpoch: string;
				};
				assertOwnerEpoch(entry, payload.ownerEpoch);
				return backend.readMany(payload.requests, payload.generation);
			}
			case "scanPage": {
				const payload = request.payload as OpfsScanPagePayload;
				assertOwnerEpoch(entry, payload.ownerEpoch);
				return backend.scanPage(payload);
			}
			case "commit": {
				const payload = request.payload as OpfsCommitPayload;
				backend.commitChanges(payload);
				announceState(request.storageName, entry, backend);
				return { stats: payload.stats };
			}
		}
	});
	entry.queue = operation.then(
		() => undefined,
		() => undefined,
	);
	try {
		const result = await operation;
		postResponse({
			kind: "response",
			requestId: request.requestId,
			clientId: request.clientId,
			ok: true,
			result,
		});
		return true;
	} catch (error) {
		postResponse({
			kind: "response",
			requestId: request.requestId,
			clientId: request.clientId,
			ok: false,
			error: serializeError(error),
		});
		return true;
	}
}

function rememberCompletedRequest(requestId: string): void {
	completedRequests.set(requestId, Date.now());
	pruneCompletedRequests(Date.now());
}

function pruneCompletedRequests(now: number): void {
	const cutoff = now - COMPLETED_REQUEST_TTL_MS;
	for (const [requestId, completedAt] of completedRequests) {
		if (completedAt >= cutoff) break;
		completedRequests.delete(requestId);
	}
}

function getEntry(name: string): BackendEntry {
	let entry = backends.get(name);
	if (!entry) {
		entry = {
			clients: new Map(),
			queue: Promise.resolve(),
		};
		backends.set(name, entry);
	}
	return entry;
}

async function ensureBackend(
	name: string,
	entry: BackendEntry,
): Promise<OpfsBackend | undefined> {
	if (entry.backend) return entry.backend;
	if (entry.fatalError) return undefined;
	if (!entry.opening) {
		entry.opening = OpfsBackend.open(name)
			.then((backend) => {
				entry.backend = backend;
				entry.epoch = crypto.randomUUID();
				return backend;
			})
			.catch((error) => {
				if (isAlreadyOwned(error)) return undefined;
				entry.fatalError = error;
				return undefined;
			})
			.finally(() => {
				entry.opening = undefined;
			});
	}
	return entry.opening;
}

function assertOwnerEpoch(entry: BackendEntry, ownerEpoch: string): void {
	if (entry.epoch !== ownerEpoch) {
		throw storageError(
			"LIX_STORAGE_READ_EXPIRED",
			"read transaction belongs to a previous OPFS owner",
		);
	}
}

function isAlreadyOwned(error: unknown): boolean {
	return (
		error instanceof Error &&
		((error as Error & { code?: unknown }).code === "LIX_STORAGE_FENCED" ||
			error.message.includes("already open"))
	);
}

function scheduleIdleClose(name: string, entry: BackendEntry): void {
	if (entry.clients.size > 0 || entry.idleTimer) return;
	// Keep the SQLite/OPFS handle warm for the common close/reopen path, but
	// bound that cache: every distinct SAH-pool VFS owns browser resources, and
	// a burst across repositories must not stall the next open.
	const warmIdle = [...backends].filter(
		([otherName, other]) => otherName !== name && other.idleTimer,
	);
	while (warmIdle.length >= MAX_WARM_IDLE_BACKENDS) {
		const oldest = warmIdle.shift();
		if (oldest) closeIdleBackend(...oldest);
	}
	entry.idleTimer = setTimeout(() => {
		closeIdleBackend(name, entry);
	}, 30_000);
}

function closeIdleBackend(name: string, entry: BackendEntry): void {
	if (entry.clients.size > 0) return;
	if (entry.idleTimer) clearTimeout(entry.idleTimer);
	entry.idleTimer = undefined;
	backends.delete(name);
	if (entry.backend) void entry.backend.close().catch(() => undefined);
}

function postResponse(response: OpfsRpcResponse): void {
	channel.postMessage(response);
}

function storageState(
	storageName: string,
	entry: BackendEntry,
	backend: OpfsBackend,
): OpfsStorageState {
	return {
		kind: "storageState",
		storageName,
		ownerEpoch: entry.epoch!,
		generation: backend.currentGeneration(),
	};
}

function announceState(
	storageName: string,
	entry: BackendEntry,
	backend: OpfsBackend,
): void {
	channel.postMessage(storageState(storageName, entry, backend));
}

function storageError(code: string, message: string): Error & { code: string } {
	const error = new Error(message) as Error & { code: string };
	error.name = "LixStorageError";
	error.code = code;
	return error;
}
