import type {
	LixStorageCommitResult,
	LixStorageChangeWatch,
	LixStorageErrorCode,
	LixStorageGetManyRequest,
	LixStorageKeyRange,
	LixStorageProjectedValue,
	LixStorageProvider,
	LixStoragePutEntry,
	LixStorageRead,
	LixStorageReadOptions,
	LixStorageScanOrder,
	LixStorageScanSource,
	LixStorageSpace,
	LixStorageWrite,
	LixStorageWriteOptions,
	LixStorageWriteStats,
} from "@lix-js/sdk";
import { StorageChangeNotifier } from "./change-watch.js";
import { deserializeError, OPFS_RPC_CHANNEL, type OpfsChannelMessage, type OpfsCommitPayload, type OpfsOpenResult, type OpfsRpcRequest, type OpfsScanPagePayload } from "./rpc.js";

export async function createLixStorageProvider(options: unknown): Promise<LixStorageProvider> {
	if (!options || typeof options !== "object" || !("name" in options) || typeof options.name !== "string" || options.name.length === 0) {
		throw new TypeError("OPFS storage provider requires a non-empty name");
	}
	const value = options as { name: string; mode?: unknown; channelName?: unknown };
	if (value.mode !== "shared" || value.channelName !== OPFS_RPC_CHANNEL) {
		throw storageError("LIX_STORAGE_UNSUPPORTED", "OPFS provider client requires a package-owned owner worker");
	}
	return OpfsStorageClient.open(value.name);
}

export class OpfsStorageClient implements LixStorageProvider {
	readonly #channel: BroadcastChannel;
	readonly #clientId = crypto.randomUUID();
	readonly #pending = new Map<string, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
	readonly #changes = new StorageChangeNotifier();
	#storageState: string | undefined;
	#heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	#heartbeatPending = false;
	#closed = false;

	private constructor(
		private readonly name: string,
		channelName: string,
	) {
		this.#channel = new BroadcastChannel(channelName);
		this.#channel.onmessage = (event: MessageEvent<OpfsChannelMessage>) => {
			const response = event.data;
			if (!response) return;
			if (response.kind === "storageState") {
				if (response.storageName === this.name) {
					this.#acceptStorageState(response.ownerEpoch, response.generation);
				}
				return;
			}
			if (response.kind !== "response" || response.clientId !== this.#clientId) return;
			const pending = this.#pending.get(response.requestId);
			if (!pending) return;
			this.#pending.delete(response.requestId);
			if (response.ok) pending.resolve(response.result);
			else pending.reject(deserializeError(response.error));
		};
	}

	static async open(
		name: string,
		channelName = OPFS_RPC_CHANNEL,
	): Promise<OpfsStorageClient> {
		if (typeof BroadcastChannel === "undefined") throw storageError("LIX_STORAGE_UNSUPPORTED", "OPFS shared storage requires BroadcastChannel");
		const client = new OpfsStorageClient(name, channelName);
		try {
			const state = (await client.#rpc("open", undefined, true)) as OpfsOpenResult;
			client.#acceptStorageState(state.ownerEpoch, state.generation);
			client.#heartbeatTimer = setInterval(() => void client.refreshState(), 5_000);
			return client;
		} catch (error) {
			client.#channel.close();
			throw error;
		}
	}

	async beginRead(options: LixStorageReadOptions): Promise<LixStorageRead> {
		this.#assertOpen();
		const result = (await this.#rpc("beginRead", options, true)) as { generation: number; snapshotCacheKey: string; ownerEpoch: string };
		this.#acceptStorageState(result.ownerEpoch, result.generation);
		return new RemoteRead(this, result.generation, result.snapshotCacheKey, result.ownerEpoch);
	}

	async beginWrite(options: LixStorageWriteOptions) {
		this.#assertOpen();
		return new RemoteWrite(this, options);
	}

	async watchForChanges(): Promise<LixStorageChangeWatch> {
		this.#assertOpen();
		return this.#changes.watch();
	}

	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		try { await this.#rpc("close", undefined, false); }
		finally {
			if (this.#heartbeatTimer) clearInterval(this.#heartbeatTimer);
			for (const pending of this.#pending.values()) pending.reject(storageError("LIX_STORAGE_CLOSED", "storage client is closed"));
			this.#pending.clear();
			this.#changes.close(storageError("LIX_STORAGE_CLOSED", "storage client is closed"));
			this.#channel.close();
		}
	}

	/** Package-internal liveness probe which also repairs a missed announcement. */
	async refreshState(): Promise<void> {
		if (this.#closed || this.#heartbeatPending) return;
		this.#heartbeatPending = true;
		try {
			const state = (await this.#rpc("heartbeat", undefined, true)) as OpfsOpenResult;
			this.#acceptStorageState(state.ownerEpoch, state.generation);
		} catch {
			// Ordinary operations retain their own error semantics. A liveness probe
			// is best-effort and the next interval retries owner discovery.
		} finally {
			this.#heartbeatPending = false;
		}
	}

	readMany(requests: LixStorageGetManyRequest[], generation: number, ownerEpoch: string) {
		return this.#rpc("readMany", { requests, generation, ownerEpoch }, true) as Promise<Array<LixStorageProjectedValue | null>>;
	}

	scanPage(payload: OpfsScanPagePayload) {
		return this.#rpc("scanPage", payload, true) as Promise<{ entries: Array<{ key: Uint8Array; value: LixStorageProjectedValue }>; hasMore: boolean }>;
	}

	commit(payload: OpfsCommitPayload) {
		return this.#rpc("commit", payload, false) as Promise<LixStorageCommitResult>;
	}

	async #rpc(operation: OpfsRpcRequest["operation"], payload: unknown, retry: boolean): Promise<unknown> {
		if (this.#closed && operation !== "close") throw storageError("LIX_STORAGE_CLOSED", "storage client is closed");
		const requestId = crypto.randomUUID();
		const request: OpfsRpcRequest = { kind: "request", requestId, clientId: this.#clientId, storageName: this.name, operation, payload };
		return new Promise((resolve, reject) => {
			let done = false;
			const finish = (fn: (value: unknown) => void, value: unknown) => {
				if (done) return;
				done = true;
				clearTimeout(timeout);
				if (retryTimer) clearInterval(retryTimer);
				this.#pending.delete(requestId);
				fn(value);
			};
			const timeout = setTimeout(() => finish(
				reject,
				storageError(
					operation === "commit" ? "LIX_STORAGE_COMMIT_OUTCOME_UNKNOWN" : "LIX_STORAGE_IO",
					`OPFS storage owner did not answer ${operation} within 15 seconds`,
				),
			), 15_000);
			const retryTimer = retry ? setInterval(() => this.#channel.postMessage(request), 50) : undefined;
			this.#pending.set(requestId, { resolve: (value) => finish(resolve, value), reject: (error) => finish(reject, error) });
			this.#channel.postMessage(request);
		});
	}

	#assertOpen() { if (this.#closed) throw storageError("LIX_STORAGE_CLOSED", "storage client is closed"); }

	#acceptStorageState(ownerEpoch: string, generation: number): void {
		const next = `${ownerEpoch}:${generation}`;
		if (this.#storageState === undefined) {
			this.#storageState = next;
			return;
		}
		if (this.#storageState === next) return;
		this.#storageState = next;
		this.#changes.notify();
	}
}

class RemoteRead implements LixStorageRead {
	constructor(private readonly client: OpfsStorageClient, private readonly generation: number, private readonly cacheKey: string, private readonly ownerEpoch: string) {}
	// The owner epoch is not a decimal u128. Disable derived-value caching so a
	// handoff cannot reuse a cache entry from the previous owner generation.
	snapshotCacheKey(): undefined { return undefined; }
	getMany(requests: LixStorageGetManyRequest[]) { return this.client.readMany(requests, this.generation, this.ownerEpoch); }
	beginScan(space: LixStorageSpace, range: LixStorageKeyRange, options: { projection: "keyOnly" | "fullValue"; order: LixStorageScanOrder }): Promise<LixStorageScanSource> {
		return Promise.resolve(new RemoteScan(this.client, this.generation, this.ownerEpoch, space, range, options));
	}
}

class RemoteScan implements LixStorageScanSource {
	#after: Uint8Array | undefined;
	constructor(private readonly client: OpfsStorageClient, private readonly generation: number, private readonly ownerEpoch: string, private readonly space: LixStorageSpace, private readonly range: LixStorageKeyRange, private readonly options: { projection: "keyOnly" | "fullValue"; order: LixStorageScanOrder }) {}
	nextPage(limitRows: number) {
		return this.client.scanPage({ space: this.space, range: this.range, after: this.#after, limit: limitRows, order: this.options.order, projection: this.options.projection, generation: this.generation, ownerEpoch: this.ownerEpoch }).then((page) => { this.#after = page.entries.at(-1)?.key; return page; });
	}
}

class RemoteWrite implements LixStorageWrite {
	readonly #puts = new Map<string, { space: LixStorageSpace; key: Uint8Array; value: Uint8Array }>();
	readonly #deletes = new Map<string, { space: LixStorageSpace; key: Uint8Array }>();
	readonly #deleteRanges: Array<{ space: LixStorageSpace; range: LixStorageKeyRange }> = [];
	readonly #immutablePuts = new Map<string, { space: LixStorageSpace; key: Uint8Array; value: Uint8Array }>();
	readonly #stats: LixStorageWriteStats = { putEntries: 0, deletedEntries: 0, deletedRanges: 0, writtenBytes: 0, storageCalls: 0 };
	#closed = false;
	constructor(private readonly client: OpfsStorageClient, private readonly options: LixStorageWriteOptions) {}
	async putMany(space: LixStorageSpace, entries: LixStoragePutEntry[]) {
		this.#assertOpen();
		for (const entry of entries) {
			const id = storageKey(space.id, entry.key);
			const staged = { space, key: new Uint8Array(entry.key), value: new Uint8Array(entry.value) };
			if (space.valueSemantics === "immutable") {
				const existing = this.#immutablePuts.get(id);
				if (existing) {
					if (!bytesEqual(existing.value, entry.value)) throw immutableValueError();
					continue;
				}
				this.#immutablePuts.set(id, staged);
			}
			this.#stats.putEntries += 1; this.#stats.writtenBytes += entry.value.byteLength;
			this.#deletes.delete(id); this.#puts.set(id, staged);
		}
		this.#stats.storageCalls += 1;
	}
	async deleteMany(space: LixStorageSpace, keys: Uint8Array[]) {
		this.#assertOpen();
		for (const key of keys) { const id = storageKey(space.id, key); this.#puts.delete(id); this.#deletes.set(id, { space, key: new Uint8Array(key) }); }
		this.#stats.deletedEntries += keys.length; this.#stats.storageCalls += 1;
	}
	async deleteRange(space: LixStorageSpace, range: LixStorageKeyRange) {
		this.#assertOpen();
		let removedPuts = 0;
		for (const [id, entry] of this.#puts) {
			if (entry.space.id === space.id && rangeContains(range, entry.key)) {
				this.#puts.delete(id);
				removedPuts += 1;
			}
		}
		this.#stats.deletedEntries += removedPuts;
		this.#stats.deletedRanges += 1; this.#stats.storageCalls += 1; this.#deleteRanges.push({ space, range });
	}
	async commit() {
		this.#assertOpen(); this.#closed = true;
		return this.client.commit({ deletes: [...this.#deletes.values()], puts: [...this.#puts.values()], deleteRanges: this.#deleteRanges, immutablePuts: [...this.#immutablePuts.values()], preconditions: this.options.preconditions, strictDurability: this.options.awaitDurable, stats: { ...this.#stats } });
	}
	async rollback() { this.#assertOpen(); this.#closed = true; }
	#assertOpen() { if (this.#closed) throw storageError("LIX_STORAGE_CLOSED", "storage write is closed"); }
}

function storageKey(space: number, key: Uint8Array) { return `${space}:${Array.from(key, (byte) => byte.toString(16).padStart(2, "0")).join("")}`; }
function bytesEqual(left: Uint8Array, right: Uint8Array) { return left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index]); }
function compareBytes(left: Uint8Array, right: Uint8Array) { for (let i = 0; i < Math.min(left.length, right.length); i += 1) if (left[i] !== right[i]) return left[i]! - right[i]!; return left.length - right.length; }
function rangeContains(range: LixStorageKeyRange, key: Uint8Array) {
	const lower = range.lower.kind === "unbounded" || (range.lower.kind === "included" ? compareBytes(key, range.lower.key) >= 0 : compareBytes(key, range.lower.key) > 0);
	const upper = range.upper.kind === "unbounded" || (range.upper.kind === "included" ? compareBytes(key, range.upper.key) <= 0 : compareBytes(key, range.upper.key) < 0);
	return lower && upper;
}
function immutableValueError() { return storageError("LIX_STORAGE_CORRUPTION", "immutable identity was assigned different bytes"); }
function storageError(code: LixStorageErrorCode, message: string, details?: unknown) { const error = new Error(message) as Error & { code: LixStorageErrorCode; details?: unknown }; error.name = "LixStorageError"; Object.assign(error, { code, details }); return error; }
