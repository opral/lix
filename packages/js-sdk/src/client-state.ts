import type { LixBinding } from "./binding-types.js";
import { isSnapshotPersistenceAfterCommitError } from "./snapshot-persistence.js";
import type { JsonValue, LixSnapshotStorage } from "./types.js";
import { Value } from "./value.js";

export const ACTIVE_BRANCH_CLIENT_STATE_KEY = "lix_active_branch_id";
export const ACTIVE_ACCOUNT_CLIENT_STATE_KEY = "lix_active_account_id";
const STORED_CLIENT_STATE_HEADER = "lix-client-state-v1\n";

export type LixClientState = {
	/** Returns the hydrated client-local value without a network round trip. */
	get<T extends JsonValue = JsonValue>(key: string): T | undefined;
	/** Persists a client-local value in the configured client storage. */
	set(key: string, value: JsonValue): Promise<void>;
	/** Deletes a client-local value from the configured client storage. */
	delete(key: string): Promise<void>;
	/** Subscribes to successful mutations made through this client-state handle. */
	subscribe(listener: () => void): () => void;
};

export type ManagedClientState = LixClientState & {
	close(): Promise<void>;
};

export function unavailableClientState(): LixClientState {
	const unavailable = () => {
		const error = new Error(
			"Lix client state requires client storage; pass storage to openLix()",
		) as Error & { code: string };
		error.name = "LixError";
		error.code = "LIX_CLIENT_STORAGE_REQUIRED";
		return error;
	};
	return {
		get: () => undefined,
		set: async () => {
			throw unavailable();
		},
		delete: async () => {
			throw unavailable();
		},
		subscribe: () => () => undefined,
	};
}

type ClientStateBinding = LixBinding & {
	exportSnapshot?: () => Promise<Uint8Array>;
};

export type OpenClientStateOptions = {
	readonly binding: ClientStateBinding;
	readonly saveSnapshot?: (snapshot: Uint8Array) => Promise<void>;
	readonly closeBinding?: boolean;
};

/**
 * Opens the typed client-state facade over a private local Rust Lix.
 *
 * Values are ordinary global, untracked `lix_key_value` rows. The physical
 * prefix is intentionally private so built-in Lix key/value rows never leak
 * through this small API.
 */
export async function openClientState(
	options: OpenClientStateOptions,
): Promise<ManagedLixClientState> {
	const entries = options.binding.clientStateEntries;
	if (!entries) {
		throw new Error(
			"The selected Lix binding does not support typed client state",
		);
	}
	const initial = new Map<string, JsonValue>();
	for (const entry of await entries.call(options.binding)) {
		assertClientStateKey(entry.key);
		assertJsonValue(entry.value);
		initial.set(entry.key, cloneJsonValue(entry.value));
	}
	return new ManagedLixClientState(options, initial);
}

export class ManagedLixClientState implements LixClientState {
	readonly #binding: ClientStateBinding;
	readonly #saveSnapshot: ((snapshot: Uint8Array) => Promise<void>) | undefined;
	readonly #closeBinding: boolean;
	readonly #values: Map<string, JsonValue>;
	readonly #listeners = new Set<() => void>();
	#operationQueue: Promise<void> = Promise.resolve();
	#closePromise: Promise<void> | undefined;
	#acceptingOperations = true;

	constructor(
		options: OpenClientStateOptions,
		initial: Map<string, JsonValue>,
	) {
		this.#binding = options.binding;
		this.#saveSnapshot = options.saveSnapshot;
		this.#closeBinding = options.closeBinding ?? false;
		this.#values = initial;
	}

	get<T extends JsonValue = JsonValue>(key: string): T | undefined {
		assertClientStateKey(key);
		const value = this.#values.get(key);
		return value === undefined ? undefined : (cloneJsonValue(value) as T);
	}

	set(key: string, value: JsonValue): Promise<void> {
		assertClientStateKey(key);
		assertJsonValue(value);
		this.#assertOpen();
		const nextValue = cloneJsonValue(value);
		return this.#enqueue(async () => {
			const set = this.#binding.clientStateSet;
			if (!set) throw new Error("Typed Lix client state is unavailable");
			try {
				await set.call(this.#binding, key, nextValue);
			} catch (error) {
				if (!isSnapshotPersistenceAfterCommitError(error)) throw error;
				this.#commitSet(key, nextValue);
				throw error;
			}
			this.#commitSet(key, nextValue);
			await this.#persist();
		});
	}

	delete(key: string): Promise<void> {
		assertClientStateKey(key);
		this.#assertOpen();
		return this.#enqueue(async () => {
			const deleteValue = this.#binding.clientStateDelete;
			if (!deleteValue)
				throw new Error("Typed Lix client state is unavailable");
			try {
				await deleteValue.call(this.#binding, key);
			} catch (error) {
				if (!isSnapshotPersistenceAfterCommitError(error)) throw error;
				this.#commitDelete(key);
				throw error;
			}
			this.#commitDelete(key);
			await this.#persist();
		});
	}

	subscribe(listener: () => void): () => void {
		if (typeof listener !== "function") {
			throw new TypeError("clientState.subscribe() requires a function");
		}
		this.#assertOpen();
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	async close(): Promise<void> {
		if (this.#closePromise) return this.#closePromise;
		this.#acceptingOperations = false;
		this.#closePromise = (async () => {
			await this.#operationQueue;
			this.#listeners.clear();
			if (this.#closeBinding) await this.#binding.close();
		})();
		return this.#closePromise;
	}

	#enqueue(operation: () => Promise<void>): Promise<void> {
		const result = this.#operationQueue.then(operation, operation);
		this.#operationQueue = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	async #persist(): Promise<void> {
		if (!this.#saveSnapshot) return;
		if (!this.#binding.exportSnapshot) {
			throw new Error(
				"The selected Lix binding cannot export storage snapshots",
			);
		}
		await this.#saveSnapshot(await this.#binding.exportSnapshot());
	}

	#commitSet(key: string, value: JsonValue): void {
		this.#values.set(key, value);
		this.#publish();
	}

	#commitDelete(key: string): void {
		if (this.#values.delete(key)) this.#publish();
	}

	#publish(): void {
		for (const listener of [...this.#listeners]) {
			try {
				listener();
			} catch {
				// Subscribers do not participate in the completed local transaction.
			}
		}
	}

	#assertOpen(): void {
		if (!this.#acceptingOperations) {
			throw new Error("Lix client state is closed");
		}
	}
}

/**
 * Opens client state directly over snapshot storage without starting a local
 * Lix runtime. This is used by remote Lix connections, where the storage
 * option persists client-local state rather than the remote workspace.
 */
export async function openStoredClientState(options: {
	readonly storage: LixSnapshotStorage;
	readonly namespace: string;
}): Promise<ManagedClientState> {
	const snapshot = await options.storage.load(options.namespace);
	if (snapshot !== undefined && !(snapshot instanceof Uint8Array)) {
		throw new TypeError("Client-state storage load() must return a Uint8Array");
	}
	return new StoredClientState(
		options.storage,
		options.namespace,
		decodeStoredClientState(snapshot),
	);
}

class StoredClientState implements ManagedClientState {
	readonly #storage: LixSnapshotStorage;
	readonly #namespace: string;
	readonly #values: Map<string, JsonValue>;
	readonly #listeners = new Set<() => void>();
	#operationQueue: Promise<void> = Promise.resolve();
	#closePromise: Promise<void> | undefined;
	#acceptingOperations = true;
	#dirty = false;

	constructor(
		storage: LixSnapshotStorage,
		namespace: string,
		values: Map<string, JsonValue>,
	) {
		this.#storage = storage;
		this.#namespace = namespace;
		this.#values = values;
	}

	get<T extends JsonValue = JsonValue>(key: string): T | undefined {
		assertClientStateKey(key);
		const value = this.#values.get(key);
		return value === undefined ? undefined : (cloneJsonValue(value) as T);
	}

	set(key: string, value: JsonValue): Promise<void> {
		assertClientStateKey(key);
		assertJsonValue(value);
		this.#assertOpen();
		const nextValue = cloneJsonValue(value);
		return this.#enqueue(async () => {
			this.#values.set(key, nextValue);
			this.#dirty = true;
			this.#publish();
			await this.#persist();
		});
	}

	delete(key: string): Promise<void> {
		assertClientStateKey(key);
		this.#assertOpen();
		return this.#enqueue(async () => {
			if (this.#values.delete(key)) this.#publish();
			this.#dirty = true;
			await this.#persist();
		});
	}

	subscribe(listener: () => void): () => void {
		if (typeof listener !== "function") {
			throw new TypeError("clientState.subscribe() requires a function");
		}
		this.#assertOpen();
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	async close(): Promise<void> {
		if (this.#closePromise) return this.#closePromise;
		this.#acceptingOperations = false;
		this.#closePromise = (async () => {
			await this.#operationQueue;
			if (this.#dirty) await this.#persist();
			this.#listeners.clear();
		})();
		return this.#closePromise;
	}

	#enqueue(operation: () => Promise<void>): Promise<void> {
		const result = this.#operationQueue.then(operation, operation);
		this.#operationQueue = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	async #persist(): Promise<void> {
		await this.#storage.save(
			this.#namespace,
			encodeStoredClientState(this.#values),
		);
		this.#dirty = false;
	}

	#publish(): void {
		for (const listener of [...this.#listeners]) {
			try {
				listener();
			} catch {
				// Subscribers do not participate in the completed mutation.
			}
		}
	}

	#assertOpen(): void {
		if (!this.#acceptingOperations) {
			throw new Error("Lix client state is closed");
		}
	}
}

function encodeStoredClientState(values: Map<string, JsonValue>): Uint8Array {
	const entries = [...values].map(([key, value]) => [
		key,
		cloneJsonValue(value),
	]);
	return new TextEncoder().encode(
		`${STORED_CLIENT_STATE_HEADER}${JSON.stringify(entries)}`,
	);
}

function decodeStoredClientState(
	snapshot: Uint8Array | undefined,
): Map<string, JsonValue> {
	if (snapshot === undefined) return new Map();
	const header = new TextEncoder().encode(STORED_CLIENT_STATE_HEADER);
	if (
		snapshot.length < header.length ||
		header.some((byte, index) => snapshot[index] !== byte)
	) {
		throw new Error("Stored Lix client state header is invalid");
	}

	let parsed: unknown;
	try {
		parsed = JSON.parse(
			new TextDecoder().decode(snapshot.subarray(header.length)),
		);
	} catch (error) {
		throw new Error("Stored Lix client state is invalid", { cause: error });
	}
	if (!Array.isArray(parsed)) {
		throw new Error("Stored Lix client state entries must be an array");
	}

	const values = new Map<string, JsonValue>();
	for (const entry of parsed) {
		if (!Array.isArray(entry) || entry.length !== 2) {
			throw new Error("Stored Lix client state entry is invalid");
		}
		const [key, value] = entry;
		assertClientStateKey(key);
		assertJsonValue(value);
		values.set(key, cloneJsonValue(value));
	}
	return values;
}

function assertClientStateKey(key: string): void {
	if (typeof key !== "string" || key.length === 0) {
		throw new TypeError("clientState key must be a non-empty string");
	}
}

function assertJsonValue(value: JsonValue): void {
	// Value.json owns the SDK's full JSON validation, including finite numbers,
	// well-formed strings, plain objects, and cycle detection.
	Value.json(value);
}

function cloneJsonValue(value: JsonValue): JsonValue {
	if (Array.isArray(value)) return value.map(cloneJsonValue);
	if (value && typeof value === "object") {
		return Object.fromEntries(
			Object.entries(value).map(([key, entry]) => [key, cloneJsonValue(entry)]),
		);
	}
	return value;
}
