import type { LixBinding } from "./binding-types.js";
import type { JsonValue } from "./types.js";
import { Value } from "./value.js";

export const ACTIVE_ACCOUNT_CLIENT_STATE_KEY = "lix_active_account_id";

export type LixClientState = {
	/** Reads a client-local value through the configured client storage. */
	get<T extends JsonValue = JsonValue>(key: string): Promise<T | undefined>;
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
		get: async () => undefined,
		set: async () => {
			throw unavailable();
		},
		delete: async () => {
			throw unavailable();
		},
		subscribe: () => () => undefined,
	};
}

type ClientStateBinding = LixBinding;

export type OpenClientStateOptions = {
	readonly binding: ClientStateBinding;
	readonly closeBinding?: boolean;
};

/**
 * Opens the typed client-state facade over a private local Rust Lix.
 *
 * Values are ordinary global, untracked `lix_key_value` rows. The physical
 * prefix is intentionally private so built-in Lix key/value rows never leak
 * through this small API.
 */
export function openClientState(
	options: OpenClientStateOptions,
): ManagedLixClientState {
	if (
		!options.binding.clientStateGet ||
		!options.binding.clientStateSet ||
		!options.binding.clientStateDelete
	) {
		throw new Error(
			"The selected Lix binding does not support typed client state",
		);
	}
	return new ManagedLixClientState(options);
}

export class ManagedLixClientState implements LixClientState {
	readonly #binding: ClientStateBinding;
	readonly #closeBinding: boolean;
	readonly #listeners = new Set<() => void>();
	#operationQueue: Promise<void> = Promise.resolve();
	#closePromise: Promise<void> | undefined;
	#acceptingOperations = true;

	constructor(options: OpenClientStateOptions) {
		this.#binding = options.binding;
		this.#closeBinding = options.closeBinding ?? false;
	}

	get<T extends JsonValue = JsonValue>(key: string): Promise<T | undefined> {
		assertClientStateKey(key);
		this.#assertOpen();
		return this.#enqueue(async () => {
			const get = this.#binding.clientStateGet;
			if (!get) throw new Error("Typed Lix client state is unavailable");
			const value = await get.call(this.#binding, key);
			if (value === undefined) return undefined;
			assertJsonValue(value);
			return cloneJsonValue(value) as T;
		});
	}

	set(key: string, value: JsonValue): Promise<void> {
		assertClientStateKey(key);
		assertJsonValue(value);
		this.#assertOpen();
		const nextValue = cloneJsonValue(value);
		return this.#enqueue(async () => {
			const set = this.#binding.clientStateSet;
			if (!set) throw new Error("Typed Lix client state is unavailable");
			await set.call(this.#binding, key, nextValue);
			this.#publish();
		});
	}

	delete(key: string): Promise<void> {
		assertClientStateKey(key);
		this.#assertOpen();
		return this.#enqueue(async () => {
			const deleteValue = this.#binding.clientStateDelete;
			if (!deleteValue)
				throw new Error("Typed Lix client state is unavailable");
			await deleteValue.call(this.#binding, key);
			this.#publish();
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

	#enqueue<T>(operation: () => Promise<T>): Promise<T> {
		const result = this.#operationQueue.then(operation, operation);
		this.#operationQueue = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
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

function assertClientStateKey(key: string): void {
	if (typeof key !== "string" || key.length === 0) {
		throw new TypeError("clientState key must be a non-empty string");
	}
}

function assertJsonValue(value: JsonValue): void {
	// Value.jsonb owns the SDK's full JSON validation, including finite numbers,
	// well-formed strings, plain objects, and cycle detection.
	Value.jsonb(value);
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
