import type { LixSnapshotStorage } from "./types.js";

const SNAPSHOT_FORMAT = "lix-snapshot";
const SNAPSHOT_VERSION = 1;
const DEFAULT_PREFIX = "@lix-js/sdk/snapshot/v1";

/** The subset of the browser Storage API used by {@link LocalStorage}. */
export type WebStorageLike = Pick<Storage, "getItem" | "setItem">;

export type LocalStorageOptions = {
	/**
	 * Storage implementation to wrap. Defaults to `globalThis.localStorage`.
	 * Supplying this is useful for non-browser hosts and tests.
	 */
	storage?: WebStorageLike;
	/** Prefix used to isolate Lix snapshots from other application records. */
	prefix?: string;
};

type SnapshotEnvelope = {
	format: typeof SNAPSHOT_FORMAT;
	version: typeof SNAPSHOT_VERSION;
	data: string;
};

/**
 * Persists opaque Lix snapshots in the browser's localStorage.
 *
 * Import this adapter from `@lix-js/sdk/local-storage-adapter`. Keeping it in
 * a separate entrypoint avoids referencing browser storage in applications
 * that do not use it.
 */
export class LocalStorage implements LixSnapshotStorage {
	readonly #storage: WebStorageLike;
	readonly #prefix: string;

	constructor(options: LocalStorageOptions = {}) {
		if (!options || typeof options !== "object" || Array.isArray(options)) {
			throw new TypeError("LocalStorage options must be an object");
		}
		if (
			options.prefix !== undefined &&
			(typeof options.prefix !== "string" || options.prefix.length === 0)
		) {
			throw new TypeError("LocalStorage prefix must be a non-empty string");
		}
		const storage = options.storage ?? defaultLocalStorage();
		if (
			!storage ||
			typeof storage.getItem !== "function" ||
			typeof storage.setItem !== "function"
		) {
			throw new TypeError(
				"LocalStorage storage must implement getItem() and setItem()",
			);
		}
		this.#storage = storage;
		this.#prefix = options.prefix ?? DEFAULT_PREFIX;
	}

	async load(namespace: string): Promise<Uint8Array | undefined> {
		const key = this.#key(namespace);
		const stored = this.#storage.getItem(key);
		if (stored === null) return undefined;

		let value: unknown;
		try {
			value = JSON.parse(stored);
		} catch (error) {
			throw invalidSnapshot(key, "record is not valid JSON", error);
		}
		if (!isSnapshotEnvelope(value)) {
			throw invalidSnapshot(key, "record has an unsupported format or version");
		}
		try {
			return base64ToBytes(value.data);
		} catch (error) {
			throw invalidSnapshot(key, "record contains invalid base64", error);
		}
	}

	async save(namespace: string, snapshot: Uint8Array): Promise<void> {
		const key = this.#key(namespace);
		if (!(snapshot instanceof Uint8Array)) {
			throw new TypeError("LocalStorage snapshot must be a Uint8Array");
		}
		const envelope: SnapshotEnvelope = {
			format: SNAPSHOT_FORMAT,
			version: SNAPSHOT_VERSION,
			data: bytesToBase64(snapshot),
		};
		this.#storage.setItem(key, JSON.stringify(envelope));
	}

	#key(namespace: string): string {
		if (typeof namespace !== "string" || namespace.length === 0) {
			throw new TypeError(
				"LocalStorage snapshot namespace must be a non-empty string",
			);
		}
		return `${this.#prefix}:${encodeURIComponent(namespace)}`;
	}
}

export type { LixSnapshotStorage } from "./types.js";

function defaultLocalStorage(): WebStorageLike {
	const storage = globalThis.localStorage;
	if (storage === undefined) {
		throw new Error(
			"LocalStorage requires browser localStorage or an explicit storage option",
		);
	}
	return storage;
}

function isSnapshotEnvelope(value: unknown): value is SnapshotEnvelope {
	if (!value || typeof value !== "object" || Array.isArray(value)) return false;
	const envelope = value as Record<string, unknown>;
	return (
		envelope.format === SNAPSHOT_FORMAT &&
		envelope.version === SNAPSHOT_VERSION &&
		typeof envelope.data === "string"
	);
}

function bytesToBase64(bytes: Uint8Array): string {
	let binary = "";
	const chunkSize = 0x8000;
	for (let offset = 0; offset < bytes.length; offset += chunkSize) {
		binary += String.fromCharCode(
			...bytes.subarray(offset, offset + chunkSize),
		);
	}
	return btoa(binary);
}

function base64ToBytes(base64: string): Uint8Array {
	if (!isCanonicalBase64(base64)) {
		throw new Error("invalid base64");
	}
	const binary = atob(base64);
	const bytes = new Uint8Array(binary.length);
	for (let index = 0; index < binary.length; index += 1) {
		bytes[index] = binary.charCodeAt(index);
	}
	return bytes;
}

function isCanonicalBase64(value: string): boolean {
	if (value.length === 0) return true;
	if (value.length % 4 !== 0) return false;
	return /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
		value,
	);
}

function invalidSnapshot(key: string, reason: string, cause?: unknown): Error {
	return new Error(
		`Invalid Lix snapshot in localStorage key '${key}': ${reason}`,
		{
			cause,
		},
	);
}
