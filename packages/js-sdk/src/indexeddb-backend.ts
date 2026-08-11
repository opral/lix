type IndexedDbEntry = {
	key: Uint8Array;
	value: Uint8Array;
};

type IndexedDbChanges = {
	deletes: Uint8Array[];
	puts: IndexedDbEntry[];
	strictDurability: boolean;
};

const DATABASE_VERSION = 1;
const ENTRY_STORE = "entries";

/** Internal worker-local bridge used by the WASM IndexedDB storage adapter. */
export class IndexedDbBackend {
	readonly #database: IDBDatabase;
	readonly #releaseLock: () => void;
	#closed = false;

	private constructor(database: IDBDatabase, releaseLock: () => void) {
		this.#database = database;
		this.#releaseLock = releaseLock;
	}

	static async open(name: string): Promise<IndexedDbBackend> {
		const releaseLock = await acquireDatabaseLock(name);
		try {
			const request = indexedDB.open(name, DATABASE_VERSION);
			request.onupgradeneeded = () => {
				if (!request.result.objectStoreNames.contains(ENTRY_STORE)) {
					request.result.createObjectStore(ENTRY_STORE);
				}
			};
			return new IndexedDbBackend(await openDatabase(request), releaseLock);
		} catch (error) {
			releaseLock();
			throw error;
		}
	}

	async loadEntries(): Promise<IndexedDbEntry[]> {
		const transaction = this.#database.transaction(ENTRY_STORE, "readonly");
		const store = transaction.objectStore(ENTRY_STORE);
		const entries: IndexedDbEntry[] = [];
		await new Promise<void>((resolve, reject) => {
			const request = store.openCursor();
			request.onerror = () => reject(request.error ?? transaction.error);
			request.onsuccess = () => {
				const cursor = request.result;
				if (!cursor) {
					resolve();
					return;
				}
				try {
					entries.push({
						key: copyBytes(cursor.key, "IndexedDB entry key"),
						value: copyBytes(cursor.value, "IndexedDB entry value"),
					});
				} catch (error) {
					transaction.abort();
					reject(error);
					return;
				}
				cursor.continue();
			};
		});
		await transactionDone(transaction);
		return entries;
	}

	async applyChanges(changes: IndexedDbChanges): Promise<void> {
		const transaction = this.#database.transaction(ENTRY_STORE, "readwrite", {
			durability: changes.strictDurability ? "strict" : "default",
		});
		const store = transaction.objectStore(ENTRY_STORE);
		for (const key of changes.deletes) store.delete(binaryKey(key));
		for (const entry of changes.puts) {
			store.put(entry.value, binaryKey(entry.key));
		}
		await transactionDone(transaction);
	}

	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		this.#database.close();
		this.#releaseLock();
	}
}

function acquireDatabaseLock(name: string): Promise<() => void> {
	let release!: () => void;
	const released = new Promise<void>((resolve) => {
		release = resolve;
	});
	return new Promise<() => void>((resolve, reject) => {
		void navigator.locks
			.request(
				`lix:indexeddb:${name}`,
				{ mode: "exclusive", ifAvailable: true },
				async (lock) => {
					if (!lock) {
						reject(new Error(`IndexedDB storage '${name}' is already open`));
						return;
					}
					resolve(release);
					await released;
				},
			)
			.catch(reject);
	});
}

function openDatabase(request: IDBOpenDBRequest): Promise<IDBDatabase> {
	return new Promise<IDBDatabase>((resolve, reject) => {
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
		request.onblocked = () =>
			reject(new Error("IndexedDB database open was blocked"));
	});
}

function binaryKey(value: Uint8Array): ArrayBuffer {
	return value.buffer.slice(
		value.byteOffset,
		value.byteOffset + value.byteLength,
	) as ArrayBuffer;
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
	return new Promise<void>((resolve, reject) => {
		transaction.oncomplete = () => resolve();
		transaction.onabort = () => reject(transaction.error);
		transaction.onerror = () => reject(transaction.error);
	});
}

function copyBytes(value: unknown, label: string): Uint8Array {
	if (value instanceof ArrayBuffer) {
		return new Uint8Array(value.slice(0));
	}
	if (ArrayBuffer.isView(value)) {
		return new Uint8Array(
			value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
		);
	}
	throw new Error(`${label} is not binary data`);
}
