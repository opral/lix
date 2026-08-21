import type {
	LixStorageCommitResult,
	LixStorageError,
	LixStorageErrorCode,
	LixStorageKeyRange,
	LixStoragePrecondition,
	LixStoragePutEntry,
	LixStorageSpace,
	LixStorageWrite,
	LixStorageWriteOptions,
	LixStorageWriteStats,
} from "@lix-js/sdk";

type StagedEntry = {
	space: LixStorageSpace;
	key: Uint8Array;
	value: Uint8Array;
};

type StagedDelete = {
	space: LixStorageSpace;
	key: Uint8Array;
};

type StagedDeleteRange = {
	space: LixStorageSpace;
	range: LixStorageKeyRange;
};

export type OpfsWritePayload = {
	deletes: StagedDelete[];
	puts: StagedEntry[];
	deleteRanges: StagedDeleteRange[];
	immutablePuts: StagedEntry[];
	preconditions: LixStoragePrecondition[];
	strictDurability: boolean;
	stats: LixStorageWriteStats;
};

type CommitBufferedWrite = (
	payload: OpfsWritePayload,
) => LixStorageCommitResult | Promise<LixStorageCommitResult>;

/** Package-private write state shared by direct and cross-tab OPFS adapters. */
export class BufferedOpfsWrite implements LixStorageWrite {
	readonly #puts = new Map<string, StagedEntry>();
	readonly #deletes = new Map<string, StagedDelete>();
	readonly #deleteRanges: StagedDeleteRange[] = [];
	readonly #immutablePuts = new Map<string, StagedEntry>();
	readonly #stats: LixStorageWriteStats = {
		putEntries: 0,
		deletedEntries: 0,
		deletedRanges: 0,
		writtenBytes: 0,
		storageCalls: 0,
	};
	#closed = false;

	constructor(
		private readonly options: LixStorageWriteOptions,
		private readonly commitBuffered: CommitBufferedWrite,
	) {}

	async putMany(
		space: LixStorageSpace,
		entries: LixStoragePutEntry[],
	): Promise<void> {
		this.#assertOpen();
		for (const entry of entries) {
			const id = storageKey(space.id, entry.key);
			const staged = {
				space,
				key: new Uint8Array(entry.key),
				value: new Uint8Array(entry.value),
			};
			if (space.valueSemantics === "immutable") {
				const existing = this.#immutablePuts.get(id);
				if (existing) {
					if (!bytesEqual(existing.value, entry.value)) {
						throw immutableValueError();
					}
					continue;
				}
				this.#immutablePuts.set(id, staged);
			}
			this.#stats.putEntries += 1;
			this.#stats.writtenBytes += entry.value.byteLength;
			this.#deletes.delete(id);
			this.#puts.set(id, staged);
		}
		this.#stats.storageCalls += 1;
	}

	async replaceMany(
		space: LixStorageSpace,
		entries: LixStoragePutEntry[],
	): Promise<void> {
		this.#assertOpen();
		if (
			space.valueSemantics !== "immutable" ||
			space.valueIntegrity === "contentAddressed"
		) {
			throw storageError(
				"LIX_STORAGE_CORRUPTION",
				"replaceMany requires an immutable non-content-addressed storage space",
			);
		}
		for (const entry of entries) {
			const id = storageKey(space.id, entry.key);
			const staged = {
				space,
				key: new Uint8Array(entry.key),
				value: new Uint8Array(entry.value),
			};
			this.#immutablePuts.delete(id);
			this.#deletes.delete(id);
			this.#puts.set(id, staged);
			this.#stats.putEntries += 1;
			this.#stats.writtenBytes += entry.value.byteLength;
		}
		this.#stats.storageCalls += 1;
	}

	async deleteMany(space: LixStorageSpace, keys: Uint8Array[]): Promise<void> {
		this.#assertOpen();
		for (const key of keys) {
			const id = storageKey(space.id, key);
			this.#puts.delete(id);
			this.#deletes.set(id, { space, key: new Uint8Array(key) });
		}
		this.#stats.deletedEntries += keys.length;
		this.#stats.storageCalls += 1;
	}

	async deleteRange(
		space: LixStorageSpace,
		range: LixStorageKeyRange,
	): Promise<void> {
		this.#assertOpen();
		let removedPuts = 0;
		for (const [id, entry] of this.#puts) {
			if (entry.space.id === space.id && rangeContains(range, entry.key)) {
				this.#puts.delete(id);
				removedPuts += 1;
			}
		}
		this.#stats.deletedEntries += removedPuts;
		this.#stats.deletedRanges += 1;
		this.#stats.storageCalls += 1;
		this.#deleteRanges.push({ space, range });
	}

	async commit(): Promise<LixStorageCommitResult> {
		this.#assertOpen();
		this.#closed = true;
		return this.commitBuffered({
			deletes: [...this.#deletes.values()],
			puts: [...this.#puts.values()],
			deleteRanges: this.#deleteRanges,
			immutablePuts: [...this.#immutablePuts.values()],
			preconditions: this.options.preconditions,
			strictDurability: this.options.awaitDurable,
			stats: { ...this.#stats },
		});
	}

	async rollback(): Promise<void> {
		this.#assertOpen();
		this.#closed = true;
	}

	#assertOpen(): void {
		if (this.#closed) throw writeClosedError();
	}
}

export function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
	if (left.byteLength !== right.byteLength) return false;
	for (let index = 0; index < left.byteLength; index += 1) {
		if (left[index] !== right[index]) return false;
	}
	return true;
}

export function immutableValueError(): LixStorageError {
	return storageError(
		"LIX_STORAGE_CORRUPTION",
		"immutable identity was assigned different bytes",
	);
}

function storageKey(space: number, key: Uint8Array): string {
	let encoded = `${space}:`;
	for (const byte of key) encoded += byte.toString(16).padStart(2, "0");
	return encoded;
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
	for (let index = 0; index < Math.min(left.length, right.length); index += 1) {
		if (left[index] !== right[index]) return left[index]! - right[index]!;
	}
	return left.length - right.length;
}

function rangeContains(range: LixStorageKeyRange, key: Uint8Array): boolean {
	const lower =
		range.lower.kind === "unbounded" ||
		(range.lower.kind === "included"
			? compareBytes(key, range.lower.key) >= 0
			: compareBytes(key, range.lower.key) > 0);
	const upper =
		range.upper.kind === "unbounded" ||
		(range.upper.kind === "included"
			? compareBytes(key, range.upper.key) <= 0
			: compareBytes(key, range.upper.key) < 0);
	return lower && upper;
}

function writeClosedError(): LixStorageError {
	return storageError("LIX_STORAGE_CLOSED", "OPFS write transaction is closed");
}

function storageError(
	code: LixStorageErrorCode,
	message: string,
): LixStorageError {
	const error = new Error(message) as LixStorageError;
	error.name = "LixStorageError";
	Object.assign(error, { code });
	return error;
}
