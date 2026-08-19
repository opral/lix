import type {
	LixStorageCommitResult,
	LixStorageGetManyRequest,
	LixStorageKeyRange,
	LixStoragePrecondition,
	LixStorageProjectedValue,
	LixStorageReadOptions,
	LixStorageScanOrder,
	LixStorageSpace,
	LixStorageWriteStats,
} from "@lix-js/sdk";

/** Internal protocol shared by the package-owned owner worker and its clients. */
export const OPFS_RPC_CHANNEL = "lix-js:storage-opfs:v1";

export type OpfsRpcRequest = {
	kind: "request";
	requestId: string;
	clientId: string;
	storageName: string;
	operation:
		| "open"
		| "close"
		| "heartbeat"
		| "beginRead"
		| "readMany"
		| "scanPage"
		| "commit";
	payload: unknown;
};

export type OpfsRpcResponse = {
	kind: "response";
	requestId: string;
	clientId: string;
	ok: true;
	result: unknown;
} | {
	kind: "response";
	requestId: string;
	clientId: string;
	ok: false;
	error: SerializedError;
};

/** Package-private state used to recover coalesced or missed invalidations. */
export type OpfsStorageState = {
	kind: "storageState";
	storageName: string;
	ownerEpoch: string;
	generation: number;
};

export type OpfsChannelMessage =
	| OpfsRpcRequest
	| OpfsRpcResponse
	| OpfsStorageState;

export type OpfsOpenResult = {
	ownerEpoch: string;
	generation: number;
};

export type SerializedError = {
	name: string;
	message: string;
	stack?: string;
	code?: string;
	details?: unknown;
};

export type OpfsBeginReadPayload = LixStorageReadOptions;
export type OpfsReadManyPayload = {
	requests: LixStorageGetManyRequest[];
	generation: number;
	ownerEpoch: string;
};
export type OpfsScanPagePayload = {
	space: LixStorageSpace;
	range: LixStorageKeyRange;
	after?: Uint8Array;
	limit: number;
	order: LixStorageScanOrder;
	projection: "keyOnly" | "fullValue";
	generation: number;
	ownerEpoch: string;
};
export type OpfsCommitPayload = {
	deletes: Array<{ space: LixStorageSpace; key: Uint8Array }>;
	puts: Array<{ space: LixStorageSpace; key: Uint8Array; value: Uint8Array }>;
	deleteRanges: Array<{ space: LixStorageSpace; range: LixStorageKeyRange }>;
	immutablePuts: Array<{
		space: LixStorageSpace;
		key: Uint8Array;
		value: Uint8Array;
	}>;
	preconditions: LixStoragePrecondition[];
	strictDurability: boolean;
	stats: LixStorageWriteStats;
};

export type OpfsBeginReadResult = {
	generation: number;
	snapshotCacheKey: string;
	ownerEpoch: string;
};
export type OpfsCommitResult = LixStorageCommitResult;
export type OpfsWriteStats = LixStorageWriteStats;

export function serializeError(error: unknown): SerializedError {
	if (error instanceof Error) {
		const value = error as Error & {
			code?: unknown;
			details?: unknown;
		};
		return {
			name: error.name,
			message: error.message,
			stack: error.stack,
			...(typeof value.code === "string" ? { code: value.code } : {}),
			...(value.details !== undefined ? { details: value.details } : {}),
		};
	}
	return { name: "Error", message: String(error) };
}

export function deserializeError(error: SerializedError): Error {
	const value = new Error(error.message);
	value.name = error.name;
	if (error.stack) value.stack = error.stack;
	if (error.code !== undefined) Object.assign(value, { code: error.code });
	if (error.details !== undefined)
		Object.assign(value, { details: error.details });
	return value;
}
