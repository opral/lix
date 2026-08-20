import type {
	BindingExecuteResult,
	BindingObserveEvent,
} from "../binding-types.js";
import type { NativeLixValue } from "../value.js";

export const SERVER_PROTOCOL_VERSION = 2;
export const SERVER_PROTOCOL_PATH = "/lix/v1/";

export type WireValue =
	| { kind: "null"; value: null }
	| { kind: "bool"; value: boolean }
	| { kind: "int"; value: number }
	| { kind: "float"; value: number }
	| { kind: "text"; value: string }
	| { kind: "jsonb"; value: unknown }
	| { kind: "timestamptz"; value: string }
	| { kind: "blob"; base64: string };

export type WireRequestBlobSplice = {
	kind: "blob-splice";
	baseSha256: string;
	resultSha256: string;
	prefixBytes: number;
	suffixBytes: number;
	insertBase64: string;
};

export type WireRequestValue = WireValue | WireRequestBlobSplice;

export type ServerProtocolHandshake = {
	protocolVersion: number;
	activeBranchId: string;
	activeAccountId: string;
	sessionId: string;
};

export type ServerProtocolHandshakeRequest = {
	activeBranchId?: string;
};

export type ServerProtocolExecuteRequest = {
	sql: string;
	params: WireRequestValue[];
	options?: { originKey?: string };
	cacheBlobs?: true;
};

export type ServerProtocolExecuteBatchRequest = {
	statements: Array<{
		sql: string;
		params: WireRequestValue[];
		label?: string;
	}>;
	options?: { originKey?: string };
	cacheBlobs?: true;
};

export type ServerProtocolExecuteResponse = {
	columns: string[];
	rows: WireValue[][];
	rowsAffected: number;
	notices: Array<{ code: string; message: string; hint?: string }>;
};

export type ServerProtocolExecuteBatchResponse =
	ServerProtocolExecuteResponse & {
		statementIndex: number;
		label?: string;
	};

export type ServerProtocolObserveRequest = {
	sql: string;
	params: WireValue[];
};

export type ServerProtocolObserveSubscription = ServerProtocolObserveRequest & {
	id: string;
};

export type ServerProtocolMultiplexObserveRequest = {
	subscriptions: ServerProtocolObserveSubscription[];
};

type ServerProtocolObserveEventBase = {
	sequence: number;
	mutationSequence: number;
};

export type ServerProtocolObserveBlobDelta = {
	kind: "single-blob-splice";
	baseSequence: number;
	prefixBytes: number;
	suffixBytes: number;
	insertBase64: string;
};

type ServerProtocolObserveRowSplice = {
	kind: "row-splice";
	baseSequence: number;
	prefixRows: number;
	deleteRows: number;
	insertRows: WireValue[][];
};

type ServerProtocolObserveDelta =
	| ServerProtocolObserveBlobDelta
	| ServerProtocolObserveRowSplice;

export type ServerProtocolObserveEvent = ServerProtocolObserveEventBase &
	(
		| { result: ServerProtocolExecuteResponse; delta?: never }
		| { result?: never; delta: ServerProtocolObserveDelta }
	);

export type ServerProtocolMultiplexObserveEvent = ServerProtocolObserveEvent & {
	subscriptionId: string;
};

export type ServerProtocolCreateBranchRequest = {
	id?: string;
	name: string;
	fromCommitId?: string;
};

export type ServerProtocolCreateBranchResponse = {
	id: string;
	name: string;
	hidden: boolean;
	commitId: string;
};

export type ServerProtocolCreateCheckpointResponse = {
	commitId: string;
};

export type ServerProtocolRestoreRequest = {
	commitId: string;
};

export type ServerProtocolUndoResponse = {
	branchId: string;
	targetCommitId: string;
	inverseCommitId: string;
};

export type ServerProtocolRedoResponse = {
	branchId: string;
	targetCommitId: string;
	replayCommitId: string;
};

export type ServerProtocolSwitchBranchRequest = { branchId: string };
export type ServerProtocolSwitchBranchResponse = { branchId: string };

export type ServerProtocolErrorBody = {
	error: {
		code?: string;
		message?: string;
		hint?: string;
		details?: unknown;
	};
};

export type ServerProtocolObserveErrorEvent = ServerProtocolErrorBody & {
	retryable?: boolean;
};

export type ServerProtocolMultiplexObserveErrorEvent =
	ServerProtocolObserveErrorEvent & {
		subscriptionId?: string;
	};

export function encodeWireValue(value: NativeLixValue): WireValue {
	switch (value.kind) {
		case "null":
			return { kind: "null", value: null };
		case "boolean":
			return { kind: "bool", value: value.value };
		case "integer":
			return { kind: "int", value: value.value };
		case "real":
			return { kind: "float", value: value.value };
		case "text":
			return { kind: "text", value: value.value };
		case "jsonb":
			return { kind: "jsonb", value: value.value };
		case "timestamptz":
			return { kind: "timestamptz", value: value.value };
		case "blob":
			return { kind: "blob", base64: bytesToBase64(value.blob) };
	}
}

export function decodeExecuteResult(value: unknown): BindingExecuteResult {
	const result = record(value, "execute result");
	const columns = stringArray(result.columns, "execute result columns");
	if (!Array.isArray(result.rows)) {
		throw protocolError("execute result rows must be an array");
	}
	const rows = result.rows.map((row, rowIndex) => {
		if (!Array.isArray(row)) {
			throw protocolError(`execute result row ${rowIndex} must be an array`);
		}
		if (row.length !== columns.length) {
			throw protocolError(
				`execute result row ${rowIndex} has ${row.length} values for ${columns.length} columns`,
			);
		}
		return row.map((entry) => decodeWireValue(entry));
	});
	if (
		typeof result.rowsAffected !== "number" ||
		!Number.isSafeInteger(result.rowsAffected) ||
		result.rowsAffected < 0
	) {
		throw protocolError(
			"execute result rowsAffected must be a non-negative safe integer",
		);
	}
	if (!Array.isArray(result.notices)) {
		throw protocolError("execute result notices must be an array");
	}
	const notices = result.notices.map((notice, index) => {
		const item = record(notice, `execute result notice ${index}`);
		if (typeof item.code !== "string" || typeof item.message !== "string") {
			throw protocolError(
				`execute result notice ${index} requires code and message`,
			);
		}
		if (item.hint !== undefined && typeof item.hint !== "string") {
			throw protocolError(
				`execute result notice ${index} hint must be a string`,
			);
		}
		return {
			code: item.code,
			message: item.message,
			...(item.hint === undefined ? {} : { hint: item.hint }),
		};
	});
	return { columns, rows, rowsAffected: result.rowsAffected, notices };
}

export function decodeExecuteBatchResult(value: unknown): BindingExecuteResult {
	const result = record(value, "execute batch result");
	const statementIndex = nonNegativeSafeInteger(
		result.statementIndex,
		"execute batch result statementIndex",
	);
	if (result.label !== undefined && typeof result.label !== "string") {
		throw protocolError("execute batch result label must be a string when present");
	}
	return {
		...decodeExecuteResult(value),
		statementIndex,
		...(result.label === undefined ? {} : { label: result.label }),
	};
}

export function decodeHandshake(value: unknown): ServerProtocolHandshake {
	const handshake = record(value, "Lix Server Protocol handshake");
	if (handshake.protocolVersion !== SERVER_PROTOCOL_VERSION) {
		throw protocolError(
			`unsupported Lix Server Protocol version: ${String(handshake.protocolVersion)}`,
		);
	}
	if (
		typeof handshake.activeBranchId !== "string" ||
		handshake.activeBranchId.length === 0
	) {
		throw protocolError(
			"Lix Server Protocol handshake requires activeBranchId",
		);
	}
	if (
		typeof handshake.activeAccountId !== "string" ||
		handshake.activeAccountId.length === 0
	) {
		throw protocolError(
			"Lix Server Protocol handshake requires activeAccountId",
		);
	}
	if (
		typeof handshake.sessionId !== "string" ||
		!/^[\x21-\x7e]{1,256}$/.test(handshake.sessionId)
	) {
		throw protocolError(
			"Lix Server Protocol handshake requires a valid sessionId",
		);
	}
	return {
		protocolVersion: SERVER_PROTOCOL_VERSION,
		activeBranchId: handshake.activeBranchId,
		activeAccountId: handshake.activeAccountId,
		sessionId: handshake.sessionId,
	};
}

export function decodeObserveEvent(
	value: unknown,
	base?: BindingObserveEvent,
): BindingObserveEvent {
	const event = record(value, "observe event");
	if (
		typeof event.sequence !== "number" ||
		!Number.isSafeInteger(event.sequence) ||
		event.sequence < 0
	) {
		throw protocolError(
			"observe event sequence must be a non-negative safe integer",
		);
	}
	if (
		typeof event.mutationSequence !== "number" ||
		!Number.isSafeInteger(event.mutationSequence) ||
		event.mutationSequence < 0
	) {
		throw protocolError(
			"observe event mutationSequence must be a non-negative safe integer",
		);
	}
	const hasResult = event.result !== undefined;
	const hasDelta = event.delta !== undefined;
	if (hasResult === hasDelta) {
		throw protocolError("observe event requires exactly one of result or delta");
	}
	const sequence = event.sequence;
	return {
		sequence,
		mutationSequence: event.mutationSequence,
		rows: hasResult
			? decodeExecuteResult(event.result)
			: applyObserveDelta(event.delta, sequence, base),
	};
}

function applyObserveDelta(
	value: unknown,
	sequence: number,
	base: BindingObserveEvent | undefined,
): BindingExecuteResult {
	const delta = record(value, "observe event delta");
	switch (delta.kind) {
		case "single-blob-splice":
			return applyObserveBlobDelta(delta, sequence, base);
		case "row-splice":
			return applyObserveRowSplice(delta, sequence, base);
		default:
			throw protocolError(`unknown observe delta kind: ${String(delta.kind)}`);
	}
}

function applyObserveBlobDelta(
	delta: Record<string, unknown>,
	sequence: number,
	base: BindingObserveEvent | undefined,
): BindingExecuteResult {
	const baseSequence = nonNegativeSafeInteger(
		delta.baseSequence,
		"observe delta baseSequence",
	);
	const prefixBytes = nonNegativeSafeInteger(
		delta.prefixBytes,
		"observe delta prefixBytes",
	);
	const suffixBytes = nonNegativeSafeInteger(
		delta.suffixBytes,
		"observe delta suffixBytes",
	);
	if (typeof delta.insertBase64 !== "string") {
		throw protocolError("observe delta insertBase64 must be a string");
	}
	if (
		base === undefined ||
		base.sequence !== baseSequence ||
		sequence !== baseSequence + 1
	) {
		throw protocolError("observe blob delta does not match its transport base");
	}
	const baseValue = base.rows.rows[0]?.[0];
	if (
		base.rows.columns.length !== 1 ||
		base.rows.columns[0] !== "content" ||
		base.rows.rows.length !== 1 ||
		base.rows.rows[0]?.length !== 1 ||
		base.rows.rowsAffected !== 0 ||
		base.rows.notices.length !== 0 ||
		baseValue?.kind !== "blob"
	) {
		throw protocolError("observe blob delta base is not a point blob result");
	}
	if (prefixBytes + suffixBytes > baseValue.blob.byteLength) {
		throw protocolError("observe blob delta prefix and suffix overlap");
	}
	const insert = base64ToBytes(delta.insertBase64);
	const nextLength = prefixBytes + insert.byteLength + suffixBytes;
	if (!Number.isSafeInteger(nextLength)) {
		throw protocolError("observe blob delta result is too large");
	}
	let blob: Uint8Array;
	try {
		blob = new Uint8Array(nextLength);
	} catch {
		throw protocolError("observe blob delta result is too large");
	}
	blob.set(baseValue.blob.subarray(0, prefixBytes), 0);
	blob.set(insert, prefixBytes);
	blob.set(
		baseValue.blob.subarray(baseValue.blob.byteLength - suffixBytes),
		prefixBytes + insert.byteLength,
	);
	return {
		columns: ["content"],
		rows: [[{ kind: "blob", value: null, blob }]],
		rowsAffected: 0,
			notices: [],
	};
}

function applyObserveRowSplice(
	delta: Record<string, unknown>,
	sequence: number,
	base: BindingObserveEvent | undefined,
): BindingExecuteResult {
	const baseSequence = nonNegativeSafeInteger(
		delta.baseSequence,
		"observe row delta baseSequence",
	);
	const prefixRows = nonNegativeSafeInteger(
		delta.prefixRows,
		"observe row delta prefixRows",
	);
	const deleteRows = nonNegativeSafeInteger(
		delta.deleteRows,
		"observe row delta deleteRows",
	);
	if (
		base === undefined ||
		base.sequence !== baseSequence ||
		sequence !== baseSequence + 1
	) {
		throw protocolError("observe row delta does not match its transport base");
	}
	if (!Array.isArray(delta.insertRows)) {
		throw protocolError("observe row delta insertRows must be an array");
	}
	if (
		prefixRows > base.rows.rows.length ||
		deleteRows > base.rows.rows.length - prefixRows
	) {
		throw protocolError("observe row delta splice range is outside its transport base");
	}
	const suffixStart = prefixRows + deleteRows;
	const nextRowCount =
		prefixRows + delta.insertRows.length + (base.rows.rows.length - suffixStart);
	if (
		!Number.isSafeInteger(nextRowCount) ||
		nextRowCount > 0xffff_ffff
	) {
		throw protocolError("observe row delta result is too large");
	}
	let rows: NativeLixValue[][];
	try {
		rows = new Array(nextRowCount);
	} catch {
		throw protocolError("observe row delta result is too large");
	}
	let destination = 0;
	for (let index = 0; index < prefixRows; index += 1) {
		rows[destination++] = base.rows.rows[index];
	}
	for (let rowIndex = 0; rowIndex < delta.insertRows.length; rowIndex += 1) {
		const row = delta.insertRows[rowIndex];
		if (!Array.isArray(row)) {
			throw protocolError(`observe row delta insert row ${rowIndex} must be an array`);
		}
		if (row.length !== base.rows.columns.length) {
			throw protocolError(
				`observe row delta insert row ${rowIndex} has ${row.length} values for ${base.rows.columns.length} columns`,
			);
		}
		rows[destination++] = row.map((entry) => decodeWireValue(entry));
	}
	for (let index = suffixStart; index < base.rows.rows.length; index += 1) {
		rows[destination++] = base.rows.rows[index];
	}
	return {
		columns: [...base.rows.columns],
		rows,
		rowsAffected: base.rows.rowsAffected,
		notices: base.rows.notices.map((notice) => ({ ...notice })),
	};
}

export function remoteError(
	code: string,
	message: string,
	options: { hint?: string; details?: unknown; status?: number } = {},
): Error & {
	code: string;
	hint?: string;
	details?: unknown;
	status?: number;
} {
	const error = new Error(message) as Error & {
		code: string;
		hint?: string;
		details?: unknown;
		status?: number;
	};
	error.name = "LixError";
	error.code = code;
	error.hint = options.hint;
	error.details = options.details;
	error.status = options.status;
	return error;
}

export function protocolError(message: string): Error & { code: string } {
	return remoteError("LIX_SERVER_PROTOCOL_ERROR", message);
}

export function errorFromResponseBody(
	value: unknown,
	status?: number,
): Error & { code: string } {
	const body = record(value, "Lix Server Protocol error response");
	const rawError = record(body.error, "Lix Server Protocol error response error");
	return remoteError(
		typeof rawError.code === "string"
			? rawError.code
			: "LIX_REMOTE_REQUEST_FAILED",
		typeof rawError.message === "string"
			? rawError.message
			: status === undefined
				? "Remote Lix operation failed"
				: `Remote Lix request failed with status ${status}`,
		{
			hint: typeof rawError.hint === "string" ? rawError.hint : undefined,
			details: rawError.details,
			status,
		},
	);
}

export function record(
	value: unknown,
	description: string,
): Record<string, unknown> {
	if (!value || typeof value !== "object" || Array.isArray(value)) {
		throw protocolError(`${description} must be an object`);
	}
	return value as Record<string, unknown>;
}

function decodeWireValue(value: unknown): NativeLixValue {
	const wire = record(value, "wire value");
	switch (wire.kind) {
		case "null":
			if (wire.value !== null)
				throw protocolError("null wire value is invalid");
			return { kind: "null", value: null };
		case "bool":
			if (typeof wire.value !== "boolean") {
				throw protocolError("bool wire value is invalid");
			}
			return { kind: "boolean", value: wire.value };
		case "int":
			if (typeof wire.value !== "number" || !Number.isSafeInteger(wire.value)) {
				throw protocolError("int wire value is invalid");
			}
			return { kind: "integer", value: wire.value };
		case "float":
			if (typeof wire.value !== "number" || !Number.isFinite(wire.value)) {
				throw protocolError("float wire value is invalid");
			}
			return { kind: "real", value: wire.value };
		case "text":
			if (typeof wire.value !== "string") {
				throw protocolError("text wire value is invalid");
			}
			return { kind: "text", value: wire.value };
		case "jsonb":
			assertJsonValue(wire.value, "jsonb wire value");
			return { kind: "jsonb", value: wire.value };
		case "timestamptz":
			if (typeof wire.value !== "string") {
				throw protocolError("timestamptz wire value is invalid");
			}
			return { kind: "timestamptz", value: wire.value };
		case "blob":
			if (typeof wire.base64 !== "string") {
				throw protocolError("blob wire value is invalid");
			}
			return { kind: "blob", value: null, blob: base64ToBytes(wire.base64) };
		default:
			throw protocolError(`unknown wire value kind: ${String(wire.kind)}`);
	}
}

function stringArray(value: unknown, description: string): string[] {
	if (
		!Array.isArray(value) ||
		!value.every((entry) => typeof entry === "string")
	) {
		throw protocolError(`${description} must be an array of strings`);
	}
	return [...value];
}

function nonNegativeSafeInteger(value: unknown, description: string): number {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value < 0
	) {
		throw protocolError(`${description} must be a non-negative safe integer`);
	}
	return value;
}

function assertJsonValue(
	value: unknown,
	description: string,
): asserts value is import("../types.js").JsonValue {
	if (
		value === null ||
		typeof value === "boolean" ||
		(typeof value === "number" &&
			Number.isFinite(value) &&
			(!Number.isInteger(value) || Number.isSafeInteger(value))) ||
		(typeof value === "string" && value.isWellFormed())
	) {
		return;
	}
	if (Array.isArray(value)) {
		for (const entry of value) assertJsonValue(entry, description);
		return;
	}
	if (value && typeof value === "object") {
		for (const entry of Object.values(value)) {
			assertJsonValue(entry, description);
		}
		return;
	}
	throw protocolError(`${description} is not valid Lix JSON`);
}

function bytesToBase64(bytes: Uint8Array): string {
	const nativeToBase64 = (
		bytes as Uint8Array & { toBase64?: () => string }
	).toBase64;
	if (typeof nativeToBase64 === "function") {
		return nativeToBase64.call(bytes);
	}

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
	const nativeFromBase64 = (
		Uint8Array as Uint8ArrayConstructor & {
			fromBase64?: (value: string) => Uint8Array;
		}
	).fromBase64;
	if (typeof nativeFromBase64 === "function") {
		try {
			return nativeFromBase64(base64);
		} catch {
			throw protocolError("blob wire value contains invalid base64");
		}
	}

	let binary: string;
	try {
		binary = atob(base64);
	} catch {
		throw protocolError("blob wire value contains invalid base64");
	}
	const bytes = new Uint8Array(binary.length);
	for (let index = 0; index < binary.length; index += 1) {
		bytes[index] = binary.charCodeAt(index);
	}
	return bytes;
}
