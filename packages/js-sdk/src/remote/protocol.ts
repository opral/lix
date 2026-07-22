import type {
	BindingExecuteResult,
	BindingObserveEvent,
} from "../binding-types.js";
import type { NativeLixValue } from "../value.js";

export const REMOTE_PROTOCOL_VERSION = 1;
export const REMOTE_PROTOCOL_PATH = "/lix/v1/";

export type WireValue =
	| { kind: "null"; value: null }
	| { kind: "bool"; value: boolean }
	| { kind: "int"; value: number }
	| { kind: "float"; value: number }
	| { kind: "text"; value: string }
	| { kind: "json"; value: unknown }
	| { kind: "blob"; base64: string };

export type RemoteHandshake = {
	protocolVersion: number;
	activeBranchId: string;
	sessionId: string;
};

export type RemoteExecuteRequest = {
	sql: string;
	params: WireValue[];
	options?: { originKey?: string };
};

export type RemoteExecuteBatchRequest = {
	statements: Array<{ sql: string; params: WireValue[] }>;
	options?: { originKey?: string };
};

export type RemoteExecuteResponse = {
	columns: string[];
	rows: WireValue[][];
	rowsAffected: number;
	notices: Array<{ code: string; message: string; hint?: string }>;
};

export type RemoteObserveRequest = Omit<RemoteExecuteRequest, "options">;

export type RemoteObserveSubscription = RemoteObserveRequest & {
	id: string;
};

export type RemoteMultiplexObserveRequest = {
	subscriptions: RemoteObserveSubscription[];
};

export type RemoteObserveEvent = {
	sequence: number;
	mutationSequence: number;
	result: RemoteExecuteResponse;
};

export type RemoteMultiplexObserveEvent = RemoteObserveEvent & {
	subscriptionId: string;
};

export type RemoteCreateBranchRequest = {
	id?: string;
	name: string;
	fromCommitId?: string;
};

export type RemoteCreateBranchResponse = {
	id: string;
	name: string;
	hidden: boolean;
	commitId: string;
};

export type RemoteSwitchBranchRequest = { branchId: string };
export type RemoteSwitchBranchResponse = { branchId: string };

export type RemoteErrorBody = {
	error: {
		code?: string;
		message?: string;
		hint?: string;
		details?: unknown;
	};
};

export type RemoteObserveErrorEvent = RemoteErrorBody & {
	retryable?: boolean;
};

export type RemoteMultiplexObserveErrorEvent = RemoteObserveErrorEvent & {
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
		case "json":
			return { kind: "json", value: value.value };
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

export function decodeHandshake(value: unknown): RemoteHandshake {
	const handshake = record(value, "remote handshake");
	if (handshake.protocolVersion !== REMOTE_PROTOCOL_VERSION) {
		throw protocolError(
			`unsupported remote protocol version: ${String(handshake.protocolVersion)}`,
		);
	}
	if (
		typeof handshake.activeBranchId !== "string" ||
		handshake.activeBranchId.length === 0
	) {
		throw protocolError("remote handshake requires activeBranchId");
	}
	if (
		typeof handshake.sessionId !== "string" ||
		!/^[\x21-\x7e]{1,256}$/.test(handshake.sessionId)
	) {
		throw protocolError("remote handshake requires a valid sessionId");
	}
	return {
		protocolVersion: REMOTE_PROTOCOL_VERSION,
		activeBranchId: handshake.activeBranchId,
		sessionId: handshake.sessionId,
	};
}

export function decodeObserveEvent(value: unknown): BindingObserveEvent {
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
	return {
		sequence: event.sequence,
		mutationSequence: event.mutationSequence,
		rows: decodeExecuteResult(event.result),
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
	return remoteError("LIX_REMOTE_PROTOCOL_ERROR", message);
}

export function errorFromResponseBody(
	value: unknown,
	status?: number,
): Error & { code: string } {
	const body = record(value, "remote error response");
	const rawError = record(body.error, "remote error response error");
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
			if (wire.value !== null) throw protocolError("null wire value is invalid");
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
		case "json":
			assertJsonValue(wire.value, "json wire value");
			return { kind: "json", value: wire.value };
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
	if (!Array.isArray(value) || !value.every((entry) => typeof entry === "string")) {
		throw protocolError(`${description} must be an array of strings`);
	}
	return [...value];
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
		binary += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
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
