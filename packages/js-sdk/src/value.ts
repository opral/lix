import { invalidParam } from "./errors.js";
import type { JsonValue, LixValue, SqlParam } from "./types.js";

export class Value {
	readonly kind: LixValue["kind"];
	readonly #raw: LixValue;

	private constructor(raw: LixValue) {
		validateExplicitValue(raw);
		this.#raw = cloneValue(raw);
		this.kind = this.#raw.kind;
	}

	static null() {
		return new Value({ kind: "null", value: null });
	}

	static boolean(value: boolean) {
		return new Value({ kind: "boolean", value });
	}

	static integer(value: number) {
		return new Value({ kind: "integer", value });
	}

	static real(value: number) {
		return new Value({ kind: "real", value });
	}

	static text(value: string) {
		return new Value({ kind: "text", value });
	}

	static json(value: JsonValue) {
		return new Value({ kind: "json", value });
	}

	static blob(value: Uint8Array) {
		return new Value({ kind: "blob", value });
	}

	static from(value: SqlParam) {
		return new Value(normalizeParam(value));
	}

	static _fromNative(value: LixValue) {
		return new Value(value);
	}

	_toNative() {
		return toNativeValue(this.#raw);
	}

	toJS() {
		return unwrapValue(this.#raw);
	}

	asBytes() {
		if (this.#raw.kind !== "blob") return undefined;
		return new Uint8Array(this.#raw.value);
	}
}

export type NativeLixValue =
	| Exclude<LixValue, { kind: "blob" }>
	| { kind: "blob"; value?: null; blob: Uint8Array };

export function toNativeValue(value: LixValue): NativeLixValue {
	if (value.kind !== "blob") return value;
	return {
		kind: "blob",
		value: null,
		blob: new Uint8Array(value.value),
	};
}

export function fromNativeValue(value: NativeLixValue): LixValue {
	if (value.kind !== "blob") return value;
	return {
		kind: "blob",
		value: new Uint8Array(value.blob),
	};
}

export function normalizeParam(
	value: SqlParam,
	index = 0,
	seen = new WeakSet<object>(),
): LixValue {
	if (value instanceof Value) return fromNativeValue(value._toNative());
	if (value === null) return { kind: "null", value: null };
	if (typeof value === "boolean") return { kind: "boolean", value };
	if (typeof value === "string") {
		if (!isWellFormedString(value)) {
			throw invalidParam(
				index,
				"string SQL parameters must be well-formed UTF-16",
				"string",
			);
		}
		return { kind: "text", value };
	}
	if (typeof value === "number") {
		if (!Number.isFinite(value)) {
			throw invalidParam(
				index,
				"number SQL parameters must be a finite number",
				"number",
			);
		}
		if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
			throw invalidParam(
				index,
				"integer SQL parameters must be a safe integer",
				"number",
			);
		}
		return Number.isSafeInteger(value)
			? { kind: "integer", value }
			: { kind: "real", value };
	}
	if (value instanceof Uint8Array) {
		return {
			kind: "blob",
			value: new Uint8Array(value),
		};
	}
	if (typeof value === "object" && value) {
		if (value instanceof Date) {
			throw invalidParam(index, "Date is not a valid SQL parameter", "Date");
		}
		if (ArrayBuffer.isView(value)) {
			throw invalidParam(
				index,
				"typed array SQL parameters must be Uint8Array",
				value.constructor.name,
			);
		}
		assertJsonSerializable(value, seen, index);
		return { kind: "json", value };
	}
	throw invalidParam(
		index,
		`${typeof value} is not a valid SQL parameter`,
		typeof value,
	);
}

function unwrapValue(value: LixValue): unknown {
	switch (value.kind) {
		case "null":
			return null;
		case "boolean":
		case "integer":
		case "real":
		case "text":
		case "json":
			return cloneJsonValue(value.value);
		case "blob":
			return new Uint8Array(value.value);
		default:
			return undefined;
	}
}

function assertJsonSerializable(
	value: unknown,
	seen: WeakSet<object>,
	index: number,
) {
	if (value === null) return;
	if (typeof value === "string") {
		if (!isWellFormedString(value)) {
			throw invalidParam(
				index,
				"string SQL parameters must be well-formed UTF-16",
				"string",
			);
		}
		return;
	}
	if (typeof value === "number") {
		if (!Number.isFinite(value)) {
			throw invalidParam(
				index,
				"number SQL parameters must be a finite number",
				"number",
			);
		}
		if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
			throw invalidParam(
				index,
				"integer SQL parameters must be a safe integer",
				"number",
			);
		}
		return;
	}
	if (typeof value === "boolean") return;
	if (typeof value !== "object") {
		throw invalidParam(
			index,
			`${typeof value} is not a valid SQL parameter`,
			typeof value,
		);
	}
	if (value instanceof Value) {
		throw invalidParam(
			index,
			"Value is only valid as a top-level SQL parameter",
			"Value",
		);
	}
	if (value instanceof Date) {
		throw invalidParam(index, "Date is not a valid SQL parameter", "Date");
	}
	if (ArrayBuffer.isView(value)) {
		throw invalidParam(
			index,
			"typed array SQL parameters must be top-level Uint8Array values",
			value.constructor.name,
		);
	}
	if (!Array.isArray(value) && !isPlainObject(value)) {
		throw invalidParam(
			index,
			"object SQL parameters must be JSON-compatible plain objects or arrays",
			value.constructor?.name ?? "object",
		);
	}
	if (seen.has(value)) {
		throw invalidParam(
			index,
			"JSON SQL parameters cannot contain circular references",
			"object",
		);
	}
	seen.add(value);
	const entries = Array.isArray(value) ? value : Object.values(value);
	for (const entry of entries) {
		assertJsonSerializable(entry, seen, index);
	}
	seen.delete(value);
}

function validateExplicitValue(value: LixValue) {
	switch (value.kind) {
		case "null":
			if (value.value !== null) break;
			return;
		case "boolean":
			if (typeof value.value !== "boolean") break;
			return;
		case "integer":
			if (
				typeof value.value === "number" &&
				Number.isSafeInteger(value.value)
			) {
				return;
			}
			break;
		case "real":
			if (typeof value.value === "number" && Number.isFinite(value.value)) return;
			break;
		case "text":
			if (typeof value.value !== "string") break;
			if (!isWellFormedString(value.value)) {
				throw invalidParam(
					0,
					"string SQL parameters must be well-formed UTF-16",
					"string",
				);
			}
			return;
		case "json":
			assertJsonSerializable(value.value, new WeakSet(), 0);
			return;
		case "blob":
			if (value.value instanceof Uint8Array) return;
			break;
		default:
			break;
	}
	throw invalidParam(0, "explicit Value contains an invalid native value", "Value");
}

function cloneValue(value: LixValue): LixValue {
	if (value.kind === "blob") {
		return { kind: "blob", value: new Uint8Array(value.value) };
	}
	if (value.kind === "json") {
		return { kind: "json", value: cloneJsonValue(value.value) };
	}
	return value;
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

function isPlainObject(value: object) {
	const prototype = Object.getPrototypeOf(value);
	return prototype === Object.prototype || prototype === null;
}

function isWellFormedString(value: string) {
	if (typeof value.isWellFormed === "function") return value.isWellFormed();
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			const next = value.charCodeAt(index + 1);
			if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
			index += 1;
		} else if (code >= 0xdc00 && code <= 0xdfff) {
			return false;
		}
	}
	return true;
}
