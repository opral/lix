import { invalidParam } from "./errors.js";
import type { LixValue, SqlParam } from "./types.js";

export class Value {
	readonly kind: LixValue["kind"];

	constructor(readonly raw: LixValue) {
		this.kind = raw.kind;
	}

	static from(value: SqlParam) {
		return new Value(normalizeParam(value));
	}

	asJson() {
		return unwrapValue(this.raw);
	}

	asBytes() {
		if (this.raw.kind !== "blob") return undefined;
		return Uint8Array.from(Buffer.from(this.raw.base64, "base64"));
	}
}

export function normalizeParam(
	value: SqlParam,
	index = 0,
	seen = new WeakSet<object>(),
): LixValue {
	if (value instanceof Value) return value.raw;
	if (value === null) return { kind: "null", value: null };
	if (typeof value === "boolean") return { kind: "boolean", value };
	if (typeof value === "string") {
		if (!value.isWellFormed?.() && value.toWellFormed?.() !== value) {
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
		return Number.isInteger(value)
			? { kind: "integer", value }
			: { kind: "real", value };
	}
	if (value instanceof Uint8Array) {
		return {
			kind: "blob",
			base64: Buffer.from(value).toString("base64"),
		};
	}
	if (typeof value === "object" && value) {
		if ("kind" in value && typeof value.kind === "string") {
			return value as LixValue;
		}
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
			return value.value;
		case "blob":
			return Uint8Array.from(Buffer.from(value.base64, "base64"));
		default:
			return undefined;
	}
}

function assertJsonSerializable(
	value: unknown,
	seen: WeakSet<object>,
	index: number,
) {
	if (!value || typeof value !== "object") return;
	if (seen.has(value)) {
		throw invalidParam(
			index,
			"JSON SQL parameters cannot contain circular references",
			"object",
		);
	}
	seen.add(value);
	for (const entry of Object.values(value)) {
		assertJsonSerializable(entry, seen, index);
	}
	seen.delete(value);
}
