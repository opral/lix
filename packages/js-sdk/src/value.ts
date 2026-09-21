import { invalidParam } from "./errors.js";
import type { JsonValue, LixValue, SqlParam } from "./types.js";

export class Value {
	readonly kind: LixValue["kind"];
	readonly #raw: LixValue;

	private constructor(raw: LixValue, clone = true) {
		validateExplicitValue(raw);
		this.#raw = clone ? cloneValue(raw) : raw;
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

	static jsonb(value: JsonValue) {
		return new Value({ kind: "jsonb", value });
	}

	static rowRef(value: string) {
		return new Value({ kind: "row_ref", value });
	}

	static timestamptz(value: string) {
		return new Value({ kind: "timestamptz", value });
	}

	static blob(value: Uint8Array) {
		return new Value({ kind: "blob", value });
	}

	static from(value: SqlParam) {
		return new Value(normalizeParam(value));
	}

	static _fromNative(value: LixValue) {
		// Native execute results are newly materialized for this result set. Keep
		// the native value as-is and defer the defensive copy until toJS(). This
		// avoids cloning every structured result once during row wrapping and
		// again when callers read it.
		return new Value(value, false);
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
		return { kind: "jsonb", value };
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
		case "timestamptz":
		case "jsonb":
		case "row_ref":
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
		case "jsonb":
			assertJsonSerializable(value.value, new WeakSet(), 0);
			return;
		case "row_ref":
			if (typeof value.value === "string" && isWellFormedString(value.value)) return;
			break;
		case "timestamptz":
			if (
				typeof value.value === "string" && isValidTimestamptz(value.value)
			) {
				return;
			}
			break;
		case "blob":
			if (value.value instanceof Uint8Array) return;
			break;
		default:
			break;
	}
	throw invalidParam(0, "explicit Value contains an invalid native value", "Value");
}

const TIMESTAMPTZ_PATTERN =
	/^([+-]\d{4,6}|\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(?:Z|([+-])(\d{2}):(\d{2}))$/;

function isValidTimestamptz(value: string) {
	const match = TIMESTAMPTZ_PATTERN.exec(value);
	if (!match) return false;

	const year = Number(match[1]);
	const month = Number(match[2]);
	const day = Number(match[3]);
	const hour = Number(match[4]);
	const minute = Number(match[5]);
	const second = Number(match[6]);
	if (!Number.isSafeInteger(year) || year < -262_143 || year > 262_142) {
		return false;
	}
	if (
		month < 1 ||
		month > 12 ||
		day < 1 ||
		day > daysInMonth(year, month) ||
		hour > 23 ||
		minute > 59 ||
		second > 59
	) {
		return false;
	}

	if (match[8] && (Number(match[9]) > 23 || Number(match[10]) > 59)) {
		return false;
	}
	if (
		!isWithinChronoDateTimeRange(
			year,
			month,
			day,
			hour,
			minute,
			second,
			match[8],
			Number(match[9]),
			Number(match[10]),
		)
	) {
		return false;
	}

	return true;
}

function isWithinChronoDateTimeRange(
	year: number,
	month: number,
	day: number,
	hour: number,
	minute: number,
	second: number,
	offsetSign: string | undefined,
	offsetHour: number,
	offsetMinute: number,
) {
	if (!offsetSign) return true;
	const offsetSeconds =
		(offsetHour * 60 * 60 + offsetMinute * 60) *
		(offsetSign === "-" ? -1 : 1);
	const utcSeconds = hour * 60 * 60 + minute * 60 + second - offsetSeconds;
	if (utcSeconds < 0) {
		return !(year === -262_143 && month === 1 && day === 1);
	}
	if (utcSeconds >= 24 * 60 * 60) {
		return !(year === 262_142 && month === 12 && day === 31);
	}
	return true;
}

function daysInMonth(year: number, month: number) {
	if (month === 2) {
		const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
		return leapYear ? 29 : 28;
	}
	return [4, 6, 9, 11].includes(month) ? 30 : 31;
}

function cloneValue(value: LixValue): LixValue {
	if (value.kind === "blob") {
		return { kind: "blob", value: new Uint8Array(value.value) };
	}
	if (value.kind === "jsonb") {
		return { kind: "jsonb", value: cloneJsonValue(value.value) };
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
