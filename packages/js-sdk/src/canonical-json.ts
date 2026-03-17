export type LixJsonValue =
	| null
	| boolean
	| number
	| string
	| LixJsonValue[]
	| { [key: string]: LixJsonValue };

export type CanonicalJsonText = string;

export function isLixJsonValue(value: unknown): value is LixJsonValue {
	if (
		value === null ||
		typeof value === "boolean" ||
		typeof value === "string"
	) {
		return true;
	}
	if (typeof value === "number") {
		return Number.isFinite(value);
	}
	if (Array.isArray(value)) {
		return value.every((entry) => isLixJsonValue(entry));
	}
	if (!value || typeof value !== "object") {
		return false;
	}
	if (
		value instanceof Uint8Array ||
		value instanceof ArrayBuffer ||
		ArrayBuffer.isView(value)
	) {
		return false;
	}
	return Object.values(value).every((entry) => isLixJsonValue(entry));
}

export function canonicalizeJsonValue(value: LixJsonValue): LixJsonValue {
	if (Array.isArray(value)) {
		return value.map((entry) => canonicalizeJsonValue(entry));
	}
	if (value && typeof value === "object") {
		const out: { [key: string]: LixJsonValue } = {};
		for (const key of Object.keys(value).sort()) {
			out[key] = canonicalizeJsonValue(value[key]!);
		}
		return out;
	}
	return value;
}

export function encodeCanonicalJson(
	value: LixJsonValue,
	context = "value",
): CanonicalJsonText {
	if (!isLixJsonValue(value)) {
		throw new TypeError(`${context} must be a JSON value`);
	}
	return JSON.stringify(canonicalizeJsonValue(value));
}

export function parseCanonicalJson(
	text: CanonicalJsonText,
	context = "value",
): LixJsonValue {
	if (typeof text !== "string") {
		throw new TypeError(`${context} must be canonical JSON text`);
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch (error) {
		throw new TypeError(
			`${context} must be valid canonical JSON text: ${String(error)}`,
		);
	}
	if (!isLixJsonValue(parsed)) {
		throw new TypeError(`${context} must decode to a JSON value`);
	}
	return canonicalizeJsonValue(parsed);
}
