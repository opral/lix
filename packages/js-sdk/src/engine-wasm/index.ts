export { default } from "./wasm/lix_engine.js";
export * from "./wasm/lix_engine.js";
import type { InitInput } from "./wasm/lix_engine.js";

export type JsonValue =
	| null
	| boolean
	| number
	| string
	| JsonValue[]
	| { [key: string]: JsonValue };

export type ValueKind =
	| "null"
	| "boolean"
	| "integer"
	| "real"
	| "text"
	| "json"
	| "blob";

export type LixValue =
	| { kind: "null"; value: null }
	| { kind: "boolean"; value: boolean }
	| { kind: "integer"; value: number }
	| { kind: "real"; value: number }
	| { kind: "text"; value: string }
	| { kind: "json"; value: JsonValue }
	| { kind: "blob"; base64: string };

export class Value {
	kind: ValueKind;
	value: null | boolean | number | string | JsonValue | undefined;
	base64: string | undefined;

	constructor(
		kind: ValueKind,
		value: null | boolean | number | string | JsonValue | undefined,
		base64?: string,
	) {
		this.kind = kind;
		this.value = value;
		this.base64 = base64;
	}

	static null(): Value {
		return new Value("null", null);
	}

	static integer(value: number): Value {
		if (!Number.isFinite(value) || !Number.isInteger(value)) {
			throw new TypeError("Value.integer() requires a finite integer number");
		}
		return new Value("integer", value);
	}

	static boolean(value: boolean): Value {
		return new Value("boolean", value);
	}

	static real(value: number): Value {
		if (!Number.isFinite(value)) {
			throw new TypeError("Value.real() requires a finite number");
		}
		return new Value("real", value);
	}

	static text(value: string): Value {
		if (!isWellFormedUtf16(value)) {
			throw new TypeError("Value.text() requires a well-formed UTF-16 string");
		}
		return new Value("text", value);
	}

	static json(value: JsonValue): Value {
		return new Value("json", normalizeJsonValue(value));
	}

	static blob(value: Uint8Array): Value {
		return new Value("blob", undefined, bytesToBase64(value));
	}

	static from(raw: unknown): Value {
		if (raw instanceof Value) return raw;
		if (isLixValue(raw)) {
			switch (raw.kind) {
				case "null":
					return Value.null();
				case "boolean":
					return Value.boolean(raw.value);
				case "integer":
					return Value.integer(raw.value);
				case "real":
					return Value.real(raw.value);
				case "text":
					return Value.text(raw.value);
				case "json":
					return Value.json(normalizeJsonValue(raw.value));
				case "blob":
					return new Value("blob", undefined, raw.base64);
			}
		}
		if (raw === null) return Value.null();
		if (raw === undefined) {
			throw new TypeError("undefined is not a valid SQL parameter");
		}
		if (typeof raw === "number") {
			return Number.isInteger(raw) ? Value.integer(raw) : Value.real(raw);
		}
		if (typeof raw === "boolean") return Value.boolean(raw);
		if (typeof raw === "string") return Value.text(raw);
		if (raw instanceof Uint8Array) return Value.blob(raw);
		if (raw instanceof ArrayBuffer) return Value.blob(new Uint8Array(raw));
		if (ArrayBuffer.isView(raw)) {
			throw new TypeError(
				"typed array SQL parameters must be Uint8Array; other ArrayBuffer views are ambiguous",
			);
		}
		if (raw instanceof Date) {
			throw new TypeError(
				"Date is not a valid SQL parameter; pass date.toISOString() or date.getTime() explicitly",
			);
		}
		if (raw && typeof raw === "object") {
			return Value.json(normalizeJsonValue(raw));
		}
		throw new TypeError(
			"Value.from() requires a LixValue, JSON value, or binary value",
		);
	}

	asInteger(): number | undefined {
		return this.kind === "integer" ? (this.value as number) : undefined;
	}

	asBoolean(): boolean | undefined {
		return this.kind === "boolean" ? (this.value as boolean) : undefined;
	}

	asReal(): number | undefined {
		return this.kind === "real" ? (this.value as number) : undefined;
	}

	asText(): string | undefined {
		return this.kind === "text" ? (this.value as string) : undefined;
	}

	asJson(): JsonValue | undefined {
		return this.kind === "json" ? normalizeJsonValue(this.value) : undefined;
	}

	asBlob(): Uint8Array | undefined {
		return this.kind === "blob" && this.base64 !== undefined
			? base64ToBytes(this.base64)
			: undefined;
	}

	toJSON(): LixValue {
		switch (this.kind) {
			case "null":
				return { kind: "null", value: null };
			case "boolean":
				return { kind: "boolean", value: this.asBoolean() ?? false };
			case "integer":
				return { kind: "integer", value: this.asInteger() ?? 0 };
			case "real":
				return { kind: "real", value: this.asReal() ?? 0 };
			case "text":
				return { kind: "text", value: this.asText() ?? "" };
			case "json":
				return { kind: "json", value: this.asJson() ?? null };
			case "blob":
				return { kind: "blob", base64: this.base64 ?? "" };
		}
	}
}

export type ExecuteResult = {
	columns: string[];
	rows: LixValue[][];
	rowsAffected: number;
	notices: LixNotice[];
};

export type LixNotice = {
	code: string;
	message: string;
	hint?: string;
};

/**
 * Error thrown by the Lix engine. Extends the standard `Error` with a
 * machine-readable `code`, optional `hint`, and optional structured `details`.
 *
 * Hints follow the Postgres/rustc convention: `message` states what went
 * wrong in factual terms; `hint` offers a fix when one is known. Consumers
 * typically render the hint alongside the primary message (e.g. as
 * `hint: <text>` in a CLI, secondary text in a UI).
 */
export interface LixError extends Error {
	code: string;
	hint?: string;
	details?: unknown;
}

type Assert<T extends true> = T;
type _LixErrorHasDetails = Assert<
	LixError extends { details?: unknown } ? true : false
>;
type _LixErrorDoesNotHaveData = Assert<
	"data" extends keyof LixError ? false : true
>;
type _LixErrorDoesNotHaveDescription = Assert<
	"description" extends keyof LixError ? false : true
>;

/**
 * Type guard: returns `true` when `err` is a Lix-produced error carrying a
 * structured `code` field (all engine codes start with `LIX_`).
 */
export function isLixError(err: unknown): err is LixError {
	return (
		err instanceof Error &&
		typeof (err as Partial<LixError>).code === "string" &&
		(err as LixError).code.startsWith("LIX_")
	);
}

function isLixValue(value: unknown): value is LixValue {
	if (!value || typeof value !== "object") {
		return false;
	}
	const kind = (value as { kind?: unknown }).kind;
	if (kind === "null") {
		return (value as { value?: unknown }).value === null;
	}
	if (kind === "boolean") {
		return typeof (value as { value?: unknown }).value === "boolean";
	}
	if (kind === "integer" || kind === "real") {
		const raw = (value as { value?: unknown }).value;
		if (typeof raw !== "number" || !Number.isFinite(raw)) {
			return false;
		}
		if (kind === "integer" && !Number.isInteger(raw)) {
			return false;
		}
		return true;
	}
	if (kind === "text") {
		const raw = (value as { value?: unknown }).value;
		return typeof raw === "string" && isWellFormedUtf16(raw);
	}
	if (kind === "json") {
		return isJsonValue((value as { value?: unknown }).value);
	}
	if (kind === "blob") {
		return typeof (value as { base64?: unknown }).base64 === "string";
	}
	return false;
}

function isJsonValue(value: unknown): value is JsonValue {
	try {
		normalizeJsonValue(value);
		return true;
	} catch {
		return false;
	}
}

function normalizeJsonValue(
	value: unknown,
	seen = new WeakSet<object>(),
): JsonValue {
	if (value === null || typeof value === "boolean") {
		return value;
	}
	if (typeof value === "string") {
		if (!isWellFormedUtf16(value)) {
			throw new TypeError("JSON strings must be well-formed UTF-16");
		}
		return value;
	}
	if (typeof value === "number") {
		if (!Number.isFinite(value)) {
			throw new TypeError("JSON numbers must be finite");
		}
		return value;
	}
	if (Array.isArray(value)) {
		if (seen.has(value)) {
			throw new TypeError("JSON values must not contain circular references");
		}
		seen.add(value);
		const normalized = value.map((item) => normalizeJsonValue(item, seen));
		seen.delete(value);
		return normalized;
	}
	if (!value || typeof value !== "object") {
		throw new TypeError("expected a JSON-compatible value");
	}

	if (value instanceof Date) {
		throw new TypeError("Date is not a JSON value");
	}
	const prototype = Object.getPrototypeOf(value);
	if (prototype !== Object.prototype && prototype !== null) {
		throw new TypeError("JSON objects must be plain objects");
	}
	if (seen.has(value)) {
		throw new TypeError("JSON values must not contain circular references");
	}
	seen.add(value);
	const normalized: { [key: string]: JsonValue } = {};
	for (const [key, entry] of Object.entries(value)) {
		if (!isWellFormedUtf16(key)) {
			throw new TypeError("JSON object keys must be well-formed UTF-16");
		}
		normalized[key] = normalizeJsonValue(entry, seen);
	}
	seen.delete(value);
	return normalized;
}

function isWellFormedUtf16(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			const next = value.charCodeAt(index + 1);
			if (next < 0xdc00 || next > 0xdfff) {
				return false;
			}
			index += 1;
			continue;
		}
		if (code >= 0xdc00 && code <= 0xdfff) {
			return false;
		}
	}
	return true;
}

function bytesToBase64(bytes: Uint8Array): string {
	const maybeBuffer = (
		globalThis as {
			Buffer?: {
				from(value: Uint8Array): { toString(encoding: string): string };
			};
		}
	).Buffer;
	if (maybeBuffer) {
		return maybeBuffer.from(bytes).toString("base64");
	}

	let binary = "";
	const chunkSize = 0x8000;
	for (let index = 0; index < bytes.length; index += chunkSize) {
		const chunk = bytes.subarray(index, index + chunkSize);
		binary += String.fromCharCode(...chunk);
	}
	return btoa(binary);
}

function base64ToBytes(base64: string): Uint8Array {
	const maybeBuffer = (
		globalThis as {
			Buffer?: {
				from(value: string, encoding: string): Uint8Array;
			};
		}
	).Buffer;
	if (maybeBuffer) {
		return new Uint8Array(maybeBuffer.from(base64, "base64"));
	}

	const binary = atob(base64);
	const bytes = new Uint8Array(binary.length);
	for (let index = 0; index < binary.length; index += 1) {
		bytes[index] = binary.charCodeAt(index);
	}
	return bytes;
}

const engineWasmUrl = new URL("./wasm/lix_engine.wasm", import.meta.url);

function isNodeRuntime(): boolean {
	const processLike = (
		globalThis as { process?: { versions?: { node?: string } } }
	).process;
	return (
		!!processLike &&
		typeof processLike.versions === "object" &&
		!!processLike.versions?.node
	);
}

async function tryReadNodeFileFromViteHttpUrl(
	url: URL,
): Promise<Uint8Array | undefined> {
	if (url.protocol !== "http:" && url.protocol !== "https:") {
		return undefined;
	}

	// Vitest/Vite in Node often rewrites module URLs to http://localhost with /@fs/.
	const decodedPathname = decodeURIComponent(url.pathname);
	let filePath: string | undefined;
	if (decodedPathname.startsWith("/@fs/")) {
		filePath = decodedPathname.slice("/@fs".length);
	} else if (
		url.hostname === "localhost" ||
		url.hostname === "127.0.0.1" ||
		url.hostname === "::1"
	) {
		// Some setups expose absolute filesystem paths directly on localhost.
		filePath = decodedPathname;
	}

	if (!filePath) {
		return undefined;
	}

	const fsModuleName = "node:fs/promises";
	const { readFile } = await import(fsModuleName);
	try {
		return new Uint8Array(await readFile(filePath));
	} catch {
		return undefined;
	}
}

/**
 * Returns a wasm-bindgen-compatible init input that works in both browser and Node.
 *
 * - Browser: use a URL so the runtime fetches the `.wasm` asset.
 * - Node: read bytes from disk because `fetch(file://...)` is not supported.
 */
export async function resolveEngineWasmModuleOrPath(): Promise<InitInput> {
	if (!isNodeRuntime()) {
		return engineWasmUrl;
	}

	if (engineWasmUrl.protocol === "file:") {
		const fsModuleName = "node:fs/promises";
		const urlModuleName = "node:url";
		const [{ readFile }, { fileURLToPath }] = await Promise.all([
			import(fsModuleName),
			import(urlModuleName),
		]);
		return readFile(fileURLToPath(engineWasmUrl));
	}

	if (
		engineWasmUrl.protocol === "http:" ||
		engineWasmUrl.protocol === "https:"
	) {
		const localBytes = await tryReadNodeFileFromViteHttpUrl(engineWasmUrl);
		if (localBytes) {
			return localBytes;
		}

		const response = await fetch(engineWasmUrl);
		if (!response.ok) {
			throw new Error(
				`failed to fetch wasm module from '${engineWasmUrl.toString()}': ${response.status} ${response.statusText}`,
			);
		}
		return new Uint8Array(await response.arrayBuffer());
	}

	return engineWasmUrl;
}
