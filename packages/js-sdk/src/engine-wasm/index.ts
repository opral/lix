export { default } from "./wasm/lix_engine_wasm_bindgen.js";
export * from "./wasm/lix_engine_wasm_bindgen.js";
import type { InitInput } from "./wasm/lix_engine_wasm_bindgen.js";
import type { LixJsonValue } from "../canonical-json.js";

export type JsonValue = LixJsonValue;

export type ValueKind =
	| "null"
	| "bool"
	| "int"
	| "float"
	| "text"
	| "json"
	| "blob";

export type LixValue =
	| { kind: "null"; value: null }
	| { kind: "bool"; value: boolean }
	| { kind: "int"; value: number }
	| { kind: "float"; value: number }
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
		return new Value("int", value);
	}

	static boolean(value: boolean): Value {
		return new Value("bool", value);
	}

	static real(value: number): Value {
		if (!Number.isFinite(value)) {
			throw new TypeError("Value.real() requires a finite number");
		}
		return new Value("float", value);
	}

	static text(value: string): Value {
		return new Value("text", value);
	}

	static json(value: JsonValue): Value {
		return new Value("json", value);
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
				case "bool":
					return Value.boolean(raw.value);
				case "int":
					return Value.integer(raw.value);
				case "float":
					return Value.real(raw.value);
				case "text":
					return Value.text(raw.value);
				case "json":
					return Value.json(raw.value);
				case "blob":
					return new Value("blob", undefined, raw.base64);
			}
		}
		if (raw === null || raw === undefined) return Value.null();
		if (typeof raw === "number") {
			return Number.isInteger(raw) ? Value.integer(raw) : Value.real(raw);
		}
		if (typeof raw === "boolean") return Value.boolean(raw);
		if (typeof raw === "string") return Value.text(raw);
		if (raw instanceof Uint8Array) return Value.blob(raw);
		if (raw instanceof ArrayBuffer) return Value.blob(new Uint8Array(raw));
		if (ArrayBuffer.isView(raw)) {
			return Value.blob(
				new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength),
			);
		}
		if (isJsonValue(raw)) return Value.json(raw);
		throw new TypeError(
			"Value.from() requires a canonical LixValue or scalar primitive",
		);
	}

	kindValue(): ValueKind {
		return this.kind;
	}

	asInteger(): number | undefined {
		return this.kind === "int" ? (this.value as number) : undefined;
	}

	asBoolean(): boolean | undefined {
		return this.kind === "bool" ? (this.value as boolean) : undefined;
	}

	asReal(): number | undefined {
		return this.kind === "float" ? (this.value as number) : undefined;
	}

	asText(): string | undefined {
		return this.kind === "text" ? (this.value as string) : undefined;
	}

	asJson(): JsonValue | undefined {
		return this.kind === "json" ? (this.value as JsonValue) : undefined;
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
			case "bool":
				return { kind: "bool", value: this.asBoolean() ?? false };
			case "int":
				return { kind: "int", value: this.asInteger() ?? 0 };
			case "float":
				return { kind: "float", value: this.asReal() ?? 0 };
			case "text":
				return { kind: "text", value: this.asText() ?? "" };
			case "json":
				return { kind: "json", value: this.asJson() ?? null };
			case "blob":
				return { kind: "blob", base64: this.base64 ?? "" };
		}
	}
}

export type QueryResult = {
	rows: LixValue[][];
	columns: string[];
};

export type ExecuteResult = {
	statements: QueryResult[];
};

function isLixValue(value: unknown): value is LixValue {
	if (!value || typeof value !== "object") {
		return false;
	}
	const kind = (value as { kind?: unknown }).kind;
	if (kind === "null") {
		return (value as { value?: unknown }).value === null;
	}
	if (kind === "bool") {
		return typeof (value as { value?: unknown }).value === "boolean";
	}
	if (kind === "int" || kind === "float") {
		const raw = (value as { value?: unknown }).value;
		if (typeof raw !== "number" || !Number.isFinite(raw)) {
			return false;
		}
		if (kind === "int" && !Number.isInteger(raw)) {
			return false;
		}
		return true;
	}
	if (kind === "text") {
		return typeof (value as { value?: unknown }).value === "string";
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
		return value.every((item) => isJsonValue(item));
	}
	if (!value || typeof value !== "object") {
		return false;
	}
	return Object.values(value).every((entry) => isJsonValue(entry));
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

const engineWasmUrl = new URL(
	"./wasm/lix_engine_wasm_bindgen_bg.wasm",
	import.meta.url,
);

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
