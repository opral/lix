export { default } from "./wasm/lix_engine_wasm_bindgen.js";
export * from "./wasm/lix_engine_wasm_bindgen.js";
import type { InitInput } from "./wasm/lix_engine_wasm_bindgen.js";

export type ValueKind = "Null" | "Integer" | "Real" | "Text" | "Blob";

export class Value {
  kind: ValueKind;
  value: unknown;

  constructor(kind: ValueKind, value: unknown) {
    this.kind = kind;
    this.value = value;
  }

  static null(): Value {
    return new Value("Null", null);
  }

  static integer(value: number): Value {
    return new Value("Integer", value);
  }

  static real(value: number): Value {
    return new Value("Real", value);
  }

  static text(value: string): Value {
    return new Value("Text", value);
  }

  static blob(value: Uint8Array): Value {
    return new Value("Blob", value);
  }

  static from(raw: unknown): Value {
    if (raw instanceof Value) return raw;
    if (raw && typeof raw === "object") {
      const kind = (raw as { kind?: unknown }).kind;
      const value = (raw as { value?: unknown }).value;
      if (typeof kind === "string") {
        return new Value(kind as ValueKind, value);
      }
      const kindFn = (raw as { kind?: unknown }).kind;
      if (typeof kindFn === "function") {
        const resolved = kindFn.call(raw);
        if (typeof resolved === "string") {
          if (resolved === "Integer") return Value.integer((raw as any).asInteger?.() ?? 0);
          if (resolved === "Real") return Value.real((raw as any).asReal?.() ?? 0);
          if (resolved === "Text") return Value.text((raw as any).asText?.() ?? "");
          if (resolved === "Blob") return Value.blob((raw as any).asBlob?.() ?? new Uint8Array());
          return new Value(resolved as ValueKind, value);
        }
      }
    }
    if (raw === null || raw === undefined) return Value.null();
    if (typeof raw === "number") {
      return Number.isInteger(raw) ? Value.integer(raw) : Value.real(raw);
    }
    if (typeof raw === "string") return Value.text(raw);
    if (raw instanceof Uint8Array) return Value.blob(raw);
    if (raw instanceof ArrayBuffer) return Value.blob(new Uint8Array(raw));
    return Value.text(String(raw));
  }

  kindValue(): ValueKind {
    return this.kind;
  }

  asInteger(): number | undefined {
    return this.kind === "Integer" ? (this.value as number) : undefined;
  }

  asReal(): number | undefined {
    return this.kind === "Real" ? (this.value as number) : undefined;
  }

  asText(): string | undefined {
    return this.kind === "Text" ? (this.value as string) : undefined;
  }

  asBlob(): Uint8Array | undefined {
    return this.kind === "Blob" ? (this.value as Uint8Array) : undefined;
  }

  toJSON(): { kind: ValueKind; value: unknown } {
    return { kind: this.kind, value: this.value };
  }
}

export type QueryResult = {
	rows: any[][];
	columns: string[];
};

const engineWasmUrl = new URL("./wasm/lix_engine_wasm_bindgen_bg.wasm", import.meta.url);

function isNodeRuntime(): boolean {
  const processLike = (globalThis as { process?: { versions?: { node?: string } } })
    .process;
  return (
    !!processLike &&
    typeof processLike.versions === "object" &&
    !!processLike.versions?.node
  );
}

async function tryReadNodeFileFromViteHttpUrl(url: URL): Promise<Uint8Array | undefined> {
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

  if (engineWasmUrl.protocol === "http:" || engineWasmUrl.protocol === "https:") {
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
