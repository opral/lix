import { createHash } from "node:crypto";
import { transpile as transpileComponent } from "@bytecodealliance/jco";
import { WASIShim } from "@bytecodealliance/preview2-shim/instantiation";

const COMPONENT_API_KEY = "lix:plugin/api@0.1.0";
const DETECT_EXPORTS = new Set(["detect-changes", "api#detect-changes"]);
const APPLY_EXPORTS = new Set(["apply-changes", "api#apply-changes"]);

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const wasiImports = new WASIShim({
  sandbox: {
    preopens: {},
    env: {},
    args: ["lix-plugin"],
    enableNetwork: false,
  },
}).getImportObject();

const transpiledCache = new Map();

function toUint8Array(value) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (Array.isArray(value)) {
    return Uint8Array.from(value);
  }
  return new Uint8Array();
}

function decodeJsonBytes(input) {
  const decoded = textDecoder.decode(toUint8Array(input));
  return JSON.parse(decoded);
}

function encodeJsonBytes(value) {
  return textEncoder.encode(JSON.stringify(value));
}

function normalizeWireFile(file) {
  return {
    id: String(file?.id ?? ""),
    path: String(file?.path ?? ""),
    data: toUint8Array(file?.data),
  };
}

function normalizeWireEntityChange(change) {
  const snapshot =
    change?.snapshotContent !== undefined
      ? change.snapshotContent
      : change?.snapshot_content;

  return {
    entity_id: String(change?.entityId ?? change?.entity_id ?? ""),
    schema_key: String(change?.schemaKey ?? change?.schema_key ?? ""),
    schema_version: String(change?.schemaVersion ?? change?.schema_version ?? ""),
    snapshot_content: snapshot === undefined ? null : snapshot,
  };
}

function toPluginEntityChange(change) {
  const normalized = {
    entityId: String(change?.entity_id ?? change?.entityId ?? ""),
    schemaKey: String(change?.schema_key ?? change?.schemaKey ?? ""),
    schemaVersion: String(change?.schema_version ?? change?.schemaVersion ?? ""),
  };

  const snapshot =
    change?.snapshot_content !== undefined
      ? change.snapshot_content
      : change?.snapshotContent;
  if (snapshot !== null && snapshot !== undefined) {
    normalized.snapshotContent = String(snapshot);
  }

  return normalized;
}

function hashComponentBytes(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function toDataUrl(bytes) {
  return `data:text/javascript;base64,${Buffer.from(bytes).toString("base64")}`;
}

function readTranspiledFile(files, suffix) {
  const entry = Object.entries(files).find(([name]) => name.endsWith(suffix));
  if (!entry) {
    throw new Error(`transpile output missing '${suffix}' file`);
  }
  return entry;
}

function resolveComponentApi(exportsObject) {
  if (exportsObject && typeof exportsObject.api === "object") {
    return exportsObject.api;
  }
  if (
    exportsObject &&
    typeof exportsObject[COMPONENT_API_KEY] === "object"
  ) {
    return exportsObject[COMPONENT_API_KEY];
  }
  if (
    exportsObject &&
    typeof exportsObject.detectChanges === "function" &&
    typeof exportsObject.applyChanges === "function"
  ) {
    return exportsObject;
  }

  const keys = exportsObject && typeof exportsObject === "object"
    ? Object.keys(exportsObject)
    : [];
  throw new Error(
    `transpiled component did not expose plugin api '${COMPONENT_API_KEY}' (exports: ${keys.join(", ")})`,
  );
}

async function prepareTranspiledComponent(wasmBytes) {
  const hash = hashComponentBytes(wasmBytes);
  let pending = transpiledCache.get(hash);
  if (!pending) {
    pending = (async () => {
      const name = `plugin_${hash.slice(0, 12)}`;
      const transpiled = await transpileComponent(wasmBytes, {
        name,
        instantiation: "async",
      });

      const [jsFileName, jsSourceBytes] = readTranspiledFile(transpiled.files, ".js");
      const jsModule = await import(toDataUrl(jsSourceBytes));
      if (typeof jsModule.instantiate !== "function") {
        throw new Error(`transpiled module '${jsFileName}' is missing instantiate()`);
      }

      const coreModulePromises = new Map(
        Object.entries(transpiled.files)
          .filter(([fileName]) => fileName.endsWith(".wasm"))
          .map(([fileName, fileBytes]) => [
            fileName,
            WebAssembly.compile(toUint8Array(fileBytes)),
          ]),
      );

      return {
        instantiate: jsModule.instantiate,
        coreModulePromises,
      };
    })();
    transpiledCache.set(hash, pending);
  }

  return await pending;
}

function createModuleInstance(componentApi) {
  return {
    async call(exportName, input) {
      if (DETECT_EXPORTS.has(exportName)) {
        const request = decodeJsonBytes(input);
        const before = request.before
          ? normalizeWireFile(request.before)
          : undefined;
        const after = normalizeWireFile(request.after);
        const rawChanges = await componentApi.detectChanges(before, after);
        const normalized = Array.isArray(rawChanges)
          ? rawChanges.map(normalizeWireEntityChange)
          : [];
        return encodeJsonBytes(normalized);
      }

      if (APPLY_EXPORTS.has(exportName)) {
        const request = decodeJsonBytes(input);
        const file = normalizeWireFile(request.file);
        const changes = Array.isArray(request.changes)
          ? request.changes.map(toPluginEntityChange)
          : [];
        const outputBytes = await componentApi.applyChanges(file, changes);
        return toUint8Array(outputBytes);
      }

      throw new Error(`unsupported export '${exportName}'`);
    },
    async close() {
      // no-op
    },
  };
}

export function createNodeWasmRuntime() {
  return {
    async initComponent(bytes) {
      const wasmBytes = toUint8Array(bytes);
      if (wasmBytes.byteLength === 0) {
        throw new Error("wasmRuntime.initComponent received empty bytes");
      }

      const compiled = await prepareTranspiledComponent(wasmBytes);
      const exportsObject = await compiled.instantiate(
        async (coreFileName) => {
          const coreModulePromise = compiled.coreModulePromises.get(coreFileName);
          if (!coreModulePromise) {
            throw new Error(`transpiled component requested unknown core module '${coreFileName}'`);
          }
          return await coreModulePromise;
        },
        wasiImports,
      );

      const componentApi = resolveComponentApi(exportsObject);
      return createModuleInstance(componentApi);
    },
  };
}
