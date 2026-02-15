import init, {
  openLix as openLixWasm,
  QueryResult,
  Value,
  resolveEngineWasmModuleOrPath,
} from "./engine-wasm/index.js";
import { createWasmSqliteBackend } from "./backend/wasm-sqlite.js";
import type { LixBackend, LixWasmRuntime } from "./types.js";

export type {
  LixBackend,
  LixQueryResultLike,
  LixSqlDialect,
  LixTransaction,
  LixValueLike,
  LixWasmLimits,
  LixWasmModuleInstance,
  LixWasmRuntime,
} from "./types.js";
export { QueryResult, Value } from "./engine-wasm/index.js";

export type CreateVersionOptions = {
  id?: string;
  name?: string;
  inheritsFromVersionId?: string;
  hidden?: boolean;
};

export type CreateVersionResult = {
  id: string;
  name: string;
  inheritsFromVersionId: string;
};

export type InstallPluginOptions = {
  manifestJson: string | Record<string, unknown>;
  wasmBytes: Uint8Array | ArrayBuffer;
};

export type Lix = {
  execute(sql: string, params?: ReadonlyArray<unknown>): Promise<QueryResult>;
  createVersion(args?: CreateVersionOptions): Promise<CreateVersionResult>;
  switchVersion(versionId: string): Promise<void>;
  installPlugin(args: InstallPluginOptions): Promise<void>;
  close(): Promise<void>;
};

let wasmReady: Promise<void> | null = null;

async function ensureWasmReady(): Promise<void> {
  if (!wasmReady) {
    wasmReady = resolveEngineWasmModuleOrPath()
      .then((module_or_path) => init({ module_or_path }))
      .then(() => undefined);
  }
  await wasmReady;
}

export async function openLix(
  args: {
    backend?: LixBackend;
    wasmRuntime?: LixWasmRuntime;
  } = {},
): Promise<Lix> {
  await ensureWasmReady();
  const backend = args.backend ?? (await createWasmSqliteBackend());
  const wasmLix = await openLixWasm(backend, args.wasmRuntime);
  let closed = false;

  const ensureOpen = (methodName: string): void => {
    if (closed) {
      throw new Error(`lix is closed; ${methodName}() is unavailable`);
    }
  };

  const execute = async (
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<QueryResult> => {
    ensureOpen("execute");
    return wasmLix.execute(sql, params.map((param) => Value.from(param)));
  };

  const createVersion = async (
    args2: CreateVersionOptions = {},
  ): Promise<CreateVersionResult> => {
    ensureOpen("createVersion");
    const activeVersionResult = await execute(
      "SELECT av.version_id, v.commit_id \
       FROM lix_active_version av \
       JOIN lix_version v ON v.id = av.version_id \
       ORDER BY av.id LIMIT 1",
    );
    const activeVersionRow = firstRow(activeVersionResult, "active version");
    const activeVersionId = valueAsText(
      activeVersionRow[0],
      "active_version.version_id",
    );
    const commitId = valueAsText(activeVersionRow[1], "lix_version.commit_id");

    const id =
      args2.id ??
      valueAsText(
        firstRow(await execute("SELECT lix_uuid_v7()"), "generated version id")[0],
        "lix_uuid_v7()",
      );
    const name = args2.name ?? id;
    const inheritsFromVersionId = args2.inheritsFromVersionId ?? activeVersionId;
    const hidden = args2.hidden === true ? 1 : 0;
    const workingCommitId = valueAsText(
      firstRow(await execute("SELECT lix_uuid_v7()"), "generated working commit id")[0],
      "lix_uuid_v7()",
    );

    await execute(
      "INSERT INTO lix_version (\
       id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
       ) VALUES (?, ?, ?, ?, ?, ?)",
      [id, name, inheritsFromVersionId, hidden, commitId, workingCommitId],
    );

    return { id, name, inheritsFromVersionId };
  };

  const switchVersion = async (versionId: string): Promise<void> => {
    ensureOpen("switchVersion");
    if (!versionId || typeof versionId !== "string") {
      throw new Error("switchVersion requires a non-empty versionId string");
    }
    await execute("UPDATE lix_active_version SET version_id = ?", [versionId]);
  };

  const installPlugin = async (args2: InstallPluginOptions): Promise<void> => {
    ensureOpen("installPlugin");
    if (typeof (wasmLix as any).installPlugin !== "function") {
      throw new Error("installPlugin is not available in this wasm build");
    }
    const manifestJson =
      typeof args2.manifestJson === "string"
        ? args2.manifestJson
        : JSON.stringify(args2.manifestJson);
    const wasmBytes =
      args2.wasmBytes instanceof Uint8Array
        ? args2.wasmBytes
        : new Uint8Array(args2.wasmBytes);

    await (wasmLix as any).installPlugin(manifestJson, wasmBytes);
  };

  const close = async (): Promise<void> => {
    if (closed) {
      return;
    }
    closed = true;

    let firstError: unknown;
    try {
      if (typeof (wasmLix as any).free === "function") {
        (wasmLix as any).free();
      }
    } catch (error) {
      firstError = error;
    }

    try {
      if (typeof backend.close === "function") {
        await backend.close();
      }
    } catch (error) {
      if (!firstError) {
        firstError = error;
      }
    }

    if (firstError) {
      throw firstError instanceof Error
        ? firstError
        : new Error(String(firstError));
    }
  };

  return {
    execute,
    createVersion,
    switchVersion,
    installPlugin,
    close,
  };
}

function firstRow(result: QueryResult, context: string): unknown[] {
  const rows = (result as any)?.rows;
  if (!Array.isArray(rows) || rows.length === 0 || !Array.isArray(rows[0])) {
    throw new Error(`Expected at least one row while reading ${context}`);
  }
  return rows[0] as unknown[];
}

function valueAsText(value: unknown, fieldName: string): string {
  const parsed = Value.from(value);
  const text = parsed.asText();
  if (text !== undefined) {
    return text;
  }
  const integer = parsed.asInteger();
  if (integer !== undefined) {
    return integer.toString();
  }
  throw new Error(`Expected text-like value for ${fieldName}`);
}
