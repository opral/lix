import init, {
  openLix as openLixWasm,
  QueryResult,
  Value,
  resolveEngineWasmModuleOrPath,
} from "./engine-wasm/index.js";
import { createWasmSqliteBackend } from "./backend/wasm-sqlite.js";
import type { LixWasmRuntime } from "./engine-wasm/index.js";
import type { LixBackend } from "./types.js";

export type {
  LixBackend,
  LixQueryResultLike,
  LixSqlDialect,
  LixTransaction,
  LixValueLike,
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

export type StateCommitEventFilter = {
  schemaKeys?: string[];
  entityIds?: string[];
  fileIds?: string[];
  versionIds?: string[];
  writerKeys?: string[];
  excludeWriterKeys?: string[];
  includeUntracked?: boolean;
};

export type StateCommitEventOperation = "Insert" | "Update" | "Delete";

export type StateCommitEventChange = {
  operation: StateCommitEventOperation;
  entityId: string;
  schemaKey: string;
  schemaVersion: string;
  fileId: string;
  versionId: string;
  pluginKey: string;
  snapshotContent: unknown | null;
  untracked: boolean;
  writerKey: string | null;
};

export type StateCommitEventBatch = {
  sequence: number;
  changes: StateCommitEventChange[];
};

export type StateCommitEvents = {
  tryNext(): StateCommitEventBatch | undefined;
  close(): void;
};

export type ObserveQuery = {
  sql: string;
  params?: ReadonlyArray<unknown>;
};

export type ObserveEvent = {
  sequence: number;
  rows: QueryResult;
  stateCommitSequence: number | null;
};

export type ObserveEvents = {
  next(): Promise<ObserveEvent | undefined>;
  close(): void;
};

export type OpenLixKeyValue = {
  key: string;
  value: unknown;
  versionId?: string;
  version_id?: string;
  lixcol_version_id?: string;
};

export type Lix = {
  execute(sql: string, params?: ReadonlyArray<unknown>): Promise<QueryResult>;
  stateCommitEvents(filter?: StateCommitEventFilter): StateCommitEvents;
  observe(query: ObserveQuery): ObserveEvents;
  createVersion(args?: CreateVersionOptions): Promise<CreateVersionResult>;
  switchVersion(versionId: string): Promise<void>;
  installPlugin(args: InstallPluginOptions): Promise<void>;
  /** Exports the current database as SQLite file bytes (portable `.lix` artifact). */
  exportSnapshot(): Promise<Uint8Array>;
  close(): Promise<void>;
};

let wasmReady: Promise<void> | null = null;
let defaultWasmRuntime: Promise<LixWasmRuntime | undefined> | null = null;

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
    keyValues?: ReadonlyArray<OpenLixKeyValue>;
  } = {},
): Promise<Lix> {
  await ensureWasmReady();
  const backend = args.backend ?? (await createWasmSqliteBackend());
  const wasmLix = await openLixWasm(
    backend,
    await getDefaultWasmRuntime(),
    args.keyValues ? [...args.keyValues] : undefined,
  );
  let closed = false;
  const openStateCommitEventHandles = new Set<{
    close?: () => void;
  }>();
  const openObserveHandles = new Set<{
    close?: () => void;
  }>();

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

  const stateCommitEvents = (
    filter: StateCommitEventFilter = {},
  ): StateCommitEvents => {
    ensureOpen("stateCommitEvents");
    const rawEvents = (wasmLix as any).stateCommitEvents(filter ?? {});
    if (!rawEvents || typeof rawEvents.tryNext !== "function") {
      throw new Error("stateCommitEvents is not available in this wasm build");
    }
    let localClosed = false;
    const close = () => {
      if (localClosed) return;
      localClosed = true;
      openStateCommitEventHandles.delete(rawEvents);
      if (typeof rawEvents.close === "function") {
        rawEvents.close();
      }
    };
    openStateCommitEventHandles.add(rawEvents);

    return {
      tryNext(): StateCommitEventBatch | undefined {
        if (localClosed) return undefined;
        const next = rawEvents.tryNext();
        if (next === undefined || next === null) return undefined;
        return next as StateCommitEventBatch;
      },
      close,
    };
  };

  const observe = (query: ObserveQuery): ObserveEvents => {
    ensureOpen("observe");
    if (!query || typeof query.sql !== "string" || query.sql.trim().length === 0) {
      throw new Error("observe requires a non-empty sql string");
    }
    const rawEvents = (wasmLix as any).observe({
      sql: query.sql,
      params: (query.params ?? []).map((param) => Value.from(param)),
    });
    if (!rawEvents || typeof rawEvents.next !== "function") {
      throw new Error("observe is not available in this wasm build");
    }
    let localClosed = false;
    const close = () => {
      if (localClosed) return;
      localClosed = true;
      openObserveHandles.delete(rawEvents);
      if (typeof rawEvents.close === "function") {
        rawEvents.close();
      }
    };
    openObserveHandles.add(rawEvents);

    return {
      async next(): Promise<ObserveEvent | undefined> {
        if (localClosed) return undefined;
        const next = await rawEvents.next();
        if (next === undefined || next === null) return undefined;
        return next as ObserveEvent;
      },
      close,
    };
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

  const exportSnapshot = async (): Promise<Uint8Array> => {
    ensureOpen("exportSnapshot");
    if (typeof (wasmLix as any).exportSnapshot !== "function") {
      throw new Error("exportSnapshot is not available in this wasm build");
    }
    const output = await (wasmLix as any).exportSnapshot();
    if (output instanceof Uint8Array) {
      return output;
    }
    if (output instanceof ArrayBuffer) {
      return new Uint8Array(output);
    }
    throw new Error("exportSnapshot() must return Uint8Array or ArrayBuffer");
  };

  const close = async (): Promise<void> => {
    if (closed) {
      return;
    }
    closed = true;
    for (const handle of openStateCommitEventHandles) {
      try {
        handle.close?.();
      } catch {
        // ignore close errors from individual event handles
      }
    }
    openStateCommitEventHandles.clear();
    for (const handle of openObserveHandles) {
      try {
        handle.close?.();
      } catch {
        // ignore close errors from individual observe handles
      }
    }
    openObserveHandles.clear();

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
    stateCommitEvents,
    observe,
    createVersion,
    switchVersion,
    installPlugin,
    exportSnapshot,
    close,
  };
}

async function getDefaultWasmRuntime(): Promise<LixWasmRuntime | undefined> {
  if (!defaultWasmRuntime) {
    defaultWasmRuntime = loadDefaultWasmRuntime();
  }
  return await defaultWasmRuntime;
}

async function loadDefaultWasmRuntime(): Promise<LixWasmRuntime | undefined> {
  if (!isNodeRuntime()) {
    return undefined;
  }

  const module = await import("./wasm-runtime/node.js");
  if (typeof module.createNodeWasmRuntime !== "function") {
    throw new Error("js-sdk node runtime module is missing createNodeWasmRuntime()");
  }
  return module.createNodeWasmRuntime();
}

function isNodeRuntime(): boolean {
  const globalProcess = (globalThis as {
    process?: { versions?: { node?: string } };
  }).process;
  return (
    typeof globalProcess === "object" &&
    typeof globalProcess.versions === "object" &&
    typeof globalProcess.versions?.node === "string"
  );
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
