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
  archiveBytes: Uint8Array | ArrayBuffer;
};

export type CreateCheckpointResult = {
  id: string;
  changeSetId: string;
};

export type StateCommitStreamFilter = {
  schemaKeys?: string[];
  entityIds?: string[];
  fileIds?: string[];
  versionIds?: string[];
  writerKeys?: string[];
  excludeWriterKeys?: string[];
  includeUntracked?: boolean;
};

export type StateCommitStreamOperation = "Insert" | "Update" | "Delete";

export type StateCommitStreamChange = {
  operation: StateCommitStreamOperation;
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

export type StateCommitStreamBatch = {
  sequence: number;
  changes: StateCommitStreamChange[];
};

export type StateCommitStream = {
  tryNext(): StateCommitStreamBatch | undefined;
  close(): void;
};

export type ObserveQuery = {
  sql: string;
  params?: ReadonlyArray<unknown>;
};

export type TransactionStatement = {
  sql: string;
  params?: ReadonlyArray<unknown>;
};

export type ExecuteOptions = {
  writerKey?: string | null;
};

export type SqlTransaction = {
  execute(
    sql: string,
    params?: ReadonlyArray<unknown>,
  ): Promise<QueryResult>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
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
  lixcol_version_id?: string;
  lixcol_untracked?: boolean;
};

export type Lix = {
  execute(
    sql: string,
    params?: ReadonlyArray<unknown>,
    options?: ExecuteOptions,
  ): Promise<QueryResult>;
  beginTransaction(options?: ExecuteOptions): Promise<SqlTransaction>;
  transaction<T>(
    options: ExecuteOptions,
    f: (tx: SqlTransaction) => Promise<T>,
  ): Promise<T>;
  transaction<T>(f: (tx: SqlTransaction) => Promise<T>): Promise<T>;
  executeTransaction(
    statements: ReadonlyArray<TransactionStatement>,
    options?: ExecuteOptions,
  ): Promise<QueryResult>;
  stateCommitStream(filter?: StateCommitStreamFilter): StateCommitStream;
  observe(query: ObserveQuery): ObserveEvents;
  createVersion(args?: CreateVersionOptions): Promise<CreateVersionResult>;
  createCheckpoint(): Promise<CreateCheckpointResult>;
  switchVersion(versionId: string): Promise<void>;
  installPlugin(args: InstallPluginOptions | Uint8Array | ArrayBuffer): Promise<void>;
  /** Exports the current database as SQLite file bytes (portable `.lix` artifact). */
  exportSnapshot(): Promise<Uint8Array>;
  close(): Promise<void>;
};

let wasmReady: Promise<void> | null = null;
let defaultWasmRuntime: Promise<LixWasmRuntime> | null = null;

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
  let closing = false;
  const openStateCommitStreamHandles = new Set<{
    close?: () => void;
  }>();
  const openObserveHandles = new Set<{
    close?: () => void;
  }>();
  const openSqlTransactions = new Set<{
    forceRollback: () => Promise<void>;
  }>();
  let transactionQueue: Promise<void> = Promise.resolve();

  const ensureOpen = (methodName: string): void => {
    if (closed || closing) {
      throw new Error(`lix is closed; ${methodName}() is unavailable`);
    }
  };

  const runExecute = (
    sql: string,
    params: ReadonlyArray<unknown> = [],
    options?: ExecuteOptions,
  ): Promise<QueryResult> =>
    (wasmLix as any).execute(
      sql,
      params.map((param) => Value.from(normalizeSqlParam(param))),
      normalizeExecuteOptions(options, "execute"),
    );

  const acquireTransactionSlot = async (): Promise<(() => void)> => {
    const previous = transactionQueue;
    let releaseCurrent: (() => void) | undefined;
    const current = new Promise<void>((resolve) => {
      releaseCurrent = resolve;
    });
    transactionQueue = previous.then(() => current);
    await previous;
    return () => {
      releaseCurrent?.();
    };
  };

  const runQueued = async <T>(operation: () => Promise<T>): Promise<T> => {
    const release = await acquireTransactionSlot();
    try {
      return await operation();
    } finally {
      release();
    }
  };

  const execute = async (
    sql: string,
    params: ReadonlyArray<unknown> = [],
    options?: ExecuteOptions,
  ): Promise<QueryResult> => {
    ensureOpen("execute");
    return runQueued(() => runExecute(sql, params, options));
  };

  const beginTransaction = async (
    options?: ExecuteOptions,
  ): Promise<SqlTransaction> => {
    ensureOpen("beginTransaction");
    const releaseSlot = await acquireTransactionSlot();
    const transactionOptions = normalizeExecuteOptions(options, "beginTransaction");
    let transactionClosed = false;

    try {
      await runExecute("BEGIN", [], transactionOptions);
    } catch (error) {
      releaseSlot();
      throw error;
    }

    const tx = {
      execute: async (
        sql: string,
        params: ReadonlyArray<unknown> = [],
      ): Promise<QueryResult> => {
        if (transactionClosed) {
          throw new Error("transaction is closed; execute() is unavailable");
        }
        if (closing || closed) {
          throw new Error("lix is closed; transaction.execute() is unavailable");
        }
        return runExecute(sql, params, transactionOptions);
      },
      commit: async (): Promise<void> => {
        if (transactionClosed) {
          return;
        }
        try {
          await runExecute("COMMIT", [], transactionOptions);
        } finally {
          transactionClosed = true;
          openSqlTransactions.delete(txHandle);
          releaseSlot();
        }
      },
      rollback: async (): Promise<void> => {
        if (transactionClosed) {
          return;
        }
        try {
          await runExecute("ROLLBACK", [], transactionOptions);
        } finally {
          transactionClosed = true;
          openSqlTransactions.delete(txHandle);
          releaseSlot();
        }
      },
    } satisfies SqlTransaction;

    const txHandle = {
      forceRollback: async (): Promise<void> => {
        if (transactionClosed) {
          return;
        }
        try {
          await runExecute("ROLLBACK", [], transactionOptions);
        } finally {
          transactionClosed = true;
          releaseSlot();
        }
      },
    };
    openSqlTransactions.add(txHandle);
    return tx;
  };

  async function transaction<T>(
    options: ExecuteOptions,
    f: (tx: SqlTransaction) => Promise<T>,
  ): Promise<T>;
  async function transaction<T>(f: (tx: SqlTransaction) => Promise<T>): Promise<T>;
  async function transaction<T>(
    first: ExecuteOptions | ((tx: SqlTransaction) => Promise<T>),
    second?: (tx: SqlTransaction) => Promise<T>,
  ): Promise<T> {
    ensureOpen("transaction");
    const options = typeof first === "function" ? undefined : first;
    const callback = (typeof first === "function" ? first : second) as
      | ((tx: SqlTransaction) => Promise<T>)
      | undefined;
    if (typeof callback !== "function") {
      throw new Error("transaction requires an async callback");
    }
    const tx = await beginTransaction(options);
    try {
      const value = await callback(tx);
      await tx.commit();
      return value;
    } catch (error) {
      try {
        await tx.rollback();
      } catch {
        // ignore rollback errors; original error is more relevant to caller
      }
      throw error;
    }
  }

  const executeTransaction = async (
    statements: ReadonlyArray<TransactionStatement>,
    options?: ExecuteOptions,
  ): Promise<QueryResult> => {
    ensureOpen("executeTransaction");
    if (!Array.isArray(statements)) {
      throw new Error("executeTransaction requires an array of statements");
    }
    if (typeof (wasmLix as any).executeTransaction !== "function") {
      throw new Error("executeTransaction is not available in this wasm build");
    }

    const encoded = statements.map((statement, index) => {
      const sql = String(statement?.sql ?? "").trim();
      if (sql.length === 0) {
        throw new Error(`executeTransaction statement ${index} has empty sql`);
      }
      const params = statement?.params ?? [];
      if (!Array.isArray(params)) {
        throw new Error(`executeTransaction statement ${index}.params must be an array`);
      }
      return {
        sql,
        params: params.map((param) => Value.from(normalizeSqlParam(param))),
      };
    });

    return runQueued(() =>
      (wasmLix as any).executeTransaction(
        encoded,
        normalizeExecuteOptions(options, "executeTransaction"),
      ),
    );
  };

  const stateCommitStream = (
    filter: StateCommitStreamFilter = {},
  ): StateCommitStream => {
    ensureOpen("stateCommitStream");
    const rawEvents = (wasmLix as any).stateCommitStream(filter ?? {});
    if (!rawEvents || typeof rawEvents.tryNext !== "function") {
      throw new Error("stateCommitStream is not available in this wasm build");
    }
    let localClosed = false;
    const close = () => {
      if (localClosed) return;
      localClosed = true;
      openStateCommitStreamHandles.delete(rawEvents);
      if (typeof rawEvents.close === "function") {
        rawEvents.close();
      }
    };
    openStateCommitStreamHandles.add(rawEvents);

    return {
      tryNext(): StateCommitStreamBatch | undefined {
        if (localClosed) return undefined;
        const next = rawEvents.tryNext();
        if (next === undefined || next === null) return undefined;
        return next as StateCommitStreamBatch;
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
      params: (query.params ?? []).map((param) =>
        Value.from(normalizeSqlParam(param)),
      ),
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
    if (typeof (wasmLix as any).createVersion !== "function") {
      throw new Error("createVersion is not available in this wasm build");
    }
    const raw = await runQueued(() => (wasmLix as any).createVersion(args2));
    if (!raw || typeof raw !== "object") {
      throw new Error("createVersion() must return an object");
    }
    const id = (raw as { id?: unknown }).id;
    const name = (raw as { name?: unknown }).name;
    const inheritsFromVersionId =
      (
        raw as {
          inheritsFromVersionId?: unknown;
          inherits_from_version_id?: unknown;
        }
      ).inheritsFromVersionId ??
      (raw as { inherits_from_version_id?: unknown }).inherits_from_version_id;
    if (typeof id !== "string" || id.length === 0) {
      throw new Error("createVersion() result is missing string id");
    }
    if (typeof name !== "string" || name.length === 0) {
      throw new Error("createVersion() result is missing string name");
    }
    if (typeof inheritsFromVersionId !== "string" || inheritsFromVersionId.length === 0) {
      throw new Error("createVersion() result is missing string inheritsFromVersionId");
    }
    return { id, name, inheritsFromVersionId };
  };

  const switchVersion = async (versionId: string): Promise<void> => {
    ensureOpen("switchVersion");
    if (!versionId || typeof versionId !== "string") {
      throw new Error("switchVersion requires a non-empty versionId string");
    }
    if (typeof (wasmLix as any).switchVersion !== "function") {
      throw new Error("switchVersion is not available in this wasm build");
    }
    await runQueued(() => (wasmLix as any).switchVersion(versionId));
  };

  const installPlugin = async (
    args2: InstallPluginOptions | Uint8Array | ArrayBuffer,
  ): Promise<void> => {
    ensureOpen("installPlugin");
    if (typeof (wasmLix as any).installPlugin !== "function") {
      throw new Error("installPlugin is not available in this wasm build");
    }
    const archiveBytes =
      args2 instanceof Uint8Array
        ? args2
        : args2 instanceof ArrayBuffer
          ? new Uint8Array(args2)
          : args2.archiveBytes instanceof Uint8Array
            ? args2.archiveBytes
            : new Uint8Array(args2.archiveBytes);
    if (archiveBytes.byteLength === 0) {
      throw new Error("installPlugin requires non-empty archiveBytes");
    }

    await runQueued(() => (wasmLix as any).installPlugin(archiveBytes));
  };

  const createCheckpoint = async (): Promise<CreateCheckpointResult> => {
    ensureOpen("createCheckpoint");
    if (typeof (wasmLix as any).createCheckpoint !== "function") {
      throw new Error("createCheckpoint is not available in this wasm build");
    }
    const raw = await runQueued(() => (wasmLix as any).createCheckpoint());
    if (!raw || typeof raw !== "object") {
      throw new Error("createCheckpoint() must return an object");
    }
    const id = (raw as { id?: unknown }).id;
    const changeSetId =
      (raw as { changeSetId?: unknown; change_set_id?: unknown }).changeSetId ??
      (raw as { change_set_id?: unknown }).change_set_id;
    if (typeof id !== "string" || id.length === 0) {
      throw new Error("createCheckpoint() result is missing string id");
    }
    if (typeof changeSetId !== "string" || changeSetId.length === 0) {
      throw new Error("createCheckpoint() result is missing string changeSetId");
    }
    return { id, changeSetId };
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
    if (closed || closing) {
      return;
    }
    closing = true;
    for (const handle of openStateCommitStreamHandles) {
      try {
        handle.close?.();
      } catch {
        // ignore close errors from individual event handles
      }
    }
    openStateCommitStreamHandles.clear();
    for (const handle of openObserveHandles) {
      try {
        handle.close?.();
      } catch {
        // ignore close errors from individual observe handles
      }
    }
    openObserveHandles.clear();

    for (const tx of [...openSqlTransactions]) {
      try {
        await tx.forceRollback();
      } catch {
        // ignore rollback failures while shutting down
      }
    }
    openSqlTransactions.clear();

    let firstError: unknown;
    try {
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
    } finally {
      closed = true;
      closing = false;
    }
  };

  return {
    execute,
    beginTransaction,
    transaction,
    executeTransaction,
    stateCommitStream,
    observe,
    createVersion,
    createCheckpoint,
    switchVersion,
    installPlugin,
    exportSnapshot,
    close,
  };
}

async function getDefaultWasmRuntime(): Promise<LixWasmRuntime> {
  if (!defaultWasmRuntime) {
    defaultWasmRuntime = loadDefaultWasmRuntime();
  }
  return await defaultWasmRuntime;
}

async function loadDefaultWasmRuntime(): Promise<LixWasmRuntime> {
  if (!isNodeRuntime()) {
    return createUnsupportedWasmRuntime();
  }

  const nodeRuntimeModulePath = "./wasm-runtime/node.js";
  const module = (await import(
    /* @vite-ignore */
    nodeRuntimeModulePath
  )) as {
    createNodeWasmRuntime?: () => LixWasmRuntime | Promise<LixWasmRuntime>;
  };
  if (typeof module.createNodeWasmRuntime !== "function") {
    throw new Error("js-sdk node runtime module is missing createNodeWasmRuntime()");
  }
  return await module.createNodeWasmRuntime();
}

function createUnsupportedWasmRuntime(): LixWasmRuntime {
  return {
    async initComponent(): Promise<never> {
      throw new Error(
        "js-sdk default wasm runtime is unavailable in this environment; provide a custom wasm runtime",
      );
    },
  };
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

function normalizeExecuteOptions(
  options: ExecuteOptions | undefined,
  methodName: "execute" | "executeTransaction" | "beginTransaction",
): ExecuteOptions | undefined {
  if (options === undefined) {
    return undefined;
  }
  if (!options || typeof options !== "object" || Array.isArray(options)) {
    throw new Error(`${methodName} options must be an object`);
  }
  const writerKey = (options as { writerKey?: unknown }).writerKey;
  if (writerKey !== undefined && writerKey !== null && typeof writerKey !== "string") {
    throw new Error(`${methodName} options.writerKey must be a string or null`);
  }
  if (writerKey === undefined) {
    return undefined;
  }
  return {
    writerKey,
  };
}

function normalizeSqlParam(param: unknown): unknown {
  if (param === null || param === undefined) {
    return param;
  }
  if (param instanceof Value) {
    return param;
  }
  if (isKindValueObject(param)) {
    return {
      kind: param.kind,
      value: normalizeKindValue(param.kind, param.value),
    };
  }
  if (isKindFunctionObject(param)) {
    return param;
  }
  if (param instanceof Uint8Array) {
    return param;
  }
  if (ArrayBuffer.isView(param)) {
    return new Uint8Array(param.buffer, param.byteOffset, param.byteLength);
  }
  if (param instanceof ArrayBuffer) {
    return new Uint8Array(param);
  }
  if (Array.isArray(param) || typeof param === "object") {
    return JSON.stringify(param);
  }
  return param;
}

function isKindFunctionObject(
  value: unknown,
): value is { kind: () => string; value?: unknown } {
  if (!value || typeof value !== "object") {
    return false;
  }
  const kind = (value as { kind?: unknown }).kind;
  return typeof kind === "function";
}

function isKindValueObject(
  value: unknown,
): value is { kind: "Null" | "Integer" | "Real" | "Text" | "Blob"; value: unknown } {
  if (!value || typeof value !== "object") {
    return false;
  }
  const kind = (value as { kind?: unknown }).kind;
  return (
    kind === "Null" ||
    kind === "Integer" ||
    kind === "Real" ||
    kind === "Text" ||
    kind === "Blob"
  );
}

function normalizeKindValue(
  kind: "Null" | "Integer" | "Real" | "Text" | "Blob",
  value: unknown,
): unknown {
  if (kind !== "Blob") {
    return value;
  }
  if (value instanceof Uint8Array) {
    return value;
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  return value;
}
