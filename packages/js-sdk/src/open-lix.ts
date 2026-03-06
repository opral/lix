import init, {
	initLix as initLixWasm,
	openLix as openLixWasm,
	type JsonValue,
	Value,
	resolveEngineWasmModuleOrPath,
} from "./engine-wasm/index.js";
import { createWasmSqliteBackend } from "./backend/wasm-sqlite.js";
import type { LixWasmRuntime } from "./engine-wasm/index.js";
import type {
	LixBackend,
	LixCanonicalExecuteResult,
	LixCanonicalQueryResult,
	LixCanonicalValue,
	LixRuntimeExecuteResult,
	LixRuntimeQueryResult,
	LixRuntimeValue,
} from "./types.js";

export type {
	LixBackend,
	LixCanonicalExecuteResult,
	LixCanonicalQueryResult,
	LixCanonicalValue,
	LixSqlDialect,
	LixTransaction,
	LixRuntimeExecuteResult,
	LixRuntimeQueryResult,
	LixRuntimeValue,
} from "./types.js";
export { Value } from "./engine-wasm/index.js";

export type CreateVersionOptions = {
	id?: string;
	name?: string;
	hidden?: boolean;
};

export type CreateVersionResult = {
	id: string;
	name: string;
};

export type InstallPluginOptions = {
	archiveBytes: Uint8Array | ArrayBuffer;
};

export type CreateCheckpointResult = {
	id: string;
	changeSetId: string;
};

export type ObserveQuery = {
	sql: string;
	params?: ReadonlyArray<LixRuntimeValue>;
};

export type ExecuteOptions = {
	writerKey?: string | null;
};

export type SqlTransaction = {
	execute(
		sql: string,
		params?: ReadonlyArray<LixRuntimeValue>,
	): Promise<LixRuntimeExecuteResult>;
	commit(): Promise<void>;
	rollback(): Promise<void>;
};

export type ObserveEvent = {
	sequence: number;
	rows: LixRuntimeQueryResult;
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

export type InitLixResult = {
	initialized: boolean;
};

export type Lix = {
	execute(
		sql: string,
		params?: ReadonlyArray<LixRuntimeValue>,
		options?: ExecuteOptions,
	): Promise<LixRuntimeExecuteResult>;
	beginTransaction(options?: ExecuteOptions): Promise<SqlTransaction>;
	transaction<T>(
		options: ExecuteOptions,
		f: (tx: SqlTransaction) => Promise<T>,
	): Promise<T>;
	transaction<T>(f: (tx: SqlTransaction) => Promise<T>): Promise<T>;
	observe(query: ObserveQuery): ObserveEvents;
	createVersion(args?: CreateVersionOptions): Promise<CreateVersionResult>;
	createCheckpoint(): Promise<CreateCheckpointResult>;
	switchVersion(versionId: string): Promise<void>;
	installPlugin(
		args: InstallPluginOptions | Uint8Array | ArrayBuffer,
	): Promise<void>;
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

export async function initLix(args: {
	backend: LixBackend;
	keyValues?: ReadonlyArray<OpenLixKeyValue>;
}): Promise<InitLixResult> {
	await ensureWasmReady();
	const wasmBackend = createCanonicalBackendAdapter(args.backend);
	const result = await initLixWasm(
		wasmBackend as any,
		await getDefaultWasmRuntime(),
		args.keyValues ? [...args.keyValues] : undefined,
	);
	return normalizeInitLixResult(result);
}

export async function openLix(
	args: {
		backend?: LixBackend;
		keyValues?: ReadonlyArray<OpenLixKeyValue>;
	} = {},
): Promise<Lix> {
	await ensureWasmReady();
	const backend = args.backend ?? (await createWasmSqliteBackend());
	if (!args.backend) {
		await initLix({
			backend,
			keyValues: args.keyValues,
		});
	} else if (args.keyValues && args.keyValues.length > 0) {
		throw new Error(
			"openLix({ backend, keyValues }) is not supported; call initLix({ backend, keyValues }) before openLix({ backend })",
		);
	}
	const wasmBackend = createCanonicalBackendAdapter(backend);
	const wasmLix = await openLixWasm(
		wasmBackend as any,
		await getDefaultWasmRuntime(),
	);
	let closed = false;
	let closing = false;
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
		params: ReadonlyArray<LixRuntimeValue> = [],
		options?: ExecuteOptions,
	): Promise<LixCanonicalExecuteResult> =>
		(wasmLix as any).execute(
			sql,
			params.map((param) => encodeRuntimeSqlParam(param, "execute")),
			normalizeExecuteOptions(options, "execute"),
		);

	const acquireTransactionSlot = async (): Promise<() => void> => {
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
		params: ReadonlyArray<LixRuntimeValue> = [],
		options?: ExecuteOptions,
	): Promise<LixRuntimeExecuteResult> => {
		ensureOpen("execute");
		const result = await runQueued(() => runExecute(sql, params, options));
		return decodeCanonicalExecuteResult(result);
	};

	const beginTransaction = async (
		options?: ExecuteOptions,
	): Promise<SqlTransaction> => {
		ensureOpen("beginTransaction");
		const releaseSlot = await acquireTransactionSlot();
		const transactionOptions = normalizeExecuteOptions(
			options,
			"beginTransaction",
		);
		let transactionClosed = false;
		if (typeof (wasmLix as any).beginTransaction !== "function") {
			releaseSlot();
			throw new Error("beginTransaction is not available in this wasm build");
		}

		let wasmTransaction: any;
		try {
			wasmTransaction = await (wasmLix as any).beginTransaction(
				transactionOptions,
			);
		} catch (error) {
			releaseSlot();
			throw error;
		}
		if (!wasmTransaction || typeof wasmTransaction.execute !== "function") {
			releaseSlot();
			throw new Error(
				"beginTransaction() returned an invalid transaction object",
			);
		}

		const tx = {
			execute: async (
				sql: string,
				params: ReadonlyArray<LixRuntimeValue> = [],
			): Promise<LixRuntimeExecuteResult> => {
				if (transactionClosed) {
					throw new Error("transaction is closed; execute() is unavailable");
				}
				if (closing || closed) {
					throw new Error(
						"lix is closed; transaction.execute() is unavailable",
					);
				}
				const result = await wasmTransaction.execute(
					sql,
					params.map((param: LixRuntimeValue) =>
						encodeRuntimeSqlParam(param, "transaction.execute"),
					),
				);
				return decodeCanonicalExecuteResult(
					result as LixCanonicalExecuteResult,
				);
			},
			commit: async (): Promise<void> => {
				if (transactionClosed) {
					return;
				}
				try {
					await wasmTransaction.commit();
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
					await wasmTransaction.rollback();
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
					await wasmTransaction.rollback();
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
	async function transaction<T>(
		f: (tx: SqlTransaction) => Promise<T>,
	): Promise<T>;
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

	const observe = (query: ObserveQuery): ObserveEvents => {
		ensureOpen("observe");
		if (
			!query ||
			typeof query.sql !== "string" ||
			query.sql.trim().length === 0
		) {
			throw new Error("observe requires a non-empty sql string");
		}
		const rawEvents = (wasmLix as any).observe({
			sql: query.sql,
			params: (query.params ?? []).map((param) =>
				encodeRuntimeSqlParam(param, "observe"),
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
				const event = next as {
					sequence: number;
					rows: LixCanonicalQueryResult;
				};
				return {
					sequence: event.sequence,
					rows: decodeCanonicalQueryResult(event.rows),
				};
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
		if (typeof id !== "string" || id.length === 0) {
			throw new Error("createVersion() result is missing string id");
		}
		if (typeof name !== "string" || name.length === 0) {
			throw new Error("createVersion() result is missing string name");
		}
		return { id, name };
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
			throw new Error(
				"createCheckpoint() result is missing string changeSetId",
			);
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
		throw new Error(
			"js-sdk node runtime module is missing createNodeWasmRuntime()",
		);
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
	const globalProcess = (
		globalThis as {
			process?: { versions?: { node?: string } };
		}
	).process;
	return (
		typeof globalProcess === "object" &&
		typeof globalProcess.versions === "object" &&
		typeof globalProcess.versions?.node === "string"
	);
}

function normalizeExecuteOptions(
	options: ExecuteOptions | undefined,
	methodName: "execute" | "beginTransaction",
): ExecuteOptions | undefined {
	if (options === undefined) {
		return undefined;
	}
	if (!options || typeof options !== "object" || Array.isArray(options)) {
		throw new Error(`${methodName} options must be an object`);
	}
	const writerKey = (options as { writerKey?: unknown }).writerKey;
	if (
		writerKey !== undefined &&
		writerKey !== null &&
		typeof writerKey !== "string"
	) {
		throw new Error(`${methodName} options.writerKey must be a string or null`);
	}
	if (writerKey === undefined) {
		return undefined;
	}
	return {
		writerKey,
	};
}

function normalizeInitLixResult(result: unknown): InitLixResult {
	if (!result || typeof result !== "object") {
		throw new Error("initLix() must return an object");
	}
	const initialized = (result as { initialized?: unknown }).initialized;
	if (typeof initialized !== "boolean") {
		throw new Error("initLix() result is missing boolean initialized");
	}
	return { initialized };
}

function encodeRuntimeSqlParam(
	param: LixRuntimeValue,
	context: "execute" | "transaction.execute" | "observe",
): Value {
	if (param === null || param === undefined) {
		return Value.null();
	}
	if (typeof param === "boolean") {
		return Value.boolean(param);
	}
	if (typeof param === "number") {
		return Number.isInteger(param) ? Value.integer(param) : Value.real(param);
	}
	if (typeof param === "string") {
		return Value.text(param);
	}
	if (param instanceof Uint8Array) {
		return Value.blob(param);
	}
	throw new TypeError(
		`${context} params must be runtime scalar values or Uint8Array`,
	);
}

function decodeCanonicalQueryResult(
	result: LixCanonicalQueryResult,
): LixRuntimeQueryResult {
	const rows = Array.isArray(result?.rows) ? result.rows : [];
	const columns = Array.isArray(result?.columns)
		? result.columns.filter(
				(column): column is string => typeof column === "string",
			)
		: [];

	return {
		rows: rows.map((row) =>
			Array.isArray(row)
				? row.map((value) => decodeCanonicalValue(value, "query result cell"))
				: [],
		),
		columns,
	};
}

function decodeCanonicalExecuteResult(
	result: LixCanonicalExecuteResult,
): LixRuntimeExecuteResult {
	const statements = Array.isArray(result?.statements) ? result.statements : [];
	return {
		statements: statements.map((statement) =>
			decodeCanonicalQueryResult(statement),
		),
	};
}

function decodeCanonicalValue(
	value: unknown,
	context: string,
): LixRuntimeValue {
	if (!isCanonicalLixValue(value)) {
		throw new TypeError(`${context} must be a canonical LixValue`);
	}
	switch (value.kind) {
		case "null":
			return null;
		case "bool":
			return value.value;
		case "int":
		case "float":
			return value.value;
		case "text":
			return value.value;
		case "json":
			return value.value;
		case "blob":
			return Value.from(value).asBlob() ?? new Uint8Array();
	}
}

function encodeRuntimeQueryResult(
	result: LixRuntimeQueryResult,
	context: string,
): LixCanonicalQueryResult {
	if (!result || typeof result !== "object" || !Array.isArray(result.rows)) {
		throw new TypeError(`${context} must return { rows, columns }`);
	}
	const columns = Array.isArray(result.columns)
		? result.columns.filter(
				(column): column is string => typeof column === "string",
			)
		: [];
	return {
		columns,
		rows: result.rows.map((row) =>
			Array.isArray(row)
				? row.map((value) => encodeRuntimeValue(value, `${context} cell`))
				: [],
		),
	};
}

function encodeRuntimeValue(
	value: unknown,
	context: string,
): LixCanonicalValue {
	if (value === null || value === undefined) {
		return { kind: "null", value: null };
	}
	if (typeof value === "boolean") {
		return { kind: "bool", value };
	}
	if (typeof value === "number") {
		if (!Number.isFinite(value)) {
			throw new TypeError(`${context} number must be finite`);
		}
		return Number.isInteger(value)
			? { kind: "int", value }
			: { kind: "float", value };
	}
	if (typeof value === "string") {
		return { kind: "text", value };
	}
	if (value instanceof Uint8Array) {
		return Value.blob(value).toJSON();
	}
	if (ArrayBuffer.isView(value)) {
		return Value.blob(
			new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
		).toJSON();
	}
	if (value instanceof ArrayBuffer) {
		return Value.blob(new Uint8Array(value)).toJSON();
	}
	if (isJsonRuntimeValue(value)) {
		return { kind: "json", value };
	}
	throw new TypeError(
		`${context} must be a runtime scalar value or Uint8Array`,
	);
}

function createCanonicalBackendAdapter(backend: LixBackend): {
	dialect?: "sqlite" | "postgres" | (() => "sqlite" | "postgres");
	execute(
		sql: string,
		params: ReadonlyArray<LixCanonicalValue>,
	): Promise<LixCanonicalQueryResult>;
	beginTransaction?: () => Promise<{
		dialect?: "sqlite" | "postgres" | (() => "sqlite" | "postgres");
		execute(
			sql: string,
			params: ReadonlyArray<LixCanonicalValue>,
		): Promise<LixCanonicalQueryResult>;
		commit(): Promise<void> | void;
		rollback(): Promise<void> | void;
	}>;
	exportSnapshot?: () =>
		| Promise<Uint8Array | ArrayBuffer>
		| Uint8Array
		| ArrayBuffer;
} {
	const adapted = {
		dialect: backend.dialect,
		async execute(sql: string, params: ReadonlyArray<LixCanonicalValue>) {
			const runtimeParams = params.map((param) =>
				decodeCanonicalValue(param, "backend.execute param"),
			);
			const result = await backend.execute(sql, runtimeParams);
			return encodeRuntimeQueryResult(result, "backend.execute result");
		},
	} as {
		dialect?: "sqlite" | "postgres" | (() => "sqlite" | "postgres");
		execute(
			sql: string,
			params: ReadonlyArray<LixCanonicalValue>,
		): Promise<LixCanonicalQueryResult>;
		beginTransaction?: () => Promise<{
			dialect?: "sqlite" | "postgres" | (() => "sqlite" | "postgres");
			execute(
				sql: string,
				params: ReadonlyArray<LixCanonicalValue>,
			): Promise<LixCanonicalQueryResult>;
			commit(): Promise<void> | void;
			rollback(): Promise<void> | void;
		}>;
		exportSnapshot?: () =>
			| Promise<Uint8Array | ArrayBuffer>
			| Uint8Array
			| ArrayBuffer;
	};

	if (typeof backend.beginTransaction === "function") {
		adapted.beginTransaction = async () => {
			const tx = await backend.beginTransaction!();
			return {
				dialect: tx.dialect,
				async execute(sql: string, params: ReadonlyArray<LixCanonicalValue>) {
					const runtimeParams = params.map((param) =>
						decodeCanonicalValue(param, "backend.transaction.execute param"),
					);
					const result = await tx.execute(sql, runtimeParams);
					return encodeRuntimeQueryResult(
						result,
						"backend.transaction.execute result",
					);
				},
				commit: () => tx.commit(),
				rollback: () => tx.rollback(),
			};
		};
	}

	if (typeof backend.exportSnapshot === "function") {
		adapted.exportSnapshot = () => backend.exportSnapshot!();
	}

	return adapted;
}

function isCanonicalLixValue(value: unknown): value is LixCanonicalValue {
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
		return isJsonRuntimeValue((value as { value?: unknown }).value);
	}
	if (kind === "blob") {
		return typeof (value as { base64?: unknown }).base64 === "string";
	}
	return false;
}

function isJsonRuntimeValue(value: unknown): value is JsonValue {
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
		return value.every((entry) => isJsonRuntimeValue(entry));
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
	return Object.values(value).every((entry) => isJsonRuntimeValue(entry));
}
