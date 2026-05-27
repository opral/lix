import init, {
	resolveEngineWasmModuleOrPath,
	Value,
	type LixError,
} from "./engine-wasm/index.js";
import * as wasmModule from "./engine-wasm/index.js";

export type JsonValue =
	| null
	| boolean
	| number
	| string
	| JsonValue[]
	| { [key: string]: JsonValue };

export type LixRuntimeValue = JsonValue | Uint8Array | ArrayBuffer | Value;
export type LixNativeValue = JsonValue | Uint8Array;

export type ExecuteResult = {
	columns: string[];
	rows: Row[];
	rowsAffected: number;
	notices: LixNotice[];
};

export type LixNotice = {
	code: string;
	message: string;
	hint?: string;
};

export class Row {
	readonly columns: string[];
	private readonly valuesByIndex: Value[];

	constructor(columns: string[], values: Value[]) {
		this.columns = columns;
		this.valuesByIndex = values;
	}

	get(columnName: string): LixNativeValue {
		return valueToNative(this.value(columnName));
	}

	tryGet(columnName: string): LixNativeValue | undefined {
		const value = this.tryValue(columnName);
		return value === undefined ? undefined : valueToNative(value);
	}

	value(columnName: string): Value {
		const index = this.columns.indexOf(columnName);
		if (index === -1) {
			throw createLixError(
				"LIX_COLUMN_NOT_FOUND",
				`Column "${columnName}" does not exist. Available columns: ${this.availableColumns()}`,
			);
		}
		const value = this.valuesByIndex[index];
		if (value === undefined) {
			throw createLixError(
				"LIX_COLUMN_NOT_FOUND",
				`Column "${columnName}" is outside row width ${this.valuesByIndex.length}.`,
			);
		}
		return value;
	}

	tryValue(columnName: string): Value | undefined {
		const index = this.columns.indexOf(columnName);
		return index === -1 ? undefined : this.valuesByIndex[index];
	}

	getAt(index: number): LixNativeValue {
		return valueToNative(this.valueAt(index));
	}

	valueAt(index: number): Value {
		const value = this.valuesByIndex[index];
		if (value === undefined) {
			throw createLixError(
				"LIX_COLUMN_NOT_FOUND",
				`Column index ${index} is outside row width ${this.valuesByIndex.length}.`,
			);
		}
		return value;
	}

	values(): Value[] {
		return [...this.valuesByIndex];
	}

	toObject(): Record<string, LixNativeValue> {
		return Object.fromEntries(
			this.columns.map((column, index) => [
				column,
				valueToNative(this.valueAt(index)),
			]),
		);
	}

	toValueMap(): Record<string, Value> {
		return Object.fromEntries(
			this.columns.map((column, index) => [column, this.valueAt(index)]),
		);
	}

	private availableColumns(): string {
		return this.columns.length === 0 ? "<none>" : this.columns.join(", ");
	}
}

function valueToNative(value: Value): LixNativeValue {
	switch (value.kind) {
		case "null":
			return null;
		case "boolean":
		case "integer":
		case "real":
		case "text":
		case "json":
			return value.value as JsonValue;
		case "blob":
			return value.asBlob() ?? new Uint8Array();
	}
}

export type BackendKvScanRange =
	| { kind: "prefix"; prefix: Uint8Array }
	| { kind: "range"; start: Uint8Array; end: Uint8Array };

export type BackendKvGetRequest = {
	groups: BackendKvGetGroup[];
};

export type BackendKvGetGroup = {
	namespace: string;
	keys: Uint8Array[];
};

export type BackendKvValueBatch = {
	groups: BackendKvValueGroup[];
};

export type BackendKvValueGroup = {
	namespace: string;
	values: Array<Uint8Array | null>;
};

export type BackendKvExistsBatch = {
	groups: BackendKvExistsGroup[];
};

export type BackendKvExistsGroup = {
	namespace: string;
	exists: boolean[];
};

export type BackendKvScanRequest = {
	namespace: string;
	range: BackendKvScanRange;
	after?: Uint8Array | null;
	limit: number;
};

export type BackendKvKeyPage = {
	keys: Uint8Array[];
	resumeAfter?: Uint8Array | null;
};

export type BackendKvValuePage = {
	values: Uint8Array[];
	resumeAfter?: Uint8Array | null;
};

export type BackendKvEntryPage = {
	keys: Uint8Array[];
	values: Uint8Array[];
	resumeAfter?: Uint8Array | null;
};

export type BackendKvWriteOp =
	| {
			kind: "put";
			key: Uint8Array;
			value: Uint8Array;
	  }
	| {
			kind: "delete";
			key: Uint8Array;
	  }
	| {
			kind: "deleteRange";
			range: BackendKvScanRange;
	  };

export type BackendKvWriteBatch = {
	groups: BackendKvWriteGroup[];
};

export type BackendKvWriteGroup = {
	namespace: string;
	ops: BackendKvWriteOp[];
};

export type BackendKvWriteStats = {
	puts: number;
	deletes: number;
	deleteRanges: number;
	bytesWritten: number;
};

export type LixBackendReadTransaction = {
	getValues(request: BackendKvGetRequest): BackendKvValueBatch;
	existsMany(request: BackendKvGetRequest): BackendKvExistsBatch;
	scanKeys(request: BackendKvScanRequest): BackendKvKeyPage;
	scanValues(request: BackendKvScanRequest): BackendKvValuePage;
	scanEntries(request: BackendKvScanRequest): BackendKvEntryPage;
	rollback(): void;
};

export type LixBackendWriteTransaction = LixBackendReadTransaction & {
	writeKvBatch(batch: BackendKvWriteBatch): BackendKvWriteStats;
	commit(): void;
};

export type LixBackend = {
	beginReadTransaction(): LixBackendReadTransaction;
	beginWriteTransaction(): LixBackendWriteTransaction;
	close?(): void;
};

export type OpenLixOptions = {
	backend?: LixBackend;
};

export type CreateBranchOptions = {
	id?: string;
	name: string;
	fromCommitId?: string;
};

export type CreateBranchResult = {
	id: string;
	name: string;
	hidden: boolean;
	commitId: string;
};

export type SwitchBranchOptions = {
	branchId: string;
};

export type SwitchBranchResult = {
	branchId: string;
};

export type MergeBranchOptions = {
	sourceBranchId: string;
};

export type MergeBranchOutcome =
	| "alreadyUpToDate"
	| "fastForward"
	| "mergeCommitted";

export type MergeBranchResult = {
	/**
	 * How the merge was applied. `fastForward` advances the target ref without
	 * creating a merge commit, but can still make source changes visible.
	 */
	outcome: MergeBranchOutcome;
	targetBranchId: string;
	sourceBranchId: string;
	baseCommitId: string;
	targetHeadBeforeCommitId: string;
	sourceHeadBeforeCommitId: string;
	targetHeadAfterCommitId: string;
	createdMergeCommitId: string | null;
	changeStats: MergeChangeStats;
};

export type MergeBranchPreviewResult = {
	outcome: MergeBranchOutcome;
	targetBranchId: string;
	sourceBranchId: string;
	baseCommitId: string;
	targetHeadCommitId: string;
	sourceHeadCommitId: string;
	changeStats: MergeChangeStats;
	conflicts: MergeConflict[];
};

export type MergeChangeStats = {
	total: number;
	added: number;
	modified: number;
	removed: number;
};

export type MergeConflict = {
	kind: "sameEntityChanged";
	schemaKey: string;
	entityPk: string[];
	fileId: string | null;
	target: MergeConflictSide;
	source: MergeConflictSide;
};

export type MergeConflictSide = {
	kind: "added" | "modified" | "removed";
	beforeChangeId: string | null;
	afterChangeId: string | null;
};

export type Lix = {
	/**
	 * Executes one DataFusion SQL statement against this Lix session.
	 *
	 * This is not SQLite SQL. Use the DataFusion SQL dialect; positional
	 * placeholders are `?` or `$1`, `$2`, and so on. SQLite-specific catalog tables and
	 * transaction statements such as `sqlite_master`, `BEGIN`, and `COMMIT` are
	 * not available. Use `information_schema` for catalog inspection. While a
	 * transaction is active, call `execute()` on the transaction handle instead.
	 */
	execute(
		sql: string,
		params?: ReadonlyArray<LixRuntimeValue>,
	): Promise<ExecuteResult>;
	beginTransaction(): Promise<LixTransaction>;
	activeBranchId(): Promise<string>;
	createBranch(options: CreateBranchOptions): Promise<CreateBranchResult>;
	switchBranch(options: SwitchBranchOptions): Promise<SwitchBranchResult>;
	mergeBranchPreview(
		options: MergeBranchOptions,
	): Promise<MergeBranchPreviewResult>;
	mergeBranch(options: MergeBranchOptions): Promise<MergeBranchResult>;
	close(): Promise<void>;
};

export type LixTransaction = {
	execute(
		sql: string,
		params?: ReadonlyArray<LixRuntimeValue>,
	): Promise<ExecuteResult>;
	commit(): Promise<void>;
	rollback(): Promise<void>;
};

let wasmReady: Promise<void> | null = null;

type WasmExecuteResult = {
	columns: string[];
	rows: unknown[][];
	rowsAffected: number;
	notices?: LixNotice[];
};

type WasmLix = {
	/**
	 * Executes one DataFusion SQL statement. See `Lix.execute` for the public
	 * SQL contract.
	 */
	execute(sql: string, params: unknown[]): Promise<WasmExecuteResult>;
	beginTransaction(): Promise<WasmLixTransaction>;
	activeBranchId(): Promise<string>;
	createBranch(options: CreateBranchOptions): Promise<CreateBranchResult>;
	switchBranch(options: SwitchBranchOptions): Promise<SwitchBranchResult>;
	mergeBranchPreview(
		options: MergeBranchOptions,
	): Promise<MergeBranchPreviewResult>;
	mergeBranch(options: MergeBranchOptions): Promise<MergeBranchResult>;
	close(): Promise<void>;
};

type WasmLixTransaction = {
	execute(sql: string, params: unknown[]): Promise<WasmExecuteResult>;
	commit(): Promise<void>;
	rollback(): Promise<void>;
};

async function ensureWasmReady(): Promise<void> {
	if (!wasmReady) {
		wasmReady = resolveEngineWasmModuleOrPath()
			.then((module_or_path) => init({ module_or_path }))
			.then(() => undefined);
	}
	await wasmReady;
}

export async function openLix(options: OpenLixOptions = {}): Promise<Lix> {
	await ensureWasmReady();
	try {
		const wasmLix = (await (
			wasmModule as unknown as {
				openLix(options: OpenLixOptions): Promise<WasmLix>;
			}
		).openLix(options)) as WasmLix;
		return createLixHandle(wasmLix);
	} catch (error) {
		try {
			options.backend?.close?.();
		} catch {
			// Preserve the original open failure.
		}
		throw normalizeThrownError(error);
	}
}

function createLixHandle(wasmLix: WasmLix): Lix {
	let operationQueue: Promise<void> = Promise.resolve();

	const acquireOperationSlot = async (): Promise<() => void> => {
		const previous = operationQueue;
		let releaseCurrent: (() => void) | undefined;
		const current = new Promise<void>((resolve) => {
			releaseCurrent = resolve;
		});
		operationQueue = previous.then(() => current);
		await previous;
		return () => releaseCurrent?.();
	};

	const runQueued = async <T>(operation: () => Promise<T>): Promise<T> => {
		const release = await acquireOperationSlot();
		try {
			return await operation();
		} catch (error) {
			throw normalizeThrownError(error);
		} finally {
			release();
		}
	};

	return {
		async execute(
			sql: string,
			params: ReadonlyArray<LixRuntimeValue> = [],
		): Promise<ExecuteResult> {
			validateExecuteArguments(sql, params);
			const values = params.map((param, index) =>
				valueFromExecuteParam(param, index),
			);
			const result = await runQueued(() => wasmLix.execute(sql, values));
			return normalizeExecuteResult(result);
		},

		async beginTransaction(): Promise<LixTransaction> {
			const wasmTransaction = await runQueued(() => wasmLix.beginTransaction());
			return createLixTransactionHandle(wasmTransaction, runQueued);
		},

		async activeBranchId(): Promise<string> {
			return await runQueued(() => wasmLix.activeBranchId());
		},

		async createBranch(
			options: CreateBranchOptions,
		): Promise<CreateBranchResult> {
			return await runQueued(() => wasmLix.createBranch(options));
		},

		async switchBranch(
			options: SwitchBranchOptions,
		): Promise<SwitchBranchResult> {
			return await runQueued(() => wasmLix.switchBranch(options));
		},

		async mergeBranchPreview(
			options: MergeBranchOptions,
		): Promise<MergeBranchPreviewResult> {
			return await runQueued(() => wasmLix.mergeBranchPreview(options));
		},

		async mergeBranch(
			options: MergeBranchOptions,
		): Promise<MergeBranchResult> {
			return await runQueued(() => wasmLix.mergeBranch(options));
		},

		async close(): Promise<void> {
			await runQueued(() => wasmLix.close());
		},
	};
}

function createLixTransactionHandle(
	wasmTransaction: WasmLixTransaction,
	runQueued: <T>(operation: () => Promise<T>) => Promise<T>,
): LixTransaction {
	let closed = false;
	const ensureOpen = () => {
		if (closed) {
			throw createLixError(
				"LIX_INVALID_TRANSACTION_STATE",
				"Lix transaction is closed",
			);
		}
	};

	return {
		async execute(
			sql: string,
			params: ReadonlyArray<LixRuntimeValue> = [],
		): Promise<ExecuteResult> {
			ensureOpen();
			validateExecuteArguments(sql, params);
			const values = params.map((param, index) =>
				valueFromExecuteParam(param, index),
			);
			const result = await runQueued(() =>
				wasmTransaction.execute(sql, values),
			);
			return normalizeExecuteResult(result);
		},

		async commit(): Promise<void> {
			ensureOpen();
			try {
				await runQueued(() => wasmTransaction.commit());
			} finally {
				closed = true;
			}
		},

		async rollback(): Promise<void> {
			ensureOpen();
			try {
				await runQueued(() => wasmTransaction.rollback());
			} finally {
				closed = true;
			}
		},
	};
}

function validateExecuteArguments(
	sql: unknown,
	params: unknown,
): asserts sql is string {
	if (typeof sql !== "string") {
		throw invalidArgumentError("execute", "sql", "string", sql);
	}
	if (!Array.isArray(params)) {
		throw invalidArgumentError("execute", "params", "array", params);
	}
}

function invalidArgumentError(
	operation: string,
	argument: string,
	expected: string,
	actualValue: unknown,
): LixError {
	return createLixError(
		"LIX_INVALID_ARGUMENT",
		`lix.${operation}() expected ${argument} to be ${expectedArticle(expected)} ${expected}`,
		{
			details: {
				operation,
				argument,
				expected,
				actual: runtimeTypeName(actualValue),
			},
		},
	);
}

function valueFromExecuteParam(param: LixRuntimeValue, index: number): Value {
	try {
		return Value.from(param);
	} catch (error) {
		throw invalidParamError(index, param, error);
	}
}

function invalidParamError(
	index: number,
	actualValue: unknown,
	cause: unknown,
): LixError {
	const message =
		cause instanceof Error && cause.message
			? cause.message
			: "parameter is not a valid Lix SQL value";
	return createLixError(
		"LIX_INVALID_PARAM",
		`lix.execute() invalid parameter $${index + 1}: ${message}`,
		{
			details: {
				operation: "execute",
				parameter_index: index + 1,
				argument: `params[${index}]`,
				actual: runtimeTypeName(actualValue),
			},
			cause,
		},
	);
}

function expectedArticle(expected: string): "a" | "an" {
	return /^[aeiou]/i.test(expected) ? "an" : "a";
}

function runtimeTypeName(value: unknown): string {
	if (value === null) return "null";
	if (Array.isArray(value)) return "array";
	if (value instanceof Date) return "Date";
	if (value instanceof ArrayBuffer) return "ArrayBuffer";
	if (ArrayBuffer.isView(value)) return value.constructor.name;
	return typeof value;
}

function normalizeExecuteResult(result: WasmExecuteResult): ExecuteResult {
	const columns = [...result.columns];
	return {
		columns,
		rows: result.rows.map(
			(row) =>
				new Row(
					columns,
					row.map((value) => Value.from(value)),
				),
		),
		rowsAffected: result.rowsAffected,
		notices: result.notices ?? [],
	};
}

function createLixError(
	code: string,
	message: string,
	options: { hint?: string; details?: unknown; cause?: unknown } = {},
): LixError {
	const error = new Error(message) as LixError;
	error.name = "LixError";
	error.code = code;
	if (options.hint !== undefined) {
		error.hint = options.hint;
	}
	if (options.details !== undefined) {
		error.details = options.details;
	}
	if (options.cause !== undefined) {
		(error as Error & { cause?: unknown }).cause = options.cause;
	}
	return error;
}

function normalizeThrownError(error: unknown): LixError {
	if (isLixErrorLike(error)) {
		const hint =
			typeof error.hint === "string"
				? error.hint
				: extractHintFromMessage(error.message);
		const details = "details" in error ? error.details : undefined;
		if (error instanceof Error) {
			if (hint !== undefined && error.hint === undefined) {
				error.hint = hint;
			}
			if (details !== undefined && error.details === undefined) {
				error.details = details;
			}
			return error;
		}
		const message =
			typeof error.message === "string" ? error.message : error.code;
		return createLixError(error.code, message, { hint, details });
	}

	if (error instanceof WebAssembly.RuntimeError) {
		return createLixError("LIX_WASM_RUNTIME_ERROR", error.message, {
			hint: "The Lix engine encountered a WebAssembly runtime trap. Please report this as an engine bug with the SQL statement or API call that triggered it.",
			cause: error,
		});
	}

	if (error instanceof Error) {
		return createLixError("LIX_ERROR_UNKNOWN", error.message, { cause: error });
	}

	return createLixError("LIX_ERROR_UNKNOWN", String(error));
}

function extractHintFromMessage(message: unknown): string | undefined {
	if (typeof message !== "string") return undefined;
	const match = message.match(/(?:^|\n)hint:\s*(.+)$/s);
	return match?.[1]?.trim();
}

function isLixErrorLike(error: unknown): error is {
	code: string;
	message?: string;
	hint?: string;
	details?: unknown;
} {
	return (
		typeof error === "object" &&
		error !== null &&
		typeof (error as { code?: unknown }).code === "string" &&
		(error as { code: string }).code.startsWith("LIX_")
	);
}
