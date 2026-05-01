import init, {
	resolveEngineWasmModuleOrPath,
	Value,
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
			throw new Error(
				`Column "${columnName}" does not exist. Available columns: ${this.availableColumns()}`,
			);
		}
		const value = this.valuesByIndex[index];
		if (value === undefined) {
			throw new Error(
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
			throw new Error(
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

export type TransactionBeginMode = "read" | "write" | "deferred";

export type KvScanRange =
	| { kind: "prefix"; prefix: Uint8Array }
	| { kind: "range"; start: Uint8Array; end: Uint8Array };

export type KvPair = {
	key: Uint8Array;
	value: Uint8Array;
};

export type LixBackendTransaction = {
	kvGet(namespace: string, key: Uint8Array): Uint8Array | null | undefined;
	kvScan(
		namespace: string,
		range: KvScanRange,
		limit?: number | null,
	): KvPair[];
	kvPut(namespace: string, key: Uint8Array, value: Uint8Array): void;
	kvDelete(namespace: string, key: Uint8Array): void;
	commit(): void;
	rollback(): void;
};

export type LixBackend = {
	beginTransaction(mode: TransactionBeginMode): LixBackendTransaction;
	kvGet?(namespace: string, key: Uint8Array): Uint8Array | null | undefined;
	kvScan?(
		namespace: string,
		range: KvScanRange,
		limit?: number | null,
	): KvPair[];
	close?(): void;
};

export type OpenLixOptions = {
	backend?: LixBackend;
};

export type CreateVersionOptions = {
	id?: string;
	name: string;
};

export type CreateVersionResult = {
	versionId: string;
};

export type SwitchVersionOptions = {
	versionId: string;
};

export type SwitchVersionResult = {
	versionId: string;
};

export type MergeVersionOptions = {
	sourceVersionId: string;
};

export type MergeVersionOutcome = "alreadyUpToDate" | "mergeCommitted";

export type MergeVersionResult = {
	outcome: MergeVersionOutcome;
	targetVersionId: string;
	sourceVersionId: string;
	mergeBaseCommitId: string | null;
	targetHeadBeforeCommitId: string;
	sourceHeadBeforeCommitId: string;
	targetHeadAfterCommitId: string;
	createdMergeCommitId: string | null;
	appliedChangeCount: number;
};

export type Lix = {
	execute(
		sql: string,
		params?: ReadonlyArray<LixRuntimeValue>,
	): Promise<ExecuteResult>;
	activeVersionId(): Promise<string>;
	createVersion(options: CreateVersionOptions): Promise<CreateVersionResult>;
	switchVersion(options: SwitchVersionOptions): Promise<SwitchVersionResult>;
	mergeVersion(options: MergeVersionOptions): Promise<MergeVersionResult>;
	close(): Promise<void>;
};

let wasmReady: Promise<void> | null = null;

type WasmExecuteResult = {
	columns: string[];
	rows: unknown[][];
	rowsAffected: number;
};

type WasmLix = {
	execute(sql: string, params: unknown[]): Promise<WasmExecuteResult>;
	activeVersionId(): Promise<string>;
	createVersion(options: CreateVersionOptions): Promise<CreateVersionResult>;
	switchVersion(options: SwitchVersionOptions): Promise<SwitchVersionResult>;
	mergeVersion(options: MergeVersionOptions): Promise<MergeVersionResult>;
	close(): Promise<void>;
};

async function ensureWasmReady(): Promise<void> {
	if (!wasmReady) {
		wasmReady = resolveEngineWasmModuleOrPath()
			.then((module_or_path) => init({ module_or_path }))
			.then(() => undefined);
	}
	await wasmReady;
}

export async function openLix(
	options: OpenLixOptions = {},
): Promise<Lix> {
	await ensureWasmReady();
	try {
		const wasmLix = (await (wasmModule as unknown as {
			openLix(options: OpenLixOptions): Promise<WasmLix>;
		}).openLix(options)) as WasmLix;
		return createLixHandle(wasmLix);
	} catch (error) {
		try {
			options.backend?.close?.();
		} catch {
			// Preserve the original open failure.
		}
		throw error;
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
		} finally {
			release();
		}
	};

	return {
		async execute(
			sql: string,
			params: ReadonlyArray<LixRuntimeValue> = [],
		): Promise<ExecuteResult> {
			const result = await runQueued(() =>
				wasmLix.execute(sql, params.map((param) => Value.from(param))),
			);
			return normalizeExecuteResult(result);
		},

		async activeVersionId(): Promise<string> {
			return await runQueued(() => wasmLix.activeVersionId());
		},

		async createVersion(
			options: CreateVersionOptions,
		): Promise<CreateVersionResult> {
			return await runQueued(() => wasmLix.createVersion(options));
		},

		async switchVersion(
			options: SwitchVersionOptions,
		): Promise<SwitchVersionResult> {
			return await runQueued(() => wasmLix.switchVersion(options));
		},

		async mergeVersion(options: MergeVersionOptions): Promise<MergeVersionResult> {
			return await runQueued(() => wasmLix.mergeVersion(options));
		},

		async close(): Promise<void> {
			await runQueued(() => wasmLix.close());
		},
	};
}

function normalizeExecuteResult(result: WasmExecuteResult): ExecuteResult {
	const columns = [...result.columns];
	return {
		columns,
		rows: result.rows.map(
			(row) => new Row(columns, row.map((value) => Value.from(value))),
		),
		rowsAffected: result.rowsAffected,
	};
}
