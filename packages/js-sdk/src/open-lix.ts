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

export type RowSet = {
	columns: string[];
	rows: Value[][];
};

export type ExecuteResult =
	| { kind: "rows"; rows: RowSet }
	| { kind: "affectedRows"; affectedRows: number };

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

type WasmExecuteResult =
	| {
			kind: "rows";
			rows: { columns: string[]; rows: unknown[][] };
	  }
	| { kind: "affectedRows"; affectedRows: number };

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
		return createLixHandle(wasmLix, options.backend);
	} catch (error) {
		try {
			options.backend?.close?.();
		} catch {
			// Preserve the original open failure.
		}
		throw error;
	}
}

function createLixHandle(wasmLix: WasmLix, backend?: LixBackend): Lix {
	let closed = false;
	let operationQueue: Promise<void> = Promise.resolve();

	const ensureOpen = (methodName: string): void => {
		if (closed) {
			throw new Error(`lix is closed; ${methodName}() is unavailable`);
		}
	};

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
			ensureOpen("execute");
			const result = await runQueued(() =>
				wasmLix.execute(sql, params.map((param) => Value.from(param))),
			);
			return normalizeExecuteResult(result);
		},

		async activeVersionId(): Promise<string> {
			ensureOpen("activeVersionId");
			return await runQueued(() => wasmLix.activeVersionId());
		},

		async createVersion(
			options: CreateVersionOptions,
		): Promise<CreateVersionResult> {
			ensureOpen("createVersion");
			return await runQueued(() => wasmLix.createVersion(options));
		},

		async switchVersion(
			options: SwitchVersionOptions,
		): Promise<SwitchVersionResult> {
			ensureOpen("switchVersion");
			return await runQueued(() => wasmLix.switchVersion(options));
		},

		async mergeVersion(options: MergeVersionOptions): Promise<MergeVersionResult> {
			ensureOpen("mergeVersion");
			return await runQueued(() => wasmLix.mergeVersion(options));
		},

		async close(): Promise<void> {
			if (closed) return;
			try {
				await runQueued(() => wasmLix.close());
			} finally {
				backend?.close?.();
				closed = true;
			}
		},
	};
}

function normalizeExecuteResult(result: WasmExecuteResult): ExecuteResult {
	if (result.kind === "rows") {
		return {
			kind: "rows",
			rows: {
				columns: [...result.rows.columns],
				rows: result.rows.rows.map((row) => row.map((value) => Value.from(value))),
			},
		};
	}
	return {
		kind: "affectedRows",
		affectedRows: result.affectedRows,
	};
}
