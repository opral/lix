import type {
	ExecuteResult as LixCanonicalExecuteResult,
	JsonValue,
	LixValue as LixCanonicalValue,
	QueryResult as LixCanonicalQueryResult,
} from "./engine-wasm/index.js";

export type {
	LixCanonicalExecuteResult,
	LixCanonicalValue,
	LixCanonicalQueryResult,
};

export type LixRuntimeValue = JsonValue | Uint8Array;

export type LixRuntimeQueryResult = {
	rows: LixRuntimeValue[][];
	columns: string[];
};

export type LixRuntimeExecuteResult = {
	statements: LixRuntimeQueryResult[];
};

export type LixSqlDialect = "sqlite" | "postgres";

export type LixTransaction = {
	dialect?: LixSqlDialect | (() => LixSqlDialect);
	execute(
		sql: string,
		params: ReadonlyArray<LixRuntimeValue>,
	): Promise<LixRuntimeQueryResult> | LixRuntimeQueryResult;
	commit(): Promise<void> | void;
	rollback(): Promise<void> | void;
};

export type LixBackend = {
	dialect?: LixSqlDialect | (() => LixSqlDialect);
	execute(
		sql: string,
		params: ReadonlyArray<LixRuntimeValue>,
	): Promise<LixRuntimeQueryResult> | LixRuntimeQueryResult;
	beginTransaction?: () => Promise<LixTransaction> | LixTransaction;
	export_image?: () =>
		| Promise<Uint8Array | ArrayBuffer>
		| Uint8Array
		| ArrayBuffer;
	close?: () => Promise<void> | void;
};
