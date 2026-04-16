export * from "./open-lix.js";
export * from "./backend/wasm-sqlite.js";
export * from "./builtin-schemas.js";
export type {
	CanonicalJsonText,
	LixBackend,
	LixCanonicalExecuteResult,
	LixCanonicalQueryResult,
	LixCanonicalValue,
	LixJsonValue,
	LixSqlDialect,
	LixBackendTransaction,
	LixRuntimeExecuteResult,
	LixRuntimeQueryResult,
	LixRuntimeValue,
	JsonValue,
} from "./types.js";
export { Value, isLixError } from "./engine-wasm/index.js";
export type { LixError } from "./engine-wasm/index.js";
