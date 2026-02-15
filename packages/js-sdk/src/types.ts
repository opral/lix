import type {
  LixBackend as WasmLixBackend,
  LixQueryResultLike,
  LixSqlDialect,
  LixTransaction,
  LixValueLike,
} from "./engine-wasm/index.js";

export type LixBackend = WasmLixBackend & {
  close?: () => Promise<void> | void;
};

export type {
  LixQueryResultLike,
  LixSqlDialect,
  LixTransaction,
  LixValueLike,
};
