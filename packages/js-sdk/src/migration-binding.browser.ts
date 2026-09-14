import { withConversionProvider } from "./conversion-provider.js";
import type { LixStorageConfig, LixStorageProviderModule, SyncServerBindingOptions } from "./binding-types.js";
// Generated only by build:migration:wasm, physically separate from runtime WASM.
// @ts-ignore Detached artifact is absent in source-only checks.
import init, { convertJsStorageReplicaToPartial, retryJsStorageReplicaMigrationCleanup, inspectJsStorageRepository, migrateJsStorageRepository } from "./migration-wasm/lix_js_sdk.js";
let initialization: Promise<unknown> | undefined;
export async function initializeMigration(): Promise<unknown> {
 return initialization ??= (async () => {
  const url = new URL("./migration-wasm/lix_js_sdk_bg.wasm", import.meta.url);
  if (typeof process !== "undefined" && process.versions?.node) {
   const {readFile} = await import("node:fs/promises");
   return init({module_or_path:await readFile(url)});
  }
  return init({module_or_path:url});
 })();
}
async function provider(storage:LixStorageConfig) {
 if(storage.kind!=="jsStorage") throw new TypeError("Browser migration requires persistent JavaScript storage");
 await initializeMigration();
 const module=await import(/* @vite-ignore */storage.moduleUrl) as LixStorageProviderModule;
 if(typeof module.createLixStorageProvider!=="function")throw new TypeError("Invalid storage provider module");
 return module.createLixStorageProvider(storage.options);
}
export async function convertReplicaBinding(storage:LixStorageConfig,server:SyncServerBindingOptions,branchId?:string):Promise<void> {
 const source=await provider(storage);
 return withConversionProvider(source,()=>convertJsStorageReplicaToPartial(source,server,branchId));
}
export async function retryReplicaMigrationCleanupBinding(storage:LixStorageConfig,server:SyncServerBindingOptions):Promise<number> {
 const source=await provider(storage);
 return withConversionProvider(source,()=>retryJsStorageReplicaMigrationCleanup(source,server));
}

export type RepositoryInspection = {format:number|null;role:"authority"|"partial_replica"|"full_replica"|"standalone"|"empty"|"invalid";layout:"active"|"legacy"|"interrupted"|"empty";current:boolean;protocol_epoch:number};
export type RepositoryMigrationReport = {embedded_repository_id:string;before:RepositoryInspection;after:RepositoryInspection;semantic_preservation_verified:boolean;preservation_basis:string;expected_content_digest:string;before_content_digest:string;after_content_digest:string};
type Provider = Awaited<ReturnType<LixStorageProviderModule["createLixStorageProvider"]>>;
/** Caller owns the provider and its physical exclusion fence across all phases. */
export async function inspectStorageProvider(source:Provider):Promise<RepositoryInspection> {
 await initializeMigration();
 return inspectJsStorageRepository(source);
}
/** Does not close the provider or publish a browser active-store pointer. */
export type MigrationLimits = {maxChanges:number;maxPreflightBytes:number};
export async function migrateStorageProvider(source:Provider, limits?:MigrationLimits):Promise<RepositoryMigrationReport> {
 await initializeMigration();
 return migrateJsStorageRepository(source, limits);
}
/** Explicit full->partial conversion within the caller's fenced destination. */
export async function convertStorageProviderToPartial(source:Provider,server:SyncServerBindingOptions,branchId?:string):Promise<void> {
 await initializeMigration();
 return convertJsStorageReplicaToPartial(source,server,branchId);
}
