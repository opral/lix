/** Detached migration entry. Normal SDK opening never imports this module. */
import { isJsProviderLixStorage, isLixStorage, type LixStorage } from "./storage-adapter.js";
import type { LixServerOptions } from "./types.js";
import { openStorages, storageAlreadyOpen } from "./storage-ownership.js";
export type ConvertReplicaToPartialOptions = {storage:LixStorage;server:LixServerOptions;branchId?:string};
/** Explicit conversion of closed full-replica storage; pending edits are preserved. */
export async function convertReplicaToPartial(options:ConvertReplicaToPartialOptions):Promise<void> {
 if(!options||(!isLixStorage(options.storage)&&!isJsProviderLixStorage(options.storage))||!options.server)throw new TypeError("Conversion requires storage and server");
 if(options.branchId!==undefined&&(typeof options.branchId!=="string"||!options.branchId))throw new TypeError("branchId must be a nonempty string");
 const storage=options.storage;
 if(openStorages.has(storage))throw storageAlreadyOpen();
 openStorages.add(storage);
 try {
  const registration=storage.lixStorage;
  const config=isJsProviderLixStorage(storage)?{kind:"jsStorage" as const,moduleUrl:storage.lixStorage.moduleUrl,options:storage.lixStorage.options}:
   (registration as {config:import("./binding-types.js").LixStorageConfig}).config;
  const {convertReplicaWorkerOperation}=await import("./worker/client.js");
  await convertReplicaWorkerOperation(config,options.server,options.branchId);
 }finally {openStorages.delete(storage);}
}

export type RetryReplicaMigrationCleanupOptions = {storage:LixStorage;server:LixServerOptions};
/** Retry durable migration cleanup on closed storage. Returns newly completed cleanups. */
export async function retryReplicaMigrationCleanup(options:RetryReplicaMigrationCleanupOptions):Promise<number> {
 if(!options||(!isLixStorage(options.storage)&&!isJsProviderLixStorage(options.storage))||!options.server)throw new TypeError("Migration cleanup requires storage and server");
 const storage=options.storage;
 if(openStorages.has(storage))throw storageAlreadyOpen();
 openStorages.add(storage);
 try {
  const config=isJsProviderLixStorage(storage)?{kind:"jsStorage" as const,moduleUrl:storage.lixStorage.moduleUrl,options:storage.lixStorage.options}:
   (storage.lixStorage as {config:import("./binding-types.js").LixStorageConfig}).config;
  const {retryReplicaMigrationCleanupWorkerOperation}=await import("./worker/client.js");
  return await retryReplicaMigrationCleanupWorkerOperation(config,options.server);
 }finally {openStorages.delete(storage);}
}

export type {RepositoryInspection,RepositoryMigrationReport,MigrationLimits} from "./migration-binding.browser.js";
type MigrationBinding = typeof import("./migration-binding.browser.js");
export async function initializeMigration():Promise<unknown> {
 return (await import("./migration-binding.browser.js")).initializeMigration();
}
export async function inspectStorageProvider(...args:Parameters<MigrationBinding["inspectStorageProvider"]>) {
 return (await import("./migration-binding.browser.js")).inspectStorageProvider(...args);
}
export async function migrateStorageProvider(...args:Parameters<MigrationBinding["migrateStorageProvider"]>) {
 return (await import("./migration-binding.browser.js")).migrateStorageProvider(...args);
}
export async function convertStorageProviderToPartial(...args:Parameters<MigrationBinding["convertStorageProviderToPartial"]>) {
 return (await import("./migration-binding.browser.js")).convertStorageProviderToPartial(...args);
}
export {fetchTransport} from "./http-transport.js";
