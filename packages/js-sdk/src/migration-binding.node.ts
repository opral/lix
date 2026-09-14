import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
type MigrationAddon = {
 convertFilesystemReplicaToPartial(path:string,syncAllFiles:boolean,url:string,headers:[string,string][],branchId?:string):Promise<void>;
 retryFilesystemReplicaMigrationCleanup(path:string,syncAllFiles:boolean,url:string,headers:[string,string][]):Promise<number>;
};
export function loadMigrationAddon(): MigrationAddon {
 return require("../lix_js_sdk_migration.node") as MigrationAddon;
}
