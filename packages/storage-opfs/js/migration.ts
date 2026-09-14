/** Separate migration entry: never imported by ordinary storage opening. */
export interface OpfsReplicaMigration {
  sourceName: string;
  destinationName: string;
  server: {url: string; headers: [string, string][]};
  expectedSourceDigest?: string;
}
export interface OpfsReplicaMigrationResult {
  destinationName: string;
  sourceDigest: string;
  empty?: boolean;
}
export function migrateOpfsReplica(
  options: OpfsReplicaMigration,
  onProgress?: (phase: string) => void,
): Promise<OpfsReplicaMigrationResult> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL("./migration.worker.js", import.meta.url), {type: "module", name: "lix-opfs-migration"});
    worker.onerror = () => {worker.terminate(); reject(new Error("The migration worker stopped; the source is retained."));};
    worker.onmessage = ({data}) => {
      if (data.phase) {onProgress?.(data.phase); return;}
      // The worker owns releasing both physical locks. Do not terminate it on
      // success before its finally clause has closed the storage providers.
      if (data.ok) resolve(data);
      else reject(Object.assign(new Error(data.error.message), {code: data.error.code}));
    };
    worker.postMessage(options);
  });
}
