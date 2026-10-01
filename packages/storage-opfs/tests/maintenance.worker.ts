import { inspectStorageProvider, migrateStorageProvider } from "@lix-js/sdk/migration";
// @ts-expect-error bundled direct entry
import { OpfsBackend } from "../dist/direct.js";
import type { OpfsBackend as Backend } from "../js/provider.js";

self.onmessage = async ({ data }: MessageEvent<{ name: string }>) => {
 const provider: Backend = await OpfsBackend.open(data.name);
 let result: unknown;
 try {
  const before = await inspectStorageProvider(provider);
  const beforeDigest = await provider.migrationDigest();
  const report = await migrateStorageProvider(provider);
  const after = await inspectStorageProvider(provider);
  const afterDigest = await provider.migrationDigest();
  result = { before, after, report, beforeDigest, afterDigest };
 } catch (error) {
  result = { error: String(error) };
 } finally {
  await provider.close();
 }
 self.postMessage(result);
};
