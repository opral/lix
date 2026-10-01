import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";

test("maintenance APIs use the ordinary WASM and preserve a current OPFS repository", async () => {
 const name = `shared-wasm-maintenance-${crypto.randomUUID()}`;
 const lix = await openLix({ storage: new OpfsStorage({ name }) });
 try {
  await lix.execute("INSERT INTO lix_key_value (key, value) VALUES ('maintenance-probe', 'preserved')");
 } finally {
  await lix.close();
 }
 const worker = new Worker(new URL("./maintenance.worker.ts", import.meta.url), { type: "module" });
 try {
  const result = await new Promise<any>((resolve, reject) => {
   worker.onmessage = event => resolve(event.data);
   worker.onerror = event => reject(new Error(event.message));
   worker.postMessage({ name });
  });
  expect(result.error).toBeUndefined();
  expect(result.before.current).toBe(true);
  expect(result.after).toEqual(result.before);
  expect(result.report.semantic_preservation_verified).toBe(true);
  expect(result.afterDigest).toBe(result.beforeDigest);
 } finally {
  worker.terminate();
 }
 const reopened = await openLix({ storage: new OpfsStorage({ name }) });
 try {
  const result = await reopened.execute("SELECT value FROM lix_key_value WHERE key = 'maintenance-probe'");
  expect(result.rows).toEqual([{ value: "preserved" }]);
 } finally {
  await reopened.close();
 }
}, 30_000);
