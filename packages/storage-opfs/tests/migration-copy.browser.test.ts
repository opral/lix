import { expect, test } from "vitest";
test("detached copy preserves all logical spaces and source bytes under physical ownership", async () => {
 const worker = new Worker(new URL("./migration-copy.worker.ts",import.meta.url),{type:"module"});
 try {
  const result = await new Promise<any>((resolve,reject)=>{worker.onmessage=e=>resolve(e.data);worker.onerror=e=>reject(new Error(e.message));worker.postMessage({});});
  expect(result.error).toBeUndefined();
  expect(result.before).toMatch(/^[a-f0-9]{64}$/);
  expect(result.copied).toBe(result.before);
  expect(result.targetDigest).toBe(result.before);
  expect(result.after).toBe(result.before);
  expect(result.nonemptyRejected).toBe(true);
 } finally {worker.terminate();}
},30_000);
